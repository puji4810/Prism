#include "scheduler.h"

#include <algorithm>
#include <limits>

namespace prism
{
	namespace
	{
		constexpr std::size_t kMinThreads = 2;
		constexpr std::size_t kLazyFallbackPriority = (std::numeric_limits<std::size_t>::max)();

		std::size_t DefaultThreadCount(std::size_t requested)
		{
			const std::size_t hw = std::max<std::size_t>(std::thread::hardware_concurrency(), 1);
			return std::max<std::size_t>({ hw, requested, kMinThreads });
		}
	}

	ThreadPoolScheduler::Context ThreadPoolScheduler::CaptureContext() { return Context(std::this_thread::get_id()); }

	ThreadPoolScheduler::ThreadPoolScheduler(std::size_t num_threads)
	    : work_threads_(DefaultThreadCount(num_threads))
	{
		pending_list_.reserve(work_threads_.size());
		for (auto& t : work_threads_)
		{
			pending_list_.push_back(&t);
			t.Start(*this);
		}

		lazy_thread_ = std::jthread([this] { LazyLoop(); });
		priority_thread_ = std::jthread([this] { PriorityLoop(); });
	}

	ThreadPoolScheduler::~ThreadPoolScheduler()
	{
		Exit();

		if (priority_thread_.joinable())
		{
			priority_thread_.join();
		}
		if (lazy_thread_.joinable())
		{
			lazy_thread_.join();
		}
		for (auto& t : work_threads_)
		{
			t.Join();
		}
	}

	bool ThreadPoolScheduler::IsExitRequested() const noexcept { return exit_flag_.load(std::memory_order_acquire); }

	void ThreadPoolScheduler::Exit() noexcept
	{
		if (exit_flag_.exchange(true, std::memory_order_acq_rel))
		{
			return;
		}

		lazy_waiter_.release();
		priority_waiter_.release();
		for (auto& t : work_threads_)
		{
			t.Wake();
		}
	}

	void ThreadPoolScheduler::Submit(Job job, std::size_t priority)
	{
		{
			std::lock_guard lock(priority_mutex_);
			priority_queue_.push(PriorityTask{ std::move(job), priority });
		}
		priority_waiter_.release();
	}

	void ThreadPoolScheduler::SubmitAfter(std::chrono::steady_clock::time_point deadline, Job job)
	{
		{
			std::lock_guard lock(lazy_mutex_);
			lazy_queue_.push(LazyTask{ std::move(job), deadline });
		}
		lazy_waiter_.release();
	}

	void ThreadPoolScheduler::SubmitIn(Context ctx, Job job)
	{
		for (auto& t : work_threads_)
		{
			if (t.Id() == ctx.thread_id_)
			{
				t.Push(std::move(job));
				return;
			}
		}

		Submit(std::move(job));
	}

	bool ThreadPoolScheduler::TryDispatch(Job job)
	{
		std::lock_guard lock(pending_mutex_);
		if (pending_list_.empty())
		{
			return false;
		}

		auto* t = pending_list_.back();
		pending_list_.pop_back();
		t->PushDispatched(std::move(job));
		return true;
	}

	void ThreadPoolScheduler::PriorityLoop()
	{
		while (true)
		{
			priority_waiter_.acquire();
			if (IsExitRequested())
			{
				break;
			}

			std::lock_guard lock(priority_mutex_);
			// Over-notification from worker threads may result in empty queue
			if (priority_queue_.empty())
			{
				continue;
			}

			// Try to dispatch top-priority task to idle worker.
			// Only pop from queue after successful dispatch to ensure tasks aren't lost.
			if (TryDispatch(std::move(priority_queue_.top().job)))
			{
				priority_queue_.pop();
			}
		}
	}

	void ThreadPoolScheduler::LazyLoop()
	{
		while (true)
		{
			lazy_waiter_.acquire();
			if (IsExitRequested())
			{
				break;
			}

			std::unique_lock lock(lazy_mutex_);
			if (lazy_queue_.empty())
			{
				continue;
			}

			auto task = lazy_queue_.top();
			const auto now = std::chrono::steady_clock::now();
			if (task.deadline <= now)
			{
				// Deadline expired: remove from queue and dispatch immediately
				lazy_queue_.pop();
				// Wake LazyLoop again if more tasks remain
				if (!lazy_queue_.empty())
				{
					lazy_waiter_.release();
				}
				lock.unlock();

				// Try direct dispatch to idle worker; fallback to priority queue if all busy
				if (!TryDispatch(std::move(task.job)))
				{
					Submit(std::move(task.job), kLazyFallbackPriority);
				}
			}
			else
			{
				// Not yet ready: wait until deadline or new task insertion wakes us
				const auto deadline = task.deadline;
				lock.unlock();

				// Wait with timeout; returns true if woken by new task, false if deadline expired
				(void)lazy_waiter_.try_acquire_until(deadline);
				// Unconditionally release to re-enter loop and check top task again
				lazy_waiter_.release();
			}
		}
	}

	void ThreadPoolScheduler::WorkThread::Start(ThreadPoolScheduler& scheduler)
	{
		thread_ = std::jthread([this, &scheduler] { Consume(scheduler); });
	}

	void ThreadPoolScheduler::WorkThread::Join()
	{
		if (thread_.joinable())
		{
			thread_.join();
		}
	}

	std::thread::id ThreadPoolScheduler::WorkThread::Id() const { return thread_.get_id(); }

	void ThreadPoolScheduler::WorkThread::Push(Job job)
	{
		{
			std::lock_guard lock(mutex_);
			queue_.push_back(std::move(job));
		}
		semaphore_.release();
	}

	void ThreadPoolScheduler::WorkThread::PushDispatched(Job job)
	{
		{
			std::lock_guard lock(mutex_);
			queue_.push_back(std::move(job));
			return_to_pending_ = true;
		}
		semaphore_.release();
	}

	void ThreadPoolScheduler::WorkThread::Wake() { semaphore_.release(); }

	void ThreadPoolScheduler::WorkThread::Consume(ThreadPoolScheduler& scheduler) noexcept
	{
		while (true)
		{
			semaphore_.acquire();
			if (scheduler.IsExitRequested())
			{
				break;
			}

			Job job;
			{
				std::lock_guard lock(mutex_);
				if (queue_.empty())
				{
					continue;
				}
				job = std::move(queue_.front());
				queue_.pop_front();
			}

			job();

			// Return to pending_list_ only if this task was dispatched from PriorityLoop/LazyLoop
			// (not from direct Push/SubmitIn, which is for affinity and doesn't need re-registration)
			if (return_to_pending_)
			{
				return_to_pending_ = false;
				{
					std::lock_guard lock(scheduler.pending_mutex_);
					scheduler.pending_list_.push_back(this);
				}
				scheduler.priority_waiter_.release();
			}
		}
	}
}
