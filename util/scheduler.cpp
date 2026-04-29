#include "scheduler.h"
#include "runtime_metrics.h"

#include <algorithm>
#include <limits>
#include <cassert>
#include <cstdio>

namespace prism
{
	namespace
	{
		constexpr std::size_t kMinThreads = 2;
		constexpr std::size_t kLazyFallbackPriority = (std::numeric_limits<std::size_t>::max)();
		constexpr auto kStealBackoff = std::chrono::microseconds(100);

		// Thread-local state: set by each WorkThread at the start of Consume(), cleared on exit.
		// Used by CaptureContext() to determine whether the calling thread belongs to a specific
		// ThreadPoolScheduler instance, and which worker index it is.
		thread_local const ThreadPoolScheduler* t_current_scheduler = nullptr;
		thread_local std::size_t t_current_worker_index = 0;

		std::size_t DefaultThreadCount(std::size_t requested)
		{
			const std::size_t hw = std::max<std::size_t>(std::thread::hardware_concurrency(), 1);
			if (requested > 0)
				return std::max<std::size_t>(requested, kMinThreads);
			return std::max<std::size_t>(hw, kMinThreads);
		}

		std::uint64_t NextRandom(std::uint64_t& state) noexcept
		{
			state ^= state << 13;
			state ^= state >> 7;
			state ^= state << 17;
			return state;
		}
	}

	ThreadPoolScheduler::Context ThreadPoolScheduler::CaptureContext() const
	{
		if (t_current_scheduler == this)
			return Context(this, t_current_worker_index);
		return Context{ };
	}

	ThreadPoolScheduler::ThreadPoolScheduler(std::size_t num_threads)
	    : work_threads_(DefaultThreadCount(num_threads))
	{
		pending_list_.reserve(work_threads_.size());
		for (std::size_t i = 0; i < work_threads_.size(); ++i)
		{
			pending_list_.push_back(&work_threads_[i]);
			work_threads_[i].Start(*this, i);
		}

		lazy_thread_ = std::jthread([this] { LazyLoop(); });
		priority_thread_ = std::jthread([this] { PriorityLoop(); });
	}

	ThreadPoolScheduler::~ThreadPoolScheduler()
	{
		Exit();

		// Step 1: join dispatcher threads so they are no longer reading/writing queues.
		if (lazy_thread_.joinable())
		{
			lazy_thread_.join();
		}
		if (priority_thread_.joinable())
		{
			priority_thread_.join();
		}

		// Step 2: promote any delayed residue before workers stop draining so shutdown-visible
		// worker execution/stealing can still consume the promoted work.
		while (!lazy_queue_.empty())
		{
			auto task = lazy_queue_.top();
			lazy_queue_.pop();
			if (!TryPushToWorker(std::move(task.job), ChooseLeastLoadedWorker(), false, true))
			{
				std::lock_guard lock(priority_mutex_);
				priority_queue_.push(PriorityTask{ std::move(task.job), kLazyFallbackPriority });
			}
		}
		for (auto& t : work_threads_)
		{
			t.Wake();
		}

		// Step 3: workers drain their local queues (and may steal peer work) until empty.
		for (auto& t : work_threads_)
		{
			t.Join();
		}

		// Step 4: sequential drain — all threads are stopped, no more races.
		// Loop until stable: tasks executed during drain may call Submit() again,
		// creating new entries that must also be drained.
		// Mark this (destructor) thread as belonging to the scheduler so that
		// re-entrant Submit* calls from within drained jobs satisfy the debug guard.
		const auto* prev_scheduler = t_current_scheduler;
		t_current_scheduler = this;
		bool work_remains = true;
		while (work_remains)
		{
			work_remains = false;

			// 4a. Promote any delayed residue that was submitted during drain.
			while (!lazy_queue_.empty())
			{
				auto task = lazy_queue_.top();
				lazy_queue_.pop();
				if (TryPushToWorker(std::move(task.job), ChooseLeastLoadedWorker(), false, true))
				{
					work_remains = true;
					continue;
				}
				try
				{
					task.job();
				}
				catch (...)
				{
					std::terminate();
				}
				work_remains = true;
			}

			// 4b. Drain all remaining priority tasks inline.
			while (!priority_queue_.empty())
			{
				auto job = std::move(const_cast<Job&>(priority_queue_.top().job));
				priority_queue_.pop();
				if (TryPushToWorker(std::move(job), ChooseLeastLoadedWorker(), false, true))
				{
					work_remains = true;
					continue;
				}
				try
				{
					job();
				}
				catch (...)
				{
					std::terminate();
				}
				work_remains = true;
			}

			// 4c. Drain each worker's local queue (tasks dispatched but not yet started).
			for (auto& t : work_threads_)
			{
				if (t.DrainRemaining())
					work_remains = true;
			}
		}
		t_current_scheduler = prev_scheduler;
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
		// Debug-only guard: external-thread submission after shutdown is unsupported.
		// Worker re-entrant submissions during shutdown are supported and drained.
		assert(!IsExitRequested() || (t_current_scheduler == this));

		if (priority == 0)
		{
			if (t_current_scheduler == this)
			{
				if (TryPushToWorker(std::move(job), t_current_worker_index, false, true))
				{
					RuntimeMetrics::Instance().foreground_fastpath_submits.fetch_add(1, std::memory_order_relaxed);
					return;
				}
			}

			const auto worker_index = ChooseLeastLoadedWorker();
			if (TryPushToWorker(std::move(job), worker_index, false, true))
			{
				RuntimeMetrics::Instance().foreground_fastpath_submits.fetch_add(1, std::memory_order_relaxed);
				return;
			}
		}

		RuntimeMetrics::Instance().foreground_fallback_submits.fetch_add(1, std::memory_order_relaxed);
		{
			std::lock_guard lock(priority_mutex_);
			priority_queue_.push(PriorityTask{ std::move(job), priority });
		}
		priority_waiter_.release();
	}

	void ThreadPoolScheduler::SubmitAfter(std::chrono::steady_clock::time_point deadline, Job job)
	{
		// Debug-only guard: external-thread submission after shutdown is unsupported.
		assert(!IsExitRequested() || (t_current_scheduler == this));

		{
			std::lock_guard lock(lazy_mutex_);
			lazy_queue_.push(LazyTask{ std::move(job), deadline });
		}
		lazy_waiter_.release();
	}

	void ThreadPoolScheduler::SubmitIn(Context ctx, Job job)
	{
		// Debug-only guard: external-thread submission after shutdown is unsupported.
		// Worker re-entrant submissions during shutdown are supported and drained.
		assert(!IsExitRequested() || (t_current_scheduler == this));

		// Only honor affinity if the context belongs to this scheduler instance.
		if (ctx.scheduler_ == this)
		{
			auto& t = work_threads_[ctx.worker_index_];
			t.Push(std::move(job));
			return;
		}

		// Invalid context or foreign scheduler: fall back to normal Submit.
		// Debug-only mismatch trace: helps diagnose misrouted affinity submissions.
#ifndef NDEBUG
		std::fprintf(stderr,
		    "[prism::scheduler] SubmitIn context mismatch: this=%p ctx.scheduler_=%p ctx.worker_index_=%zu."
		    " Falling back to Submit().\n",
		    static_cast<const void*>(this), static_cast<const void*>(ctx.scheduler_), ctx.worker_index_);
#endif
		Submit(std::move(job));
	}

	bool ThreadPoolScheduler::TryPushToWorker(Job job, std::size_t worker_index, bool dispatched, bool stealable)
	{
		if (worker_index >= work_threads_.size())
		{
			return false;
		}

		auto& worker = work_threads_[worker_index];
		if (dispatched)
		{
			worker.PushDispatched(std::move(job));
		}
		else
		{
			worker.Push(std::move(job), stealable);
		}
		return true;
	}

	std::size_t ThreadPoolScheduler::ChooseLeastLoadedWorker() const
	{
		std::size_t best_index = 0;
		std::size_t best_load = work_threads_.front().Load();
		for (std::size_t i = 1; i < work_threads_.size(); ++i)
		{
			const auto load = work_threads_[i].Load();
			if (load < best_load)
			{
				best_load = load;
				best_index = i;
			}
		}
		return best_index;
	}

	bool ThreadPoolScheduler::TryDispatch(Job& job)
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
			if (TryDispatch(const_cast<Job&>(priority_queue_.top().job)))
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

				const auto worker_index = ChooseLeastLoadedWorker();
				if (!TryPushToWorker(std::move(task.job), worker_index, false, true))
				{
					{
						std::lock_guard priority_lock(priority_mutex_);
						priority_queue_.push(PriorityTask{ std::move(task.job), kLazyFallbackPriority });
					}
					priority_waiter_.release();
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

	void ThreadPoolScheduler::WorkThread::Start(ThreadPoolScheduler& scheduler, std::size_t worker_index)
	{
		thread_ = std::jthread([this, &scheduler, worker_index] { Consume(scheduler, worker_index); });
	}

	void ThreadPoolScheduler::WorkThread::Join()
	{
		if (thread_.joinable())
		{
			thread_.join();
		}
	}

	bool ThreadPoolScheduler::WorkThread::DrainRemaining() noexcept
	{
		// Called from destructor after thread has been joined — no concurrent access.
		bool did_work = false;
		while (!queue_.empty())
		{
			auto job = std::move(queue_.front().job);
			queue_.pop_front();
			load_.fetch_sub(1, std::memory_order_relaxed);
			try
			{
				job();
			}
			catch (...)
			{
				std::terminate();
			}
			did_work = true;
		}
		return did_work;
	}

	std::thread::id ThreadPoolScheduler::WorkThread::Id() const { return thread_.get_id(); }

	void ThreadPoolScheduler::WorkThread::Push(Job job)
	{
		{
			std::lock_guard lock(mutex_);
			queue_.push_back(QueuedJob{ std::move(job), false, true });
			load_.fetch_add(1, std::memory_order_relaxed);
		}
		semaphore_.release();
	}

	void ThreadPoolScheduler::WorkThread::Push(Job job, bool stealable)
	{
		{
			std::lock_guard lock(mutex_);
			queue_.push_back(QueuedJob{ std::move(job), false, stealable });
			load_.fetch_add(1, std::memory_order_relaxed);
		}
		semaphore_.release();
	}

	void ThreadPoolScheduler::WorkThread::PushDispatched(Job job)
	{
		{
			std::lock_guard lock(mutex_);
			queue_.push_back(QueuedJob{ std::move(job), true, true });
			load_.fetch_add(1, std::memory_order_relaxed);
		}
		semaphore_.release();
	}

	std::size_t ThreadPoolScheduler::WorkThread::Load() const noexcept
	{
		return load_.load(std::memory_order_relaxed);
	}

	void ThreadPoolScheduler::WorkThread::Wake() { semaphore_.release(); }

	bool ThreadPoolScheduler::WorkThread::TrySteal(
	    ThreadPoolScheduler& scheduler, std::size_t worker_index, std::uint64_t& rng_state)
	{
		RuntimeMetrics::Instance().steal_attempts.fetch_add(1, std::memory_order_relaxed);

		if (scheduler.work_threads_.size() <= 1)
		{
			return false;
		}

		std::size_t victim_index = NextRandom(rng_state) % (scheduler.work_threads_.size() - 1);
		if (victim_index >= worker_index)
		{
			++victim_index;
		}

		auto& victim = scheduler.work_threads_[victim_index];
		std::scoped_lock lock(mutex_, victim.mutex_);
		if (!queue_.empty())
		{
			return false;
		}

		std::size_t stealable_count = 0;
		for (const auto& queued : victim.queue_)
		{
			if (queued.stealable)
			{
				++stealable_count;
			}
		}
		if (stealable_count == 0)
		{
			return false;
		}

		const std::size_t steal_count = std::max<std::size_t>(1, stealable_count / 2);
		std::vector<QueuedJob> stolen;
		stolen.reserve(steal_count);
		for (std::size_t i = victim.queue_.size(); i > 0 && stolen.size() < steal_count; --i)
		{
			auto& candidate = victim.queue_[i - 1];
			if (!candidate.stealable)
			{
				continue;
			}
			stolen.push_back(std::move(candidate));
			victim.queue_.erase(victim.queue_.begin() + static_cast<std::ptrdiff_t>(i - 1));
		}
		if (stolen.empty())
		{
			return false;
		}

		for (auto& queued : stolen)
		{
			queued.stolen = true;
			queue_.push_front(std::move(queued));
		}
		victim.load_.fetch_sub(stolen.size(), std::memory_order_relaxed);
		load_.fetch_add(stolen.size(), std::memory_order_relaxed);

		RuntimeMetrics::Instance().steal_successes.fetch_add(1, std::memory_order_relaxed);
		return true;
	}

	void ThreadPoolScheduler::WorkThread::Consume(ThreadPoolScheduler& scheduler, std::size_t worker_index) noexcept
	{
		// Register this thread with the scheduler instance so CaptureContext() can validate.
		t_current_scheduler = &scheduler;
		t_current_worker_index = worker_index;
		std::uint64_t rng_state = (static_cast<std::uint64_t>(worker_index) + 1) * 0x9e3779b97f4a7c15ull;

		while (true)
		{
			semaphore_.acquire();

			while (true)
			{
				QueuedJob queued;
				bool queue_empty_after = false;
				bool has_job = false;
				{
					std::lock_guard lock(mutex_);
					if (!queue_.empty())
					{
						queued = std::move(queue_.front());
						queue_.pop_front();
						load_.fetch_sub(1, std::memory_order_relaxed);
						queue_empty_after = queue_.empty();
						has_job = true;
					}
				}

			if (has_job)
				{
					try
					{
						queued.job();
					}
					catch (...)
					{
						std::terminate();
					}

					if (queued.stolen)
					{
						RuntimeMetrics::Instance().stolen_jobs_completed.fetch_add(1, std::memory_order_relaxed);
					}
					else
					{
						RuntimeMetrics::Instance().worker_local_jobs_completed.fetch_add(1, std::memory_order_relaxed);
					}

					bool should_reregister = queue_empty_after;
					if (queued.stolen && !should_reregister)
					{
						std::lock_guard lock(mutex_);
						should_reregister = queue_.empty();
					}

					// Re-register as pending only when the queue becomes empty.
					// Per-task queue_empty_after still prevents affinity work from re-registering
					// while dispatcher-owned work remains queued behind it.
					// Stolen batches need a post-execution empty check because the worker may
					// drain to idle after the last stolen job even when the pre-pop state was non-empty.
					if (should_reregister)
					{
						{
							std::lock_guard lock(scheduler.pending_mutex_);
							scheduler.pending_list_.push_back(this);
						}
						scheduler.priority_waiter_.release();
					}

					if (scheduler.IsExitRequested())
					{
						std::lock_guard lock(mutex_);
						if (queue_.empty())
						{
							break;
						}
					}
					continue;
				}

				if (TrySteal(scheduler, worker_index, rng_state))
				{
					continue;
				}

				if (scheduler.IsExitRequested())
				{
					break;
				}

				if (!semaphore_.try_acquire_for(kStealBackoff))
				{
					break;
				}
			}

			if (scheduler.IsExitRequested())
			{
				std::lock_guard lock(mutex_);
				if (queue_.empty())
				{
					break;
				}
			}
		}

		// Clear thread-local state when worker exits.
		t_current_scheduler = nullptr;
		t_current_worker_index = 0;
	}

}
