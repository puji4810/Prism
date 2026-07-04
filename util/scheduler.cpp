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

	thread_local const ThreadPoolScheduler* ThreadPoolScheduler::current_scheduler_ = nullptr;
	thread_local std::size_t ThreadPoolScheduler::current_worker_index_ = 0;

	ThreadPoolScheduler::Context ThreadPoolScheduler::CaptureContext() const
	{
		if (current_scheduler_ == this)
			return Context(this, current_worker_index_);
		return Context{ };
	}

	ThreadPoolScheduler::ThreadPoolScheduler(std::size_t num_threads)
	    : work_threads_(DefaultThreadCount(num_threads))
	{
		for (std::size_t i = 0; i < work_threads_.size(); ++i)
		{
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
		PromoteLazyResidueToWorkers();
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
		const auto* prev_scheduler = current_scheduler_;
		current_scheduler_ = this;
		bool work_remains = true;
		while (work_remains)
		{
			work_remains = false;

			DrainLazyQueueToEmpty(work_remains);
			DrainPriorityQueueToEmpty(work_remains);
			DrainWorkerLocalQueues(work_remains);
		}
		current_scheduler_ = prev_scheduler;
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

	// ── Policy helpers ─────────────────────────────────────────────────

	bool ThreadPoolScheduler::ShouldUseFastPath(std::size_t priority) noexcept { return priority == 0; }

	bool ThreadPoolScheduler::ShouldAcceptSubmitDuringShutdown() const noexcept
	{
		return !IsExitRequested() || (current_scheduler_ == this);
	}

	bool ThreadPoolScheduler::ShouldPromoteLazyTask(const LazyTask& task, std::chrono::steady_clock::time_point now) noexcept
	{
		return task.deadline <= now;
	}

	// ── Mechanics helpers ──────────────────────────────────────────────

	void ThreadPoolScheduler::PushToPriorityQueue(Job job, std::size_t priority, bool wake)
	{
		{
			std::lock_guard lock(priority_mutex_);
			priority_queue_.push(PriorityTask{ std::move(job), priority });
		}
		if (wake)
		{
			priority_waiter_.release();
		}
	}

	void ThreadPoolScheduler::Submit(Job job, std::size_t priority)
	{
		SubmitJob(std::move(job), priority);
	}

	void ThreadPoolScheduler::SubmitAfter(std::chrono::steady_clock::time_point deadline, Job job)
	{
		SubmitAfterJob(deadline, std::move(job));
	}

	void ThreadPoolScheduler::SubmitIn(Context ctx, Job job)
	{
		SubmitInJob(ctx, std::move(job));
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

	std::size_t ThreadPoolScheduler::ChooseSubmissionWorker()
	{
		const auto worker_count = work_threads_.size();
		if (worker_count == 0)
		{
			return 0;
		}
		return submit_cursor_.fetch_add(1, std::memory_order_relaxed) % worker_count;
	}

	void ThreadPoolScheduler::DispatchExpiredTask(LazyTask&& task)
	{
		const auto worker_index = ChooseLeastLoadedWorker();
		if (!TryPushToWorker(std::move(task.job), worker_index, true, true))
		{
			PushToPriorityQueue(std::move(task.job), kLazyFallbackPriority, /*wake=*/true);
		}
	}

	void ThreadPoolScheduler::PromoteLazyResidueToWorkers()
	{
		while (!lazy_queue_.empty())
		{
			auto task = std::move(const_cast<LazyTask&>(lazy_queue_.top()));
			lazy_queue_.pop();
			if (!TryPushToWorker(std::move(task.job), ChooseLeastLoadedWorker(), true, true))
			{
				std::lock_guard lock(priority_mutex_);
				priority_queue_.push(PriorityTask{ std::move(task.job), kLazyFallbackPriority });
			}
		}
	}

	void ThreadPoolScheduler::DrainLazyQueueToEmpty(bool& work_remains)
	{
		while (!lazy_queue_.empty())
		{
			auto task = std::move(const_cast<LazyTask&>(lazy_queue_.top()));
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
	}

	void ThreadPoolScheduler::DrainPriorityQueueToEmpty(bool& work_remains)
	{
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
	}

	void ThreadPoolScheduler::DrainWorkerLocalQueues(bool& work_remains)
	{
		for (auto& t : work_threads_)
		{
			if (t.DrainRemaining())
				work_remains = true;
		}
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

			while (true)
			{
				PriorityTask task;
				{
					std::lock_guard lock(priority_mutex_);
					// Over-notification from worker threads may result in an empty queue.
					if (priority_queue_.empty())
					{
						break;
					}
					task = std::move(const_cast<PriorityTask&>(priority_queue_.top()));
					priority_queue_.pop();
				}

				(void)TryPushToWorker(std::move(task.job), ChooseLeastLoadedWorker(), true, true);
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

			const auto deadline = lazy_queue_.top().deadline;
			const auto now = std::chrono::steady_clock::now();
			if (deadline <= now) // POLICY: deadline expired?
			{
				auto task = std::move(const_cast<LazyTask&>(lazy_queue_.top()));
				lazy_queue_.pop();
				if (!lazy_queue_.empty())
				{
					lazy_waiter_.release();
				}
				lock.unlock();

				DispatchExpiredTask(std::move(task));
			}
			else
			{
				lock.unlock();

				(void)lazy_waiter_.try_acquire_until(deadline);
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
			queue_.push_back(QueuedJob{ std::move(job), true });
			load_.fetch_add(1, std::memory_order_relaxed);
		}
		semaphore_.release();
	}

	void ThreadPoolScheduler::WorkThread::Push(Job job, bool stealable)
	{
		{
			std::lock_guard lock(mutex_);
			queue_.push_back(QueuedJob{ std::move(job), stealable });
			load_.fetch_add(1, std::memory_order_relaxed);
		}
		semaphore_.release();
	}

	void ThreadPoolScheduler::WorkThread::PushDispatched(Job job)
	{
		{
			std::lock_guard lock(mutex_);
			queue_.push_back(QueuedJob{ std::move(job), true });
			load_.fetch_add(1, std::memory_order_relaxed);
		}
		semaphore_.release();
	}

	std::size_t ThreadPoolScheduler::WorkThread::Load() const noexcept { return load_.load(std::memory_order_relaxed); }

	void ThreadPoolScheduler::WorkThread::Wake() { semaphore_.release(); }

	bool ThreadPoolScheduler::WorkThread::TrySteal(ThreadPoolScheduler& scheduler, std::size_t worker_index, std::uint64_t& rng_state)
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

	bool ThreadPoolScheduler::WorkThread::TryDequeueJob(QueuedJob& out)
	{
		std::lock_guard lock(mutex_);
		if (queue_.empty())
			return false;
		out = std::move(queue_.front());
		queue_.pop_front();
		load_.fetch_sub(1, std::memory_order_relaxed);
		return true;
	}

	bool ThreadPoolScheduler::WorkThread::HandleJobCompletion(QueuedJob& job, ThreadPoolScheduler& scheduler) noexcept
	{
		try
		{
			job.job();
		}
		catch (...)
		{
			std::terminate();
		}

		if (job.stolen)
		{
			RuntimeMetrics::Instance().stolen_jobs_completed.fetch_add(1, std::memory_order_relaxed);
		}
		else
		{
			RuntimeMetrics::Instance().worker_local_jobs_completed.fetch_add(1, std::memory_order_relaxed);
		}

		if (scheduler.IsExitRequested())
		{
			std::lock_guard lock(mutex_);
			return queue_.empty();
		}
		return false;
	}

	void ThreadPoolScheduler::WorkThread::Consume(ThreadPoolScheduler& scheduler, std::size_t worker_index) noexcept
	{
		// Register this thread with the scheduler instance so CaptureContext() can validate.
		current_scheduler_ = &scheduler;
		current_worker_index_ = worker_index;
		std::uint64_t rng_state = (static_cast<std::uint64_t>(worker_index) + 1) * 0x9e3779b97f4a7c15ull;

		while (true)
		{
			semaphore_.acquire();

			while (true)
			{
				QueuedJob queued;

				if (TryDequeueJob(queued))
				{
					if (HandleJobCompletion(queued, scheduler))
						break;
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
		current_scheduler_ = nullptr;
		current_worker_index_ = 0;
	}

	void ThreadPoolScheduler::RecordForegroundFastPathSubmit() noexcept
	{
		RuntimeMetrics::Instance().foreground_fastpath_submits.fetch_add(1, std::memory_order_relaxed);
	}

	void ThreadPoolScheduler::RecordForegroundFallbackSubmit() noexcept
	{
		RuntimeMetrics::Instance().foreground_fallback_submits.fetch_add(1, std::memory_order_relaxed);
	}

}
