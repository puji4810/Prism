#include "runtime_executor.h"

#include "runtime_metrics.h"

#include <algorithm>
#include <unordered_map>

namespace prism
{
	namespace
	{
		constexpr std::size_t kCpuContinuationPriority = 0;
		constexpr std::size_t kReadExecutorThreadCount = 8;

		// For Singleton, here:
		// https://puji4810.github.io/2025/12/26/Singleton/
		// we dont need a synchronized destruction
		std::mutex& RegistryMutex()
		{
			static std::mutex mutex;
			return mutex;
		}

		// Never call destructor of registry
		auto& RuntimeRegistry()
		{
			static auto* registry = new std::unordered_map<ThreadPoolScheduler*, std::weak_ptr<RuntimeBundle>>();
			return *registry;
		}

#ifndef PRISM_RUNTIME_METRICS
		[[maybe_unused]] constexpr std::size_t kRuntimeMetricsSizeAnchor = sizeof(prism::RuntimeMetrics);
#endif

#ifdef PRISM_RUNTIME_METRICS
		void RecordLaneSubmit(BlockingExecutorLane lane, std::size_t current_depth)
		{
			auto& metrics = RuntimeMetrics::Instance();
			switch (lane)
			{
			case BlockingExecutorLane::kRead:
				metrics.blocking_jobs_submitted.fetch_add(1, std::memory_order_relaxed);
				if (current_depth > metrics.blocking_peak_queue_depth.load(std::memory_order_relaxed))
				{
					metrics.blocking_peak_queue_depth.store(current_depth, std::memory_order_relaxed);
				}
				break;
			case BlockingExecutorLane::kCompaction:
				metrics.compaction_queue_jobs_submitted.fetch_add(1, std::memory_order_relaxed);
				if (current_depth > metrics.compaction_peak_queue_depth.load(std::memory_order_relaxed))
				{
					metrics.compaction_peak_queue_depth.store(current_depth, std::memory_order_relaxed);
				}
				break;
			case BlockingExecutorLane::kGeneric:
				break;
			}
		}

		void RecordLaneCompletion(BlockingExecutorLane lane, uint64_t wait_us, uint64_t exec_us)
		{
			auto& metrics = RuntimeMetrics::Instance();
			switch (lane)
			{
			case BlockingExecutorLane::kRead:
				metrics.blocking_enqueue_wait_total_us.fetch_add(wait_us, std::memory_order_relaxed);
				metrics.blocking_exec_time_total_us.fetch_add(exec_us, std::memory_order_relaxed);
				metrics.blocking_jobs_completed.fetch_add(1, std::memory_order_relaxed);
				break;
			case BlockingExecutorLane::kCompaction:
				metrics.compaction_enqueue_wait_total_us.fetch_add(wait_us, std::memory_order_relaxed);
				metrics.compaction_exec_time_total_us.fetch_add(exec_us, std::memory_order_relaxed);
				metrics.compaction_queue_jobs_completed.fetch_add(1, std::memory_order_relaxed);
				break;
			case BlockingExecutorLane::kGeneric:
				break;
			}
		}
#endif
	}

	ThreadPoolExecutor::ThreadPoolExecutor(ThreadPoolScheduler& scheduler)
	    : scheduler_(&scheduler)
	{
	}

	void ThreadPoolExecutor::Submit(std::function<void()> work) { scheduler_->Submit(std::move(work), kCpuContinuationPriority); }

	BlockingExecutor::BlockingExecutor(std::size_t thread_count, BlockingExecutorLane lane)
	    : lane_(lane)
	{
		const std::size_t actual_threads = std::max<std::size_t>(thread_count, 1);
		workers_.reserve(actual_threads);
		for (std::size_t i = 0; i < actual_threads; ++i)
		{
			workers_.emplace_back([this] { WorkerLoop(); });
		}
	}

	BlockingExecutor::~BlockingExecutor()
	{
		{
			std::lock_guard lock(mutex_);
			stopping_ = true;
		}
		cv_.notify_all();
	}

	void BlockingExecutor::Submit(std::function<void()> work)
	{
#ifdef PRISM_RUNTIME_METRICS
		auto submit_time = std::chrono::steady_clock::now();
		{
			std::lock_guard lock(mutex_);
			auto current_depth = queue_.size();
			RecordLaneSubmit(lane_, current_depth);
			queue_.push_back([lane = lane_, submit_time, work = std::move(work)]() mutable {
				auto exec_start = std::chrono::steady_clock::now();
				auto wait_us = static_cast<uint64_t>(
				    std::chrono::duration_cast<std::chrono::microseconds>(exec_start - submit_time).count());
				work();
				auto exec_end = std::chrono::steady_clock::now();
				auto exec_us = static_cast<uint64_t>(
				    std::chrono::duration_cast<std::chrono::microseconds>(exec_end - exec_start).count());
				RecordLaneCompletion(lane, wait_us, exec_us);
			});
		}
		cv_.notify_one();
#else
		{
			std::lock_guard lock(mutex_);
			queue_.push_back(std::move(work));
		}
		cv_.notify_one();
#endif
	}

	bool BlockingExecutor::Empty() const
	{
		std::lock_guard lock(mutex_);
		return queue_.empty();
	}

	void BlockingExecutor::WorkerLoop()
	{
		while (true)
		{
			std::function<void()> job;
			{
				std::unique_lock lock(mutex_);
				cv_.wait(lock, [this] { return stopping_ || !queue_.empty(); });
				if (stopping_ && queue_.empty())
				{
					return;
				}
				job = std::move(queue_.front());
				queue_.pop_front();
			}

			try
			{
				job();
			}
			catch (...)
			{
				std::terminate();
			}
		}
	}

	SerialLane::SerialLane()
	    : worker_([this] { WorkerLoop(); })
	{
	}

	SerialLane::~SerialLane()
	{
		{
			std::lock_guard lock(mutex_);
			stopping_ = true;
		}
		cv_.notify_all();
	}

	void SerialLane::Submit(std::function<void()> work)
	{
		{
			std::lock_guard lock(mutex_);
			queue_.push_back(std::move(work));
		}
		cv_.notify_one();
	}

	bool SerialLane::Empty() const
	{
		std::lock_guard lock(mutex_);
		return queue_.empty();
	}

	bool SerialLane::Done() const
	{
		std::lock_guard lock(mutex_);
		return queue_.empty() && !running_;
	}

	void SerialLane::WorkerLoop()
	{
		while (true)
		{
			std::function<void()> job;
			{
				std::unique_lock lock(mutex_);
				cv_.wait(lock, [this] { return stopping_ || !queue_.empty(); });
				if (stopping_ && queue_.empty())
				{
					return;
				}
				job = std::move(queue_.front());
				queue_.pop_front();
				running_ = true;
			}

			try
			{
				job();
			}
			catch (...)
			{
				std::terminate();
			}

			{
				std::lock_guard lock(mutex_);
				running_ = false;
			}
		}
	}

	ExecutorSchedulerAdapter::ExecutorSchedulerAdapter(
	    IContinuationExecutor& executor, IScheduler* blocking_scheduler, IScheduler* continuation_scheduler)
	    : executor_(&executor)
	    , blocking_scheduler_(blocking_scheduler != nullptr ? blocking_scheduler : this)
	    , continuation_scheduler_(continuation_scheduler != nullptr ? continuation_scheduler : this)
	{
	}

	void ExecutorSchedulerAdapter::Submit(Job job, std::size_t /*priority*/) { executor_->Submit(std::move(job)); }

	IScheduler* ExecutorSchedulerAdapter::BlockingScheduler() noexcept { return blocking_scheduler_; }

	IScheduler* ExecutorSchedulerAdapter::ContinuationScheduler() noexcept { return continuation_scheduler_; }

	RuntimeBundle::RuntimeBundle(ThreadPoolScheduler& scheduler)
	    : cpu_executor_impl(scheduler)
	    , cpu_executor(&cpu_executor_impl)
	    , timer_source(&scheduler)
	    , read_executor(kReadExecutorThreadCount, BlockingExecutorLane::kRead)
	    , compaction_executor(1, BlockingExecutorLane::kCompaction)
	    , serial_lane()
	    , cpu_scheduler(cpu_executor_impl)
	    , read_scheduler(read_executor)
	    , compaction_scheduler(compaction_executor)
	    , serial_scheduler(serial_lane)
	    , runtime_scheduler(cpu_executor_impl, &read_scheduler, &cpu_scheduler)
	{
	}

	std::shared_ptr<RuntimeBundle> AcquireRuntimeBundle(ThreadPoolScheduler& scheduler)
	{
		std::lock_guard lock(RegistryMutex());
		auto& registry = RuntimeRegistry();
		auto it = registry.find(&scheduler);
		if (it != registry.end())
		{
			if (auto existing = it->second.lock())
			{
				return existing;
			}
		}

		auto runtime = std::make_shared<RuntimeBundle>(scheduler);
		registry[&scheduler] = runtime;
		return runtime;
	}

} // namespace prism
