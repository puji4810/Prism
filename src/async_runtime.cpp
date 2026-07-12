#include "async_runtime.h"

#include "runtime_metrics.h"

#include <algorithm>
#include <chrono>
#include <random>
#include <utility>

namespace prism
{
	namespace
	{
		thread_local const BlockingExecutor* tls_blocking_executor = nullptr;
		thread_local std::size_t tls_blocking_worker_index = 0;

#ifndef PRISM_RUNTIME_METRICS
		[[maybe_unused]] constexpr std::size_t kRuntimeMetricsSizeAnchor = sizeof(prism::RuntimeMetrics);
#endif

#ifdef PRISM_RUNTIME_METRICS
		void RecordRoleSubmit(BlockingExecutorRole role, std::size_t current_depth)
		{
			auto& metrics = RuntimeMetrics::Instance();
			switch (role)
			{
			case BlockingExecutorRole::kDbRead:
				metrics.db_read_jobs_submitted.fetch_add(1, std::memory_order_relaxed);
				if (current_depth > metrics.db_read_peak_queue_depth.load(std::memory_order_relaxed))
				{
					metrics.db_read_peak_queue_depth.store(current_depth, std::memory_order_relaxed);
				}
				break;
			case BlockingExecutorRole::kBlockingIo:
				metrics.blocking_io_jobs_submitted.fetch_add(1, std::memory_order_relaxed);
				if (current_depth > metrics.blocking_io_peak_queue_depth.load(std::memory_order_relaxed))
				{
					metrics.blocking_io_peak_queue_depth.store(current_depth, std::memory_order_relaxed);
				}
				break;
			case BlockingExecutorRole::kCompaction:
				metrics.compaction_queue_jobs_submitted.fetch_add(1, std::memory_order_relaxed);
				if (current_depth > metrics.compaction_peak_queue_depth.load(std::memory_order_relaxed))
				{
					metrics.compaction_peak_queue_depth.store(current_depth, std::memory_order_relaxed);
				}
				break;
			case BlockingExecutorRole::kGeneric:
				break;
			}
		}

		void RecordRoleCompletion(BlockingExecutorRole role, uint64_t wait_us, uint64_t exec_us)
		{
			auto& metrics = RuntimeMetrics::Instance();
			switch (role)
			{
			case BlockingExecutorRole::kDbRead:
				metrics.db_read_enqueue_wait_total_us.fetch_add(wait_us, std::memory_order_relaxed);
				metrics.db_read_exec_time_total_us.fetch_add(exec_us, std::memory_order_relaxed);
				metrics.db_read_jobs_completed.fetch_add(1, std::memory_order_relaxed);
				break;
			case BlockingExecutorRole::kBlockingIo:
				metrics.blocking_io_enqueue_wait_total_us.fetch_add(wait_us, std::memory_order_relaxed);
				metrics.blocking_io_exec_time_total_us.fetch_add(exec_us, std::memory_order_relaxed);
				metrics.blocking_io_jobs_completed.fetch_add(1, std::memory_order_relaxed);
				break;
			case BlockingExecutorRole::kCompaction:
				metrics.compaction_enqueue_wait_total_us.fetch_add(wait_us, std::memory_order_relaxed);
				metrics.compaction_exec_time_total_us.fetch_add(exec_us, std::memory_order_relaxed);
				metrics.compaction_queue_jobs_completed.fetch_add(1, std::memory_order_relaxed);
				break;
			case BlockingExecutorRole::kGeneric:
				break;
			}
		}
#endif

		constexpr auto kStealBackoff = std::chrono::microseconds(100);

		std::uint64_t NextRandom(std::uint64_t& state) noexcept
		{
			state ^= state << 13;
			state ^= state >> 7;
			state ^= state << 17;
			return state;
		}
	}

	BlockingExecutor::BlockingExecutor(std::size_t thread_count, BlockingExecutorRole role, BlockingExecutorOptions options)
	    : role_(role)
	    , options_(options)
	{
		const std::size_t actual_threads = std::max<std::size_t>(thread_count, 1);
		shards_.reserve(actual_threads);
		for (std::size_t i = 0; i < actual_threads; ++i)
		{
			shards_.push_back(std::make_unique<Shard>());
		}
		workers_.reserve(actual_threads);
		for (std::size_t i = 0; i < actual_threads; ++i)
		{
			workers_.emplace_back([this, i] { WorkerLoop(i); });
		}
	}

	BlockingExecutor::~BlockingExecutor()
	{
		if (IsCurrentWorker())
		{
			std::terminate();
		}

		stopping_.store(true, std::memory_order_release);
		WakeAll();

		for (auto& worker : workers_)
		{
			if (worker.joinable())
			{
				worker.request_stop();
				worker.join();
			}
		}
		workers_.clear();

		for (auto& shard : shards_)
		{
			while (!shard->queue.empty())
			{
				auto job = std::move(shard->queue.front());
				shard->queue.pop_front();
				shard->load.fetch_sub(1, std::memory_order_relaxed);
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
		shards_.clear();
	}

	void BlockingExecutor::Submit(Job work)
	{
		const std::size_t worker_count = shards_.size();
		std::size_t target_index = 0;
		bool self_submit = false;

		if (tls_blocking_executor == this)
		{
			target_index = tls_blocking_worker_index;
			self_submit = true;
		}
		if (!self_submit && worker_count > 0)
		{
			target_index = submit_cursor_.fetch_add(1, std::memory_order_relaxed) % worker_count;
		}

		auto& shard = *shards_[target_index];
#ifdef PRISM_RUNTIME_METRICS
		auto submit_time = std::chrono::steady_clock::now();
		std::size_t current_depth;
		{
			std::lock_guard lock(shard.mutex);
			current_depth = shard.queue.size();
			RecordRoleSubmit(role_, current_depth);
			shard.queue.emplace_back([role = role_, submit_time, work = std::move(work)]() mutable {
				auto exec_start = std::chrono::steady_clock::now();
				auto wait_us
				    = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(exec_start - submit_time).count());
				work();
				auto exec_end = std::chrono::steady_clock::now();
				auto exec_us = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(exec_end - exec_start).count());
				RecordRoleCompletion(role, wait_us, exec_us);
			});
		}
#else
		{
			std::lock_guard lock(shard.mutex);
			shard.queue.push_back(std::move(work));
		}
#endif
		shard.load.fetch_add(1, std::memory_order_relaxed);
		shard.semaphore.release();
	}

	bool BlockingExecutor::Empty() const
	{
		for (const auto& shard : shards_)
		{
			std::lock_guard lock(shard->mutex);
			if (!shard->queue.empty())
			{
				return false;
			}
		}
		return true;
	}

	bool BlockingExecutor::IsCurrentWorker() const noexcept
	{
		return tls_blocking_executor == this;
	}

	bool BlockingExecutor::TryPopOwn(Shard& shard, Job& out)
	{
		std::lock_guard lock(shard.mutex);
		if (shard.queue.empty())
		{
			return false;
		}
		out = std::move(shard.queue.front());
		shard.queue.pop_front();
		shard.load.fetch_sub(1, std::memory_order_relaxed);
		return true;
	}

	bool BlockingExecutor::TrySteal(std::size_t self_index, std::uint64_t& rng_state, Job& out)
	{
		const std::size_t worker_count = shards_.size();
		if (!options_.enable_stealing || worker_count <= 1)
		{
			return false;
		}
#ifdef PRISM_RUNTIME_METRICS
		switch (role_)
		{
		case BlockingExecutorRole::kDbRead:
			RuntimeMetrics::Instance().db_read_steal_attempts.fetch_add(1, std::memory_order_relaxed);
			break;
		case BlockingExecutorRole::kBlockingIo:
			RuntimeMetrics::Instance().blocking_io_steal_attempts.fetch_add(1, std::memory_order_relaxed);
			break;
		case BlockingExecutorRole::kCompaction:
		case BlockingExecutorRole::kGeneric:
			break;
		}
#endif

		const std::size_t victim_index = NextVictim(self_index, worker_count, rng_state);
		auto& self_shard = *shards_[self_index];
		auto& victim = *shards_[victim_index];

		std::scoped_lock lock(self_shard.mutex, victim.mutex);
		if (!self_shard.queue.empty() || victim.queue.empty())
		{
			return false;
		}

		const std::size_t steal_count = std::max<std::size_t>(1, victim.queue.size() / 2);
		for (std::size_t i = 0; i < steal_count; ++i)
		{
			self_shard.queue.push_front(std::move(victim.queue.back()));
			victim.queue.pop_back();
		}
		victim.load.fetch_sub(steal_count, std::memory_order_relaxed);
		self_shard.load.fetch_add(steal_count, std::memory_order_relaxed);

		out = std::move(self_shard.queue.front());
		self_shard.queue.pop_front();
		self_shard.load.fetch_sub(1, std::memory_order_relaxed);
#ifdef PRISM_RUNTIME_METRICS
		switch (role_)
		{
		case BlockingExecutorRole::kDbRead:
			RuntimeMetrics::Instance().db_read_steal_successes.fetch_add(1, std::memory_order_relaxed);
			break;
		case BlockingExecutorRole::kBlockingIo:
			RuntimeMetrics::Instance().blocking_io_steal_successes.fetch_add(1, std::memory_order_relaxed);
			break;
		case BlockingExecutorRole::kCompaction:
		case BlockingExecutorRole::kGeneric:
			break;
		}
#endif
		return true;
	}

	std::size_t BlockingExecutor::NextVictim(std::size_t self_index, std::size_t worker_count, std::uint64_t& rng_state)
	{
		std::size_t victim = NextRandom(rng_state) % (worker_count - 1);
		if (victim >= self_index)
		{
			++victim;
		}
		return victim;
	}

	void BlockingExecutor::WakeAll()
	{
		for (auto& shard : shards_)
		{
			shard->semaphore.release();
		}
	}

	bool BlockingExecutor::AnyShardNonEmpty() const noexcept
	{
		for (const auto& shard : shards_)
		{
			if (shard->load.load(std::memory_order_relaxed) > 0)
			{
				return true;
			}
		}
		return false;
	}

	void BlockingExecutor::WorkerLoop(std::size_t worker_index)
	{
		tls_blocking_executor = this;
		tls_blocking_worker_index = worker_index;
		auto& shard = *shards_[worker_index];
		std::uint64_t rng_state = (static_cast<std::uint64_t>(worker_index) + 1) * 0x9e3779b97f4a7c15ull;

		while (true)
		{
			shard.semaphore.acquire();

			while (true)
			{
				Job job;
				if (TryPopOwn(shard, job))
				{
					try
					{
						job();
					}
					catch (...)
					{
						std::terminate();
					}
					continue;
				}

				if (TrySteal(worker_index, rng_state, job))
				{
					try
					{
						job();
					}
					catch (...)
					{
						std::terminate();
					}
					continue;
				}

				if (stopping_.load(std::memory_order_acquire))
				{
					break;
				}

				if (!shard.semaphore.try_acquire_for(kStealBackoff))
				{
					break;
				}
			}

			if (stopping_.load(std::memory_order_acquire) && !AnyShardNonEmpty())
			{
				break;
			}
		}
		tls_blocking_executor = nullptr;
		tls_blocking_worker_index = 0;
	}

	SerialExecutor::SerialExecutor()
	    : worker_([this] { WorkerLoop(); })
	{
	}

	SerialExecutor::~SerialExecutor()
	{
		{
			std::lock_guard lock(mutex_);
			stopping_ = true;
		}
		cv_.notify_all();
	}

	void SerialExecutor::Submit(Job work)
	{
		{
			std::lock_guard lock(mutex_);
			queue_.push_back(std::move(work));
		}
		cv_.notify_one();
	}

	bool SerialExecutor::Empty() const
	{
		std::lock_guard lock(mutex_);
		return queue_.empty();
	}

	bool SerialExecutor::Done() const
	{
		std::lock_guard lock(mutex_);
		return queue_.empty() && !running_;
	}

	bool SerialExecutor::IsCurrentWorker() const noexcept { return worker_.joinable() && worker_.get_id() == std::this_thread::get_id(); }

	void SerialExecutor::WorkerLoop()
	{
		while (true)
		{
			Job job;
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

	AsyncRuntime::AsyncRuntime(CpuThreadPool& cpu_pool, AsyncRuntimeOptions options)
	    : cpu_executor_(cpu_pool)
	    , db_read_executor_(
	          options.db_read_threads != 4 ? options.db_read_threads : options.db_threads,
	          BlockingExecutorRole::kDbRead,
	          BlockingExecutorOptions{ .enable_stealing = true })
	    , db_write_executor_()
	    , blocking_io_executor_(options.blocking_io_threads, BlockingExecutorRole::kBlockingIo)
	    , io_dispatcher_(blocking_io_executor_)
	    , compaction_executor_(1, BlockingExecutorRole::kCompaction, BlockingExecutorOptions{ .enable_stealing = false })
	    , serial_file_executor_()
	{
	}

	bool AsyncRuntime::IsCurrentWorker() const noexcept
	{
		return db_read_executor_.IsCurrentWorker() || db_write_executor_.IsCurrentWorker() || blocking_io_executor_.IsCurrentWorker()
		    || compaction_executor_.IsCurrentWorker() || serial_file_executor_.IsCurrentWorker();
	}

} // namespace prism
