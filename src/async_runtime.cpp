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

			constexpr std::size_t kDefaultLaneThreads = 4;
		// Serial lanes each own exactly one dedicated worker.
		constexpr std::size_t kSerialLaneThreads = 3;
		constexpr std::size_t kMinLaneThreads = kSerialLaneThreads + 2;

		// Resolves db-read / blocking-io worker counts from options. When the
		// requested lane total exceeds max_lane_threads, the parallel lanes are
		// scaled down proportionally while the serial lanes stay dedicated.
		std::pair<std::size_t, std::size_t> ResolveLaneThreads(const AsyncRuntimeOptions& options)
		{
			std::size_t read = options.db_read_threads != kDefaultLaneThreads ? options.db_read_threads : options.db_threads;
			std::size_t io = options.blocking_io_threads;
			if (options.max_lane_threads != 0 && read + io + kSerialLaneThreads > options.max_lane_threads)
			{
				const std::size_t parallel = std::max(options.max_lane_threads, kMinLaneThreads) - kSerialLaneThreads;
				const std::size_t want = read + io;
				io = want == 0 ? 1 : std::max<std::size_t>(1, (parallel * io) / want);
				read = std::max<std::size_t>(1, parallel - io);
			}
			return { read, io };
		}

		// Sleeping workers rescan peers on these idle timeouts: a worker blocked
		// inside a job cannot drain its own queue, so rescans bound how long
		// queued work can sit behind it while every peer sleeps. The busy
		// cadence applies while any worker is not registered for sleep; a fully
		// idle pool cannot gain work without a wakeup and only gets the slow tick.
		constexpr std::chrono::microseconds kBusyRescanInterval{ 20 };
		constexpr std::chrono::milliseconds kIdleRescanInterval{ 4 };

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
		idle_workers_.store(actual_threads, std::memory_order_relaxed);
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
			Job job;
			while (shard->TryPop(job))
			{
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
		auto wake = [this] { idle_workers_.fetch_sub(1, std::memory_order_relaxed); };
#ifdef PRISM_RUNTIME_METRICS
		auto submit_time = std::chrono::steady_clock::now();
		Job instrumented = [role = role_, submit_time, work = std::move(work)]() mutable {
			auto exec_start = std::chrono::steady_clock::now();
			auto wait_us
			    = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(exec_start - submit_time).count());
			work();
			auto exec_end = std::chrono::steady_clock::now();
			auto exec_us = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(exec_end - exec_start).count());
			RecordRoleCompletion(role, wait_us, exec_us);
		};
		const std::size_t depth = shard.Emplace(std::move(instrumented), wake);
		RecordRoleSubmit(role_, depth);
#else
		const std::size_t depth = shard.Emplace(std::move(work), wake);
#endif
		if (depth >= kShareThreshold && idle_workers_.load(std::memory_order_relaxed) != 0)
		{
			ShareWork(target_index);
		}
	}

	bool BlockingExecutor::Empty() const
	{
		for (const auto& shard : shards_)
		{
			if (shard->Load() != 0)
			{
				return false;
			}
		}
		return true;
	}

	bool BlockingExecutor::IsCurrentWorker() const noexcept { return tls_blocking_executor == this; }

	void BlockingExecutor::ShareWork(std::size_t source_index)
	{
		if (!options_.enable_stealing)
		{
			return;
		}
		auto& source = *shards_[source_index];
		for (std::size_t offset = 1; offset < shards_.size(); ++offset)
		{
			auto& peer = *shards_[(source_index + offset) % shards_.size()];
			if (!peer.waiting_.load(std::memory_order_relaxed))
			{
				continue;
			}
			std::scoped_lock lock(source.mutex_, peer.mutex_);
			if (source.queue_.size() < kShareThreshold)
			{
				return;
			}
			if (!peer.waiting_.load(std::memory_order_relaxed) || !peer.queue_.empty())
			{
				continue;
			}
			peer.TakeBatchFrom(source, kMaxStealBatch, [](Job&) {});
			peer.NotifyWaiting([this] { idle_workers_.fetch_sub(1, std::memory_order_relaxed); });
			return; // One useful wake per batch, never a broadcast.
		}
	}

	bool BlockingExecutor::TrySteal(std::size_t self_index, std::uint64_t& rng_state, bool before_sleep)
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

		const std::size_t start = (self_index + 1 + NextRandom(rng_state) % (worker_count - 1)) % worker_count;
		auto& self_shard = *shards_[self_index];
		// While active, try one victim. After registering sleep, check every peer
		// under its queue mutex: a producer either observes our registration or we
		// observe its backlog. The full scan closes the missed-wake race.
		for (std::size_t offset = 0; offset < (before_sleep ? worker_count : 1); ++offset)
		{
			const std::size_t victim_index = (start + offset) % worker_count;
			if (victim_index == self_index)
			{
				continue;
			}
			auto& victim = *shards_[victim_index];
			// Approximate-load filter: skip locking clearly-empty victims. A stale
			// zero can only delay a steal, never lose it — the idle rescan retries.
			if (victim.Load() == 0)
			{
				continue;
			}
			std::scoped_lock lock(self_shard.mutex_, victim.mutex_);
			if (!self_shard.queue_.empty())
			{
				return false; // A producer supplied local work while we checked peers.
			}
			if (victim.queue_.empty())
			{
				continue;
			}
			self_shard.TakeBatchFrom(victim, kMaxStealBatch, [](Job&) {});
			self_shard.NotifyWaiting([this] { idle_workers_.fetch_sub(1, std::memory_order_relaxed); });
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
		return false;
	}

	void BlockingExecutor::WakeAll()
	{
		for (auto& shard : shards_)
		{
			std::lock_guard lock(shard->mutex_);
			shard->NotifyWaiting([this] { idle_workers_.fetch_sub(1, std::memory_order_relaxed); });
		}
	}

	void BlockingExecutor::WorkerLoop(std::size_t worker_index)
	{
		tls_blocking_executor = this;
		tls_blocking_worker_index = worker_index;
		auto& shard = *shards_[worker_index];
		std::uint64_t rng_state = (static_cast<std::uint64_t>(worker_index) + 1) * 0x9e3779b97f4a7c15ull;

		auto run = [](Job& job) {
			try
			{
				job();
			}
			catch (...)
			{
				std::terminate();
			}
		};

		shard.semaphore_.acquire();
		while (true)
		{
			Job job;
			if (shard.TryPop(job))
			{
				run(job);
				continue;
			}

			if (TrySteal(worker_index, rng_state, false))
			{
				continue;
			}
			{
				std::lock_guard lock(shard.mutex_);
				// Queue publication, sleep registration and notification share this
				// mutex. A submit either prevents sleeping or supplies its wake token.
				if (!shard.queue_.empty())
				{
					continue;
				}
				if (stopping_.load(std::memory_order_acquire))
				{
					break;
				}
				shard.MarkWaiting([this] { idle_workers_.fetch_add(1, std::memory_order_relaxed); });
			}
			TrySteal(worker_index, rng_state, true);
			// Even a successful steal supplies a token through NotifyWaiting.
			// Consume exactly one before processing jobs or registering sleep
			// again. While any peer is not registered for sleep, rescan on the
			// busy cadence: a worker blocked inside a job cannot drain its own
			// queue, and sleeping peers never rescan, so without this a burst
			// tail could sit queued while all peers sleep.
			while (true)
			{
				const bool peers_active = idle_workers_.load(std::memory_order_relaxed) < shards_.size();
				if (shard.semaphore_.try_acquire_for(peers_active ? kBusyRescanInterval : kIdleRescanInterval))
				{
					break;
				}
				if (peers_active)
				{
					TrySteal(worker_index, rng_state, true);
				}
			}
		}

		// Shutdown drain: finish every job still reachable before exiting.
		while (true)
		{
			Job job;
			if (shard.TryPop(job))
			{
				run(job);
				continue;
			}
			if (!TrySteal(worker_index, rng_state, true))
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
	    , db_read_executor_(ResolveLaneThreads(options).first, BlockingExecutorRole::kDbRead,
	          BlockingExecutorOptions{ .enable_stealing = true })
	    , db_write_executor_()
	    , blocking_io_executor_(ResolveLaneThreads(options).second, BlockingExecutorRole::kBlockingIo)
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
