#ifndef PRISM_ASYNC_RUNTIME_H
#define PRISM_ASYNC_RUNTIME_H

#include "io_reactor.h"
#include "scheduler.h"

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <mutex>
#include <new>
#include <semaphore>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace prism
{
	enum class BlockingExecutorRole
	{
		kGeneric,
		kDbRead,
		kBlockingIo,
		kCompaction,
	};

	struct BlockingExecutorOptions
	{
		bool enable_stealing = true;
	};

	// BlockingExecutor: sharded-queue + work-stealing thread pool.
	//
	// Architecture:
	// - N worker threads, each owning a Shard (deque + mutex + semaphore + load counter).
	// - Submit() routes to a target shard:
	//     * Re-entrant submit from a worker of this executor -> that worker's own shard
	//       (preserves locality for recursive-submit / continuation patterns).
	//     * External submit -> round-robin via atomic submit_cursor_.
	// - WorkerLoop: pop from own shard; when empty, steal from peers (random victim,
	//   steal up to half of victim's stealable jobs, LIFO-biased for cache locality);
	//   when nothing stealable, block on the semaphore with a short try_acquire_for
	//   backoff so shutdown stays responsive.
	// - Shutdown: stopping_ is set, all shard semaphores are released; workers drain
	//   remaining queued work (own + stealable from peers) before exiting. Destructor
	//   joins all workers and then sequentially drains any residue (defensive).
	//
	// Public API is identical to the prior single-deque implementation:
	//   ctor(thread_count, role), dtor, Submit(Job), Empty(), IsCurrentWorker().
	// No caller depends on global FIFO ordering across the queue; with N>1 workers
	// such ordering was already non-deterministic. Lane isolation between distinct
	// BlockingExecutor instances is preserved (each instance has its own shards).
	class BlockingExecutor final
	{
	public:
		using Job = InlineJob;

		explicit BlockingExecutor(std::size_t thread_count = 1,
		    BlockingExecutorRole role = BlockingExecutorRole::kGeneric,
		    BlockingExecutorOptions options = {});
		~BlockingExecutor();

		BlockingExecutor(const BlockingExecutor&) = delete;
		BlockingExecutor& operator=(const BlockingExecutor&) = delete;
		BlockingExecutor(BlockingExecutor&&) = delete;
		BlockingExecutor& operator=(BlockingExecutor&&) = delete;

		void Submit(Job work);
		// Execute immediately only when called re-entrantly from one of this
		// executor's workers. Returning false lets the caller construct the
		// queued fallback lazily, after the hot inline check.
		template <typename F>
		    requires(!std::is_same_v<std::decay_t<F>, Job> && std::is_invocable_r_v<void, std::decay_t<F>&>)
		bool TryRunInline(F&& work)
		{
			if (!IsCurrentWorker())
			{
				return false;
			}
			work();
			return true;
		}
		bool Empty() const;
		bool IsCurrentWorker() const noexcept;

	private:
		struct alignas(64) Shard
		{
			std::mutex mutex;
			std::deque<Job> queue;
			std::counting_semaphore<> semaphore{ 0 };
			std::atomic<std::size_t> load{ 0 };
		};

		void WorkerLoop(std::size_t worker_index);
		bool TryPopOwn(Shard& shard, Job& out);
		bool TrySteal(std::size_t self_index, std::uint64_t& rng_state, Job& out);
		void WakeAll();
		bool AnyShardNonEmpty() const noexcept;
		static std::size_t NextVictim(std::size_t self_index, std::size_t worker_count, std::uint64_t& rng_state);

		BlockingExecutorRole role_{ BlockingExecutorRole::kGeneric };
		BlockingExecutorOptions options_;
		std::atomic<bool> stopping_{ false };
		std::atomic<std::size_t> submit_cursor_{ 0 };
		std::vector<std::unique_ptr<Shard>> shards_;
		std::vector<std::jthread> workers_;
	};

	class SerialExecutor final
	{
	public:
		using Job = InlineJob;

		SerialExecutor();
		~SerialExecutor();

		SerialExecutor(const SerialExecutor&) = delete;
		SerialExecutor& operator=(const SerialExecutor&) = delete;
		SerialExecutor(SerialExecutor&&) = delete;
		SerialExecutor& operator=(SerialExecutor&&) = delete;

		void Submit(Job work);
		bool Empty() const;
		bool Done() const;
		bool IsCurrentWorker() const noexcept;

	private:
		void WorkerLoop();

		mutable std::mutex mutex_;
		std::condition_variable cv_;
		std::deque<Job> queue_;
		std::jthread worker_;
		bool stopping_{ false };
		bool running_{ false };
	};

	struct AsyncRuntimeOptions
	{
		std::size_t db_read_threads = 4;
		std::size_t db_threads = 4;
		std::size_t blocking_io_threads = 4;
	};

	class AsyncRuntime
	{
	public:
		explicit AsyncRuntime(CpuThreadPool& cpu_pool, AsyncRuntimeOptions options = {});

		AsyncRuntime(const AsyncRuntime&) = delete;
		AsyncRuntime& operator=(const AsyncRuntime&) = delete;
		AsyncRuntime(AsyncRuntime&&) = delete;
		AsyncRuntime& operator=(AsyncRuntime&&) = delete;

		CpuThreadPool& CpuExecutor() noexcept { return cpu_executor_; }
		BlockingExecutor& DbReadExecutor() noexcept { return db_read_executor_; }
		BlockingExecutor& DbExecutor() noexcept { return db_read_executor_; }
		SerialExecutor& DbWriteExecutor() noexcept { return db_write_executor_; }
		BlockingExecutor& BlockingIoExecutor() noexcept { return blocking_io_executor_; }
		BlockingExecutor& CompactionExecutor() noexcept { return compaction_executor_; }
		SerialExecutor& SerialFileExecutor() noexcept { return serial_file_executor_; }
		IoDispatcher& Io() noexcept { return io_dispatcher_; }

		const CpuThreadPool& CpuExecutor() const noexcept { return cpu_executor_; }
		bool IsCurrentWorker() const noexcept;

	private:
		CpuThreadPool& cpu_executor_;
		BlockingExecutor db_read_executor_;
		SerialExecutor db_write_executor_;
		BlockingExecutor blocking_io_executor_;
		IoDispatcher io_dispatcher_;
		BlockingExecutor compaction_executor_;
		SerialExecutor serial_file_executor_;
	};

} // namespace prism

#endif
