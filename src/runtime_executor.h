#ifndef PRISM_RUNTIME_EXECUTOR_H
#define PRISM_RUNTIME_EXECUTOR_H

#include "scheduler.h"

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace prism
{
	enum class AsyncEnvBackendMode
	{
		kDefault,
		kThreadPool,
		kBlockingLane,
	};

	// The interface for all continuation executors
	// All the async execution backends should implement this interface
	class IContinuationExecutor
	{
	public:
		virtual ~IContinuationExecutor() = default;
		virtual void Submit(std::function<void()> work) = 0;
	};

	enum class BlockingExecutorLane
	{
		kGeneric,
		kRead,
		kCompaction,
	};

	// A thread pool executor that uses a ThreadPoolScheduler to execute continuations
	// Works in ThreadPoolExecutor are CPU bound
	// which with no blocking
	class ThreadPoolExecutor final: public IContinuationExecutor
	{
	public:
		explicit ThreadPoolExecutor(ThreadPoolScheduler& scheduler);

		void Submit(std::function<void()> work) override;

	private:
		ThreadPoolScheduler* scheduler_;
	};

	// For IO or other likely blocking operations.
	// Thread count is caller-selected per lane.
	class BlockingExecutor final: public IContinuationExecutor
	{
	public:
		explicit BlockingExecutor(std::size_t thread_count = 1,
		    BlockingExecutorLane lane = BlockingExecutorLane::kGeneric);
		~BlockingExecutor();

		BlockingExecutor(const BlockingExecutor&) = delete;
		BlockingExecutor& operator=(const BlockingExecutor&) = delete;
		BlockingExecutor(BlockingExecutor&&) = delete;
		BlockingExecutor& operator=(BlockingExecutor&&) = delete;

		void Submit(std::function<void()> work) override;
		bool Empty() const;
		bool IsCurrentWorker() const noexcept;

	private:
		void WorkerLoop();

		mutable std::mutex mutex_;
		std::condition_variable cv_;
		std::deque<std::function<void()>> queue_;
		BlockingExecutorLane lane_{ BlockingExecutorLane::kGeneric };
		bool stopping_{ false };
		std::vector<std::jthread> workers_;
	};

	// A FIFO lane that executes continuations serially
	// Executes continuations in order
	class SerialLane final: public IContinuationExecutor
	{
	public:
		SerialLane();
		~SerialLane();

		SerialLane(const SerialLane&) = delete;
		SerialLane& operator=(const SerialLane&) = delete;
		SerialLane(SerialLane&&) = delete;
		SerialLane& operator=(SerialLane&&) = delete;

		void Submit(std::function<void()> work) override;
		bool Empty() const;
		bool Done() const;
		bool IsCurrentWorker() const noexcept;

	private:
		void WorkerLoop();

		mutable std::mutex mutex_;
		std::condition_variable cv_;
		std::deque<std::function<void()>> queue_;
		std::jthread worker_;
		bool stopping_{ false };
		bool running_{ false };
	};

	class ExecutorSchedulerAdapter final: public IScheduler
	{
	public:
		explicit ExecutorSchedulerAdapter(
		    IContinuationExecutor& executor, IScheduler* blocking_scheduler = nullptr, IScheduler* continuation_scheduler = nullptr);

		void Submit(Job job, std::size_t priority = 0) override;
		IScheduler* BlockingScheduler() noexcept override;
		IScheduler* ContinuationScheduler() noexcept override;

	private:
		IContinuationExecutor* executor_;
		IScheduler* blocking_scheduler_;
		IScheduler* continuation_scheduler_;
	};

	// RuntimeBundle owns the complete async execution environment for a DB instance.
	// It concretely constructs executors and scheduler adapters, wiring the routing
	// relationships declared by IScheduler::BlockingScheduler/ContinuationScheduler.
	//
	// Physical threads (only 4 sources):
	//   ThreadPoolScheduler (external, shared) — N worker threads, priority + timer
	//   BlockingExecutor(4) — async file-I/O / metadata lane
	//   BlockingExecutor(1) — background compaction lane (single-flight preserved)
	//   SerialLane — 1 FIFO worker for ordered file writes
	//
	// Scheduler routing:
	//   foreground_db_scheduler.Submit(job) → cpu_executor_impl → ThreadPoolScheduler::Submit(job, 0)
	//     (worker-local fast path / stealing-enabled shared CPU pool)
	//   runtime_scheduler.Submit(job) → cpu_executor_impl → ThreadPoolScheduler
	//   runtime_scheduler.BlockingScheduler() → read_scheduler → read_executor
	//   runtime_scheduler.ContinuationScheduler() → cpu_scheduler → cpu_executor_impl
	//     (available for explicit continuations; AsyncOp resumes inline after work completion)
	//   read_scheduler.Submit(job) → read_executor
	//   compaction_scheduler.Submit(job) → compaction_executor
	//   serial_scheduler.Submit(job) → serial_lane
	//
	// Ownership: RuntimeBundle is held via shared_ptr and registered by ThreadPoolScheduler
	// key in a global weak registry (AcquireRuntimeBundle). If the last reference is
	// released from a bundle-owned worker, deletion is deferred to a cleanup thread
	// so no executor ever joins itself.
	struct RuntimeBundle
	{
		explicit RuntimeBundle(ThreadPoolScheduler& scheduler);
		bool IsCurrentWorker() const noexcept;

		RuntimeBundle(const RuntimeBundle&) = delete;
		RuntimeBundle& operator=(const RuntimeBundle&) = delete;
		RuntimeBundle(RuntimeBundle&&) = delete;
		RuntimeBundle& operator=(RuntimeBundle&&) = delete;

		// -- Physical executors (IContinuationExecutor, owns or wraps threads) --

		// cpu_executor_impl wraps the shared ThreadPoolScheduler for CPU-bound continuations.
		// cpu_executor is just a pointer alias to cpu_executor_impl for external access.
		ThreadPoolExecutor cpu_executor_impl;
		IContinuationExecutor* cpu_executor;

		// timer_source points to the same ThreadPoolScheduler, used as a registry key
		// by AcquireRuntimeBundle(). Named "timer_source" because ThreadPoolScheduler
		// provides SubmitAfter() — reserved for future timer use.
		ThreadPoolScheduler* timer_source;

		// read_executor: async file-I/O blocking lane. AsyncEnv defaults here so
		// file reads and metadata calls stay isolated from foreground DB work and
		// from the single-flight compaction lane.
		BlockingExecutor read_executor;

		// compaction_executor: single-threaded FIFO dedicated to background
		// compaction/flush work. Kept isolated to preserve one-compaction-per-DB
		// semantics without contending with foreground reads.
		BlockingExecutor compaction_executor;

		// serial_lane: single-threaded FIFO for ordered file writes (Append/Flush/Sync).
		// Guarantees submission order = execution order without blocking shared workers.
		SerialLane serial_lane;

		// -- Scheduler adapters (IScheduler, wraps an IContinuationExecutor) --

		// cpu_scheduler: wraps cpu_executor_impl. BlockingScheduler/ContinuationScheduler
		// both return this (self), so work and coroutine resume share the same thread pool.
		ExecutorSchedulerAdapter cpu_scheduler;

		// read_scheduler: wraps read_executor. Blocking work runs on this lane, while
		// AsyncOp resumes inline on the completing worker to avoid a second queue hop.
		// RuntimeBundle destruction is deferred by AcquireRuntimeBundle() if the last
		// reference is released here.
		ExecutorSchedulerAdapter read_scheduler;

		// compaction_scheduler: wraps compaction_executor. Used only for explicit
		// direct background work submission, not runtime_scheduler routing.
		ExecutorSchedulerAdapter compaction_scheduler;

		// serial_scheduler: wraps serial_lane for ordered writes.
		ExecutorSchedulerAdapter serial_scheduler;

		// runtime_scheduler: legacy split-routing adapter. Direct Submit() stays on the
		// shared CPU pool, BlockingScheduler() redirects blocking work to read_scheduler,
		// and explicit continuations resolve back to cpu_scheduler. AsyncDB and AsyncEnv
		// no longer use this adapter; it remains only for explicit legacy callers and
		// routing tests that verify the split-lane contract.
		ExecutorSchedulerAdapter runtime_scheduler;

		// foreground_db_scheduler: wraps cpu_executor_impl and keeps both blocking work and
		// continuations on the shared thread pool. AsyncDB foreground operations use this
		// adapter so CPU-resident DB work lands on ThreadPoolScheduler::Submit(job, 0),
		// bypassing the old priority dispatcher/read-lane bottleneck while preserving
		// explicit read/compaction/serial isolation for blocking subsystems.
		ExecutorSchedulerAdapter foreground_db_scheduler;

		// -- I/O backend configuration --

		// async_env_backend: controls which scheduler AsyncEnv uses for file I/O.
		AsyncEnvBackendMode async_env_backend{ AsyncEnvBackendMode::kDefault };
	};

	std::shared_ptr<RuntimeBundle> AcquireRuntimeBundle(ThreadPoolScheduler& scheduler);

} // namespace prism

#endif
