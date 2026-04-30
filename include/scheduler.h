#ifndef PRISM_SCHEDULER_H
#define PRISM_SCHEDULER_H

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <queue>
#include <semaphore>
#include <thread>
#include <vector>

namespace prism
{
	// IScheduler: Minimal abstract scheduler interface.
	// Allows AsyncOp and other components to submit work without depending on
	// the concrete ThreadPoolScheduler. Enables deterministic test doubles.
	class IScheduler
	{
	public:
		using Job = std::function<void()>;

		virtual ~IScheduler() = default;

		// Submit a job with the given priority (higher = sooner).
		// Thread-safe. Must be callable from any thread.
		virtual void Submit(Job job, std::size_t priority = 0) = 0;

		// Routing hooks used by AsyncOp to split work execution from coroutine resumption.
		// BlockingScheduler()  — returns the scheduler where the actual work (I/O, CPU) is submitted.
		// ContinuationScheduler() — returns the scheduler where the coroutine is resumed after work.
		// Default (inherited) implementation returns `this`, routing both through the same scheduler.
		// Test doubles and ExecutorSchedulerAdapter override these to split blocking/continuation lanes.
		virtual IScheduler* BlockingScheduler() noexcept { return this; }
		virtual IScheduler* ContinuationScheduler() noexcept { return this; }
	};

	// ThreadPoolScheduler: Worker-local thread pool with fallback priority and delayed task support.
	//
	// Architecture (split-role topology):
	// - N worker threads (WorkThread), each with its own task queue
	// - Foreground immediate Submit() pushes directly to worker-local queues
	// - 1 priority dispatcher thread: fallback/background path only
	// - 1 lazy dispatcher thread: processes delayed tasks (lazy_queue_), dispatches when ready
	// - pending_list_: tracks idle workers for fallback/background dispatch only
	//
	// Task Submission Paths:
	// 1. Submit(job, priority): Immediate execution
	//    - priority == 0: throughput-first fast path to worker-local queue
	//      * worker self-submit -> same worker queue
	//      * external submit -> least-loaded worker queue
	//    - priority > 0: fallback/background priority queue
	//
	// 2. SubmitAfter(deadline, job): Delayed execution
	//    - Stored in lazy_queue_ until deadline expires
	//    - LazyLoop wakes up at deadline, pushes directly to a worker when possible
	//    - Falls back to priority queue (max priority) if direct routing is unsuitable
	//
	// 3. SubmitIn(ctx, job): Strict affinity to specific worker thread
	//    - When ctx is valid for this scheduler: directly pushes to that worker's local queue
	//    - Jobs remain stealable (work-stealing still applies even to affinity-pinned tasks)
	//    - When ctx is invalid or belongs to a different scheduler: falls back to Submit()
	//      (default priority-0 fast path with least-loaded-worker dispatch)
	//    - Used for continuation on same thread (cache locality)
	//
	// Fallback Dispatch:
	// - Each worker has its own queue (mutex-protected deque)
	// - Each worker tracks an approximate atomic load counter for fast external balancing
	// - Workers re-enter pending_list_ only for fallback/background dispatch
	// - QueuedJob tracks dispatch/pinning metadata; `dispatched` preserves pending-list invariants
	//
	// - Shutdown Protocol:
	// - Exit() sets exit_flag_, wakes all threads.
	// - Draining: Dispatcher threads and worker threads drain their respective queues before exiting.
	// - Late Submissions: Submissions during shutdown from threads *external* to this scheduler are unsupported.
	//   However, submissions from jobs already running on this scheduler's worker threads are supported and drained.
	// - Delayed Tasks: Pending SubmitAfter tasks are promoted to immediate execution during shutdown.
	// - Destruction: Destructor calls Exit() and joins all threads. It is an error to call Submit*() concurrently during destruction.
	// - Exception Policy: Any exception escaping a Job is a bug; the scheduler catches and calls std::terminate() (fail-fast).
	// - Destructor joins all threads (safe cleanup)
	//
	// Thread Safety:
	// - pending_list_: protected by pending_mutex_
	// - priority_queue_: protected by priority_mutex_
	// - lazy_queue_: protected by lazy_mutex_
	// - Each WorkThread's queue_: protected by its own mutex_
	// - Semaphores (priority_waiter_, lazy_waiter_, WorkThread::semaphore_) for coordination
	class ThreadPoolScheduler : public IScheduler
	{
	public:
		// Context: Opaque handle to a worker thread, captured by CaptureContext().
		// Used for affinity-based task submission (SubmitIn).
		class Context
		{
			friend class ThreadPoolScheduler;

		public:
			Context() = default;
			bool operator==(const Context& rhs) const = default;

			// Returns true if this context was captured from a worker thread of its scheduler instance.
			bool IsValid() const noexcept { return scheduler_ != nullptr; }

		private:
			explicit Context(const ThreadPoolScheduler* scheduler, std::size_t worker_index)
			    : scheduler_(scheduler)
			    , worker_index_(worker_index)
			{
			}

			const ThreadPoolScheduler* scheduler_{ nullptr };
			std::size_t worker_index_{ 0 };
		};

		using Job = IScheduler::Job;

		// Constructs thread pool with `num_threads` workers.
		// If num_threads == 0, defaults to max(hardware_concurrency, kMinThreads).
		// If num_threads > 0, uses exactly max(num_threads, kMinThreads) workers.

		explicit ThreadPoolScheduler(std::size_t num_threads = 0);
		~ThreadPoolScheduler();

		ThreadPoolScheduler(const ThreadPoolScheduler&) = delete;
		ThreadPoolScheduler& operator=(const ThreadPoolScheduler&) = delete;
		ThreadPoolScheduler(ThreadPoolScheduler&&) = delete;
		ThreadPoolScheduler& operator=(ThreadPoolScheduler&&) = delete;

		// Captures current thread context for SubmitIn().
		// Returns a valid Context only if called from a worker thread of THIS scheduler instance.
		// Returns an invalid Context if called from any other thread (including workers of other schedulers).
		Context CaptureContext() const;

		// Submit job with priority (higher number = higher priority).
		void Submit(Job job, std::size_t priority = 0) override;

		// Submit job to execute after deadline/delay.
		void SubmitAfter(std::chrono::steady_clock::time_point deadline, Job job);
		void SubmitAfter(std::chrono::milliseconds delay, Job job)
		{
			SubmitAfter(std::chrono::steady_clock::now() + delay, std::move(job));
		}

		// Submit job with strict same-worker affinity (for cache locality).
		//
		// Strict affinity: when ctx was captured from a worker of THIS scheduler instance,
		// the job is pushed directly to that exact worker's local queue. The job remains
		// stealable — other workers may steal it under load, but the affinity-push
		// maximizes the chance the same-worker executes it.
		//
		// Fallback: when ctx is invalid, default-constructed, or captured from a different
		// scheduler instance, the job is submitted via Submit() (priority-0 fast path with
		// least-loaded-worker dispatch). This fallback guarantees progress; no job is lost.
		//
		// During shutdown: only worker re-entrant SubmitIn calls are accepted
		// (identical to Submit/SubmitAfter policy — see ShouldAcceptSubmitDuringShutdown).
		void SubmitIn(Context ctx, Job job);
		// Returns the number of worker threads in the pool.
		std::size_t WorkerCount() const { return work_threads_.size(); }

	private:
		// WorkThread: Worker with its own task queue.
		// Consumes tasks from its queue, re-enters pending_list_ when idle.
		class WorkThread
		{
		public:
			WorkThread() = default;
			~WorkThread() = default;

			WorkThread(const WorkThread&) = delete;
			WorkThread& operator=(const WorkThread&) = delete;
			WorkThread(WorkThread&&) = delete;
			WorkThread& operator=(WorkThread&&) = delete;

			void Start(ThreadPoolScheduler& scheduler, std::size_t worker_index);
			void Join();
			std::thread::id Id() const;

			// Push: Direct submission (used by SubmitIn for affinity)
			void Push(Job job);
			void Push(Job job, bool stealable);

			// PushDispatched: Submission from dispatcher (marks job as dispatched=true)
			void PushDispatched(Job job);
			std::size_t Load() const noexcept;

			// Wake: Signal semaphore (used during shutdown)
			void Wake();

			// DrainRemaining: Execute all tasks left in queue after thread has stopped.
			// Called from destructor after Join(), so no concurrency.
			// Returns true if any tasks were drained.
			bool DrainRemaining() noexcept;

		private:
			struct QueuedJob
			{
				Job job;
				bool dispatched{ false };
				bool stealable{ true };
				bool stolen{ false };
			};

			void Consume(ThreadPoolScheduler& scheduler, std::size_t worker_index) noexcept;
			bool TrySteal(ThreadPoolScheduler& scheduler, std::size_t worker_index, std::uint64_t& rng_state);
			bool TryDequeueJob(QueuedJob& out, bool& queue_empty);
			bool HandleJobCompletion(const QueuedJob& job, bool queue_empty_after,
			                        ThreadPoolScheduler& scheduler) noexcept;

			// Mechanics: register this worker as idle in pending_list_ and signal
			// the priority dispatcher so it can route work here.
			void RegisterAsIdleWorker(ThreadPoolScheduler& scheduler);

			std::counting_semaphore<> semaphore_{ 0z };
			std::jthread thread_{};
			std::mutex mutex_;
			std::deque<QueuedJob> queue_;
			std::atomic<std::size_t> load_{ 0 };
		};

		struct PriorityTask
		{
			Job job;
			std::size_t priority;

			bool operator<(const PriorityTask& rhs) const noexcept { return priority < rhs.priority; }
		};

		struct LazyTask
		{
			Job job;
			std::chrono::steady_clock::time_point deadline;

			bool operator<(const LazyTask& rhs) const noexcept { return rhs.deadline < deadline; }
		};

		// TryDispatch: Find idle worker and assign job. Returns false if no idle workers.
		// Only consumes `job` on success so callers can safely retry/fallback.
		bool TryDispatch(Job& job);
		WorkThread* TryReserveIdleWorker();
		void ReturnReservedIdleWorker(WorkThread* worker);
		bool TryPushToWorker(Job job, std::size_t worker_index, bool dispatched, bool stealable);
		std::size_t ChooseLeastLoadedWorker() const;

		// PriorityLoop: Dispatcher thread that processes priority_queue_
		void PriorityLoop();

		// LazyLoop: Dispatcher thread that processes delayed tasks (lazy_queue_)
		void LazyLoop();

		// DispatchExpiredTask: push a ready lazy task to a worker or fall back to the priority queue.
		void DispatchExpiredTask(LazyTask&& task);
		// PromoteLazyResidueToWorkers: pre-join promotion of delayed tasks so workers can consume them.
		void PromoteLazyResidueToWorkers();
		// DrainLazyQueueToEmpty: post-join drain of lazy queue, executing inline when no worker available.
		void DrainLazyQueueToEmpty(bool& work_remains);
		// DrainPriorityQueueToEmpty: post-join drain of priority queue, executing inline when no worker available.
		void DrainPriorityQueueToEmpty(bool& work_remains);
		// DrainWorkerLocalQueues: drain each worker's remaining local-queue tasks after thread join.
		void DrainWorkerLocalQueues(bool& work_remains);

		bool IsExitRequested() const noexcept;
		void Exit() noexcept;

		// ── Policy helpers (path-selection decisions) ──────────────────
		// ShouldUseFastPath: returns true when priority==0 enables the
		//   worker-local submit fast path rather than priority-queue dispatch.
		static bool ShouldUseFastPath(std::size_t priority) noexcept;

		// ShouldAcceptSubmitDuringShutdown: gate that permits only worker
		//   re-entrant submits after exit_flag_ is set.
		bool ShouldAcceptSubmitDuringShutdown() const noexcept;

		// ShouldPromoteLazyTask: returns true when a delayed task's deadline
		//   has expired, meaning it should be dispatched immediately.
		static bool ShouldPromoteLazyTask(
		    const LazyTask& task,
		    std::chrono::steady_clock::time_point now) noexcept;

		// ── Mechanics helpers (queue/thread operations) ─────────────────
		// PushToPriorityQueue: enqueue a job into priority_queue_ and
		//   optionally wake the priority dispatcher thread.
		void PushToPriorityQueue(Job job, std::size_t priority, bool wake = true);

		std::vector<WorkThread> work_threads_;
		std::vector<WorkThread*> pending_list_;
		std::mutex pending_mutex_;

		std::priority_queue<LazyTask> lazy_queue_;
		std::priority_queue<PriorityTask> priority_queue_;

		std::counting_semaphore<> lazy_waiter_{ 0z };
		std::counting_semaphore<> priority_waiter_{ 0z };
		std::mutex lazy_mutex_;
		std::mutex priority_mutex_;

		std::jthread lazy_thread_;
		std::jthread priority_thread_;

		std::atomic<bool> exit_flag_{ false };
	};
}

#endif
