#ifndef PRISM_SCHEDULER_H
#define PRISM_SCHEDULER_H

#include <atomic>
#include <chrono>
#include <cstddef>
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
	};

	// ThreadPoolScheduler: Multi-queue dispatch thread pool with priority and delayed task support.
	//
	// Architecture (push-based multi-queue dispatch):
	// - N worker threads (WorkThread), each with its own task queue
	// - 1 priority dispatcher thread: processes priority_queue_, dispatches to idle workers
	// - 1 lazy dispatcher thread: processes delayed tasks (lazy_queue_), dispatches when ready
	// - pending_list_: tracks idle workers ready to accept tasks
	//
	// Task Submission Paths:
	// 1. Submit(job, priority): Immediate execution via priority queue
	//    - Higher priority tasks execute first
	//    - Dispatcher tries to find an idle worker (TryDispatch)
	//    - If no idle workers, waits until one becomes available
	//
	// 2. SubmitAfter(deadline, job): Delayed execution
	//    - Stored in lazy_queue_ until deadline expires
	//    - LazyLoop wakes up at deadline, dispatches task
	//    - Falls back to priority queue (max priority) if no idle workers
	//
	// 3. SubmitIn(ctx, job): Affinity to specific worker thread
	//    - Directly pushes to that worker's queue (bypasses dispatchers)
	//    - Used for continuation on same thread (cache locality)
	//
	// Idle Worker Registration & Dispatch:
	// - Each worker has its own queue (mutex-protected deque)
	// - When a worker becomes idle, it re-enters pending_list_
	// - Dispatchers assign new tasks to idle workers (PushDispatched)
	// - QueuedJob: per-task metadata; `dispatched` flag drives pending re-registration.
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

		// Submit job to specific worker thread (for cache locality).
		// Precondition: ctx must be valid (captured via CaptureContext on a worker thread of this scheduler).
		// If ctx is invalid or from a different scheduler, the job is submitted via the default priority path.
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

			// PushDispatched: Submission from dispatcher (marks job as dispatched=true)
			void PushDispatched(Job job);

			// Wake: Signal semaphore (used during shutdown)
			void Wake();

			// DrainRemaining: Execute all tasks left in queue after thread has stopped.
			// Called from destructor after Join(), so no concurrency.
			// Returns true if any tasks were drained.
			bool DrainRemaining() noexcept;

		private:
			void Consume(ThreadPoolScheduler& scheduler, std::size_t worker_index) noexcept;

			std::counting_semaphore<> semaphore_{ 0z };
			std::jthread thread_{};
			std::mutex mutex_;
			struct QueuedJob
			{
				Job job;
				bool dispatched{ false };
			};
			std::deque<QueuedJob> queue_;
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

		// PriorityLoop: Dispatcher thread that processes priority_queue_
		void PriorityLoop();

		// LazyLoop: Dispatcher thread that processes delayed tasks (lazy_queue_)
		void LazyLoop();

		bool IsExitRequested() const noexcept;
		void Exit() noexcept;

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
