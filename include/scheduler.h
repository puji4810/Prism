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
	// ThreadPoolScheduler: Work-stealing thread pool with priority and delayed task support.
	//
	// Architecture:
	// - N worker threads (WorkThread), each with its own task queue (work-stealing source)
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
	// Work-Stealing Mechanism:
	// - Each worker has its own queue (mutex-protected deque)
	// - When a worker becomes idle, it re-enters pending_list_
	// - Dispatchers assign new tasks to idle workers (PushDispatched)
	// - return_to_pending_ flag: worker re-enters pending_list_ after consuming dispatched task
	//
	// Shutdown Protocol:
	// - Exit() sets exit_flag_, wakes all threads
	// - Dispatcher threads exit their loops
	// - Worker threads drain their queues and exit
	// - Destructor joins all threads (safe cleanup)
	//
	// Thread Safety:
	// - pending_list_: protected by pending_mutex_
	// - priority_queue_: protected by priority_mutex_
	// - lazy_queue_: protected by lazy_mutex_
	// - Each WorkThread's queue_: protected by its own mutex_
	// - Semaphores (priority_waiter_, lazy_waiter_, WorkThread::semaphore_) for coordination
	class ThreadPoolScheduler
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

		private:
			explicit Context(std::thread::id id)
			    : thread_id_(id)
			{
			}

			std::thread::id thread_id_;
		};

		using Job = std::function<void()>;

		// Constructs thread pool with `num_threads` workers.
		// If num_threads == 0, defaults to max(hardware_concurrency, 2).
		explicit ThreadPoolScheduler(std::size_t num_threads = 0);
		~ThreadPoolScheduler();

		ThreadPoolScheduler(const ThreadPoolScheduler&) = delete;
		ThreadPoolScheduler& operator=(const ThreadPoolScheduler&) = delete;
		ThreadPoolScheduler(ThreadPoolScheduler&&) = delete;
		ThreadPoolScheduler& operator=(ThreadPoolScheduler&&) = delete;

		// Captures current thread context for SubmitIn().
		static Context CaptureContext();

		// Submit job with priority (higher number = higher priority).
		void Submit(Job job, std::size_t priority = 0);

		// Submit job to execute after deadline/delay.
		void SubmitAfter(std::chrono::steady_clock::time_point deadline, Job job);
		void SubmitAfter(std::chrono::milliseconds delay, Job job)
		{
			SubmitAfter(std::chrono::steady_clock::now() + delay, std::move(job));
		}

		// Submit job to specific worker thread (for cache locality).
		void SubmitIn(Context ctx, Job job);

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

			void Start(ThreadPoolScheduler& scheduler);
			void Join();
			std::thread::id Id() const;

			// Push: Direct submission (used by SubmitIn for affinity)
			void Push(Job job);

			// PushDispatched: Submission from dispatcher (sets return_to_pending_ flag)
			void PushDispatched(Job job);

			// Wake: Signal semaphore (used during shutdown)
			void Wake();

		private:
			void Consume(ThreadPoolScheduler& scheduler) noexcept;

			std::counting_semaphore<> semaphore_{ 0z };
			std::jthread thread_{};
			std::mutex mutex_;
			std::deque<Job> queue_;
			bool return_to_pending_ = false; // If true, re-enter pending_list_ after next job
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
		bool TryDispatch(Job job);

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
