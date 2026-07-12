#ifndef PRISM_SCHEDULER_H
#define PRISM_SCHEDULER_H

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <deque>
#include <functional>
#include <mutex>
#include <new>
#include <queue>
#include <semaphore>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace prism
{
	class InlineJob
	{
	public:
		static constexpr std::size_t kInlineBytes = 128;

		InlineJob() noexcept = default;

		template <typename F>
		    requires(!std::is_same_v<std::decay_t<F>, InlineJob> && std::is_invocable_r_v<void, std::decay_t<F>&>)
		InlineJob(F&& f)
		{
			using Fn = std::decay_t<F>;
			Emplace<Fn>(std::forward<F>(f));
		}

		InlineJob(const InlineJob&) = delete;
		InlineJob& operator=(const InlineJob&) = delete;

		InlineJob(InlineJob&& other) noexcept { MoveFrom(std::move(other)); }

		InlineJob& operator=(InlineJob&& other) noexcept
		{
			if (this != &other)
			{
				Reset();
				MoveFrom(std::move(other));
			}
			return *this;
		}

		~InlineJob() { Reset(); }

		explicit operator bool() const noexcept { return invoke_ != nullptr; }

		void operator()()
		{
			assert(invoke_ != nullptr);
			invoke_(ptr_);
		}

	private:
		using InvokeFn = void (*)(void*);
		using DestroyFn = void (*)(void*) noexcept;
		using DeleteFn = void (*)(void*) noexcept;
		using MoveFn = void (*)(void*, void*) noexcept;

		template <typename Fn>
		static constexpr bool kUseInlineStorage = sizeof(Fn) <= kInlineBytes && alignof(Fn) <= alignof(std::max_align_t)
		    && std::is_nothrow_move_constructible_v<Fn>;

		void* StoragePtr() noexcept { return static_cast<void*>(storage_); }
		const void* StoragePtr() const noexcept { return static_cast<const void*>(storage_); }

		template <typename Fn, typename F>
		void Emplace(F&& f)
		{
			invoke_ = [](void* ptr) { (*static_cast<Fn*>(ptr))(); };
			destroy_ = [](void* ptr) noexcept { std::destroy_at(static_cast<Fn*>(ptr)); };
			delete_ = [](void* ptr) noexcept { delete static_cast<Fn*>(ptr); };
			move_ = [](void* src, void* dst) noexcept {
				std::construct_at(static_cast<Fn*>(dst), std::move(*static_cast<Fn*>(src)));
			};

			if constexpr (kUseInlineStorage<Fn>)
			{
				ptr_ = StoragePtr();
				std::construct_at(static_cast<Fn*>(ptr_), std::forward<F>(f));
				heap_allocated_ = false;
			}
			else
			{
				ptr_ = new Fn(std::forward<F>(f));
				heap_allocated_ = true;
			}
		}

		void Reset() noexcept
		{
			if (invoke_ == nullptr)
			{
				return;
			}
			if (heap_allocated_)
			{
				delete_(ptr_);
			}
			else
			{
				destroy_(ptr_);
			}
			ptr_ = nullptr;
			invoke_ = nullptr;
			destroy_ = nullptr;
			delete_ = nullptr;
			move_ = nullptr;
			heap_allocated_ = false;
		}

		void MoveFrom(InlineJob&& other) noexcept
		{
			ptr_ = nullptr;
			invoke_ = other.invoke_;
			destroy_ = other.destroy_;
			delete_ = other.delete_;
			move_ = other.move_;
			heap_allocated_ = other.heap_allocated_;

			if (other.invoke_ == nullptr)
			{
				return;
			}

			if (other.heap_allocated_)
			{
				ptr_ = other.ptr_;
				other.ptr_ = nullptr;
			}
			else
			{
				ptr_ = StoragePtr();
				move_(other.ptr_, ptr_);
				other.destroy_(other.ptr_);
			}

			other.invoke_ = nullptr;
			other.destroy_ = nullptr;
			other.delete_ = nullptr;
			other.move_ = nullptr;
			other.heap_allocated_ = false;
		}

		alignas(std::max_align_t) std::byte storage_[kInlineBytes];
		void* ptr_{ nullptr };
		InvokeFn invoke_{ nullptr };
		DestroyFn destroy_{ nullptr };
		DeleteFn delete_{ nullptr };
		MoveFn move_{ nullptr };
		bool heap_allocated_{ false };
	};

	struct ExecutorRef
	{
		using Job = InlineJob;

		void* executor = nullptr;
		void (*submit)(void*, Job) = nullptr;

		ExecutorRef() = default;

		template <typename Executor>
		    requires(!std::is_same_v<std::decay_t<Executor>, ExecutorRef>)
		explicit ExecutorRef(Executor& target)
		    : executor(&target)
		    , submit([](void* ptr, Job job) { static_cast<Executor*>(ptr)->Submit(std::move(job)); })
		{
		}

		void Submit(Job job) const { submit(executor, std::move(job)); }
	};

	// CpuThreadPool: Worker-local thread pool with fallback priority and delayed task support.
	//
	// Architecture (split-role topology):
	// - N worker threads (WorkThread), each with its own task queue
	// - Foreground immediate Submit() pushes directly to worker-local queues
	// - 1 priority dispatcher thread: fallback/background path only
	// - 1 lazy dispatcher thread: processes delayed tasks (lazy_queue_), dispatches when ready
	//
	// Task Submission Paths:
	// 1. Submit(job): Immediate priority-0 execution
	//    - throughput-first fast path to worker-local queue
	//    - worker self-submit -> same worker queue
	//    - external submit -> round-robin worker queue
	//
	// 2. SubmitWithPriority(job, priority): Background/fallback execution
	//    - priority == 0 uses the same fast path as Submit(job)
	//    - priority > 0 uses the fallback/background priority queue
	//
	// 3. SubmitAfter(deadline, job): Delayed execution
	//    - Stored in lazy_queue_ until deadline expires
	//    - LazyLoop wakes up at deadline, pushes directly to a worker when possible
	//    - Falls back to priority queue (max priority) if direct routing is unsuitable
	//
	// 4. SubmitIn(ctx, job): Strict affinity to specific worker thread
	//    - When ctx is valid for this scheduler: directly pushes to that worker's local queue
	//    - Jobs remain stealable (work-stealing still applies even to affinity-pinned tasks)
	//    - When ctx is invalid or belongs to a different scheduler: falls back to Submit()
	//      (default priority-0 fast path with least-loaded-worker dispatch)
	//    - Used for continuation on same thread (cache locality)
	//
	// Fallback Dispatch:
	// - Each worker has its own queue (mutex-protected deque)
	// - Each worker tracks an approximate atomic load counter for fast external balancing
	// - QueuedJob tracks pinning metadata for stealing and affinity
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
	// - priority_queue_: protected by priority_mutex_
	// - lazy_queue_: protected by lazy_mutex_
	// - Each WorkThread's queue_: protected by its own mutex_
	// - Semaphores (priority_waiter_, lazy_waiter_, WorkThread::semaphore_) for coordination
	class CpuThreadPool
	{
	public:
		// Context: Opaque handle to a worker thread, captured by CaptureContext().
		// Used for affinity-based task submission (SubmitIn).
		class Context
		{
			friend class CpuThreadPool;

		public:
			Context() = default;
			bool operator==(const Context& rhs) const = default;

			// Returns true if this context was captured from a worker thread of its scheduler instance.
			bool IsValid() const noexcept { return scheduler_ != nullptr; }

		private:
			explicit Context(const CpuThreadPool* scheduler, std::size_t worker_index)
			    : scheduler_(scheduler)
			    , worker_index_(worker_index)
			{
			}

			const CpuThreadPool* scheduler_{ nullptr };
			std::size_t worker_index_{ 0 };
		};

		using Job = InlineJob;

		// Constructs thread pool with `num_threads` workers.
		// If num_threads == 0, defaults to max(hardware_concurrency, kMinThreads).
		// If num_threads > 0, uses exactly max(num_threads, kMinThreads) workers.

		explicit CpuThreadPool(std::size_t num_threads = 0);
		~CpuThreadPool();

		CpuThreadPool(const CpuThreadPool&) = delete;
		CpuThreadPool& operator=(const CpuThreadPool&) = delete;
		CpuThreadPool(CpuThreadPool&&) = delete;
		CpuThreadPool& operator=(CpuThreadPool&&) = delete;

		// Captures current thread context for SubmitIn().
		// Returns a valid Context only if called from a worker thread of THIS scheduler instance.
		// Returns an invalid Context if called from any other thread (including workers of other schedulers).
		Context CaptureContext() const;

		void Submit(Job job);
		template <typename F>
		    requires(!std::is_same_v<std::decay_t<F>, Job>)
		void Submit(F&& job)
		{
			SubmitJob(std::forward<F>(job), 0);
		}

		// Submit job with priority (higher number = higher priority).
		void SubmitWithPriority(Job job, std::size_t priority);
		template <typename F>
		    requires(!std::is_same_v<std::decay_t<F>, Job>)
		void SubmitWithPriority(F&& job, std::size_t priority)
		{
			SubmitJob(std::forward<F>(job), priority);
		}

		// Submit job to execute after deadline/delay.
		void SubmitAfter(std::chrono::steady_clock::time_point deadline, Job job);
		template <typename F>
		    requires(!std::is_same_v<std::decay_t<F>, Job>)
		void SubmitAfter(std::chrono::steady_clock::time_point deadline, F&& job)
		{
			SubmitAfterJob(deadline, std::forward<F>(job));
		}
		void SubmitAfter(std::chrono::milliseconds delay, Job job)
		{
			SubmitAfter(std::chrono::steady_clock::now() + delay, std::move(job));
		}
		template <typename F>
		    requires(!std::is_same_v<std::decay_t<F>, Job>)
		void SubmitAfter(std::chrono::milliseconds delay, F&& job)
		{
			SubmitAfter(std::chrono::steady_clock::now() + delay, std::forward<F>(job));
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
		template <typename F>
		    requires(!std::is_same_v<std::decay_t<F>, Job>)
		void SubmitIn(Context ctx, F&& job)
		{
			SubmitInJob(ctx, std::forward<F>(job));
		}
		// Returns the number of worker threads in the pool.
		std::size_t WorkerCount() const { return work_threads_.size(); }

	private:
		// WorkThread: Worker with its own task queue.
		// Consumes tasks from its queue and steals from peers under load.
		class WorkThread
		{
		public:
			WorkThread() = default;
			~WorkThread() = default;

			WorkThread(const WorkThread&) = delete;
			WorkThread& operator=(const WorkThread&) = delete;
			WorkThread(WorkThread&&) = delete;
			WorkThread& operator=(WorkThread&&) = delete;

			void Start(CpuThreadPool& scheduler, std::size_t worker_index);
			void Join();
			std::thread::id Id() const;

			// Push: Direct submission (used by SubmitIn for affinity)
			void Push(Job job);
			void Push(Job job, bool stealable);

			// PushDispatched: Submission from dispatcher (marks job as dispatched=true)
			void PushDispatched(Job job);
			template <typename F>
			void Emplace(F&& job, bool stealable)
			{
				{
					std::lock_guard lock(mutex_);
					queue_.emplace_back(std::forward<F>(job), stealable);
					load_.fetch_add(1, std::memory_order_relaxed);
				}
				semaphore_.release();
			}
			template <typename F>
			void EmplaceDispatched(F&& job)
			{
				Emplace(std::forward<F>(job), true);
			}
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
				QueuedJob() = default;
				template <typename F>
				QueuedJob(F&& fn, bool is_stealable)
				    : job(std::forward<F>(fn))
				    , stealable(is_stealable)
				{
				}

				Job job;
				bool stealable{ true };
#ifdef PRISM_RUNTIME_METRICS
				bool stolen{ false };
#endif
			};

			void Consume(CpuThreadPool& scheduler, std::size_t worker_index) noexcept;
			bool TrySteal(CpuThreadPool& scheduler, std::size_t worker_index, std::uint64_t& rng_state);
			bool TryDequeueJob(QueuedJob& out);
			bool HandleJobCompletion(QueuedJob& job, CpuThreadPool& scheduler) noexcept;

			std::counting_semaphore<> semaphore_{ 0z };
			std::jthread thread_{};
			std::mutex mutex_;
			std::deque<QueuedJob> queue_;
			std::atomic<std::size_t> load_{ 0 };
		};

		struct PriorityTask
		{
			PriorityTask() = default;
			template <typename F>
			PriorityTask(F&& fn, std::size_t task_priority)
			    : job(std::forward<F>(fn))
			    , priority(task_priority)
			{
			}

			Job job;
			std::size_t priority;

			bool operator<(const PriorityTask& rhs) const noexcept { return priority < rhs.priority; }
		};

		struct LazyTask
		{
			LazyTask() = default;
			template <typename F>
			LazyTask(F&& fn, std::chrono::steady_clock::time_point task_deadline)
			    : job(std::forward<F>(fn))
			    , deadline(task_deadline)
			{
			}

			Job job;
			std::chrono::steady_clock::time_point deadline;

			bool operator<(const LazyTask& rhs) const noexcept { return rhs.deadline < deadline; }
		};

		bool TryPushToWorker(Job job, std::size_t worker_index, bool dispatched, bool stealable);
		template <typename F>
		bool TryEmplaceToWorker(F&& job, std::size_t worker_index, bool dispatched, bool stealable)
		{
			if (worker_index >= work_threads_.size())
			{
				return false;
			}

			auto& worker = work_threads_[worker_index];
			if (dispatched)
			{
				worker.EmplaceDispatched(std::forward<F>(job));
			}
			else
			{
				worker.Emplace(std::forward<F>(job), stealable);
			}
			return true;
		}
		std::size_t ChooseSubmissionWorker();
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
		template <typename F>
		void PushToPriorityQueue(F&& job, std::size_t priority, bool wake = true)
		{
			{
				std::lock_guard lock(priority_mutex_);
				priority_queue_.emplace(std::forward<F>(job), priority);
			}
			if (wake)
			{
				priority_waiter_.release();
			}
		}

		template <typename F>
		void SubmitJob(F&& job, std::size_t priority)
		{
			assert(ShouldAcceptSubmitDuringShutdown());

			if (ShouldUseFastPath(priority))
			{
				if (current_scheduler_ == this)
				{
					if (TryEmplaceToWorker(std::forward<F>(job), current_worker_index_, false, true))
					{
#ifdef PRISM_RUNTIME_METRICS
						RecordForegroundFastPathSubmit();
#endif
						return;
					}
				}

				const auto worker_index = ChooseSubmissionWorker();
				if (TryEmplaceToWorker(std::forward<F>(job), worker_index, false, true))
				{
#ifdef PRISM_RUNTIME_METRICS
					RecordForegroundFastPathSubmit();
#endif
					return;
				}
			}

#ifdef PRISM_RUNTIME_METRICS
			RecordForegroundFallbackSubmit();
#endif
			PushToPriorityQueue(std::forward<F>(job), priority, /*wake=*/true);
		}

		template <typename F>
		void SubmitAfterJob(std::chrono::steady_clock::time_point deadline, F&& job)
		{
			assert(ShouldAcceptSubmitDuringShutdown());

			{
				std::lock_guard lock(lazy_mutex_);
				lazy_queue_.emplace(std::forward<F>(job), deadline);
			}
			lazy_waiter_.release();
		}

		template <typename F>
		void SubmitInJob(Context ctx, F&& job)
		{
			assert(ShouldAcceptSubmitDuringShutdown());

			if (ctx.scheduler_ == this && TryEmplaceToWorker(std::forward<F>(job), ctx.worker_index_, false, true))
			{
				return;
			}

#ifndef NDEBUG
			std::fprintf(stderr,
			    "[prism::scheduler] SubmitIn context mismatch: this=%p ctx.scheduler_=%p ctx.worker_index_=%zu."
			    " Falling back to Submit().\n",
			    static_cast<const void*>(this), static_cast<const void*>(ctx.scheduler_), ctx.worker_index_);
#endif
			SubmitJob(std::forward<F>(job), 0);
		}

		std::vector<WorkThread> work_threads_;
		std::atomic<std::size_t> submit_cursor_{ 0 };

		std::priority_queue<LazyTask> lazy_queue_;
		std::priority_queue<PriorityTask> priority_queue_;

		std::counting_semaphore<> lazy_waiter_{ 0z };
		std::counting_semaphore<> priority_waiter_{ 0z };
		std::mutex lazy_mutex_;
		std::mutex priority_mutex_;

		std::jthread lazy_thread_;
		std::jthread priority_thread_;

		std::atomic<bool> exit_flag_{ false };

#ifdef PRISM_RUNTIME_METRICS
		static void RecordForegroundFastPathSubmit() noexcept;
		static void RecordForegroundFallbackSubmit() noexcept;
#endif

		static thread_local const CpuThreadPool* current_scheduler_;
		static thread_local std::size_t current_worker_index_;
	};
}

#endif
