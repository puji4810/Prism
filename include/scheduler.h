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
	class ThreadPoolScheduler
	{
	public:
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

		explicit ThreadPoolScheduler(std::size_t num_threads = 0);
		~ThreadPoolScheduler();

		ThreadPoolScheduler(const ThreadPoolScheduler&) = delete;
		ThreadPoolScheduler& operator=(const ThreadPoolScheduler&) = delete;
		ThreadPoolScheduler(ThreadPoolScheduler&&) = delete;
		ThreadPoolScheduler& operator=(ThreadPoolScheduler&&) = delete;

		static Context CaptureContext();

		void Submit(Job job, std::size_t priority = 0);
		void SubmitAfter(std::chrono::steady_clock::time_point deadline, Job job);
		void SubmitAfter(std::chrono::milliseconds delay, Job job)
		{
			SubmitAfter(std::chrono::steady_clock::now() + delay, std::move(job));
		}
		void SubmitIn(Context ctx, Job job);

	private:
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

			void Push(Job job);
			void PushDispatched(Job job);
			void Wake();

		private:
			void Consume(ThreadPoolScheduler& scheduler) noexcept;

			std::counting_semaphore<> semaphore_{ 0z };
			std::jthread thread_{};
			std::mutex mutex_;
			std::deque<Job> queue_;
			bool return_to_pending_ = false;
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

		bool TryDispatch(Job job);
		void PriorityLoop();
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
