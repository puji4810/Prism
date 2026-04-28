#ifndef PRISM_ASYNC_OP_H
#define PRISM_ASYNC_OP_H

#include "scheduler.h"

#ifdef PRISM_RUNTIME_METRICS
#include "runtime_metrics.h"
#endif

#include <atomic>
#include <coroutine>
#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <utility>

namespace prism
{
	// AsyncOp<T>: A C++20 coroutine awaitable that bridges synchronous work with asynchronous execution.
	//
	// Design Philosophy:
	// - This is NOT a coroutine function (no co_await/co_return), so it creates ZERO coroutine frames.
	// - It returns an awaitable object that can be co_await'ed by user-defined Task/coroutines.
	// - Decouples Prism's async API from any specific Task implementation (users can use their own).
	//
	// Usage Pattern:
	//   AsyncOp<Result<std::string>> GetAsync(...) {
	//       return AsyncOp<Result<std::string>>(scheduler_, [this, key] {
	//           return db_->Get(...);  // Synchronous work executed in thread pool
	//       });
	//   }
	//
	//   // User coroutine:
	//   Task<void> UserCode() {
	//       auto result = co_await GetAsync(...);  // Suspends until work completes
	//   }
	//
	// Lifecycle:
	// 1. Construction: Captures scheduler and work lambda in shared State.
	//    Precondition: scheduler must remain valid until the operation completes.
	// 2. co_await: Triggers operator co_await(), returns Awaiter.
	//    AsyncOp object itself is temporary; its State is managed by Awaiter.
	// 3. Suspension: Awaiter::await_suspend() submits work to thread pool.
	//    Precondition: The awaiting coroutine must remain alive until resumption.
	// 4. Execution: Work runs on background thread, stores result in State.
	// 5. Resumption: Thread pool resumes coroutine, Awaiter::await_resume() returns result.
	//
	// Suspend/Resume Handshake (race safety):
	// There is a classic race between the worker completing and the coroutine suspending:
	//
	//   Thread A (coroutine): enters await_suspend(), submits work, then sets state to kSuspended.
	//   Thread B (worker):    completes work, then attempts to resume the coroutine.
	//
	// If Thread B runs to completion BEFORE Thread A transitions state to kSuspended,
	// calling handle.resume() before the coroutine is actually suspended is UB.
	//
	// Protocol (atomic state machine, three states):
	//   kSuspending → either kCompleted (worker wins) or kSuspended (coroutine wins)
	//
	//   await_suspend():
	//     1. Submit work.
	//     2. Try CAS: kSuspending → kSuspended.
	//        - If success: coroutine is now fully suspended; worker will call resume() later. Return true.
	//        - If fail (state is already kCompleted): worker finished first; do NOT suspend. Return false.
	//
	//   Worker lambda:
	//     1. Execute work (store result/exception).
	//     2. Try CAS: kSuspending → kCompleted.
	//        - If success: coroutine is not yet suspended; await_suspend will return false. Do NOT resume.
	//        - If fail (state is kSuspended): coroutine is suspended; call handle.resume().
	//
	// This guarantees:
	//   - Exactly-once resume (either CAS succeeds; the other path handles the "other won" case).
	//   - No resume-before-suspend (worker only resumes if coroutine has already transitioned to kSuspended).
	//   - No lost wakeup (if worker finishes first, await_suspend returns false and coroutine continues immediately).
	//
	// Thread Safety:
	// - State is shared via std::shared_ptr to ensure lifetime across thread boundaries
	// - Work execution and result storage happen on thread pool threads
	// - Coroutine resumption happens inline on the worker that completed the operation.
	//   This avoids bouncing hot-path continuations through the same executor queue.
	template <typename T>
	class AsyncOp
	{
	private:
		struct State;

	public:
		using ValueType = T;
		using Work = std::move_only_function<T()>;

		AsyncOp(IScheduler& scheduler, Work work)
		    : state_(std::make_shared<State>(&scheduler, std::move(work)))
		{
		}

		struct Awaiter
		{
			std::shared_ptr<State> state;

			bool await_ready() const noexcept { return false; }

			// Returns true to suspend the coroutine, or false if worker already finished.
			// See class-level protocol comment for the full CAS handshake.
			bool await_suspend(std::coroutine_handle<> handle) const
			{
				state->handle = handle;
#ifdef PRISM_RUNTIME_METRICS
				state->suspend_time = std::chrono::steady_clock::now();
#endif

				IScheduler* blocking_scheduler = state->scheduler->BlockingScheduler();

				blocking_scheduler->Submit([st = state] {
					try
					{
						st->value = st->work();
					}
					catch (...)
					{
						st->exception = std::current_exception();
					}
#ifdef PRISM_RUNTIME_METRICS
					{
						auto resume_time = std::chrono::steady_clock::now();
						auto delay_us = static_cast<uint64_t>(
						    std::chrono::duration_cast<std::chrono::microseconds>(resume_time - st->suspend_time)
						        .count());
						auto& rm = RuntimeMetrics::Instance();
						rm.continuation_delay_total_us.fetch_add(delay_us, std::memory_order_relaxed);
						rm.continuation_count.fetch_add(1, std::memory_order_relaxed);
					}
#endif
					auto expected = State::kSuspending;
					if (st->status.compare_exchange_strong(
					        expected, State::kCompleted, std::memory_order_acq_rel, std::memory_order_acquire))
					{
						return;
					}
					st->handle.resume();
				});

				// Coroutine tries to win the handshake: kSuspending → kSuspended.
				auto expected = State::kSuspending;
				if (state->status.compare_exchange_strong(
				        expected, State::kSuspended, std::memory_order_acq_rel, std::memory_order_acquire))
				{
					// Won: worker has not finished; suspend and let worker resume us.
					return true;
				}
				// Lost: worker already finished (kCompleted); skip suspension.
				return false;
			}

			T await_resume() const
			{
				if (state->exception)
				{
					std::rethrow_exception(state->exception);
				}
				return std::move(*state->value);
			}
		};

		Awaiter operator co_await() && noexcept { return Awaiter{ std::move(state_) }; }

	private:
		struct State
		{
			static constexpr int kSuspending = 0;
			static constexpr int kCompleted = 1;
			static constexpr int kSuspended = 2;

			State(IScheduler* sched, Work w)
			    : scheduler(sched)
			    , work(std::move(w))
			{
			}

			IScheduler* scheduler;
			Work work;
			std::optional<T> value;
			std::exception_ptr exception;
			std::coroutine_handle<> handle;
			std::atomic<int> status{ kSuspending };
#ifdef PRISM_RUNTIME_METRICS
			std::chrono::steady_clock::time_point suspend_time;
#endif
		};

		std::shared_ptr<State> state_;
	};

	// Specialization for AsyncOp<void>: Operations that don't return a value.
	// Same design as AsyncOp<T> including the identical suspend/resume handshake.
	template <>
	class AsyncOp<void>
	{
	private:
		struct State;

	public:
		using Work = std::move_only_function<void()>;

		AsyncOp(IScheduler& scheduler, Work work)
		    : state_(std::make_shared<State>(&scheduler, std::move(work)))
		{
		}

		struct Awaiter
		{
			std::shared_ptr<State> state;

			bool await_ready() const noexcept { return false; }

			bool await_suspend(std::coroutine_handle<> handle) const
			{
				state->handle = handle;
#ifdef PRISM_RUNTIME_METRICS
				state->suspend_time = std::chrono::steady_clock::now();
#endif
				IScheduler* blocking_scheduler = state->scheduler->BlockingScheduler();

				blocking_scheduler->Submit([st = state] {
					try
					{
						st->work();
					}
					catch (...)
					{
						st->exception = std::current_exception();
					}
#ifdef PRISM_RUNTIME_METRICS
					{
						auto resume_time = std::chrono::steady_clock::now();
						auto delay_us = static_cast<uint64_t>(
						    std::chrono::duration_cast<std::chrono::microseconds>(resume_time - st->suspend_time)
						        .count());
						auto& rm = RuntimeMetrics::Instance();
						rm.continuation_delay_total_us.fetch_add(delay_us, std::memory_order_relaxed);
						rm.continuation_count.fetch_add(1, std::memory_order_relaxed);
					}
#endif
					auto expected = State::kSuspending;
					if (st->status.compare_exchange_strong(
					        expected, State::kCompleted, std::memory_order_acq_rel, std::memory_order_acquire))
					{
						return;
					}
					st->handle.resume();
				});

				auto expected = State::kSuspending;
				if (state->status.compare_exchange_strong(
				        expected, State::kSuspended, std::memory_order_acq_rel, std::memory_order_acquire))
				{
					// Coroutine wins: suspend and wait for worker.
					return true;
				}
				// Worker wins: already done; don't suspend.
				return false;
			}

			void await_resume() const
			{
				if (state->exception)
				{
					std::rethrow_exception(state->exception);
				}
			}
		};

		Awaiter operator co_await() && noexcept { return Awaiter{ std::move(state_) }; }

	private:
		struct State
		{
			static constexpr int kSuspending = 0;
			static constexpr int kCompleted = 1;
			static constexpr int kSuspended = 2;

			State(IScheduler* sched, Work w)
			    : scheduler(sched)
			    , work(std::move(w))
			{
			}

			IScheduler* scheduler;
			Work work;
			std::exception_ptr exception;
			std::coroutine_handle<> handle;
			std::atomic<int> status{ kSuspending };
#ifdef PRISM_RUNTIME_METRICS
			std::chrono::steady_clock::time_point suspend_time;
#endif
		};

		std::shared_ptr<State> state_;
	};
}

#endif
