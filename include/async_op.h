#ifndef PRISM_ASYNC_OP_H
#define PRISM_ASYNC_OP_H

#include "scheduler.h"

#include <coroutine>
#include <exception>
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
	// 1. Construction: Captures scheduler and work lambda in shared State
	// 2. co_await: Triggers operator co_await(), returns Awaiter
	// 3. Suspension: Awaiter::await_suspend() submits work to thread pool
	// 4. Execution: Work runs on background thread, stores result in State
	// 5. Resumption: Thread pool resumes coroutine, Awaiter::await_resume() returns result
	//
	// Thread Safety:
	// - State is shared via std::shared_ptr to ensure lifetime across thread boundaries
	// - Work execution and result storage happen on thread pool threads
	// - Coroutine resumption happens on the same thread that completes the work
	template <typename T>
	class AsyncOp
	{
	private:
		struct State;

	public:
		using ValueType = T;
		using Work = std::function<T()>;

		// Constructs an AsyncOp that will execute `work` on `scheduler` when awaited.
		// Work is captured by value (via std::function), so lifetime is managed automatically.
		AsyncOp(ThreadPoolScheduler& scheduler, Work work)
		    : state_(std::make_shared<State>(State{ &scheduler, std::move(work) }))
		{
		}

		// Awaiter: Implements the C++20 awaitable protocol.
		// This is what the compiler interacts with when you write "co_await asyncOp".
		struct Awaiter
		{
			std::shared_ptr<State> state;

			// await_ready(): Should we suspend the coroutine?
			// Always returns false because we always need to offload work to thread pool.
			// (Future optimization: could return true if result is cached/fast-path available)
			bool await_ready() const noexcept { return false; }

			// await_suspend(): Called when coroutine suspends. This is where work gets scheduled.
			// - Submits work to thread pool
			// - When work completes, thread pool thread resumes the coroutine via handle.resume()
			// - Captures shared_ptr<State> to keep state alive until work completes
			void await_suspend(std::coroutine_handle<> handle) const
			{
				auto st = state;
				st->scheduler->Submit([st, handle] {
					try
					{
						st->value = st->work(); // Execute the actual work
					}
					catch (...)
					{
						st->exception = std::current_exception(); // Capture exceptions
					}
					handle.resume(); // Resume the coroutine on this thread pool thread
				});
			}

			// await_resume(): Called when coroutine resumes. Returns the result to caller.
			// - Rethrows exception if work threw
			// - Otherwise returns the computed value
			T await_resume() const
			{
				if (state->exception)
				{
					std::rethrow_exception(state->exception);
				}
				return std::move(*state->value);
			}
		};

		// operator co_await(): Enables "co_await asyncOp" syntax.
		// Rvalue-qualified (&&) to ensure AsyncOp is only awaited once (move-only semantics).
		Awaiter operator co_await() && noexcept { return Awaiter{ std::move(state_) }; }

	private:
		// State: Shared state between AsyncOp, Awaiter, and thread pool worker.
		// Lifetime managed by shared_ptr to survive across thread boundaries.
		struct State
		{
			ThreadPoolScheduler* scheduler; // Where to submit work
			Work work; // The actual work to execute
			std::optional<T> value; // Result storage (set by worker thread)
			std::exception_ptr exception; // Exception storage (if work throws)
		};

		std::shared_ptr<State> state_;
	};

	// Specialization for AsyncOp<void>: Operations that don't return a value.
	// Same design as AsyncOp<T>, but without value storage/return.
	template <>
	class AsyncOp<void>
	{
	private:
		struct State;

	public:
		using Work = std::function<void()>;

		AsyncOp(ThreadPoolScheduler& scheduler, Work work)
		    : state_(std::make_shared<State>(State{ &scheduler, std::move(work) }))
		{
		}

		struct Awaiter
		{
			std::shared_ptr<State> state;

			bool await_ready() const noexcept { return false; }

			void await_suspend(std::coroutine_handle<> handle) const
			{
				auto st = state;
				st->scheduler->Submit([st, handle] {
					try
					{
						st->work();
					}
					catch (...)
					{
						st->exception = std::current_exception();
					}
					handle.resume();
				});
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
			ThreadPoolScheduler* scheduler;
			Work work;
			std::exception_ptr exception;
		};

		std::shared_ptr<State> state_;
	};
}

#endif
