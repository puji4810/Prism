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
	template <typename T>
	class AsyncOp
	{
	private:
		struct State;

	public:
		using ValueType = T;
		using Work = std::function<T()>;

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
						st->value = st->work();
					}
					catch (...)
					{
						st->exception = std::current_exception();
					}
					handle.resume();
				});
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
			ThreadPoolScheduler* scheduler;
			Work work;
			std::optional<T> value;
			std::exception_ptr exception;
		};

		std::shared_ptr<State> state_;
	};

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
