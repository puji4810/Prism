#ifndef PRISM_TESTS_CORO_TASK_H
#define PRISM_TESTS_CORO_TASK_H

#include <chrono>
#include <coroutine>
#include <optional>
#include <semaphore>
#include <stdexcept>

namespace prism::tests
{

	template <typename T>
	class Task
	{
	public:
		struct promise_type
		{
			std::binary_semaphore done{ 0 };
			std::optional<T> value;
			std::exception_ptr exception;

			Task get_return_object() { return Task(std::coroutine_handle<promise_type>::from_promise(*this)); }
			std::suspend_never initial_suspend() noexcept { return {}; }
			auto final_suspend() noexcept
			{
				struct Awaiter
				{
					promise_type* promise;
					bool await_ready() noexcept { return false; }
					void await_suspend(std::coroutine_handle<>) noexcept { promise->done.release(); }
					void await_resume() noexcept {}
				};
				return Awaiter{ this };
			}
			void unhandled_exception() { exception = std::current_exception(); }
			void return_value(T v) { value = std::move(v); }
		};

		explicit Task(std::coroutine_handle<promise_type> handle)
		    : handle_(handle)
		{
		}

		Task(Task&& rhs) noexcept
		    : handle_(rhs.handle_)
		{
			rhs.handle_ = {};
		}

		Task& operator=(Task&& rhs) noexcept
		{
			if (this == &rhs)
			{
				return *this;
			}
			if (handle_)
			{
				handle_.destroy();
			}
			handle_ = rhs.handle_;
			rhs.handle_ = {};
			return *this;
		}

		Task(const Task&) = delete;
		Task& operator=(const Task&) = delete;

		~Task()
		{
			if (handle_)
			{
				handle_.destroy();
			}
		}

		T SyncWait()
		{
			handle_.promise().done.acquire();
			if (handle_.promise().exception)
			{
				std::rethrow_exception(handle_.promise().exception);
			}
			return std::move(*handle_.promise().value);
		}

		template <typename Rep, typename Period>
		std::optional<T> SyncWaitFor(std::chrono::duration<Rep, Period> timeout)
		{
			if (!handle_.promise().done.try_acquire_for(timeout))
			{
				return std::nullopt; // Timeout occurred
			}
			if (handle_.promise().exception)
			{
				std::rethrow_exception(handle_.promise().exception);
			}
			return std::move(*handle_.promise().value);
		}

	private:
		std::coroutine_handle<promise_type> handle_;
	};

	template <>
	class Task<void>
	{
	public:
		struct promise_type
		{
			std::binary_semaphore done{ 0 };
			std::exception_ptr exception;

			Task get_return_object() { return Task(std::coroutine_handle<promise_type>::from_promise(*this)); }
			std::suspend_never initial_suspend() noexcept { return {}; }
			auto final_suspend() noexcept
			{
				struct Awaiter
				{
					promise_type* promise;
					bool await_ready() noexcept { return false; }
					void await_suspend(std::coroutine_handle<>) noexcept { promise->done.release(); }
					void await_resume() noexcept {}
				};
				return Awaiter{ this };
			}
			void unhandled_exception() { exception = std::current_exception(); }
			void return_void() {}
		};

		explicit Task(std::coroutine_handle<promise_type> handle)
		    : handle_(handle)
		{
		}

		Task(Task&& rhs) noexcept
		    : handle_(rhs.handle_)
		{
			rhs.handle_ = {};
		}

		Task& operator=(Task&& rhs) noexcept
		{
			if (this == &rhs)
			{
				return *this;
			}
			if (handle_)
			{
				handle_.destroy();
			}
			handle_ = rhs.handle_;
			rhs.handle_ = {};
			return *this;
		}

		Task(const Task&) = delete;
		Task& operator=(const Task&) = delete;

		~Task()
		{
			if (handle_)
			{
				handle_.destroy();
			}
		}

		void SyncWait()
		{
			handle_.promise().done.acquire();
			if (handle_.promise().exception)
			{
				std::rethrow_exception(handle_.promise().exception);
			}
		}

		template <typename Rep, typename Period>
		bool SyncWaitFor(std::chrono::duration<Rep, Period> timeout)
		{
			if (!handle_.promise().done.try_acquire_for(timeout))
			{
				return false; // Timeout occurred
			}
			if (handle_.promise().exception)
			{
				std::rethrow_exception(handle_.promise().exception);
			}
			return true; // Success
		}

	private:
		std::coroutine_handle<promise_type> handle_;
	};

} // namespace prism::tests

#endif // PRISM_TESTS_CORO_TASK_H
