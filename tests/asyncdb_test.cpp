#include "asyncdb.h"

#include <coroutine>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <semaphore>
#include <stdexcept>

#include <gtest/gtest.h>

using namespace prism;

namespace
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
#include <gtest/gtest.h>
			if (handle_.promise().exception)
			{
				std::rethrow_exception(handle_.promise().exception);
			}
		}

	private:
		std::coroutine_handle<promise_type> handle_;
	};

	class AsyncDBTest: public ::testing::Test
	{
	protected:
		void SetUp() override
		{
			std::error_code ec;
			std::filesystem::remove_all("test_async_db", ec);
		}

		void TearDown() override
		{
			std::error_code ec;
			std::filesystem::remove_all("test_async_db", ec);
		}
	};
}

TEST_F(AsyncDBTest, BasicPutGet)
{
	ThreadPoolScheduler scheduler(4);

	Options options;
	options.create_if_missing = true;

	auto open = [&]() -> Task<std::unique_ptr<AsyncDB>> {
		auto db_res = co_await AsyncDB::OpenAsync(scheduler, options, "test_async_db");
		if (!db_res.has_value())
		{
			throw std::runtime_error(db_res.error().ToString());
		}
		co_return std::move(db_res.value());
	}();

	auto adb = open.SyncWait();

	auto put = [&]() -> Task<Status> { co_return co_await adb->PutAsync(WriteOptions(), "k", "v"); }();

	Status s = put.SyncWait();
	EXPECT_TRUE(s.ok()) << s.ToString();

	auto get = [&]() -> Task<Result<std::string>> { co_return co_await adb->GetAsync(ReadOptions(), "k"); }();

	auto r = get.SyncWait();
	ASSERT_TRUE(r.has_value()) << r.error().ToString();
	EXPECT_EQ(r.value(), "v");
}

int main(int argc, char** argv)
{
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
