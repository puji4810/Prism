#include "asyncdb.h"

#include <coroutine>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <semaphore>
#include <stdexcept>

#include "coro_task.h"

using namespace prism;
using namespace prism::tests;

namespace
{

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

TEST_F(AsyncDBTest, OpenAsyncFailure)
{
	ThreadPoolScheduler scheduler(4);
	Options options;
	options.create_if_missing = false; // do NOT create - path doesn't exist so Open must fail

	// Use a path that cannot be created to force DB::Open failure.
	auto open_task = [&]() -> Task<void> {
		auto db_res = co_await AsyncDB::OpenAsync(scheduler, options, "/nonexistent_path/no_db");
		EXPECT_FALSE(db_res.has_value()) << "Expected Open to fail on nonexistent path without create_if_missing";
	}();
	open_task.SyncWait();
}

TEST_F(AsyncDBTest, DestroyBeforeAwait)
{
	ThreadPoolScheduler scheduler(4);
	Options options;
	options.create_if_missing = true;

	// Obtain an AsyncOp from PutAsync, then destroy the AsyncDB before awaiting.
	// With shared_ptr capture the lambda keeps DB alive - must not UAF.
	AsyncOp<Status> put_op = [&]() {
		auto open_task = [&]() -> Task<std::unique_ptr<AsyncDB>> {
			auto db_res = co_await AsyncDB::OpenAsync(scheduler, options, "test_async_db");
			if (!db_res.has_value())
			{
				throw std::runtime_error(db_res.error().ToString());
			}
			co_return std::move(db_res.value());
		}();
		auto adb = open_task.SyncWait();

		// Capture AsyncOp before adb is destroyed
		auto op = adb->PutAsync(WriteOptions(), "destroy_key", "destroy_val");
		// adb goes out of scope here -> AsyncDB destroyed
		return op;
	}();

	// Await after AsyncDB already destroyed - DB kept alive by shared_ptr in lambda
	auto task = [&]() -> Task<void> {
		Status s = co_await std::move(put_op);
		EXPECT_TRUE(s.ok()) << s.ToString();
	}();
	task.SyncWait();
}

int main(int argc, char** argv)
{
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
