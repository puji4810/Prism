#include "asyncdb.h"

#include <filesystem>
#include <gtest/gtest.h>
#include <stdexcept>

#include "coro_task.h"

using namespace prism;
using namespace prism::tests;

namespace
{
	Task<Result<AsyncDB>> OpenAsyncViaLegacyDBOpen(ThreadPoolScheduler& scheduler, const Options& options, const std::string& path)
	{
		auto db_res = DB::Open(options, path);
		if (!db_res.has_value())
		{
			co_return std::unexpected(db_res.error());
		}

		co_return AsyncDB(scheduler, std::shared_ptr<DB>(std::move(db_res.value())));
	}

	Task<AsyncDB> OpenValueHandleAsyncDB(ThreadPoolScheduler& scheduler, const Options& options, const std::string& path)
	{
		auto db_res = co_await AsyncDB::OpenAsync(scheduler, options, path);
		if (!db_res.has_value())
		{
			throw std::runtime_error(db_res.error().ToString());
		}

		co_return std::move(db_res.value());
	}

	class AsyncDBTest: public ::testing::Test
	{
	protected:
		void SetUp() override
		{
			std::error_code ec;
			std::filesystem::remove_all("test_async_db", ec);
			std::filesystem::remove_all("test_async_db_legacy", ec);
			std::filesystem::remove_all("test_async_db_value", ec);
			std::filesystem::remove_all("test_async_db_missing_legacy", ec);
			std::filesystem::remove_all("test_async_db_missing_value", ec);
		}

		void TearDown() override
		{
			std::error_code ec;
			std::filesystem::remove_all("test_async_db", ec);
			std::filesystem::remove_all("test_async_db_legacy", ec);
			std::filesystem::remove_all("test_async_db_value", ec);
			std::filesystem::remove_all("test_async_db_missing_legacy", ec);
			std::filesystem::remove_all("test_async_db_missing_value", ec);
		}
	};

}

TEST_F(AsyncDBTest, BasicPutGet)
{
	ThreadPoolScheduler scheduler(4);

	Options options;
	options.create_if_missing = true;

	auto adb = OpenValueHandleAsyncDB(scheduler, options, "test_async_db").SyncWait();

	auto put = [&]() -> Task<Status> { co_return co_await adb.PutAsync(WriteOptions(), "k", "v"); }();

	Status s = put.SyncWait();
	EXPECT_TRUE(s.ok()) << s.ToString();

	auto get = [&]() -> Task<Result<std::string>> { co_return co_await adb.GetAsync(ReadOptions(), "k"); }();

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
		auto adb = OpenValueHandleAsyncDB(scheduler, options, "test_async_db").SyncWait();

		// Capture AsyncOp before adb is destroyed
		auto op = adb.PutAsync(WriteOptions(), "destroy_key", "destroy_val");
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

// ValueHandleMoveConstruction: A moved-to AsyncDB handle works correctly,
// and a moved-from handle is safely destructible (no double-free, no UAF).
TEST_F(AsyncDBTest, ValueHandleMoveConstruction)
{
	ThreadPoolScheduler scheduler(4);
	Options options;
	options.create_if_missing = true;

	auto adb1 = OpenValueHandleAsyncDB(scheduler, options, "test_async_db").SyncWait();

	// Move-construct a second AsyncDB from the first
	AsyncDB adb2(std::move(adb1));
	// adb1 now holds moved-from state (db_ = nullptr); adb2 owns the DB

	// Verify moved-to handle works (Put/Get)
	auto put_task = [&]() -> Task<Status> { co_return co_await adb2.PutAsync(WriteOptions(), "move_key", "move_val"); }();
	Status put_status = put_task.SyncWait();
	EXPECT_TRUE(put_status.ok()) << put_status.ToString();

	auto get_task = [&]() -> Task<Result<std::string>> { co_return co_await adb2.GetAsync(ReadOptions(), "move_key"); }();
	auto get_result = get_task.SyncWait();
	ASSERT_TRUE(get_result.has_value()) << get_result.error().ToString();
	EXPECT_EQ(get_result.value(), "move_val");

	// adb1 is moved-from (db_ = nullptr) and will be destroyed at end of scope
	// This must not crash or double-free - the moved-from AsyncDB is safely destructible

	// Verify adb2 still works after adb1 is moved-from
	auto get_task2 = [&]() -> Task<Result<std::string>> { co_return co_await adb2.GetAsync(ReadOptions(), "move_key"); }();
	auto get_result2 = get_task2.SyncWait();
	ASSERT_TRUE(get_result2.has_value()) << get_result2.error().ToString();
	EXPECT_EQ(get_result2.value(), "move_val");
}

TEST_F(AsyncDBTest, LegacyAndValueHandleOpenParity)
{
	ThreadPoolScheduler scheduler(4);
	Options options;
	options.create_if_missing = true;

	auto legacy_open = OpenAsyncViaLegacyDBOpen(scheduler, options, "test_async_db_legacy").SyncWait();
	auto value_db = OpenValueHandleAsyncDB(scheduler, options, "test_async_db_value").SyncWait();

	ASSERT_TRUE(legacy_open.has_value()) << legacy_open.error().ToString();
	auto legacy_db = std::move(legacy_open.value());

	auto legacy_put = [&]() -> Task<Status> { co_return co_await legacy_db.PutAsync(WriteOptions(), "k", "v"); }().SyncWait();
	auto value_put = [&]() -> Task<Status> { co_return co_await value_db.PutAsync(WriteOptions(), "k", "v"); }().SyncWait();
	EXPECT_EQ(legacy_put.ok(), value_put.ok());

	auto legacy_get = [&]() -> Task<Result<std::string>> { co_return co_await legacy_db.GetAsync(ReadOptions(), "k"); }().SyncWait();
	auto value_get = [&]() -> Task<Result<std::string>> { co_return co_await value_db.GetAsync(ReadOptions(), "k"); }().SyncWait();
	ASSERT_TRUE(legacy_get.has_value()) << legacy_get.error().ToString();
	ASSERT_TRUE(value_get.has_value()) << value_get.error().ToString();
	EXPECT_EQ(legacy_get.value(), value_get.value());

	auto legacy_delete = [&]() -> Task<Status> { co_return co_await legacy_db.DeleteAsync(WriteOptions(), "k"); }().SyncWait();
	auto value_delete = [&]() -> Task<Status> { co_return co_await value_db.DeleteAsync(WriteOptions(), "k"); }().SyncWait();
	EXPECT_EQ(legacy_delete.ok(), value_delete.ok());

	Options missing_options;
	missing_options.create_if_missing = false;
	auto legacy_missing = OpenAsyncViaLegacyDBOpen(scheduler, missing_options, "test_async_db_missing_legacy").SyncWait();
	auto value_missing = [&]() -> Task<Result<AsyncDB>> {
		co_return co_await AsyncDB::OpenAsync(scheduler, missing_options, "test_async_db_missing_value");
	}()
	                                  .SyncWait();
	EXPECT_EQ(legacy_missing.has_value(), value_missing.has_value());
	ASSERT_FALSE(legacy_missing.has_value());
	ASSERT_FALSE(value_missing.has_value());
	EXPECT_EQ(legacy_missing.error().IsInvalidArgument(), value_missing.error().IsInvalidArgument());
	EXPECT_NE(legacy_missing.error().ToString().find("does not exist (create_if_missing is false)"), std::string::npos);
	EXPECT_NE(value_missing.error().ToString().find("does not exist (create_if_missing is false)"), std::string::npos);
}
