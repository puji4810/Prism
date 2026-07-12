#include "asyncdb.h"
#include "../src/async_runtime.h"
#include "db_impl.h"

#include <filesystem>
#include <gtest/gtest.h>

#include "coro_task.h"

using namespace prism;
using namespace prism::tests;

namespace
{
	class NoViewRandomAccessFile final: public RandomAccessFile
	{
	public:
		explicit NoViewRandomAccessFile(std::unique_ptr<RandomAccessFile> file)
		    : file_(std::move(file))
		{
		}

		Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override
		{
			return file_->Read(offset, n, result, scratch);
		}

		Result<std::size_t> ReadAt(uint64_t offset, std::span<std::byte> dst) const override
		{
			return file_->ReadAt(offset, dst);
		}

	private:
		std::unique_ptr<RandomAccessFile> file_;
	};

	class NoViewEnv final: public EnvWrapper
	{
	public:
		explicit NoViewEnv(Env* target)
		    : EnvWrapper(target)
		{
		}

		Result<std::unique_ptr<RandomAccessFile>> NewRandomAccessFile(const std::string& filename) override
		{
			auto file = target()->NewRandomAccessFile(filename);
			if (!file.has_value())
			{
				return std::unexpected(file.error());
			}
			return std::unique_ptr<RandomAccessFile>(new NoViewRandomAccessFile(std::move(file.value())));
		}
	};

	class AsyncDBTest: public ::testing::Test
	{
	protected:
		void SetUp() override
		{
			std::error_code ec;
			std::filesystem::remove_all("test_async_db", ec);
			std::filesystem::remove_all("test_async_db_left", ec);
			std::filesystem::remove_all("test_async_db_right", ec);
			std::filesystem::remove_all("test_async_db_missing_left", ec);
			std::filesystem::remove_all("test_async_db_missing_right", ec);
		}

		void TearDown() override
		{
			std::error_code ec;
			std::filesystem::remove_all("test_async_db", ec);
			std::filesystem::remove_all("test_async_db_left", ec);
			std::filesystem::remove_all("test_async_db_right", ec);
			std::filesystem::remove_all("test_async_db_missing_left", ec);
			std::filesystem::remove_all("test_async_db_missing_right", ec);
		}
	};

	Task<void> PublicOpenAcrossIndependentPathsImpl(AsyncRuntime& runtime, Options options)
	{
		auto left_db_res = co_await AsyncDB::OpenAsync(runtime, options, "test_async_db_left");
		if (!left_db_res.has_value())
		{
			ADD_FAILURE() << "Left DB open failed";
			co_return;
		}
		auto left_db = std::move(left_db_res.value());

		auto right_db_res = co_await AsyncDB::OpenAsync(runtime, options, "test_async_db_right");
		if (!right_db_res.has_value())
		{
			ADD_FAILURE() << "Right DB open failed";
			co_return;
		}
		auto right_db = std::move(right_db_res.value());

		auto left_put = co_await left_db.PutAsync(WriteOptions(), "k", "v");
		auto right_put = co_await right_db.PutAsync(WriteOptions(), "k", "v");
		EXPECT_TRUE(left_put.ok());
		EXPECT_TRUE(right_put.ok());

		auto left_get = co_await left_db.GetAsync(ReadOptions(), "k");
		auto right_get = co_await right_db.GetAsync(ReadOptions(), "k");
		if (!left_get.has_value() || !right_get.has_value())
		{
			ADD_FAILURE() << "Get failed";
			co_return;
		}
		EXPECT_EQ(left_get.value(), "v");
		EXPECT_EQ(right_get.value(), "v");

		auto left_delete = co_await left_db.DeleteAsync(WriteOptions(), "k");
		auto right_delete = co_await right_db.DeleteAsync(WriteOptions(), "k");
		EXPECT_TRUE(left_delete.ok());
		EXPECT_TRUE(right_delete.ok());

		Options missing_options;
		missing_options.create_if_missing = false;
		auto left_missing = co_await AsyncDB::OpenAsync(runtime, missing_options, "test_async_db_missing_left");
		auto right_missing = co_await AsyncDB::OpenAsync(runtime, missing_options, "test_async_db_missing_right");

		EXPECT_EQ(left_missing.has_value(), right_missing.has_value());
		EXPECT_FALSE(left_missing.has_value());
		EXPECT_FALSE(right_missing.has_value());
		if (!left_missing.has_value())
		{
			EXPECT_TRUE(left_missing.error().IsInvalidArgument());
		}
		if (!right_missing.has_value())
		{
			EXPECT_TRUE(right_missing.error().IsInvalidArgument());
		}
		co_return;
	}
}

TEST_F(AsyncDBTest, BasicPutGet)
{
	CpuThreadPool scheduler(4);
	AsyncRuntime runtime(scheduler);

	Options options;
	options.create_if_missing = true;

	auto task = [](AsyncRuntime& runtime, Options options) -> Task<void> {
		auto adb_res = co_await AsyncDB::OpenAsync(runtime, options, "test_async_db");
		if (!adb_res.has_value())
		{
			ADD_FAILURE() << "Open failed";
			co_return;
		}
		auto adb = std::move(adb_res.value());

		Status s = co_await adb.PutAsync(WriteOptions(), "k", "v");
		EXPECT_TRUE(s.ok()) << s.ToString();

		auto r = co_await adb.GetAsync(ReadOptions(), "k");
		if (!r.has_value())
		{
			ADD_FAILURE() << "Get failed";
			co_return;
		}
		EXPECT_EQ(r.value(), "v");

		Status del_status = co_await adb.DeleteAsync(WriteOptions(), "k");
		EXPECT_TRUE(del_status.ok()) << del_status.ToString();

		auto missing_result = co_await adb.GetAsync(ReadOptions(), "k");
		EXPECT_TRUE(missing_result.error().IsNotFound()) << missing_result.error().ToString();
		co_return;
	}(runtime, options);
	task.SyncWait();
}

TEST_F(AsyncDBTest, GetAsyncResumesInlineOnCompletingReadWorker)
{
	bool resumed_on_read_worker = false;
	std::optional<Task<void>> task;
	{
		CpuThreadPool scheduler(2);
		AsyncRuntime runtime(scheduler);
		Options options;
		options.create_if_missing = true;

		task.emplace([](AsyncRuntime& runtime, Options options, bool* resumed_on_read_worker) -> Task<void> {
			auto db_result = co_await AsyncDB::OpenAsync(runtime, options, "test_async_db");
			if (!db_result.has_value())
			{
				ADD_FAILURE() << "Open failed: " << db_result.error().ToString();
				co_return;
			}
			auto db = std::move(db_result.value());
			Status put = co_await db.PutAsync(WriteOptions(), "fast-key", "fast-value");
			if (!put.ok())
			{
				ADD_FAILURE() << "Put failed: " << put.ToString();
				co_return;
			}

			auto get = co_await db.GetAsync(ReadOptions(), "fast-key");
			if (!get.has_value())
			{
				ADD_FAILURE() << "Get failed: " << get.error().ToString();
				co_return;
			}
			EXPECT_EQ(get.value(), "fast-value");
			*resumed_on_read_worker = runtime.DbReadExecutor().IsCurrentWorker();
			}(runtime, options, &resumed_on_read_worker));

		task->SyncWait();
	}
	task.reset();
	EXPECT_TRUE(resumed_on_read_worker);
}

TEST_F(AsyncDBTest, GetAsyncReadsFlushedSstBlockMiss)
{
	CpuThreadPool scheduler(4);
	AsyncRuntime runtime(scheduler);

	Options options;
	options.create_if_missing = true;
	options.write_buffer_size = 8 * 1024;
	options.block_size = 256;

	auto task = [](AsyncRuntime& runtime, Options options) -> Task<void> {
		auto adb_res = co_await AsyncDB::OpenAsync(runtime, options, "test_async_db");
		if (!adb_res.has_value())
		{
			ADD_FAILURE() << "Open failed";
			co_return;
		}
		auto adb = std::move(adb_res.value());

		const std::string payload(1024, 'x');
		for (int i = 0; i < 32; ++i)
		{
			Status s = co_await adb.PutAsync(WriteOptions(), "sst_key_" + std::to_string(i), payload + std::to_string(i));
			if (!s.ok())
			{
				ADD_FAILURE() << "Put failed: " << s.ToString();
				co_return;
			}
		}

		DBImpl* impl = CompactionStateAccess::GetDBImpl(CompactionStateAccess::GetDatabase(adb));
		int file_count = 0;
		for (int level = 0; level < kNumLevels; ++level)
		{
			file_count += impl->TEST_NumLevelFiles(level);
		}
		if (file_count <= 0)
		{
			ADD_FAILURE() << "test setup should flush at least one SST file";
			co_return;
		}

		ReadOptions read_options;
		read_options.fill_cache = false;
		auto result = co_await adb.GetAsync(read_options, "sst_key_0");
		if (!result.has_value())
		{
			ADD_FAILURE() << "SST GetAsync failed: " << result.error().ToString();
			co_return;
		}
		EXPECT_EQ(result.value(), payload + "0");
		co_return;
	}(runtime, options);
	task.SyncWait();
}

TEST_F(AsyncDBTest, GetAsyncResumesAfterAsynchronousSstRead)
{
	std::optional<Task<void>> task;
	{
		// Keep the coroutine frame alive until the runtime and its workers have
		// drained. SyncWait() is released from final_suspend(), while the worker's
		// resume() call may still be unwinding.
		NoViewEnv env(Env::Default());
		CpuThreadPool scheduler(4);
		AsyncRuntime runtime(scheduler);

		Options options;
		options.create_if_missing = true;
		options.env = &env;
		options.write_buffer_size = 8 * 1024;
		options.block_size = 256;

		task.emplace([](AsyncRuntime& runtime, CpuThreadPool& scheduler, Options options) -> Task<void> {
		auto db_result = co_await AsyncDB::OpenAsync(runtime, options, "test_async_db");
		if (!db_result.has_value())
		{
			ADD_FAILURE() << "Open failed: " << db_result.error().ToString();
			co_return;
		}
		auto db = std::move(db_result.value());
		const std::string payload(1024, 'a');
		for (int i = 0; i < 32; ++i)
		{
			Status put = co_await db.PutAsync(WriteOptions(), "async_sst_" + std::to_string(i), payload);
			if (!put.ok())
			{
				ADD_FAILURE() << "Put failed: " << put.ToString();
				co_return;
			}
		}

		ReadOptions read_options;
		read_options.fill_cache = false;
		auto get = co_await db.GetAsync(read_options, "async_sst_0");
		if (!get.has_value())
		{
			ADD_FAILURE() << "Get failed: " << get.error().ToString();
			co_return;
		}
		EXPECT_EQ(get.value(), payload);
		EXPECT_TRUE(scheduler.CaptureContext().IsValid());
		EXPECT_FALSE(runtime.DbReadExecutor().IsCurrentWorker());
		co_return;
		}(runtime, scheduler, options));
		task->SyncWait();
	}
	task.reset();
}

TEST_F(AsyncDBTest, OpenAsyncFailure)
{
	CpuThreadPool scheduler(4);
	AsyncRuntime runtime(scheduler);
	Options options;
	options.create_if_missing = false;

	auto open_task = [](AsyncRuntime& runtime, Options options) -> Task<void> {
		auto db_res = co_await AsyncDB::OpenAsync(runtime, options, "/nonexistent_path/no_db");
		EXPECT_FALSE(db_res.has_value());
		co_return;
	}(runtime, options);
	open_task.SyncWait();
}

TEST_F(AsyncDBTest, DestroyBeforeAwait)
{
	CpuThreadPool scheduler(4);
	AsyncRuntime runtime(scheduler);
	Options options;
	options.create_if_missing = true;

	auto task = [](AsyncRuntime& runtime, Options options) -> Task<void> {
		std::optional<AsyncWriteOp> put_op;
		{
			auto adb_res = co_await AsyncDB::OpenAsync(runtime, options, "test_async_db");
			if (!adb_res.has_value())
			{
				ADD_FAILURE() << "Open failed";
				co_return;
			}
			auto adb = std::move(adb_res.value());
			put_op.emplace(adb.PutAsync(WriteOptions(), "destroy_key", "destroy_val"));
		}
		Status s = co_await std::move(*put_op);
		EXPECT_TRUE(s.ok()) << s.ToString();
		co_return;
	}(runtime, options);
	task.SyncWait();
}

TEST_F(AsyncDBTest, ValueHandleMoveConstruction)
{
	CpuThreadPool scheduler(4);
	AsyncRuntime runtime(scheduler);
	Options options;
	options.create_if_missing = true;

	auto task = [](AsyncRuntime& runtime, Options options) -> Task<void> {
		auto adb_res = co_await AsyncDB::OpenAsync(runtime, options, "test_async_db");
		if (!adb_res.has_value())
		{
			ADD_FAILURE() << "Open failed";
			co_return;
		}
		auto adb1 = std::move(adb_res.value());

		AsyncDB adb2(std::move(adb1));

		Status put_status = co_await adb2.PutAsync(WriteOptions(), "move_key", "move_val");
		EXPECT_TRUE(put_status.ok()) << put_status.ToString();

		auto get_result = co_await adb2.GetAsync(ReadOptions(), "move_key");
		if (!get_result.has_value())
		{
			ADD_FAILURE() << "Get failed";
			co_return;
		}
		EXPECT_EQ(get_result.value(), "move_val");
		co_return;
	}(runtime, options);
	task.SyncWait();
}

TEST_F(AsyncDBTest, PublicOpenAcrossIndependentPaths)
{
	CpuThreadPool scheduler(4);
	AsyncRuntime runtime(scheduler);
	Options options;
	options.create_if_missing = true;

	auto task = [](AsyncRuntime& runtime, Options options) -> Task<void> {
		PublicOpenAcrossIndependentPathsImpl(runtime, options).SyncWait();
		co_return;
	}(runtime, options);
	task.SyncWait();
}

TEST_F(AsyncDBTest, SnapshotReadSurvivesCallerTeardown)
{
	CpuThreadPool scheduler(4);
	AsyncRuntime runtime(scheduler);
	Options options;
	options.create_if_missing = true;

	auto task = [](AsyncRuntime& runtime, Options options) -> Task<void> {
		std::optional<AsyncGetOp> get_op;
		{
			auto adb_res = co_await AsyncDB::OpenAsync(runtime, options, "test_async_db");
			if (!adb_res.has_value())
			{
				ADD_FAILURE() << "Open failed";
				co_return;
			}
			auto adb = std::move(adb_res.value());

			co_await adb.PutAsync(WriteOptions(), "snap_key", "v1");
			Snapshot snapshot = adb.CaptureSnapshot();
			ReadOptions read_options;
			read_options.snapshot_handle = snapshot;

			co_await adb.PutAsync(WriteOptions(), "snap_key", "v2");
			get_op.emplace(adb.GetAsync(read_options, "snap_key"));
		}
		auto result = co_await std::move(*get_op);
		if (!result.has_value())
		{
			ADD_FAILURE() << "Get failed";
			co_return;
		}
		EXPECT_EQ("v1", result.value());
		co_return;
	}(runtime, options);
	task.SyncWait();
}

TEST_F(AsyncDBTest, SnapshotCopyAcrossCoroutineBoundaryKeepsViewAlive)
{
	CpuThreadPool scheduler(4);
	AsyncRuntime runtime(scheduler);
	Options options;
	options.create_if_missing = true;

	auto task = [](AsyncRuntime& runtime, Options options) -> Task<void> {
		auto adb_res = co_await AsyncDB::OpenAsync(runtime, options, "test_async_db");
		if (!adb_res.has_value())
		{
			ADD_FAILURE() << "Open failed";
			co_return;
		}
		auto adb = std::move(adb_res.value());

		co_await adb.PutAsync(WriteOptions(), "bound_key", "original");

		Snapshot snap = adb.CaptureSnapshot();
		ReadOptions ro;
		ro.snapshot_handle = snap; // copy snap by value into ro

		co_await adb.PutAsync(WriteOptions(), "bound_key", "updated");

		// Pass ro (which holds a copy of snap) into GetAsync; the internal lambda
		// captures opts = options, so the Snapshot shared_ptr survives the co_await.
		auto result = co_await adb.GetAsync(std::move(ro), "bound_key");
		if (!result.has_value())
		{
			ADD_FAILURE() << "Get failed: " << result.error().ToString();
			co_return;
		}
		EXPECT_EQ(result.value(), "original");
		co_return;
	}(runtime, options);
	task.SyncWait();
}

TEST_F(AsyncDBTest, SnapshotRejectsMovedFromHandle)
{
	CpuThreadPool scheduler(4);
	AsyncRuntime runtime(scheduler);
	Options options;
	options.create_if_missing = true;

	auto task = [](AsyncRuntime& runtime, Options options) -> Task<void> {
		auto adb_res = co_await AsyncDB::OpenAsync(runtime, options, "test_async_db");
		if (!adb_res.has_value())
		{
			ADD_FAILURE() << "Open failed";
			co_return;
		}
		auto adb = std::move(adb_res.value());

		co_await adb.PutAsync(WriteOptions(), "mf_key", "val");

		Snapshot src = adb.CaptureSnapshot();
		Snapshot moved = std::move(src); // src.state_ is now nullptr (moved-from)

		// Using the moved-from src: ResolveSnapshotSequence sees state_ == nullptr
		// and returns EmptySnapshotHandleStatus() → InvalidArgument.
		ReadOptions ro_src;
		ro_src.snapshot_handle = src;
		auto err_result = co_await adb.GetAsync(ro_src, "mf_key");
		EXPECT_FALSE(err_result.has_value());
		if (!err_result.has_value())
		{
			EXPECT_TRUE(err_result.error().IsInvalidArgument()) << err_result.error().ToString();
		}

		// Using the valid moved-to snapshot: should succeed and return the written value.
		ReadOptions ro_moved;
		ro_moved.snapshot_handle = moved;
		auto ok_result = co_await adb.GetAsync(ro_moved, "mf_key");
		if (!ok_result.has_value())
		{
			ADD_FAILURE() << "Get with valid snapshot failed: " << ok_result.error().ToString();
			co_return;
		}
		EXPECT_EQ(ok_result.value(), "val");
		co_return;
	}(runtime, options);
	task.SyncWait();
}

TEST_F(AsyncDBTest, SnapshotRejectsForeignHandle)
{
	CpuThreadPool scheduler(4);
	AsyncRuntime runtime(scheduler);
	Options options;
	options.create_if_missing = true;

	auto task = [](AsyncRuntime& runtime, Options options) -> Task<void> {
		auto left_res = co_await AsyncDB::OpenAsync(runtime, options, "test_async_db_left");
		if (!left_res.has_value())
		{
			ADD_FAILURE() << "Left DB open failed";
			co_return;
		}
		auto left_db = std::move(left_res.value());

		auto right_res = co_await AsyncDB::OpenAsync(runtime, options, "test_async_db_right");
		if (!right_res.has_value())
		{
			ADD_FAILURE() << "Right DB open failed";
			co_return;
		}
		auto right_db = std::move(right_res.value());

		co_await left_db.PutAsync(WriteOptions(), "fk", "lv");
		Snapshot left_snap = left_db.CaptureSnapshot();

		// left_snap.state_->registry points to left DB's SnapshotRegistry.
		// right DB's ResolveSnapshotSequence checks registry identity and rejects it.
		ReadOptions ro;
		ro.snapshot_handle = left_snap;
		auto result = co_await right_db.GetAsync(ro, "fk");
		EXPECT_FALSE(result.has_value());
		if (!result.has_value())
		{
			EXPECT_TRUE(result.error().IsInvalidArgument()) << result.error().ToString();
		}
		co_return;
	}(runtime, options);
	task.SyncWait();
}

TEST_F(AsyncDBTest, SnapshotDestructionAfterDBWrapperTeardownIsSafe)
{
	CpuThreadPool scheduler(4);
	AsyncRuntime runtime(scheduler);
	Options options;
	options.create_if_missing = true;

	auto task = [](AsyncRuntime& runtime, Options options) -> Task<void> {
		Snapshot snap;
		{
			auto adb_res = co_await AsyncDB::OpenAsync(runtime, options, "test_async_db");
			if (!adb_res.has_value())
			{
				ADD_FAILURE() << "Open failed";
				co_return;
			}
			auto adb = std::move(adb_res.value());
			snap = adb.CaptureSnapshot();
		} // adb goes out of scope here; the AsyncDB wrapper and underlying Database are torn down.
		  // snap.state_ still holds a shared_ptr<SnapshotRegistry>, keeping the registry alive.

		// Overwriting snap calls SnapshotState::~SnapshotState() → registry->Release().
		// This must not crash or trigger sanitizer errors even though the DB is gone.
		snap = Snapshot{ };

		SUCCEED();
		co_return;
	}(runtime, options);
	task.SyncWait();
}
