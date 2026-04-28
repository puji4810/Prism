#include "db.h"
#include "db_impl.h"
#include "write_batch.h"
#include "env.h"

#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <thread>
#include <vector>

using namespace prism;

namespace
{
	// Characters used for large value generation
	const char kValueChar = 'X';

	bool WaitUntil(const std::function<bool()>& condition, std::chrono::milliseconds timeout)
	{
		const auto deadline = std::chrono::steady_clock::now() + timeout;
		while (std::chrono::steady_clock::now() < deadline)
		{
			if (condition())
			{
				return true;
			}
			std::this_thread::yield();
		}
		return condition();
	}
}

// ===========================================================================
// TODO(wal-rotation): Test coverage for same-sync/same-epoch group boundaries
// - mixed sync batching prohibition + WAL rotation boundaries
// - same-sync/same-epoch group selection without rotation crossings
// - grouped append/sync fanout + sequence ordering preservation
// ===========================================================================

// Test fixture for DB tests
class DBTest: public ::testing::Test
{
protected:
	void SetUp() override
	{
		// Clean up any existing test databases before each test
		std::error_code ec;
		std::filesystem::remove_all("test_db", ec);
		std::filesystem::remove_all("test_db_large", ec);
		std::filesystem::remove_all("test_db_multi_large", ec);
		std::filesystem::remove_all("test_db_open_path", ec);
		std::filesystem::remove_all("test_db_missing_database_path", ec);
	}

	void TearDown() override
	{
		// Clean up after each test
		std::error_code ec;
		std::filesystem::remove_all("test_db", ec);
		std::filesystem::remove_all("test_db_large", ec);
		std::filesystem::remove_all("test_db_multi_large", ec);
		std::filesystem::remove_all("test_db_open_path", ec);
		std::filesystem::remove_all("test_db_missing_database_path", ec);
	}
};

TEST_F(DBTest, BasicPutGetDelete)
{
	auto res = Database::Open("test_db");
	ASSERT_TRUE(res.has_value());

	auto db = std::move(res.value());

	// Put
	Status s1 = db.Put("key1", "value1");
	EXPECT_TRUE(s1.ok()) << "Put should succeed: " << s1.ToString();

	// Get
	std::string value;
	auto s2 = db.Get("key1");
	ASSERT_TRUE(s2.has_value()) << "Get should succeed";
	value = s2.value();
	EXPECT_EQ("value1", value) << "Value should match";

	// Delete
	Status s3 = db.Delete("key1");
	EXPECT_TRUE(s3.ok()) << "Delete should succeed: " << s3.ToString();

	// Get after delete
	std::string value2;
	auto s4 = db.Get("key1");
	EXPECT_TRUE(s4.error().IsNotFound()) << "Get should return NotFound after delete";
}

TEST_F(DBTest, DatabaseHandleBasicPutGetDelete)
{
	auto res = Database::Open("test_db");
	ASSERT_TRUE(res.has_value());

	auto db = std::move(res.value());

	Status s1 = db.Put("key1", "value1");
	EXPECT_TRUE(s1.ok()) << "Put should succeed: " << s1.ToString();

	auto s2 = db.Get("key1");
	ASSERT_TRUE(s2.has_value()) << "Get should succeed";
	EXPECT_EQ("value1", s2.value()) << "Value should match";

	Status s3 = db.Delete("key1");
	EXPECT_TRUE(s3.ok()) << "Delete should succeed: " << s3.ToString();

	auto s4 = db.Get("key1");
	EXPECT_TRUE(s4.error().IsNotFound()) << "Get should return NotFound after delete";
}

// Characterization baseline for engine-facing open expectations.
TEST_F(DBTest, DatabaseHandleOpenBaseline)
{
	Options options;
	options.create_if_missing = true;

	auto res = Database::Open(options, "test_db");
	ASSERT_TRUE(res.has_value()) << res.error().ToString();

	auto db = std::move(res.value());
	ASSERT_TRUE(db.Put("engine_baseline_key", "engine_baseline_value").ok());

	auto get_result = db.Get("engine_baseline_key");
	ASSERT_TRUE(get_result.has_value()) << get_result.error().ToString();
	EXPECT_EQ(get_result.value(), "engine_baseline_value");
}

TEST_F(DBTest, BatchWrite)
{
	auto res = Database::Open("test_db");
	ASSERT_TRUE(res.has_value());

	auto db = std::move(res.value());

	WriteBatch batch;
	batch.Put("batch_key1", "batch_value1");
	batch.Put("batch_key2", "batch_value2");
	batch.Delete("key1");

	Status s = db.Write(batch);
	EXPECT_TRUE(s.ok()) << "Batch write should succeed: " << s.ToString();

	auto s1 = db.Get("batch_key1");
	ASSERT_TRUE(s1.has_value());
	EXPECT_EQ("batch_value1", s1.value());

	auto s2 = db.Get("batch_key2");
	ASSERT_TRUE(s2.has_value());
	EXPECT_EQ("batch_value2", s2.value());
}

TEST_F(DBTest, Recovery)
{
	{
		auto res = Database::Open("test_db");
		ASSERT_TRUE(res.has_value());

		auto db = std::move(res.value());
		Status s = db.Put("persistent_key", "persistent_value");
		ASSERT_TRUE(s.ok()) << s.ToString();
	}

	{
		auto res = Database::Open("test_db");
		ASSERT_TRUE(res.has_value());

		auto db = std::move(res.value());
		auto s = db.Get("persistent_key");
		ASSERT_TRUE(s.has_value());
		EXPECT_EQ("persistent_value", s.value()) << "Recovered value should match";
	}
}

TEST_F(DBTest, DatabaseHandleRecovery)
{
	{
		auto res = Database::Open("test_db");
		ASSERT_TRUE(res.has_value());

		auto db = std::move(res.value());
		Status s = db.Put("persistent_key", "persistent_value");
		ASSERT_TRUE(s.ok()) << s.ToString();
	}

	{
		auto res = Database::Open("test_db");
		ASSERT_TRUE(res.has_value());

		auto db = std::move(res.value());
		auto s = db.Get("persistent_key");
		ASSERT_TRUE(s.has_value());
		EXPECT_EQ("persistent_value", s.value()) << "Recovered value should match";
	}
}

TEST_F(DBTest, LargeValueFragmentation)
{
	// Create a large value that will require fragmentation (> 32KB)
	std::string large_value(40000, 'X'); // 40KB of 'X'

	{
		auto res = Database::Open("test_db_large");
		ASSERT_TRUE(res.has_value());
		auto db = std::move(res.value());

		Status s1 = db.Put("large_key", large_value);
		ASSERT_TRUE(s1.ok()) << "Put large value should succeed: " << s1.ToString();

		auto s2 = db.Get("large_key");
		ASSERT_TRUE(s2.has_value());
		EXPECT_EQ(large_value, s2.value()) << "Large value should match after fragmentation";
	}

	{
		auto res = Database::Open("test_db_large");
		ASSERT_TRUE(res.has_value());
		auto db = std::move(res.value());
		auto s3 = db.Get("large_key");
		ASSERT_TRUE(s3.has_value());
		EXPECT_EQ(large_value, s3.value()) << "Recovered large value should match";
	}
}

TEST_F(DBTest, MultipleLargeRecords)
{
	auto res = Database::Open("test_db_multi_large");
	ASSERT_TRUE(res.has_value());

	auto db = std::move(res.value());

	// Write multiple large values
	std::string value1(50000, 'A'); // 50KB
	std::string value2(35000, 'B'); // 35KB
	std::string value3(45000, 'C'); // 45KB

	Status s1 = db.Put("key1", value1);
	Status s2 = db.Put("key2", value2);
	Status s3 = db.Put("key3", value3);

	ASSERT_TRUE(s1.ok()) << s1.ToString();
	ASSERT_TRUE(s2.ok()) << s2.ToString();
	ASSERT_TRUE(s3.ok()) << s3.ToString();

	// Verify all values
	auto r1 = db.Get("key1");
	auto r2 = db.Get("key2");
	auto r3 = db.Get("key3");

	ASSERT_TRUE(r1.has_value());
	ASSERT_TRUE(r2.has_value());
	ASSERT_TRUE(r3.has_value());

	EXPECT_EQ(value1, r1.value());
	EXPECT_EQ(value2, r2.value());
	EXPECT_EQ(value3, r3.value());
}

TEST_F(DBTest, Iterator)
{
	Options options;
	options.create_if_missing = true;
	options.write_buffer_size = 256;
	auto res = Database::Open(options, "test_db");
	ASSERT_TRUE(res.has_value());

	auto db = std::move(res.value());

	ASSERT_TRUE(db.Put("b", "2").ok());
	ASSERT_TRUE(db.Put("a", "1").ok());
	ASSERT_TRUE(db.Put("c", "3").ok());
	ASSERT_TRUE(db.Delete("b").ok());

	ReadOptions ro;
	std::unique_ptr<Iterator> it = db.NewIterator(ro);
	it->SeekToFirst();

	ASSERT_TRUE(it->Valid());
	EXPECT_EQ(it->key().ToString(), "a");
	EXPECT_EQ(it->value().ToString(), "1");

	it->Next();
	ASSERT_TRUE(it->Valid());
	EXPECT_EQ(it->key().ToString(), "c");
	EXPECT_EQ(it->value().ToString(), "3");

	it->Next();
	EXPECT_FALSE(it->Valid());
	EXPECT_TRUE(it->status().ok()) << it->status().ToString();
}

TEST_F(DBTest, ThreadSafeConcurrentPutGet)
{
	auto res = Database::Open("test_db");
	ASSERT_TRUE(res.has_value());
	auto db = std::move(res.value());

	constexpr int kWriterThreads = 4;
	constexpr int kReaderThreads = 4;
	constexpr int kKeysPerWriter = 200;

	std::atomic<bool> start{ false };
	std::atomic<bool> writers_done{ false };

	auto key_for = [](int writer, int i) { return "key_" + std::to_string(writer) + "_" + std::to_string(i); };
	auto value_for = [](int writer, int i) { return "value_" + std::to_string(writer) + "_" + std::to_string(i); };

	std::vector<std::thread> threads;
	threads.reserve(kWriterThreads + kReaderThreads);

	for (int t = 0; t < kWriterThreads; ++t)
	{
		threads.emplace_back([&, t] {
			while (!start.load(std::memory_order_acquire))
			{
			}
			for (int i = 0; i < kKeysPerWriter; ++i)
			{
				ASSERT_TRUE(db.Put(key_for(t, i), value_for(t, i)).ok());
			}
		});
	}

	for (int t = 0; t < kReaderThreads; ++t)
	{
		threads.emplace_back([&] {
			while (!start.load(std::memory_order_acquire))
			{
			}
			while (!writers_done.load(std::memory_order_acquire))
			{
				for (int w = 0; w < kWriterThreads; ++w)
				{
					for (int i = 0; i < kKeysPerWriter; i += 17)
					{
						auto r = db.Get(key_for(w, i));
						if (r.has_value())
						{
							ASSERT_EQ(r.value(), value_for(w, i));
						}
						else
						{
							ASSERT_TRUE(r.error().IsNotFound()) << r.error().ToString();
						}
					}
				}
			}
		});
	}

	start.store(true, std::memory_order_release);

	for (int i = 0; i < kWriterThreads; ++i)
	{
		threads[i].join();
	}
	writers_done.store(true, std::memory_order_release);
	for (int i = kWriterThreads; i < kWriterThreads + kReaderThreads; ++i)
	{
		threads[i].join();
	}

	for (int w = 0; w < kWriterThreads; ++w)
	{
		for (int i = 0; i < kKeysPerWriter; ++i)
		{
			auto r = db.Get(key_for(w, i));
			ASSERT_TRUE(r.has_value());
			ASSERT_EQ(r.value(), value_for(w, i));
		}
	}
}

TEST_F(DBTest, SnapshotReadRemainsStableAcrossWrites)
{
	auto res = Database::Open("test_db");
	ASSERT_TRUE(res.has_value());
	auto db = std::move(res.value());

	ASSERT_TRUE(db.Put("k1", "v1").ok());

	Snapshot snap = db.CaptureSnapshot();

	ASSERT_TRUE(db.Put("k1", "v2").ok());

	ReadOptions snap_opts;
	snap_opts.snapshot_handle = snap;
	auto snap_result = db.Get(snap_opts, "k1");
	ASSERT_TRUE(snap_result.has_value());
	EXPECT_EQ("v1", snap_result.value()) << "Snapshot read should return value at snapshot time";

	auto current_result = db.Get("k1");
	ASSERT_TRUE(current_result.has_value());
	EXPECT_EQ("v2", current_result.value()) << "Current read should return latest value";

	ASSERT_TRUE(db.Delete("k1").ok());

	snap_result = db.Get(snap_opts, "k1");
	ASSERT_TRUE(snap_result.has_value());
	EXPECT_EQ("v1", snap_result.value()) << "Snapshot read should still return v1 after delete";

	current_result = db.Get("k1");
	EXPECT_TRUE(current_result.error().IsNotFound()) << "Current read should return NotFound after delete";
}

TEST_F(DBTest, SnapshotIteratorSeesStableView)
{
	auto res = Database::Open("test_db");
	ASSERT_TRUE(res.has_value());
	auto db = std::move(res.value());

	ASSERT_TRUE(db.Put("a", "1").ok());
	ASSERT_TRUE(db.Put("b", "2").ok());
	ASSERT_TRUE(db.Put("c", "3").ok());

	Snapshot snap = db.CaptureSnapshot();

	ASSERT_TRUE(db.Delete("b").ok());
	ASSERT_TRUE(db.Put("c", "33").ok());
	ASSERT_TRUE(db.Put("d", "4").ok());

	ReadOptions snap_opts;
	snap_opts.snapshot_handle = snap;
	std::unique_ptr<Iterator> it = db.NewIterator(snap_opts);

	it->SeekToFirst();
	ASSERT_TRUE(it->Valid());
	EXPECT_EQ(it->key().ToString(), "a");
	EXPECT_EQ(it->value().ToString(), "1");

	it->Next();
	ASSERT_TRUE(it->Valid());
	EXPECT_EQ(it->key().ToString(), "b") << "Iterator should see deleted key 'b'";
	EXPECT_EQ(it->value().ToString(), "2");

	it->Next();
	ASSERT_TRUE(it->Valid());
	EXPECT_EQ(it->key().ToString(), "c");
	EXPECT_EQ(it->value().ToString(), "3") << "Iterator should see original value for 'c'";

	it->Next();
	EXPECT_FALSE(it->Valid()) << "Iterator should not see key 'd' added after snapshot";
	EXPECT_TRUE(it->status().ok()) << it->status().ToString();
}

TEST_F(DBTest, SnapshotReadOptionsCopyKeepsViewAlive)
{
	auto res = Database::Open("test_db");
	ASSERT_TRUE(res.has_value());
	auto db = std::move(res.value());

	ASSERT_TRUE(db.Put("copy_key", "before").ok());

	ReadOptions copied_options;
	{
		Snapshot snapshot = db.CaptureSnapshot();
		ReadOptions original_options;
		original_options.snapshot_handle = snapshot;
		copied_options = original_options;
	}

	ASSERT_TRUE(db.Put("copy_key", "after").ok());

	auto snapshot_result = db.Get(copied_options, "copy_key");
	ASSERT_TRUE(snapshot_result.has_value()) << snapshot_result.error().ToString();
	EXPECT_EQ("before", snapshot_result.value());
}

TEST_F(DBTest, SnapshotRejectsCrossDatabaseUse)
{
	auto left_result = Database::Open("test_db");
	ASSERT_TRUE(left_result.has_value()) << left_result.error().ToString();
	auto left_db = std::move(left_result.value());

	auto right_result = Database::Open("test_db_large");
	ASSERT_TRUE(right_result.has_value()) << right_result.error().ToString();
	auto right_db = std::move(right_result.value());

	ASSERT_TRUE(left_db.Put("shared_key", "left_value").ok());
	ASSERT_TRUE(right_db.Put("shared_key", "right_value").ok());

	ReadOptions wrong_options;
	wrong_options.snapshot_handle = left_db.CaptureSnapshot();

	auto get_result = right_db.Get(wrong_options, "shared_key");
	ASSERT_FALSE(get_result.has_value());
	EXPECT_TRUE(get_result.error().IsInvalidArgument()) << get_result.error().ToString();

	auto iter = right_db.NewIterator(wrong_options);
	ASSERT_NE(iter, nullptr);
	EXPECT_FALSE(iter->Valid());
	EXPECT_TRUE(iter->status().IsInvalidArgument()) << iter->status().ToString();
}

TEST_F(DBTest, SnapshotDuplicateSequenceHandlesReleaseIndependently)
{
	Options options;
	options.create_if_missing = true;

	auto open_result = DBImpl::OpenInternal(options, "test_db");
	ASSERT_TRUE(open_result.has_value()) << open_result.error().ToString();
	auto db = std::move(open_result.value());

	ASSERT_TRUE(db->Put("dup_key", "v1").ok());

	Snapshot first = db->CaptureSnapshot();
	Snapshot second = db->CaptureSnapshot();
	EXPECT_EQ(2U, db->TEST_ActiveSnapshotCount());

	auto oldest_sequence = db->GetOldestLiveSnapshotSequence();
	ASSERT_TRUE(oldest_sequence.has_value());

	ASSERT_TRUE(db->Put("dup_key", "v2").ok());

	ReadOptions first_options;
	first_options.snapshot_handle = first;
	auto first_result = db->Get(first_options, "dup_key");
	ASSERT_TRUE(first_result.has_value()) << first_result.error().ToString();
	EXPECT_EQ("v1", first_result.value());

	ReadOptions second_options;
	second_options.snapshot_handle = second;
	auto second_result = db->Get(second_options, "dup_key");
	ASSERT_TRUE(second_result.has_value()) << second_result.error().ToString();
	EXPECT_EQ("v1", second_result.value());

	first = Snapshot();
	first_options.snapshot_handle.reset();
	EXPECT_EQ(1U, db->TEST_ActiveSnapshotCount());
	EXPECT_EQ(oldest_sequence, db->GetOldestLiveSnapshotSequence());

	second_result = db->Get(second_options, "dup_key");
	ASSERT_TRUE(second_result.has_value()) << second_result.error().ToString();
	EXPECT_EQ("v1", second_result.value());

	second = Snapshot();
	second_options.snapshot_handle.reset();
	EXPECT_EQ(0U, db->TEST_ActiveSnapshotCount());
	EXPECT_FALSE(db->GetOldestLiveSnapshotSequence().has_value());
}

TEST_F(DBTest, SnapshotRejectsDefaultConstructedHandle)
{
	auto open_result = Database::Open("test_db");
	ASSERT_TRUE(open_result.has_value()) << open_result.error().ToString();
	auto db = std::move(open_result.value());

	ASSERT_TRUE(db.Put("default_key", "value").ok());

	ReadOptions read_options;
	read_options.snapshot_handle = Snapshot();

	auto get_result = db.Get(read_options, "default_key");
	ASSERT_FALSE(get_result.has_value());
	EXPECT_TRUE(get_result.error().IsInvalidArgument()) << get_result.error().ToString();

	auto iter = db.NewIterator(read_options);
	ASSERT_NE(iter, nullptr);
	EXPECT_FALSE(iter->Valid());
	EXPECT_TRUE(iter->status().IsInvalidArgument()) << iter->status().ToString();
}

TEST_F(DBTest, SnapshotRejectsMovedFromHandle)
{
	auto open_result = Database::Open("test_db");
	ASSERT_TRUE(open_result.has_value()) << open_result.error().ToString();
	auto db = std::move(open_result.value());

	ASSERT_TRUE(db.Put("move_key", "before").ok());

	Snapshot source = db.CaptureSnapshot();
	Snapshot valid = std::move(source);

	ASSERT_TRUE(db.Put("move_key", "after").ok());

	ReadOptions moved_from_options;
	moved_from_options.snapshot_handle = source;
	auto moved_from_result = db.Get(moved_from_options, "move_key");
	ASSERT_FALSE(moved_from_result.has_value());
	EXPECT_TRUE(moved_from_result.error().IsInvalidArgument()) << moved_from_result.error().ToString();

	ReadOptions valid_options;
	valid_options.snapshot_handle = valid;
	auto valid_result = db.Get(valid_options, "move_key");
	ASSERT_TRUE(valid_result.has_value()) << valid_result.error().ToString();
	EXPECT_EQ("before", valid_result.value());
}

TEST_F(DBTest, SnapshotLastOwnerReleaseUnpinsState)
{
	Options options;
	options.create_if_missing = true;

	auto open_result = DBImpl::OpenInternal(options, "test_db");
	ASSERT_TRUE(open_result.has_value()) << open_result.error().ToString();
	auto db = std::move(open_result.value());

	EXPECT_EQ(0U, db->TEST_ActiveSnapshotCount());

	Snapshot snapshot = db->CaptureSnapshot();
	EXPECT_EQ(1U, db->TEST_ActiveSnapshotCount());

	{
		Snapshot copy_one = snapshot;
		Snapshot copy_two = copy_one;
		EXPECT_EQ(1U, db->TEST_ActiveSnapshotCount());
	}

	EXPECT_EQ(1U, db->TEST_ActiveSnapshotCount());
	snapshot = Snapshot();
	EXPECT_EQ(0U, db->TEST_ActiveSnapshotCount());
}

TEST_F(DBTest, SnapshotDestructionAfterDBTeardownIsSafe)
{
	Options options;
	options.create_if_missing = true;

	Snapshot snapshot;
	{
		auto open_result = DBImpl::OpenInternal(options, "test_db");
		ASSERT_TRUE(open_result.has_value()) << open_result.error().ToString();
		auto db = std::move(open_result.value());

		ASSERT_TRUE(db->Put("teardown_key", "value").ok());
		snapshot = db->CaptureSnapshot();
		EXPECT_EQ(1U, db->TEST_ActiveSnapshotCount());
	}

	snapshot = Snapshot();
	SUCCEED();
}

TEST_F(DBTest, SnapshotRejectsReopenedInstanceHandle)
{
	const std::string test_path = "test_db_reopen_snapshot";
	std::error_code ec;
	std::filesystem::remove_all(test_path, ec);

	Options options;
	options.create_if_missing = true;

	Snapshot snapshot;
	{
		auto open_result = DBImpl::OpenInternal(options, test_path);
		ASSERT_TRUE(open_result.has_value()) << open_result.error().ToString();
		auto db = std::move(open_result.value());

		ASSERT_TRUE(db->Put("reopen_key", "value").ok());
		snapshot = db->CaptureSnapshot();
	}

	{
		auto reopen_result = DBImpl::OpenInternal(options, test_path);
		ASSERT_TRUE(reopen_result.has_value()) << reopen_result.error().ToString();
		auto db = std::move(reopen_result.value());

		ReadOptions read_options;
		read_options.snapshot_handle = snapshot;

		auto get_result = db->Get(read_options, "reopen_key");
		ASSERT_FALSE(get_result.has_value());
		EXPECT_TRUE(get_result.error().IsInvalidArgument()) << get_result.error().ToString();

		auto iter = db->NewIterator(read_options);
		ASSERT_NE(iter, nullptr);
		EXPECT_FALSE(iter->Valid());
		EXPECT_TRUE(iter->status().IsInvalidArgument()) << iter->status().ToString();
	}

	std::filesystem::remove_all(test_path, ec);
}

TEST_F(DBTest, DatabaseHandleOpen)
{
	const std::string default_path = "test_db_open_path";
	const std::string missing_database_path = "test_db_missing_database_path";
	std::error_code ec;
	std::filesystem::remove_all(default_path, ec);
	std::filesystem::remove_all(missing_database_path, ec);

	{
		auto res = Database::Open(default_path);
		ASSERT_TRUE(res.has_value());
		auto db = std::move(res.value());
		ASSERT_TRUE(db.Put("key", "value").ok());
		auto get_res = db.Get("key");
		ASSERT_TRUE(get_res.has_value());
		EXPECT_EQ(get_res.value(), "value");
		ASSERT_TRUE(db.Delete("key").ok());
		auto get_after = db.Get("key");
		EXPECT_TRUE(get_after.error().IsNotFound());
	}

	Options missing_options;
	missing_options.create_if_missing = false;
	auto database_missing = Database::Open(missing_options, missing_database_path);
	ASSERT_FALSE(database_missing.has_value());
	EXPECT_NE(database_missing.error().ToString().find("does not exist (create_if_missing is false)"), std::string::npos);
}

// ===========================================================================
// Characterization tests for Database::Open ownership/lifetime behavior
// These tests lock current behavior BEFORE any RAII refactoring.
// ===========================================================================

// Helper Env wrapper that can inject failures after lock acquisition
class FaultAfterLockEnv: public EnvWrapper
{
public:
	explicit FaultAfterLockEnv(Env* base)
	    : EnvWrapper(base)
	    , fail_get_children_{ false }
	{
	}

	void SetFailGetChildren(bool v) { fail_get_children_.store(v, std::memory_order_release); }

	Result<std::vector<std::string>> GetChildren(const std::string& dir) override
	{
		if (fail_get_children_.load(std::memory_order_acquire))
			return std::unexpected(Status::IOError(dir, "injected GetChildren failure"));
		return target()->GetChildren(dir);
	}

private:
	std::atomic<bool> fail_get_children_;
};

// When Database::Open fails after acquiring the DB lock, the lock must be released
// so that a subsequent open on the same path can succeed.
TEST_F(DBTest, OpenFailureReleasesLockAndAllowsRetry)
{
	const std::string test_path = "test_db_lock_release";
	std::error_code ec;
	std::filesystem::remove_all(test_path, ec);

	FaultAfterLockEnv fault_env(Env::Default());

	// First open attempt: inject failure after lock acquisition
	// The lock is acquired in Recover() before GetChildren is called
	Options opts;
	opts.env = &fault_env;
	opts.create_if_missing = true;

	fault_env.SetFailGetChildren(true);
	auto res1 = Database::Open(opts, test_path);
	EXPECT_FALSE(res1.has_value()) << "First open should fail due to injected fault";
	EXPECT_TRUE(res1.error().IsIOError()) << "Error should be IOError: " << res1.error().ToString();

	// Reset fault injection
	fault_env.SetFailGetChildren(false);

	// Second open attempt: should succeed because lock was released
	auto res2 = Database::Open(opts, test_path);
	ASSERT_TRUE(res2.has_value()) << "Second open should succeed after lock release: " << res2.error().ToString();

	auto db = std::move(res2.value());
	EXPECT_TRUE(db.Put("key", "value").ok());

	// Cleanup
	std::filesystem::remove_all(test_path, ec);
}

// Opening a non-existent DB with create_if_missing=false should return an error.
TEST_F(DBTest, OpenHonorsCreateIfMissingFalse)
{
	const std::string test_path = "test_db_create_if_missing_false";
	std::error_code ec;
	std::filesystem::remove_all(test_path, ec);

	Options opts;
	opts.create_if_missing = false; // Do not create if missing

	auto res = Database::Open(opts, test_path);
	EXPECT_FALSE(res.has_value()) << "Open should fail for non-existent DB with create_if_missing=false";
	EXPECT_TRUE(res.error().IsInvalidArgument()) << "Error should be InvalidArgument: " << res.error().ToString();

	// Cleanup (should be no-op since DB was never created)
	std::filesystem::remove_all(test_path, ec);
}

// Opening an existing DB with error_if_exists=true should return an error.
TEST_F(DBTest, OpenHonorsErrorIfExistsTrue)
{
	const std::string test_path = "test_db_error_if_exists";
	std::error_code ec;
	std::filesystem::remove_all(test_path, ec);

	// First, create the database
	{
		Options create_opts;
		create_opts.create_if_missing = true;
		auto res = Database::Open(create_opts, test_path);
		ASSERT_TRUE(res.has_value()) << "Initial DB creation should succeed: " << res.error().ToString();
		auto db = std::move(res.value());
		EXPECT_TRUE(db.Put("key", "value").ok());
	}

	// Now try to open with error_if_exists=true
	{
		Options error_opts;
		error_opts.error_if_exists = true;
		auto res = Database::Open(error_opts, test_path);
		EXPECT_FALSE(res.has_value()) << "Open should fail for existing DB with error_if_exists=true";
		EXPECT_TRUE(res.error().IsInvalidArgument()) << "Error should be InvalidArgument: " << res.error().ToString();
	}

	// Cleanup
	std::filesystem::remove_all(test_path, ec);
}

TEST_F(DBTest, SnapshotSurvivesFlushAndCompactionUntilRelease)
{
	// Use a very small write buffer so that the ~200 filler writes force multiple memtable flushes.
	Options options;
	options.create_if_missing = true;
	// Prism's Arena allocates a 4104-byte HEAD node for every fresh SkipList,
	// which already exceeds the 4096-byte limit and causes every write — including
	// the sync-barrier below — to overflow immediately.  Using 8192 (2 × block)
	// keeps the HEAD-node footprint inside the limit while still triggering ~3
	// automatic L0 flushes across 200 filler entries (one every ~62 entries when
	// the second Arena block is allocated).  That keeps the compaction score below
	// 1 so no SST-compaction BGWork is queued, which ensures the background thread
	// is fully idle by the time TEST_RunBackgroundCompactionOnce is called.
	options.write_buffer_size = 4096 * 2; // 8 KiB — must exceed the 4104-byte Arena HEAD-node footprint

	auto open_result = DBImpl::OpenInternal(options, "test_db");
	ASSERT_TRUE(open_result.has_value()) << open_result.error().ToString();
	auto db = std::move(open_result.value());

	// 1. Write the initial value and immediately capture a snapshot.
	ASSERT_TRUE(db->Put("snap_key", "snap_value").ok());
	Snapshot snap = db->CaptureSnapshot();

	// 2. Overwrite the key after the snapshot has been taken.
	ASSERT_TRUE(db->Put("snap_key", "new_value").ok());

	// 3. Write ~200 filler entries to fill the write buffer and trigger automatic flushes.
	const std::string filler_value(32, kValueChar);
	for (int i = 0; i < 200; ++i)
	{
		ASSERT_TRUE(db->Put("fill_" + std::to_string(i), filler_value).ok());
	}

	// Sync barrier: a single small write blocks inside MakeRoomForWrite while
	// imm_ != nullptr, so it cannot return until any in-flight automatic flush
	// has fully finished (background thread signalled + imm_ cleared).
	// Without this, the last filler write may have scheduled a background
	// compaction that is still holding the mutex inside WriteLevel0Table when
	// TEST_RunBackgroundCompactionOnce tries to compact the same imm_ — a race
	// that leads to a null-deref in MemTable::Unref().
	ASSERT_TRUE(db->Put("sync_barrier", "x").ok());

	// 4. Drive background compaction up to 8 times to push data through levels.
	for (int i = 0; i < 8; ++i)
	{
		Status s = db->TEST_RunBackgroundCompactionOnce();
		if (!s.ok())
			break;
	}

	// 5. Snapshot read must still return the value visible at snapshot time.
	ReadOptions snap_opts;
	snap_opts.snapshot_handle = snap;
	auto snap_result = db->Get(snap_opts, "snap_key");
	ASSERT_TRUE(snap_result.has_value()) << snap_result.error().ToString();
	EXPECT_EQ("snap_value", snap_result.value())
	    << "Snapshot read must survive flush and compaction";

	// 6. Non-snapshot read must return the latest overwritten value.
	auto current_result = db->Get("snap_key");
	ASSERT_TRUE(current_result.has_value()) << current_result.error().ToString();
	EXPECT_EQ("new_value", current_result.value())
	    << "Current read must return latest value after compaction";

	// 7. Release the snapshot; the snapshot list should now be empty.
	snap = Snapshot();
	snap_opts.snapshot_handle.reset();
	EXPECT_EQ(0U, db->TEST_ActiveSnapshotCount());
}

// ─────────────────────────────────────────────────────────────────────────────
// Invariant guard: unsnapshotted visibility
//
// An unsnapshotted read must only observe writes whose sequence has been fully
// committed (published), never a sequence that was reserved by a concurrent
// write but not yet applied.
//
// Currently this invariant is upheld because Get() acquires shared_lock and
// ResolveSnapshotSequence() returns sequence_ - 1, while writes increment
// sequence_ under unique_lock.  After the SuperVersion migration removes the
// read-path lock, sequence_ - 1 becomes unsafe because writes reserve sequence
// numbers BEFORE WAL/apply.  This test documents the expected behavior so that
// the migration must provide an equivalent visible-sequence mechanism.
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(DBTest, UnsnapshottedReadObservesCommittedStateNotReservedSequence)
{
	auto res = Database::Open("test_db");
	ASSERT_TRUE(res.has_value());
	auto db = std::move(res.value());

	// Baseline: write an initial value.
	ASSERT_TRUE(db.Put("vis_key", "v1").ok());

	// Verify basic read-your-writes: after Put returns, Get sees the value.
	auto r1 = db.Get("vis_key");
	ASSERT_TRUE(r1.has_value());
	EXPECT_EQ("v1", r1.value()) << "Get() after Put() must see the committed value";

	// Overwrite and verify the new value is visible.
	ASSERT_TRUE(db.Put("vis_key", "v2").ok());
	auto r2 = db.Get("vis_key");
	ASSERT_TRUE(r2.has_value());
	EXPECT_EQ("v2", r2.value()) << "Get() must see the latest committed overwrite";

	// Concurrent stress: multiple writers and readers.  Every read must return
	// a value that was actually written (never garbage from a reserved-but-
	// unapplied sequence).  With the current lock-based implementation this
	// is trivially true; after the SuperVersion migration the visible_sequence
	// mechanism must preserve it.
	constexpr int kWriters = 4;
	constexpr int kReaders = 4;
	constexpr int kKeysPerWriter = 100;

	std::atomic<bool> go{ false };
	std::atomic<bool> writers_done{ false };
	std::vector<std::thread> threads;

	auto key_for = [](int w, int i) { return "cvis_" + std::to_string(w) + "_" + std::to_string(i); };
	auto val_for = [](int w, int i) { return "cv_" + std::to_string(w) + "_" + std::to_string(i); };

	for (int w = 0; w < kWriters; ++w)
	{
		threads.emplace_back([&, w] {
			while (!go.load(std::memory_order_acquire)) {}
			for (int i = 0; i < kKeysPerWriter; ++i)
			{
				ASSERT_TRUE(db.Put(key_for(w, i), val_for(w, i)).ok());
			}
		});
	}

	for (int r = 0; r < kReaders; ++r)
	{
		threads.emplace_back([&] {
			while (!go.load(std::memory_order_acquire)) {}
			while (!writers_done.load(std::memory_order_acquire))
			{
				for (int w = 0; w < kWriters; ++w)
				{
					for (int i = 0; i < kKeysPerWriter; i += 11)
					{
						auto result = db.Get(key_for(w, i));
						if (result.has_value())
						{
							EXPECT_EQ(result.value(), val_for(w, i))
							    << "Read returned a value that was never written – "
							    << "possible reserved-sequence leak";
						}
						else
						{
							EXPECT_TRUE(result.error().IsNotFound());
						}
					}
				}
			}
		});
	}

	go.store(true, std::memory_order_release);
	for (int i = 0; i < kWriters; ++i) threads[i].join();
	writers_done.store(true, std::memory_order_release);
	for (int i = kWriters; i < kWriters + kReaders; ++i) threads[i].join();

	// Final verification: all keys readable.
	for (int w = 0; w < kWriters; ++w)
	{
		for (int i = 0; i < kKeysPerWriter; ++i)
		{
			auto result = db.Get(key_for(w, i));
			ASSERT_TRUE(result.has_value()) << key_for(w, i) << " missing after all writes complete";
			EXPECT_EQ(result.value(), val_for(w, i));
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Invariant guard: Get/Iterator visibility alignment
//
// When Get() and NewIterator() both use no-snapshot ReadOptions, they must
// observe the same point-in-time state (same visible sequence).  This guards
// against a future regression where the two paths resolve their read snapshot
// differently.
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(DBTest, GetIteratorVisibilityAlignmentNoSnapshot)
{
	auto res = Database::Open("test_db");
	ASSERT_TRUE(res.has_value());
	auto db = std::move(res.value());

	// Write a known dataset.
	for (int i = 0; i < 50; ++i)
	{
		ASSERT_TRUE(db.Put("align_" + std::to_string(i), "val_" + std::to_string(i)).ok());
	}

	// Capture the state via Get() for every key.
	std::vector<std::string> get_values;
	for (int i = 0; i < 50; ++i)
	{
		auto r = db.Get("align_" + std::to_string(i));
		ASSERT_TRUE(r.has_value());
		get_values.push_back(r.value());
	}

	// Capture the state via a no-snapshot iterator.
	std::unique_ptr<Iterator> it = db.NewIterator(ReadOptions());
	ASSERT_NE(it, nullptr);
	it->SeekToFirst();

	int idx = 0;
	while (it->Valid())
	{
		// The iterator may see keys from other tests if the DB is shared,
		// but for our align_* keys the values must match.
		std::string key = it->key().ToString();
		if (key.substr(0, 6) == "align_")
		{
			int kidx = std::stoi(key.substr(6));
			ASSERT_LT(kidx, 50);
			EXPECT_EQ(it->value().ToString(), get_values[kidx])
			    << "Iterator and Get() disagree on value for " << key;
			++idx;
		}
		it->Next();
	}
	EXPECT_EQ(idx, 50) << "Iterator should visit all 50 align_* keys";
	EXPECT_TRUE(it->status().ok()) << it->status().ToString();
}

// ─────────────────────────────────────────────────────────────────────────────
// Invariant guard: shutdown/reopen rejects stale snapshot handles
//
// Opens a DB via DBImpl::OpenInternal, captures a snapshot, closes the DB,
// reopens a fresh instance, and verifies that the stale snapshot (kept alive
// outside the close scope) is deterministically rejected for both Get() and
// NewIterator().  Uses DBImpl::OpenInternal so the Snapshot handle survives
// across the close/reopen boundary.
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(DBTest, ShutdownRejectsStaleSnapshotDeterministically)
{
	Options opts;
	opts.create_if_missing = true;

	std::unique_ptr<DBImpl> db1;
	{
		auto open = DBImpl::OpenInternal(opts, "test_db");
		ASSERT_TRUE(open.has_value());
		db1 = std::move(open.value());
		ASSERT_TRUE(db1->Put("stale_key", "original").ok());
	}

	Snapshot stale = db1->CaptureSnapshot();
	ASSERT_TRUE(db1->Put("stale_key", "after").ok());

	db1.reset();

	auto open2 = DBImpl::OpenInternal(opts, "test_db");
	ASSERT_TRUE(open2.has_value());
	auto db2 = std::move(open2.value());

	ReadOptions ro;
	ro.snapshot_handle = stale;

	auto get_res = db2->Get(ro, "stale_key");
	ASSERT_FALSE(get_res.has_value());
	EXPECT_TRUE(get_res.error().IsInvalidArgument()) << get_res.error().ToString();

	auto iter = db2->NewIterator(ro);
	ASSERT_NE(iter, nullptr);
	EXPECT_FALSE(iter->Valid()) << "Iterator from stale snapshot should not be valid";
	EXPECT_TRUE(iter->status().IsInvalidArgument()) << iter->status().ToString();

	auto plain = db2->Get("stale_key");
	ASSERT_TRUE(plain.has_value()) << plain.error().ToString();
	EXPECT_EQ("after", plain.value());
}

TEST_F(DBTest, GetSurvivesMemtableRotationRace)
{
	Options opts;
	opts.create_if_missing = true;
	opts.write_buffer_size = 128;

	auto open = DBImpl::OpenInternal(opts, "test_db");
	ASSERT_TRUE(open.has_value()) << open.error().ToString();
	auto db = std::move(open.value());
	auto* impl = db.get();

	impl->TEST_HoldBackgroundCompaction(true);
	ASSERT_TRUE(db->Put("hot_key", "hot_value").ok());
	ASSERT_NE(impl->TEST_CurrentSuperVersion(), nullptr);

	const int baseline_refs = impl->TEST_CurrentVersionRefs();
	std::atomic<bool> reader_started{ false };
	std::atomic<bool> rotation_done{ false };
	std::atomic<bool> stop{ false };
	std::atomic<int> post_rotation_reads{ 0 };
	std::atomic<int> bad_reads{ 0 };
	std::atomic<int> null_super_versions{ 0 };

	std::thread reader([&] {
		reader_started.store(true, std::memory_order_release);
		while (!stop.load(std::memory_order_acquire))
		{
			if (impl->TEST_CurrentSuperVersion() == nullptr)
			{
				null_super_versions.fetch_add(1, std::memory_order_relaxed);
			}

			auto result = db->Get("hot_key");
			if (!result.has_value() || result.value() != "hot_value")
			{
				bad_reads.fetch_add(1, std::memory_order_relaxed);
			}

			if (rotation_done.load(std::memory_order_acquire))
			{
				post_rotation_reads.fetch_add(1, std::memory_order_relaxed);
			}
		}
	});

	ASSERT_TRUE(WaitUntil([&] { return reader_started.load(std::memory_order_acquire); }, std::chrono::milliseconds(500)));

	for (int i = 0; i < 64 && !impl->TEST_HasImmutableMemTable(); ++i)
	{
		ASSERT_TRUE(db->Put("fill_rotation_" + std::to_string(i), std::string(64, 'r')).ok());
	}
	ASSERT_TRUE(impl->TEST_HasImmutableMemTable());
	ASSERT_NE(impl->TEST_CurrentSuperVersion(), nullptr);

	rotation_done.store(true, std::memory_order_release);
	ASSERT_TRUE(WaitUntil(
	    [&] { return post_rotation_reads.load(std::memory_order_acquire) >= 200; }, std::chrono::milliseconds(2000)));

	stop.store(true, std::memory_order_release);
	reader.join();

	impl->TEST_HoldBackgroundCompaction(false);
	ASSERT_TRUE(WaitUntil([&] { return !impl->TEST_HasImmutableMemTable(); }, std::chrono::milliseconds(5000)));

	EXPECT_EQ(0, bad_reads.load(std::memory_order_acquire));
	EXPECT_EQ(0, null_super_versions.load(std::memory_order_acquire));
	EXPECT_LE(impl->TEST_CurrentVersionRefs(), baseline_refs);
}

TEST_F(DBTest, GetSurvivesCompactionVersionTurnoverRace)
{
	Options opts;
	opts.create_if_missing = true;
	opts.write_buffer_size = 128;

	auto open = DBImpl::OpenInternal(opts, "test_db");
	ASSERT_TRUE(open.has_value()) << open.error().ToString();
	auto db = std::move(open.value());
	auto* impl = db.get();
	impl->TEST_HoldBackgroundCompaction(true);

	ASSERT_TRUE(db->Put("compaction_hot", "stable_value").ok());
	for (int i = 0; i < 64 && !impl->TEST_HasImmutableMemTable(); ++i)
	{
		ASSERT_TRUE(db->Put("compaction_fill_" + std::to_string(i), std::string(32, kValueChar)).ok());
	}
	ASSERT_TRUE(impl->TEST_HasImmutableMemTable());

	SuperVersion* initial_sv = impl->TEST_CurrentSuperVersion();
	ASSERT_NE(initial_sv, nullptr);
	const int baseline_refs = impl->TEST_CurrentVersionRefs();

	std::atomic<bool> reader_started{ false };
	std::atomic<bool> turnover_done{ false };
	std::atomic<bool> stop{ false };
	std::atomic<int> post_turnover_reads{ 0 };
	std::atomic<int> bad_reads{ 0 };
	std::atomic<int> null_super_versions{ 0 };

	std::thread reader([&] {
		reader_started.store(true, std::memory_order_release);
		while (!stop.load(std::memory_order_acquire))
		{
			if (impl->TEST_CurrentSuperVersion() == nullptr)
			{
				null_super_versions.fetch_add(1, std::memory_order_relaxed);
			}

			auto result = db->Get("compaction_hot");
			if (!result.has_value() || result.value() != "stable_value")
			{
				bad_reads.fetch_add(1, std::memory_order_relaxed);
			}

			if (turnover_done.load(std::memory_order_acquire))
			{
				post_turnover_reads.fetch_add(1, std::memory_order_relaxed);
			}
		}
	});

	ASSERT_TRUE(WaitUntil([&] { return reader_started.load(std::memory_order_acquire); }, std::chrono::milliseconds(500)));

	Status s = impl->TEST_RunBackgroundCompactionOnce();
	ASSERT_TRUE(s.ok()) << s.ToString();
	ASSERT_FALSE(impl->TEST_HasImmutableMemTable());
	ASSERT_NE(impl->TEST_CurrentSuperVersion(), initial_sv) << "Flush install should publish a new SuperVersion";

	turnover_done.store(true, std::memory_order_release);
	ASSERT_TRUE(WaitUntil(
	    [&] { return post_turnover_reads.load(std::memory_order_acquire) >= 200; }, std::chrono::milliseconds(2000)));

	stop.store(true, std::memory_order_release);
	reader.join();

	EXPECT_EQ(0, bad_reads.load(std::memory_order_acquire));
	EXPECT_EQ(0, null_super_versions.load(std::memory_order_acquire));
	EXPECT_LE(impl->TEST_CurrentVersionRefs(), baseline_refs);
}
