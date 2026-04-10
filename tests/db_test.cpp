#include "db.h"
#include "write_batch.h"
#include "env.h"

#include <gtest/gtest.h>
#include <atomic>
#include <filesystem>
#include <thread>
#include <vector>

using namespace prism;

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
	}

	void TearDown() override
	{
		// Clean up after each test
		std::error_code ec;
		std::filesystem::remove_all("test_db", ec);
		std::filesystem::remove_all("test_db_large", ec);
		std::filesystem::remove_all("test_db_multi_large", ec);
	}
};

TEST_F(DBTest, BasicPutGetDelete)
{
	auto res = DB::Open("test_db");
	ASSERT_TRUE(res.has_value());

	auto db = std::move(res.value());

	// Put
	Status s1 = db->Put("key1", "value1");
	EXPECT_TRUE(s1.ok()) << "Put should succeed: " << s1.ToString();

	// Get
	std::string value;
	auto s2 = db->Get("key1");
	ASSERT_TRUE(s2.has_value()) << "Get should succeed";
	value = s2.value();
	EXPECT_EQ("value1", value) << "Value should match";

	// Delete
	Status s3 = db->Delete("key1");
	EXPECT_TRUE(s3.ok()) << "Delete should succeed: " << s3.ToString();

	// Get after delete
	std::string value2;
	auto s4 = db->Get("key1");
	EXPECT_TRUE(s4.error().IsNotFound()) << "Get should return NotFound after delete";
}

TEST_F(DBTest, BatchWrite)
{
	auto res = DB::Open("test_db");
	ASSERT_TRUE(res.has_value());

	auto db = std::move(res.value());

	WriteBatch batch;
	batch.Put("batch_key1", "batch_value1");
	batch.Put("batch_key2", "batch_value2");
	batch.Delete("key1");

	Status s = db->Write(batch);
	EXPECT_TRUE(s.ok()) << "Batch write should succeed: " << s.ToString();

	auto s1 = db->Get("batch_key1");
	ASSERT_TRUE(s1.has_value());
	EXPECT_EQ("batch_value1", s1.value());

	auto s2 = db->Get("batch_key2");
	ASSERT_TRUE(s2.has_value());
	EXPECT_EQ("batch_value2", s2.value());
}

TEST_F(DBTest, Recovery)
{
	{
		auto res = DB::Open("test_db");
		ASSERT_TRUE(res.has_value());

		auto db = std::move(res.value());
		Status s = db->Put("persistent_key", "persistent_value");
		ASSERT_TRUE(s.ok()) << s.ToString();
	}

	{
		auto res = DB::Open("test_db");
		ASSERT_TRUE(res.has_value());

		auto db = std::move(res.value());
		auto s = db->Get("persistent_key");
		ASSERT_TRUE(s.has_value());
		EXPECT_EQ("persistent_value", s.value()) << "Recovered value should match";
	}
}

TEST_F(DBTest, LargeValueFragmentation)
{
	auto res = DB::Open("test_db_large");
	ASSERT_TRUE(res.has_value());

	auto db = std::move(res.value());

	// Create a large value that will require fragmentation (> 32KB)
	std::string large_value(40000, 'X'); // 40KB of 'X'

	// Put the large value
	Status s1 = db->Put("large_key", large_value);
	ASSERT_TRUE(s1.ok()) << "Put large value should succeed: " << s1.ToString();

	// Get it back
	auto s2 = db->Get("large_key");
	ASSERT_TRUE(s2.has_value());
	EXPECT_EQ(large_value, s2.value()) << "Large value should match after fragmentation";

	// Test recovery with large value
	db.reset(); // Close DB

	res = DB::Open("test_db_large");
	ASSERT_TRUE(res.has_value());

	db = std::move(res.value());
	auto s3 = db->Get("large_key");
	ASSERT_TRUE(s3.has_value());
	EXPECT_EQ(large_value, s3.value()) << "Recovered large value should match";
}

TEST_F(DBTest, MultipleLargeRecords)
{
	auto res = DB::Open("test_db_multi_large");
	ASSERT_TRUE(res.has_value());

	auto db = std::move(res.value());

	// Write multiple large values
	std::string value1(50000, 'A'); // 50KB
	std::string value2(35000, 'B'); // 35KB
	std::string value3(45000, 'C'); // 45KB

	Status s1 = db->Put("key1", value1);
	Status s2 = db->Put("key2", value2);
	Status s3 = db->Put("key3", value3);

	ASSERT_TRUE(s1.ok()) << s1.ToString();
	ASSERT_TRUE(s2.ok()) << s2.ToString();
	ASSERT_TRUE(s3.ok()) << s3.ToString();

	// Verify all values
	auto r1 = db->Get("key1");
	auto r2 = db->Get("key2");
	auto r3 = db->Get("key3");

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
	auto res = DB::Open(options, "test_db");
	ASSERT_TRUE(res.has_value());

	auto db = std::move(res.value());

	ASSERT_TRUE(db->Put("b", "2").ok());
	ASSERT_TRUE(db->Put("a", "1").ok());
	ASSERT_TRUE(db->Put("c", "3").ok());
	ASSERT_TRUE(db->Delete("b").ok());

	ReadOptions ro;
	std::unique_ptr<Iterator> it(db->NewIterator(ro));
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
	auto res = DB::Open("test_db");
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
				ASSERT_TRUE(db->Put(key_for(t, i), value_for(t, i)).ok());
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
						auto r = db->Get(key_for(w, i));
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
			auto r = db->Get(key_for(w, i));
			ASSERT_TRUE(r.has_value());
			ASSERT_EQ(r.value(), value_for(w, i));
		}
	}
}

TEST_F(DBTest, SnapshotReadRemainsStableAcrossWrites)
{
	auto res = DB::Open("test_db");
	ASSERT_TRUE(res.has_value());
	auto db = std::move(res.value());

	ASSERT_TRUE(db->Put("k1", "v1").ok());

	const Snapshot* snap = db->GetSnapshot();
	ASSERT_NE(snap, nullptr);

	ASSERT_TRUE(db->Put("k1", "v2").ok());

	ReadOptions snap_opts;
	snap_opts.snapshot = snap;
	auto snap_result = db->Get(snap_opts, "k1");
	ASSERT_TRUE(snap_result.has_value());
	EXPECT_EQ("v1", snap_result.value()) << "Snapshot read should return value at snapshot time";

	auto current_result = db->Get("k1");
	ASSERT_TRUE(current_result.has_value());
	EXPECT_EQ("v2", current_result.value()) << "Current read should return latest value";

	ASSERT_TRUE(db->Delete("k1").ok());

	snap_result = db->Get(snap_opts, "k1");
	ASSERT_TRUE(snap_result.has_value());
	EXPECT_EQ("v1", snap_result.value()) << "Snapshot read should still return v1 after delete";

	current_result = db->Get("k1");
	EXPECT_TRUE(current_result.error().IsNotFound()) << "Current read should return NotFound after delete";

	db->ReleaseSnapshot(snap);
}

TEST_F(DBTest, SnapshotIteratorSeesStableView)
{
	auto res = DB::Open("test_db");
	ASSERT_TRUE(res.has_value());
	auto db = std::move(res.value());

	ASSERT_TRUE(db->Put("a", "1").ok());
	ASSERT_TRUE(db->Put("b", "2").ok());
	ASSERT_TRUE(db->Put("c", "3").ok());

	const Snapshot* snap = db->GetSnapshot();
	ASSERT_NE(snap, nullptr);

	ASSERT_TRUE(db->Delete("b").ok());
	ASSERT_TRUE(db->Put("c", "33").ok());
	ASSERT_TRUE(db->Put("d", "4").ok());

	ReadOptions snap_opts;
	snap_opts.snapshot = snap;
	std::unique_ptr<Iterator> it(db->NewIterator(snap_opts));

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

	db->ReleaseSnapshot(snap);
}

// ===========================================================================
// Characterization tests for DB::Open ownership/lifetime behavior
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

// When DB::Open fails after acquiring the DB lock, the lock must be released
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
	auto res1 = DB::Open(opts, test_path);
	EXPECT_FALSE(res1.has_value()) << "First open should fail due to injected fault";
	EXPECT_TRUE(res1.error().IsIOError()) << "Error should be IOError: " << res1.error().ToString();

	// Reset fault injection
	fault_env.SetFailGetChildren(false);

	// Second open attempt: should succeed because lock was released
	auto res2 = DB::Open(opts, test_path);
	ASSERT_TRUE(res2.has_value()) << "Second open should succeed after lock release: " << res2.error().ToString();

	auto db = std::move(res2.value());
	EXPECT_TRUE(db->Put("key", "value").ok());

	// Cleanup
	db.reset();
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

	auto res = DB::Open(opts, test_path);
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
		auto res = DB::Open(create_opts, test_path);
		ASSERT_TRUE(res.has_value()) << "Initial DB creation should succeed: " << res.error().ToString();
		auto db = std::move(res.value());
		EXPECT_TRUE(db->Put("key", "value").ok());
	}

	// Now try to open with error_if_exists=true
	{
		Options error_opts;
		error_opts.error_if_exists = true;
		auto res = DB::Open(error_opts, test_path);
		EXPECT_FALSE(res.has_value()) << "Open should fail for existing DB with error_if_exists=true";
		EXPECT_TRUE(res.error().IsInvalidArgument()) << "Error should be InvalidArgument: " << res.error().ToString();
	}

	// Cleanup
	std::filesystem::remove_all(test_path, ec);
}
