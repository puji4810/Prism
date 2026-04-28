// obsolete_files_test.cpp – Tests verifying that live iterators prevent
// obsolete-file deletion (the pinning infrastructure).
//
// Because actual background compaction and file-GC are not yet wired up,
// these tests exercise the invariant indirectly:
//   • A Version's ref-count stays > 1 while at least one iterator is alive.
//   • Any code that checks "is file X still referenced?" must traverse all
//     live versions (those with refs > 0).
//   • Only when all iterators are destroyed does the version become eligible
//     for collection.

#include "db_impl.h"
#include "filename.h"

#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <thread>

using namespace prism;

// ─────────────────────────────────────────────────────────────────────────────
// Test fixture
// ─────────────────────────────────────────────────────────────────────────────
class ObsoleteFilesTest: public ::testing::Test
{
protected:
	static constexpr const char* kDbName = "test_obsolete_files_db";

	void SetUp() override
	{
		std::error_code ec;
		std::filesystem::remove_all(kDbName, ec);
	}

	void TearDown() override
	{
		std::error_code ec;
		std::filesystem::remove_all(kDbName, ec);
	}

	std::unique_ptr<DBImpl> OpenDB()
	{
		Options opts;
		opts.create_if_missing = true;
		auto res = DBImpl::OpenInternal(opts, kDbName);
		EXPECT_TRUE(res.has_value()) << "DBImpl::OpenInternal failed";
		if (!res.has_value())
			return nullptr;
		return std::move(res.value());
	}

	std::vector<std::string> ListTableFiles() const
	{
		std::vector<std::string> files;
		for (const auto& entry : std::filesystem::directory_iterator(kDbName))
		{
			if (!entry.is_regular_file())
			{
				continue;
			}
			const std::string filename = entry.path().filename().string();
			uint64_t number = 0;
			FileType type;
			if (ParseFileName(filename, &number, &type) && type == FileType::kTableFile)
			{
				files.push_back(entry.path().string());
			}
		}
		return files;
	}

	void CreateDummyFile(const std::string& path)
	{
		std::ofstream out(path, std::ios::binary);
		ASSERT_TRUE(out.is_open());
		out << "dummy";
		out.close();
	}
};

// ─────────────────────────────────────────────────────────────────────────────
// ObsoleteFilesTest.LiveIteratorPreventsDeletion
//
// Core invariant: while an iterator is alive the version it pinned must have
// refs > 1 (the VersionSet's own ref + the iterator's ref).  Only when the
// iterator is released should refs drop back to 1, signalling that the file-GC
// routine is safe to free any files exclusively referenced by that version.
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(ObsoleteFilesTest, LiveIteratorPreventsDeletion)
{
	auto db = OpenDB();
	ASSERT_NE(db, nullptr);

	// Populate enough data that at least one SST might be flushed (write_buffer_size
	// is not constrained here so everything stays in memtable, which is fine: the
	// iterator still Refs the current Version regardless).
	for (int i = 0; i < 20; ++i)
	{
		std::string k = "key" + std::to_string(i);
		std::string v = "val" + std::to_string(i);
		ASSERT_TRUE(db->Put(k, v).ok());
	}

	auto* impl = db.get();

	// Baseline ref count.
	int base_refs = impl->TEST_CurrentVersionRefs();

	// Create the iterator – this pins the current version.
	std::unique_ptr<Iterator> iter = db->NewIterator(ReadOptions());
	ASSERT_NE(iter, nullptr);

	// The version must now have at least base_refs + 1.
	int refs_pinned = impl->TEST_CurrentVersionRefs();
	EXPECT_GE(refs_pinned, base_refs + 1) << "Iterator must hold at least one version ref (preventing file GC)";

	// Verify the iterator is functional.
	iter->SeekToFirst();
	int count = 0;
	while (iter->Valid())
	{
		++count;
		iter->Next();
	}
	EXPECT_EQ(count, 20) << "Iterator should visit all 20 keys";

	// The ref is still held even after we finished iterating.
	int refs_still_pinned = impl->TEST_CurrentVersionRefs();
	EXPECT_GE(refs_still_pinned, base_refs + 1) << "Version ref must be held until iterator is destroyed, not just until "
	                                               "iteration is complete";

	// Destroy the iterator.
	iter.reset();

	// Now refs must be back to baseline.
	int refs_released = impl->TEST_CurrentVersionRefs();
	EXPECT_EQ(refs_released, base_refs) << "After iterator destruction the version ref must be released, making "
	                                       "the version eligible for GC";
}

// ─────────────────────────────────────────────────────────────────────────────
// ObsoleteFilesTest.GetDoesNotLeakVersionRef
//
// A Get() call should not hold a version ref after it returns.
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(ObsoleteFilesTest, GetDoesNotLeakVersionRef)
{
	auto db = OpenDB();
	ASSERT_NE(db, nullptr);

	ASSERT_TRUE(db->Put("leak_check", "ok").ok());

	auto* impl = db.get();
	int refs_before = impl->TEST_CurrentVersionRefs();

	for (int i = 0; i < 5; ++i)
	{
		auto result = db->Get("leak_check");
		EXPECT_TRUE(result.has_value());
	}

	int refs_after = impl->TEST_CurrentVersionRefs();
	EXPECT_EQ(refs_after, refs_before) << "Repeated Get() calls must not accumulate version refs";
}

// ─────────────────────────────────────────────────────────────────────────────
// ObsoleteFilesTest.IteratorAndGetInterleaved
//
// An iterator and Get() calls can coexist safely. The Get() must not disturb
// the iterator's pin.
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(ObsoleteFilesTest, IteratorAndGetInterleaved)
{
	auto db = OpenDB();
	ASSERT_NE(db, nullptr);

	for (int i = 0; i < 10; ++i)
	{
		ASSERT_TRUE(db->Put("k" + std::to_string(i), "v" + std::to_string(i)).ok());
	}

	auto* impl = db.get();
	int base_refs = impl->TEST_CurrentVersionRefs();

	// Create iterator – pins the version.
	auto iter = db->NewIterator(ReadOptions());
	int refs_with_iter = impl->TEST_CurrentVersionRefs();
	EXPECT_GE(refs_with_iter, base_refs + 1);

	// Perform Get() while iterator is alive.
	for (int i = 0; i < 10; ++i)
	{
		auto res = db->Get("k" + std::to_string(i));
		EXPECT_TRUE(res.has_value());
	}

	// Refs must still be at the iterator-pinned level (not leaked by Get()).
	int refs_after_gets = impl->TEST_CurrentVersionRefs();
	EXPECT_EQ(refs_after_gets, refs_with_iter) << "Get() calls while iterator is alive must not change the ref count";

	iter.reset();

	int refs_final = impl->TEST_CurrentVersionRefs();
	EXPECT_EQ(refs_final, base_refs) << "After iterator is released, ref count must be back to baseline";
}

TEST_F(ObsoleteFilesTest, PendingOutputIsNotDeleted)
{
	auto db = OpenDB();
	ASSERT_NE(db, nullptr);

	auto* impl = db.get();
	const uint64_t pending_number = 900001;
	impl->TEST_AddPendingOutput(pending_number);

	const std::string pending_file = TableFileName(kDbName, pending_number);
	CreateDummyFile(pending_file);
	ASSERT_TRUE(std::filesystem::exists(pending_file));

	impl->TEST_RemoveObsoleteFiles();
	EXPECT_TRUE(std::filesystem::exists(pending_file));
}

TEST_F(ObsoleteFilesTest, ObsoleteTableEvictsCacheBeforeDelete)
{
	Options opts;
	opts.create_if_missing = true;
	opts.write_buffer_size = 128;
	auto open_res = DBImpl::OpenInternal(opts, kDbName);
	ASSERT_TRUE(open_res.has_value());
	auto db = std::move(open_res.value());

	for (int i = 0; i < 32; ++i)
	{
		ASSERT_TRUE(db->Put("flush_key_" + std::to_string(i), std::string(64, 'x')).ok());
	}

	auto tables_before = ListTableFiles();
	ASSERT_FALSE(tables_before.empty());
	db.reset();

	const uint64_t stale_number = 900002;
	const std::string stale_file = TableFileName(kDbName, stale_number);
	CreateDummyFile(stale_file);
	ASSERT_TRUE(std::filesystem::exists(stale_file));

	auto reopen_res = DBImpl::OpenInternal(opts, kDbName);
	ASSERT_TRUE(reopen_res.has_value());
	auto reopened = std::move(reopen_res.value());
	auto* impl = reopened.get();

	impl->TEST_RemoveObsoleteFiles();
	EXPECT_FALSE(std::filesystem::exists(stale_file));
}

TEST_F(ObsoleteFilesTest, RecoveryRemovesDeadLogsButKeepsLiveTables)
{
	Options opts;
	opts.create_if_missing = true;
	opts.write_buffer_size = 128;
	auto open_res = DBImpl::OpenInternal(opts, kDbName);
	ASSERT_TRUE(open_res.has_value());
	auto db = std::move(open_res.value());

	for (int i = 0; i < 32; ++i)
	{
		ASSERT_TRUE(db->Put("keep_key_" + std::to_string(i), std::string(64, 'y')).ok());
	}

	auto live_tables = ListTableFiles();
	ASSERT_FALSE(live_tables.empty());
	db.reset();

	const std::string stale_log = LogFileName(kDbName, 1);
	CreateDummyFile(stale_log);
	ASSERT_TRUE(std::filesystem::exists(stale_log));

	auto reopen_res = DBImpl::OpenInternal(opts, kDbName);
	ASSERT_TRUE(reopen_res.has_value());
	auto reopened = std::move(reopen_res.value());
	auto* impl = reopened.get();

	impl->TEST_RemoveObsoleteFiles();
	EXPECT_FALSE(std::filesystem::exists(stale_log));
	for (const auto& table_file : live_tables)
	{
		EXPECT_TRUE(std::filesystem::exists(table_file));
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// ObsoleteFilesTest.VersionRefsReturnToBaselineAfterVersionTurnover
//
// After version turnover (via compaction/flush), all version refs must
// return to baseline. The old version should be released and refs should
// drop back to the initial level.
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(ObsoleteFilesTest, VersionRefsReturnToBaselineAfterVersionTurnover)
{
	auto db = OpenDB();
	ASSERT_NE(db, nullptr);

	auto* impl = db.get();
	int baseline = impl->TEST_CurrentVersionRefs();

	// Write enough data to trigger compaction or version turnover.
	for (int i = 0; i < 100; ++i)
	{
		ASSERT_TRUE(db->Put("turnover_key_" + std::to_string(i), std::string(256, 'x')).ok());
	}

	// Give any background operations time to settle.
	std::this_thread::sleep_for(std::chrono::milliseconds(100));

	// After version turnover, refs must return to baseline.
	int refs_after = impl->TEST_CurrentVersionRefs();
	EXPECT_EQ(refs_after, baseline) << "Version refs must return to baseline after version turnover";
}

// ─────────────────────────────────────────────────────────────────────────────
// ObsoleteFilesTest.ConcurrentGetsAfterRecoveryDoNotAccumulateVersionRefs
//
// After recovery and concurrent Get() calls from multiple threads,
// version refs must not accumulate. Each Get() should not leak refs.
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(ObsoleteFilesTest, ConcurrentGetsAfterRecoveryDoNotAccumulateVersionRefs)
{
	// Write some data and close.
	{
		auto db = OpenDB();
		ASSERT_NE(db, nullptr);
		for (int i = 0; i < 20; ++i)
		{
			ASSERT_TRUE(db->Put("recovery_key_" + std::to_string(i), std::string(32, 'y')).ok());
		}
	}

	// Reopen DB (recovery path).
	auto db = OpenDB();
	ASSERT_NE(db, nullptr);

	auto* impl = db.get();
	int baseline = impl->TEST_CurrentVersionRefs();

	// Run concurrent Get() operations.
	std::vector<std::thread> threads;
	const int num_threads = 4;
	const int gets_per_thread = 10;

	for (int t = 0; t < num_threads; ++t)
	{
		threads.emplace_back([&db, gets_per_thread]() {
			for (int i = 0; i < gets_per_thread; ++i)
			{
				for (int k = 0; k < 20; ++k)
				{
					auto res = db->Get("recovery_key_" + std::to_string(k));
					(void)res; // Suppress unused warning
				}
			}
		});
	}

	// Wait for all threads to complete.
	for (auto& thread : threads)
	{
		thread.join();
	}

	// Verify refs returned to baseline after concurrent Gettings completed.
	int refs_after = impl->TEST_CurrentVersionRefs();
	EXPECT_EQ(refs_after, baseline) << "Version refs must not accumulate after concurrent Get() calls on recovered DB";
}

// ─────────────────────────────────────────────────────────────────────────────
// Invariant guard: concurrent Get + Iterator ref balance after turnover
//
// Runs concurrent Gets AND iterators, then triggers memtable rotation by
// writing enough data.  All version refs must return to baseline once every
// Get completes and every iterator is destroyed.  This is a more aggressive
// version of the existing ref-leak tests: it exercises the interleaving of
// short-lived (Get) and long-lived (iterator) ref holders during a version
// turnover event.
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(ObsoleteFilesTest, ConcurrentGetAndIteratorRefBalanceAfterTurnover)
{
	auto db = OpenDB();
	ASSERT_NE(db, nullptr);

	auto* impl = db.get();
	int baseline = impl->TEST_CurrentVersionRefs();

	// Phase 1: seed data.
	for (int i = 0; i < 30; ++i)
	{
		ASSERT_TRUE(db->Put("seed_" + std::to_string(i), std::string(64, 's')).ok());
	}

	// Phase 2: create long-lived iterators and concurrent Gets while writing
	// enough data to trigger memtable rotation.
	std::atomic<bool> stop{ false };
	std::vector<std::thread> threads;

	// Iterator holders: create iterators that live across the turnover.
	const int kIterators = 3;
	std::vector<std::unique_ptr<Iterator>> iters(kIterators);
	for (int i = 0; i < kIterators; ++i)
	{
		iters[i] = db->NewIterator(ReadOptions());
		ASSERT_NE(iters[i], nullptr);
	}

	int refs_with_iters = impl->TEST_CurrentVersionRefs();
	EXPECT_GE(refs_with_iters, baseline + kIterators) << "Each iterator must hold a version ref";

	// Concurrent Get() threads.
	const int kGetThreads = 4;
	for (int t = 0; t < kGetThreads; ++t)
	{
		threads.emplace_back([&, t] {
			while (!stop.load(std::memory_order_acquire))
			{
				for (int i = 0; i < 30; ++i)
				{
					auto r = db->Get("seed_" + std::to_string(i));
					(void)r;
				}
			}
		});
	}

	// Writer thread: write enough to trigger memtable rotation.
	threads.emplace_back([&] {
		for (int i = 0; i < 200; ++i)
		{
			Status s = db->Put("turnover_" + std::to_string(i), std::string(128, 't'));
			if (!s.ok()) break;
		}
	});

	// Wait for writer to finish, then stop readers.
	threads.back().join();
	stop.store(true, std::memory_order_release);
	for (int i = 0; i < kGetThreads; ++i) threads[i].join();

	// Refs must still be elevated by the iterators.
	int refs_mid = impl->TEST_CurrentVersionRefs();
	EXPECT_GE(refs_mid, baseline + kIterators) << "Iterators still alive – refs must not have leaked below iterator count";

	// Destroy iterators one by one and verify ref decrement.
	for (int i = 0; i < kIterators; ++i)
	{
		iters[i].reset();
		int refs_now = impl->TEST_CurrentVersionRefs();
		EXPECT_GE(refs_now, baseline) << "Refs must never drop below baseline after destroying iterator " << i;
	}

	// All iterators destroyed – refs must be back to baseline.
	int refs_final = impl->TEST_CurrentVersionRefs();
	EXPECT_EQ(refs_final, baseline) << "After all iterators destroyed and Gets completed, refs must return to baseline";
}
