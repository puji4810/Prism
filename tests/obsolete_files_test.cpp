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

#include "db.h"
#include "db_impl.h"
#include "version_set.h"
#include "write_batch.h"

#include <filesystem>
#include <gtest/gtest.h>
#include <string>

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

	std::unique_ptr<DB> OpenDB()
	{
		Options opts;
		opts.create_if_missing = true;
		auto res = DB::Open(opts, kDbName);
		EXPECT_TRUE(res.has_value()) << "DB::Open failed";
		if (!res.has_value())
			return nullptr;
		return std::move(res.value());
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

	auto* impl = static_cast<DBImpl*>(db.get());

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

	auto* impl = static_cast<DBImpl*>(db.get());
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

	auto* impl = static_cast<DBImpl*>(db.get());
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
