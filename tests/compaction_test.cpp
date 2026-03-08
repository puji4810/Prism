// compaction_test.cpp – Tests for pinned read-view and iterator-safe file lifetimes.
//
// These tests verify that:
//  1. An iterator created via NewIterator() pins the current Version (and mem/imm) so
//     that any background cleanup cannot free files still referenced by the iterator.
//  2. Get() pins the current Version only for the duration of the lookup, not beyond.

#include "db.h"
#include "db_impl.h"
#include "version_set.h"
#include "write_batch.h"

#include <filesystem>
#include <gtest/gtest.h>
#include <string>
#include <thread>

using namespace prism;

// ─────────────────────────────────────────────────────────────────────────────
// Test fixture
// ─────────────────────────────────────────────────────────────────────────────
class CompactionTest: public ::testing::Test
{
protected:
	static constexpr const char* kDbName = "test_compaction_db";

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
// CompactionTest.IteratorPinsFilesUntilRelease
//
// Verifies that after creating an iterator the Version ref-count is > 0,
// and that once the iterator is destroyed the ref drops back to 1 (the
// VersionSet's own reference).
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(CompactionTest, IteratorPinsFilesUntilRelease)
{
	auto db = OpenDB();
	ASSERT_NE(db, nullptr);

	// Write some data so there's something to iterate.
	ASSERT_TRUE(db->Put("alpha", "1").ok());
	ASSERT_TRUE(db->Put("beta", "2").ok());
	ASSERT_TRUE(db->Put("gamma", "3").ok());

	auto* impl = static_cast<DBImpl*>(db.get());

	// Before creating iterator: the current version has exactly 1 ref
	// (held by VersionSet::current_).
	Version* v_before = impl->TEST_CurrentVersion();
	ASSERT_NE(v_before, nullptr);
	int refs_before = impl->TEST_CurrentVersionRefs();
	EXPECT_EQ(refs_before, 1) << "VersionSet should hold exactly one ref before any iterator";

	{
		// Create an iterator – this must Ref() the current version.
		std::unique_ptr<Iterator> iter = db->NewIterator(ReadOptions());
		ASSERT_NE(iter, nullptr);

		int refs_with_iter = impl->TEST_CurrentVersionRefs();
		// The iterator holds an additional ref; total must be >= 2
		// (VersionSet ref + iterator ref).
		EXPECT_GE(refs_with_iter, 2) << "Iterator should hold at least one ref on the current version";

		// Iterate to confirm functionality still works.
		iter->SeekToFirst();
		EXPECT_TRUE(iter->Valid());

		// Iterator goes out of scope here – destructor must call the registered
		// cleanup which Unref()s the version.
	}

	// After iterator destruction the ref should be back to 1.
	int refs_after = impl->TEST_CurrentVersionRefs();
	EXPECT_EQ(refs_after, 1) << "After iterator destruction, version ref count should return to 1";
}

// ─────────────────────────────────────────────────────────────────────────────
// CompactionTest.GetUsesPinnedCurrentVersion
//
// Verifies that Get() temporarily pins the version but releases the pin after
// returning – the ref count after Get() must be the same as before.
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(CompactionTest, GetUsesPinnedCurrentVersion)
{
	auto db = OpenDB();
	ASSERT_NE(db, nullptr);

	ASSERT_TRUE(db->Put("key", "value").ok());

	auto* impl = static_cast<DBImpl*>(db.get());

	int refs_before = impl->TEST_CurrentVersionRefs();

	// Perform a Get().
	auto result = db->Get("key");
	EXPECT_TRUE(result.has_value());
	EXPECT_EQ(result.value(), "value");

	// After Get() completes, the ref count must be exactly the same as before.
	int refs_after = impl->TEST_CurrentVersionRefs();
	EXPECT_EQ(refs_after, refs_before) << "Get() must release its version pin before returning";
}

// ─────────────────────────────────────────────────────────────────────────────
// CompactionTest.MultipleIteratorsPinIndependently
//
// Multiple concurrent iterators each hold their own version ref.
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(CompactionTest, MultipleIteratorsPinIndependently)
{
	auto db = OpenDB();
	ASSERT_NE(db, nullptr);

	ASSERT_TRUE(db->Put("x", "1").ok());
	ASSERT_TRUE(db->Put("y", "2").ok());

	auto* impl = static_cast<DBImpl*>(db.get());
	int base_refs = impl->TEST_CurrentVersionRefs();

	{
		auto iter1 = db->NewIterator(ReadOptions());
		auto iter2 = db->NewIterator(ReadOptions());

		int refs_two = impl->TEST_CurrentVersionRefs();
		EXPECT_GE(refs_two, base_refs + 2) << "Two iterators should each add a ref";

		{
			// Destroy iter1 first.
			iter1.reset();
			int refs_one = impl->TEST_CurrentVersionRefs();
			EXPECT_GE(refs_one, base_refs + 1) << "After destroying iter1 there should still be iter2's ref";
		}
		// iter2 goes out of scope here.
	}

	int refs_final = impl->TEST_CurrentVersionRefs();
	EXPECT_EQ(refs_final, base_refs) << "After all iterators are destroyed refs should be back to baseline";
}
