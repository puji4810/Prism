// compaction_test.cpp – Tests for pinned read-view and iterator-safe file lifetimes.
//
// These tests verify that:
//  1. An iterator created via NewIterator() pins the current Version (and mem/imm) so
//     that any background cleanup cannot free files still referenced by the iterator.
//  2. Get() pins the current Version only for the duration of the lookup, not beyond.

#include "db.h"
#include "dbformat.h"
#include "db_impl.h"
#include "env.h"
#include "filename.h"
#include "table/table_builder.h"
#include "version_set.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <gtest/gtest.h>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace prism;

namespace
{
	class FailingTableEnv final: public EnvWrapper
	{
	public:
		explicit FailingTableEnv(Env* target)
		    : EnvWrapper(target)
		{
		}

		void SetFailTableWrites(bool fail) { fail_table_writes_.store(fail, std::memory_order_release); }

		Result<std::unique_ptr<WritableFile>> NewWritableFile(const std::string& fname) override
		{
			if (fail_table_writes_.load(std::memory_order_acquire))
			{
				uint64_t number = 0;
				FileType type;
				const std::filesystem::path path(fname);
				if (ParseFileName(path.filename().string(), &number, &type) && type == FileType::kTableFile)
				{
					return std::unexpected(Status::IOError("injected table writable-file failure"));
				}
			}
			return target()->NewWritableFile(fname);
		}

	private:
		std::atomic<bool> fail_table_writes_{ false };
	};

	uint64_t CreateSingleKeyTableFile(DBImpl* impl, const std::string& dbname, const std::string& user_key, const std::string& value)
	{
		const uint64_t file_number = impl->TEST_NewFileNumber();
		const std::string filename = TableFileName(dbname, file_number);

		auto file_result = impl->TEST_Env()->NewWritableFile(filename);
		if (!file_result.has_value())
		{
			return 0;
		}
		auto file = std::move(file_result.value());

		InternalKey internal_key(user_key, 100, kTypeValue);
		TableBuilder builder(impl->TEST_Options(), file.get());
		builder.Add(internal_key.Encode(), value);

		Status s = builder.Finish();
		if (s.ok())
		{
			s = file->Sync();
		}
		if (s.ok())
		{
			s = file->Close();
		}
		if (!s.ok())
		{
			impl->TEST_Env()->RemoveFile(filename);
			return 0;
		}
		return file_number;
	}
}

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

	auto* impl = db.get();

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

	auto* impl = db.get();

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

	auto* impl = db.get();
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

TEST_F(CompactionTest, BackgroundErrorBecomesSticky)
{
	FailingTableEnv env(Env::Default());
	env.SetFailTableWrites(true);

	Options opts;
	opts.env = &env;
	opts.create_if_missing = true;
	opts.write_buffer_size = 128;

	auto open = DBImpl::OpenInternal(opts, kDbName);
	ASSERT_TRUE(open.has_value()) << open.error().ToString();
	auto db = std::move(open.value());

	Status first_error;
	for (int i = 0; i < 512; ++i)
	{
		Status s = db->Put("k" + std::to_string(i), std::string(64, 'v'));
		if (!s.ok())
		{
			first_error = s;
			break;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	ASSERT_FALSE(first_error.ok()) << "expected write to fail after background flush error";

	env.SetFailTableWrites(false);
	Status second_error = db->Put("after_error", "value");
	EXPECT_FALSE(second_error.ok());
	EXPECT_EQ(second_error.ToString(), first_error.ToString());
}

TEST_F(CompactionTest, TrivialMoveInstallsViaManifestOnly)
{
	Options opts;
	opts.create_if_missing = true;

	auto open = DBImpl::OpenInternal(opts, kDbName);
	ASSERT_TRUE(open.has_value()) << open.error().ToString();
	auto db = std::move(open.value());
	auto* impl = db.get();

	const uint64_t moved_file = CreateSingleKeyTableFile(impl, kDbName, "k", "v");
	ASSERT_NE(moved_file, 0ULL);

	Status add_status = impl->TEST_AddFileToVersion(
	    1, moved_file, 64ULL * 1024ULL * 1024ULL, InternalKey("k", 100, kTypeValue), InternalKey("k", 100, kTypeValue));
	ASSERT_TRUE(add_status.ok()) << add_status.ToString();

	const int table_files_before = static_cast<int>(std::count_if(std::filesystem::directory_iterator(kDbName),
	    std::filesystem::directory_iterator(), [](const std::filesystem::directory_entry& entry) {
		    uint64_t number = 0;
		    FileType type;
		    return ParseFileName(entry.path().filename().string(), &number, &type) && type == FileType::kTableFile;
	    }));

	Status s = impl->TEST_RunBackgroundCompactionOnce();
	EXPECT_TRUE(s.ok()) << s.ToString();

	const std::vector<FileMetaData> l1 = impl->TEST_LevelFilesCopy(1);
	const std::vector<FileMetaData> l2 = impl->TEST_LevelFilesCopy(2);
	EXPECT_TRUE(std::none_of(l1.begin(), l1.end(), [moved_file](const FileMetaData& file) { return file.number == moved_file; }));
	EXPECT_TRUE(std::any_of(l2.begin(), l2.end(), [moved_file](const FileMetaData& file) { return file.number == moved_file; }));

	const int table_files_after = static_cast<int>(std::count_if(std::filesystem::directory_iterator(kDbName),
	    std::filesystem::directory_iterator(), [](const std::filesystem::directory_entry& entry) {
		    uint64_t number = 0;
		    FileType type;
		    return ParseFileName(entry.path().filename().string(), &number, &type) && type == FileType::kTableFile;
	    }));
	EXPECT_EQ(table_files_after, table_files_before);
}
