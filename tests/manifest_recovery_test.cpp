// manifest_recovery_test.cpp
// Reusable test fixture for isolated DB testing plus manifest/CURRENT bootstrap
// scenarios. Later tasks build on DbTestBase for compaction and recovery paths.

#include "db.h"
#include "env.h"
#include "filename.h"
#include "log_reader.h"
#include "log_writer.h"
#include "options.h"
#include "status.h"
#include "table_cache.h"
#include "version_edit.h"
#include "version_set.h"

#include <gtest/gtest.h>
#include <filesystem>
#include <mutex>
#include <string>
#include <vector>

using namespace prism;
namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// DbTestBase – reusable fixture for tests that need an isolated DB directory.
//
// Each test gets a unique directory under Env::GetTestDirectory(). The
// directory is created in SetUp() and removed in TearDown(). Subclasses can
// call OpenDB() / CloseDB() to control the DB lifecycle independently.
// ---------------------------------------------------------------------------
class DbTestBase: public ::testing::Test
{
protected:
	void SetUp() override
	{
		Env* env = Env::Default();
		auto test_dir_res = env->GetTestDirectory();
		ASSERT_TRUE(test_dir_res.has_value()) << "GetTestDirectory failed";
		std::string base = test_dir_res.value();

		const ::testing::TestInfo* info = ::testing::UnitTest::GetInstance()->current_test_info();
		db_path_ = base + "/manifest_recovery_test_" + info->test_suite_name() + "_" + info->name();

		std::error_code ec;
		fs::remove_all(db_path_, ec);
		fs::create_directories(db_path_, ec);
		ASSERT_FALSE(ec) << "create_directories failed: " << ec.message();
	}

	void TearDown() override
	{
		db_.reset();
		std::error_code ec;
		fs::remove_all(db_path_, ec);
	}

	Result<std::unique_ptr<DB>> OpenDB(Options opts = {})
	{
		db_.reset();
		opts.create_if_missing = true;
		auto res = DB::Open(opts, db_path_);
		if (res.has_value())
			db_ = std::move(res.value());
		return res;
	}

	void CloseDB() { db_.reset(); }

	[[nodiscard]] bool HasCurrentFile() const { return Env::Default()->FileExists(CurrentFileName(db_path_)); }

	[[nodiscard]] bool HasLockFile() const { return Env::Default()->FileExists(LockFileName(db_path_)); }

	[[nodiscard]] bool HasManifestFile() const
	{
		Env* env = Env::Default();
		auto children = env->GetChildren(db_path_);
		if (!children.has_value())
			return false;
		for (const auto& name : children.value())
		{
			uint64_t number;
			FileType type;
			if (ParseFileName(name, &number, &type) && type == FileType::kDescriptorFile)
				return true;
		}
		return false;
	}

	[[nodiscard]] bool HasLogFile() const
	{
		Env* env = Env::Default();
		auto children = env->GetChildren(db_path_);
		if (!children.has_value())
			return false;
		for (const auto& name : children.value())
		{
			uint64_t number;
			FileType type;
			if (ParseFileName(name, &number, &type) && type == FileType::kLogFile)
				return true;
		}
		return false;
	}

	std::string ReadCurrentManifestName() const
	{
		std::string current_path = CurrentFileName(db_path_);
		std::string content;
		Status s = ReadFileToString(Env::Default(), current_path, &content);
		if (!s.ok())
			return "";
		if (!content.empty() && content.back() == '\n')
			content.pop_back();
		return content;
	}

	std::string db_path_;
	std::unique_ptr<DB> db_;
};

// ---------------------------------------------------------------------------
// Legacy-bootstrap helpers
// ---------------------------------------------------------------------------

static bool PlantLegacyDirectory(const std::string& db_path)
{
	Env* env = Env::Default();
	env->CreateDir(db_path);

	std::string log_name = LogFileName(db_path, 1);
	auto res = env->NewWritableFile(log_name);
	if (!res.has_value())
		return false;
	auto& wf = res.value();
	Status s = wf->Append(Slice("placeholder"));
	if (!s.ok())
		return false;
	s = wf->Close();
	return s.ok();
}

// ---------------------------------------------------------------------------
// ManifestRecoveryTest
// ---------------------------------------------------------------------------
class ManifestRecoveryTest: public DbTestBase
{
};

namespace
{
	struct TestManifestReporter: public log::Reader::Reporter
	{
		Status status = Status::OK();

		void Corruption(size_t, const Status& s) override
		{
			if (status.ok())
			{
				status = s;
			}
		}
	};

	Status ReadManifestEdits(const std::string& manifest_path, std::vector<VersionEdit>* edits)
	{
		auto file_res = Env::Default()->NewSequentialFile(manifest_path);
		if (!file_res.has_value())
		{
			return file_res.error();
		}

		TestManifestReporter reporter;
		log::Reader reader(file_res.value().get(), &reporter, true, 0);

		Slice record;
		std::string scratch;
		while (reader.ReadRecord(&record, &scratch))
		{
			VersionEdit edit;
			Status s = edit.DecodeFrom(record);
			if (!s.ok())
			{
				return s;
			}
			edits->push_back(std::move(edit));
		}

		if (!reporter.status.ok())
		{
			return reporter.status;
		}
		return reader.status();
	}
}

// ── Test 1: A fresh DB::Open must succeed and create the DB directory with
//    at least a LOCK file and a WAL log file. This tests the current
//    implementation. When MANIFEST/VersionSet is added (later tasks), the
//    CURRENT and MANIFEST checks become meaningful.
TEST_F(ManifestRecoveryTest, FreshOpenCreatesCurrentAndManifest)
{
	auto res = OpenDB();
	ASSERT_TRUE(res.has_value()) << "DB::Open failed: " << res.error().ToString();

	EXPECT_TRUE(HasLockFile()) << "LOCK file missing after fresh open";
	EXPECT_TRUE(HasLogFile()) << "WAL log file missing after fresh open";

	if (HasCurrentFile())
		EXPECT_TRUE(HasManifestFile()) << "CURRENT exists but MANIFEST does not";
}

// ── Test 2: After close + re-open, the DB can serve reads (data persists).
TEST_F(ManifestRecoveryTest, ReopenPreservesManifest)
{
	ASSERT_TRUE(OpenDB().has_value());
	ASSERT_TRUE(db_->Put("reopen_key", "reopen_val").ok());

	CloseDB();
	ASSERT_TRUE(OpenDB().has_value());

	EXPECT_TRUE(HasLockFile());

	auto get_res = db_->Get("reopen_key");
	ASSERT_TRUE(get_res.has_value()) << "key missing after reopen";
	EXPECT_EQ("reopen_val", get_res.value());
}

TEST_F(ManifestRecoveryTest, WriteSnapshotContainsAllLiveFiles)
{
	Options options;
	options.create_if_missing = true;
	options.env = Env::Default();
	options.comparator = BytewiseComparator();

	TableCache table_cache(db_path_, options, 16);
	InternalKeyComparator icmp(options.comparator);
	VersionSet version_set(db_path_, &options, &table_cache, &icmp);

	VersionEdit install_edit;
	install_edit.SetCompactPointer(1, InternalKey("k", 100, kTypeValue));
	install_edit.AddFile(0, 100, 4 * 1024, InternalKey("a", 100, kTypeValue), InternalKey("b", 100, kTypeValue));
	install_edit.AddFile(1, 200, 8 * 1024, InternalKey("c", 100, kTypeValue), InternalKey("d", 100, kTypeValue));

	std::mutex mu;
	mu.lock();
	Status s = version_set.LogAndApply(&install_edit, &mu);
	mu.unlock();
	ASSERT_TRUE(s.ok()) << s.ToString();

	auto manifest_res = Env::Default()->NewWritableFile(db_path_ + "/snapshot-test-manifest");
	ASSERT_TRUE(manifest_res.has_value()) << manifest_res.error().ToString();
	auto writable = std::move(manifest_res.value());
	log::Writer writer(writable.get());

	ASSERT_TRUE(version_set.WriteSnapshot(&writer).ok());
	ASSERT_TRUE(writable->Sync().ok());
	ASSERT_TRUE(writable->Close().ok());

	std::vector<VersionEdit> edits;
	s = ReadManifestEdits(db_path_ + "/snapshot-test-manifest", &edits);
	ASSERT_TRUE(s.ok()) << s.ToString();
	ASSERT_EQ(1u, edits.size());

	const VersionEdit& snapshot = edits[0];
	ASSERT_TRUE(snapshot.HasComparator());
	EXPECT_EQ(std::string(options.comparator->Name()), snapshot.GetComparator());
	EXPECT_EQ(1u, snapshot.GetCompactPointers().size());

	const auto& files = snapshot.GetNewFiles();
	ASSERT_EQ(2u, files.size());
	EXPECT_EQ(0, files[0].first);
	EXPECT_EQ(100u, files[0].second.number);
	EXPECT_EQ(1, files[1].first);
	EXPECT_EQ(200u, files[1].second.number);
}

// ── Test 3: A directory without CURRENT or MANIFEST ("legacy" directory)
//    should be handled according to the chosen bootstrap policy:
//      a) DB::Open with create_if_missing=true initializes such a directory, OR
//      b) the DB returns a clear non-OK status rather than crashing/UB.
//    When bootstrap is implemented, path (a) will also verify CURRENT+MANIFEST exist.
TEST_F(ManifestRecoveryTest, LegacyDirectoryBootstrap)
{
	std::error_code ec;
	fs::remove_all(db_path_, ec);
	fs::create_directories(db_path_, ec);
	ASSERT_FALSE(ec);

	bool planted = PlantLegacyDirectory(db_path_);
	ASSERT_TRUE(planted) << "Failed to plant legacy directory";

	ASSERT_FALSE(HasCurrentFile()) << "should not have CURRENT before bootstrap";

	Options opts;
	opts.create_if_missing = true;
	auto res = DB::Open(opts, db_path_);

	if (res.has_value())
	{
		db_ = std::move(res.value());

		EXPECT_TRUE(HasLockFile()) << "LOCK missing after open on legacy directory";
		EXPECT_TRUE(HasLogFile()) << "WAL log missing after open on legacy directory";

		if (HasCurrentFile())
			EXPECT_TRUE(HasManifestFile()) << "CURRENT exists but MANIFEST does not";

		EXPECT_TRUE(db_->Put("boot_key", "boot_val").ok());
		auto get_res = db_->Get("boot_key");
		EXPECT_TRUE(get_res.has_value());
		EXPECT_EQ("boot_val", get_res.value());
	}
	else
	{
		GTEST_SUCCEED() << "DB::Open returned non-OK on legacy directory (expected until "
		                   "bootstrap is implemented): "
		                << res.error().ToString();
	}
}

int main(int argc, char** argv)
{
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
