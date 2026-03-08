// manifest_recovery_test.cpp
// Reusable test fixture for isolated DB testing plus manifest/CURRENT bootstrap
// scenarios. Later tasks build on DbTestBase for compaction and recovery paths.

#include "db.h"
#include "env.h"
#include "filename.h"
#include "options.h"
#include "status.h"

#include <gtest/gtest.h>
#include <filesystem>
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
