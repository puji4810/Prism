// manifest_recovery_test.cpp
// Reusable test fixture for isolated DB testing plus manifest/CURRENT bootstrap
// scenarios. Later tasks build on DbTestBase for compaction and recovery paths.

#include "db.h"
#include "db_impl.h"
#include "env.h"
#include "filename.h"
#include "log_reader.h"
#include "log_writer.h"
#include "options.h"
#include "status.h"
#include "table/table_builder.h"
#include "table_cache.h"
#include "version_edit.h"
#include "version_set.h"
#include "write_batch_internal.h"

#include <gtest/gtest.h>
#include <filesystem>
#include <algorithm>
#include <shared_mutex>
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
		db_res_.reset();
		std::error_code ec;
		fs::remove_all(db_path_, ec);
	}

	Result<Database> OpenDB(Options opts = { })
	{
		db_res_.reset();
		opts.create_if_missing = true;
		auto res = Database::Open(opts, db_path_);
		if (res.has_value())
			db_res_ = std::make_unique<Database>(std::move(res.value()));
		return res;
	}

	void CloseDB() { db_res_.reset(); }

	Database& db() { return *db_res_; }

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
	std::unique_ptr<Database> db_res_;
};

// ===========================================================================
// TODO(wal-rotation): Test coverage for crash/recovery around retired WAL close
// - recovery boundaries around WAL rotation + retired-close lifecycle
// - crash during retired-close remains recoverable without double-apply
// - retired WAL not deleted before VersionSet advances replay boundary
// ===========================================================================

// ---------------------------------------------------------------------------
// Legacy-bootstrap helpers
// ---------------------------------------------------------------------------

static bool PlantLegacyDirectory(const std::string& db_path)
{
	Env* env = Env::Default();
	env->CreateDir(db_path);

	Options table_options;
	table_options.env = env;
	InternalKeyComparator icmp(BytewiseComparator());
	table_options.comparator = &icmp;
	table_options.compression = CompressionType::kNoCompression;

	auto legacy_table = env->NewWritableFile(TableFileName(db_path, 10));
	if (!legacy_table.has_value())
		return false;
	TableBuilder builder(table_options, legacy_table.value().get());
	builder.Add(InternalKey("alpha", 1, kTypeValue).Encode(), Slice("table_alpha"));
	builder.Add(InternalKey("omega", 1, kTypeValue).Encode(), Slice("table_omega"));
	Status table_status = builder.Finish();
	if (!table_status.ok())
		return false;
	if (!legacy_table.value()->Close().ok())
		return false;

	std::string log_name = LogFileName(db_path, 1);
	auto res = env->NewWritableFile(log_name);
	if (!res.has_value())
		return false;
	log::Writer writer(res.value().get());
	WriteBatch batch;
	batch.Put("legacy_log_key", "legacy_log_value");
	WriteBatchInternal::SetSequence(&batch, 1);
	Status s = writer.AddRecord(WriteBatchInternal::Contents(&batch));
	if (!s.ok())
		return false;
	return res.value()->Close().ok();
}

// Helper to plant a legacy directory where log number > table number
// This tests the edge case that could trigger the LogAndApply assertion
static bool PlantLegacyDirectoryWithHighLogNumber(const std::string& db_path)
{
	Env* env = Env::Default();
	env->CreateDir(db_path);

	Options table_options;
	table_options.env = env;
	InternalKeyComparator icmp(BytewiseComparator());
	table_options.comparator = &icmp;
	table_options.compression = CompressionType::kNoCompression;

	auto legacy_table = env->NewWritableFile(TableFileName(db_path, 10));
	if (!legacy_table.has_value())
		return false;
	TableBuilder builder(table_options, legacy_table.value().get());
	builder.Add(InternalKey("legacy_high", 1, kTypeValue).Encode(), Slice("legacy_high_value"));
	builder.Add(InternalKey("legacy_low", 1, kTypeValue).Encode(), Slice("legacy_low_value"));
	Status table_status = builder.Finish();
	if (!table_status.ok())
		return false;
	if (!legacy_table.value()->Close().ok())
		return false;

	// Create a log file with number 100 (higher than table number 10)
	std::string log_name = LogFileName(db_path, 100);
	auto res = env->NewWritableFile(log_name);
	if (!res.has_value())
		return false;
	log::Writer writer(res.value().get());
	WriteBatch batch;
	batch.Put("legacy_high_log_key", "legacy_high_log_value");
	WriteBatchInternal::SetSequence(&batch, 1);
	Status s = writer.AddRecord(WriteBatchInternal::Contents(&batch));
	if (!s.ok())
		return false;
	return res.value()->Close().ok();
}

static Status CreatePlaceholderFile(const std::string& fname)
{
	auto writable = Env::Default()->NewWritableFile(fname);
	if (!writable.has_value())
	{
		return writable.error();
	}
	Status s = writable.value()->Append(Slice("placeholder"));
	if (!s.ok())
	{
		return s;
	}
	return writable.value()->Close();
}

static Status ApplyEdit(VersionSet* version_set, VersionEdit* edit)
{
	std::shared_mutex mu;
	mu.lock();
	Status s = version_set->LogAndApply(edit, &mu);
	mu.unlock();
	return s;
}

static std::vector<uint64_t> CollectFileNumbers(const Version* version, int level)
{
	std::vector<uint64_t> numbers;
	for (const FileMetaData* file : version->files(level))
	{
		numbers.push_back(file->number);
	}
	std::sort(numbers.begin(), numbers.end());
	return numbers;
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

// ── Test 1: A fresh Database::Open must succeed and create the DB directory with
//    at least a LOCK file and a WAL log file. This tests the current
//    implementation. When MANIFEST/VersionSet is added (later tasks), the
//    CURRENT and MANIFEST checks become meaningful.
TEST_F(ManifestRecoveryTest, FreshOpenCreatesCurrentAndManifest)
{
	auto res = OpenDB();
	ASSERT_TRUE(res.has_value()) << "Database::Open failed: " << res.error().ToString();

	EXPECT_TRUE(HasLockFile()) << "LOCK file missing after fresh open";
	EXPECT_TRUE(HasLogFile()) << "WAL log file missing after fresh open";

	if (HasCurrentFile())
		EXPECT_TRUE(HasManifestFile()) << "CURRENT exists but MANIFEST does not";
}

// ── Test 2: After close + re-open, the DB can serve reads (data persists).
TEST_F(ManifestRecoveryTest, ReopenPreservesManifest)
{
	ASSERT_TRUE(OpenDB().has_value());
	ASSERT_TRUE(db().Put("reopen_key", "reopen_val").ok());

	CloseDB();
	ASSERT_TRUE(OpenDB().has_value());

	EXPECT_TRUE(HasLockFile());

	auto get_res = db().Get("reopen_key");
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

	std::shared_mutex mu;
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
//      a) Database::Open with create_if_missing=true initializes such a directory, OR
//      b) the DB returns a clear non-OK status rather than crashing/UB.
//    When bootstrap is implemented, path (a) will also verify CURRENT+MANIFEST exist.
TEST_F(ManifestRecoveryTest, LegacyDirectoryBootstrap)
{
	ASSERT_TRUE(PlantLegacyDirectory(db_path_));
	ASSERT_FALSE(HasCurrentFile());

	Options options;
	options.env = Env::Default();
	options.comparator = BytewiseComparator();

	TableCache table_cache(db_path_, options, 16);
	InternalKeyComparator icmp(options.comparator);
	VersionSet version_set(db_path_, &options, &table_cache, &icmp);

	bool save_manifest = false;
	Status s = version_set.Recover(&save_manifest);
	ASSERT_TRUE(s.ok()) << s.ToString();
	EXPECT_TRUE(save_manifest);
	EXPECT_TRUE(HasCurrentFile());
	EXPECT_TRUE(HasManifestFile());

	const std::vector<uint64_t> l0_files = CollectFileNumbers(version_set.current(), 0);
	ASSERT_EQ(1u, l0_files.size());
	EXPECT_EQ(10u, l0_files[0]);

	ASSERT_TRUE(CreatePlaceholderFile(TableFileName(db_path_, 999)).ok());

	VersionSet reopen(db_path_, &options, &table_cache, &icmp);
	bool reopen_save_manifest = false;
	s = reopen.Recover(&reopen_save_manifest);
	ASSERT_TRUE(s.ok()) << s.ToString();
	const std::vector<uint64_t> reopened_l0 = CollectFileNumbers(reopen.current(), 0);
	EXPECT_EQ(l0_files, reopened_l0);
	EXPECT_TRUE(reopen.NextFileNumber() > 999u);
}

// Test: Legacy bootstrap where WAL file number (100) exceeds table file number (10).
// Verifies that VersionSet::Recover() correctly tracks the highest log number
// and advances NextFileNumber() past both the table and log high-water marks.
// This tests the invariant: edit->GetLogNumber() < next_file_number_
TEST_F(ManifestRecoveryTest, LegacyBootstrapTracksHighestLogNumber)
{
	ASSERT_TRUE(PlantLegacyDirectoryWithHighLogNumber(db_path_));
	ASSERT_FALSE(HasCurrentFile());

	Options options;
	options.env = Env::Default();
	options.comparator = BytewiseComparator();

	TableCache table_cache(db_path_, options, 16);
	InternalKeyComparator icmp(options.comparator);
	VersionSet version_set(db_path_, &options, &table_cache, &icmp);

	bool save_manifest = false;
	Status s = version_set.Recover(&save_manifest);
	ASSERT_TRUE(s.ok()) << "Recover failed: " << s.ToString();
	EXPECT_TRUE(save_manifest);
	EXPECT_TRUE(HasCurrentFile());
	EXPECT_TRUE(HasManifestFile());

	// Verify the table file was recovered
	const std::vector<uint64_t> l0_files = CollectFileNumbers(version_set.current(), 0);
	ASSERT_EQ(1u, l0_files.size());
	EXPECT_EQ(10u, l0_files[0]);

	// CRITICAL: NextFileNumber() must be > 100 (the log number), not just > 10 (the table number)
	// This ensures the invariant edit->GetLogNumber() < next_file_number_ holds
	EXPECT_GT(version_set.NextFileNumber(), 100u) << "NextFileNumber must exceed highest log file number";

	// Leave log_number at zero so DB recovery can replay every legacy WAL.
	EXPECT_EQ(0u, version_set.LogNumber());
}

TEST_F(ManifestRecoveryTest, LegacyBootstrapReadsKeysFromRecoveredSst)
{
	ASSERT_TRUE(PlantLegacyDirectory(db_path_));

	auto res = OpenDB();
	ASSERT_TRUE(res.has_value()) << "Database::Open failed: " << res.error().ToString();

	auto alpha = db().Get("alpha");
	ASSERT_TRUE(alpha.has_value()) << "alpha missing after legacy SST bootstrap";
	EXPECT_EQ("table_alpha", alpha.value());

	auto omega = db().Get("omega");
	ASSERT_TRUE(omega.has_value()) << "omega missing after legacy SST bootstrap";
	EXPECT_EQ("table_omega", omega.value());
}

TEST_F(ManifestRecoveryTest, LegacyBootstrapReplaysAllLegacyLogs)
{
	Env* env = Env::Default();
	ASSERT_TRUE(env->CreateDir(db_path_).ok());

	auto write_legacy_log = [&](uint64_t file_number, uint64_t sequence, const std::string& key, const std::string& value) {
		auto writable = env->NewWritableFile(LogFileName(db_path_, file_number));
		ASSERT_TRUE(writable.has_value()) << writable.error().ToString();

		log::Writer writer(writable.value().get());
		WriteBatch batch;
		batch.Put(key, value);
		WriteBatchInternal::SetSequence(&batch, sequence);
		ASSERT_TRUE(writer.AddRecord(WriteBatchInternal::Contents(&batch)).ok());
		ASSERT_TRUE(writable.value()->Close().ok());
	};

	write_legacy_log(3, 1, "legacy_log_a", "value_a");
	write_legacy_log(7, 2, "legacy_log_b", "value_b");
	ASSERT_FALSE(HasCurrentFile());

	auto res = OpenDB();
	ASSERT_TRUE(res.has_value()) << "Database::Open failed: " << res.error().ToString();

	auto a = db().Get("legacy_log_a");
	ASSERT_TRUE(a.has_value()) << "legacy_log_a should be recovered from lower-numbered WAL";
	EXPECT_EQ("value_a", a.value());

	auto b = db().Get("legacy_log_b");
	ASSERT_TRUE(b.has_value()) << "legacy_log_b should be recovered from higher-numbered WAL";
	EXPECT_EQ("value_b", b.value());
}

TEST_F(ManifestRecoveryTest, RecoverUsesManifestAsSourceOfTruth)
{
	Options options;
	options.env = Env::Default();
	options.comparator = BytewiseComparator();

	TableCache table_cache(db_path_, options, 16);
	InternalKeyComparator icmp(options.comparator);

	VersionSet writer(db_path_, &options, &table_cache, &icmp);
	VersionEdit base;
	base.AddFile(0, 5, 1024, InternalKey("a", 9, kTypeValue), InternalKey("b", 9, kTypeValue));
	ASSERT_TRUE(ApplyEdit(&writer, &base).ok());

	while (writer.NextFileNumber() <= 8)
	{
		writer.NewFileNumber();
	}
	writer.SetLastSequence(123);

	VersionEdit with_metadata;
	with_metadata.SetLogNumber(7);
	with_metadata.SetPrevLogNumber(6);
	with_metadata.AddFile(1, 12, 2048, InternalKey("c", 9, kTypeValue), InternalKey("d", 9, kTypeValue));
	ASSERT_TRUE(ApplyEdit(&writer, &with_metadata).ok());

	ASSERT_TRUE(CreatePlaceholderFile(TableFileName(db_path_, 900)).ok());
	ASSERT_TRUE(CreatePlaceholderFile(LogFileName(db_path_, 901)).ok());

	VersionSet recovered(db_path_, &options, &table_cache, &icmp);
	bool save_manifest = false;
	Status s = recovered.Recover(&save_manifest);
	ASSERT_TRUE(s.ok()) << s.ToString();

	EXPECT_EQ(7u, recovered.LogNumber());
	EXPECT_EQ(6u, recovered.PrevLogNumber());
	EXPECT_EQ(123u, recovered.LastSequence());
	EXPECT_TRUE(recovered.NextFileNumber() > 901u);

	const std::vector<uint64_t> l0_files = CollectFileNumbers(recovered.current(), 0);
	const std::vector<uint64_t> l1_files = CollectFileNumbers(recovered.current(), 1);
	EXPECT_EQ((std::vector<uint64_t>{ 5 }), l0_files);
	EXPECT_EQ((std::vector<uint64_t>{ 12 }), l1_files);
	EXPECT_TRUE(save_manifest);
}

TEST_F(ManifestRecoveryTest, RecoverRestoresPersistedNextFileNumberExactly)
{
	Options options;
	options.env = Env::Default();
	options.comparator = BytewiseComparator();

	const uint64_t manifest_file_number = 17;
	const uint64_t persisted_next_file = 42;

	auto manifest = Env::Default()->NewWritableFile(DescriptorFileName(db_path_, manifest_file_number));
	ASSERT_TRUE(manifest.has_value()) << manifest.error().ToString();

	log::Writer writer(manifest.value().get());
	VersionEdit snapshot;
	snapshot.SetComparatorName(options.comparator->Name());
	snapshot.SetLogNumber(0);
	snapshot.SetPrevLogNumber(0);
	snapshot.SetNextFile(persisted_next_file);
	snapshot.SetLastSequence(0);

	std::string record;
	snapshot.EncodeTo(&record);
	ASSERT_TRUE(writer.AddRecord(record).ok());
	ASSERT_TRUE(manifest.value()->Close().ok());
	ASSERT_TRUE(SetCurrentFile(Env::Default(), db_path_, manifest_file_number).ok());

	TableCache table_cache(db_path_, options, 16);
	InternalKeyComparator icmp(options.comparator);

	VersionSet recovered(db_path_, &options, &table_cache, &icmp);
	bool save_manifest = false;
	Status s = recovered.Recover(&save_manifest);
	ASSERT_TRUE(s.ok()) << s.ToString();

	EXPECT_EQ(persisted_next_file, recovered.NextFileNumber());
	EXPECT_TRUE(save_manifest);
}

TEST_F(ManifestRecoveryTest, ReuseLogsDisabledDuringManifestRollout)
{
	Options options;
	options.env = Env::Default();
	options.comparator = BytewiseComparator();
	options.reuse_logs = true;

	TableCache table_cache(db_path_, options, 16);
	InternalKeyComparator icmp(options.comparator);

	VersionSet writer(db_path_, &options, &table_cache, &icmp);
	VersionEdit edit;
	edit.AddFile(0, 4, 100, InternalKey("a", 7, kTypeValue), InternalKey("a", 6, kTypeValue));
	ASSERT_TRUE(ApplyEdit(&writer, &edit).ok());

	VersionSet recovered(db_path_, &options, &table_cache, &icmp);
	bool save_manifest = false;
	Status s = recovered.Recover(&save_manifest);
	ASSERT_TRUE(s.ok()) << s.ToString();
	EXPECT_TRUE(save_manifest);
}

TEST_F(ManifestRecoveryTest, SequenceMappingSurvivesReopen)
{
	ASSERT_TRUE(OpenDB().has_value());
	ASSERT_TRUE(db().Put("off_by_one_key", "v1").ok());

	CloseDB();
	ASSERT_TRUE(OpenDB().has_value());
	ASSERT_TRUE(db().Delete("off_by_one_key").ok());
	ASSERT_TRUE(db().Put("survivor_key", "v2").ok());

	CloseDB();
	ASSERT_TRUE(OpenDB().has_value());

	auto deleted = db().Get("off_by_one_key");
	EXPECT_TRUE(deleted.error().IsNotFound()) << deleted.error().ToString();

	auto survivor = db().Get("survivor_key");
	ASSERT_TRUE(survivor.has_value()) << survivor.error().ToString();
	EXPECT_EQ("v2", survivor.value());
}

TEST_F(ManifestRecoveryTest, StaleTableFilesOutsideManifestAreIgnored)
{
	Options options;
	options.create_if_missing = true;
	options.write_buffer_size = 1;

	ASSERT_TRUE(OpenDB(options).has_value());
	ASSERT_TRUE(db().Put("stable_key", "stable_value").ok());
	CloseDB();

	ASSERT_TRUE(CreatePlaceholderFile(TableFileName(db_path_, 999)).ok());

	ASSERT_TRUE(OpenDB(options).has_value());
	auto value = db().Get("stable_key");
	ASSERT_TRUE(value.has_value()) << value.error().ToString();
	EXPECT_EQ("stable_value", value.value());
}

// ─────────────────────────────────────────────────────────────────────────────
// LSAN Regression Tests – verify no memory leaks on clean shutdown paths
// ─────────────────────────────────────────────────────────────────────────────

// Test 1: Fresh bootstrap – create DB and cleanly close
TEST_F(ManifestRecoveryTest, FreshBootstrapOpenCloseIsLsanClean)
{
	auto res = OpenDB();
	ASSERT_TRUE(res.has_value()) << "Database::Open failed: " << res.error().ToString();

	// Verify basic state
	EXPECT_TRUE(HasLockFile()) << "LOCK file missing after fresh open";
	EXPECT_TRUE(HasLogFile()) << "WAL log file missing after fresh open";

	// Close the DB – should clean up all resources without leaks
	CloseDB();
	// LSAN will validate no leaks on test exit
}

// Test 2: Recovery path – create DB, write data, close, reopen, close
TEST_F(ManifestRecoveryTest, ReopenRecoveryOpenCloseIsLsanClean)
{
	// First open: create DB and write data
	auto res1 = OpenDB();
	ASSERT_TRUE(res1.has_value()) << "Database::Open failed: " << res1.error().ToString();
	Status s = db().Put("recovery_key", "recovery_value");
	ASSERT_TRUE(s.ok()) << "Put failed: " << s.ToString();

	// Close the DB
	CloseDB();

	// Second open: recovery path reads manifest and recreates state
	auto res2 = OpenDB();
	ASSERT_TRUE(res2.has_value()) << "Database::Open (recovery) failed: " << res2.error().ToString();

	// Verify recovery worked
	auto get_res = db().Get("recovery_key");
	ASSERT_TRUE(get_res.has_value()) << "key missing after recovery";
	EXPECT_EQ("recovery_value", get_res.value());

	// Close again – should clean up all resources without leaks
	CloseDB();
	// LSAN will validate no leaks on test exit
}

// Test 3: Legacy bootstrap – recover from legacy directory, close cleanly
TEST_F(ManifestRecoveryTest, LegacyBootstrapRecoverOpenCloseIsLsanClean)
{
	// Plant legacy directory (no CURRENT or MANIFEST)
	ASSERT_TRUE(PlantLegacyDirectory(db_path_));
	ASSERT_FALSE(HasCurrentFile()) << "legacy directory should not have CURRENT yet";

	// Recover using VersionSet (this triggers legacy bootstrap)
	Options options;
	options.env = Env::Default();
	options.comparator = BytewiseComparator();

	TableCache table_cache(db_path_, options, 16);
	InternalKeyComparator icmp(options.comparator);
	VersionSet version_set(db_path_, &options, &table_cache, &icmp);

	bool save_manifest = false;
	Status s = version_set.Recover(&save_manifest);
	ASSERT_TRUE(s.ok()) << "Recover failed: " << s.ToString();
	EXPECT_TRUE(save_manifest) << "should need to save manifest after legacy recovery";
	EXPECT_TRUE(HasCurrentFile()) << "CURRENT file should exist after legacy recovery";
	EXPECT_TRUE(HasManifestFile()) << "MANIFEST file should exist after legacy recovery";

	// VersionSet dtor will clean up resources – LSAN verifies no leaks
}

// ---------------------------------------------------------------------------
// Snapshot ephemerality regression – a Snapshot captured before a DB close
// must be rejected (InvalidArgument) by the freshly-reopened instance because
// snapshots are bound to the SnapshotRegistry of the originating DBImpl.
// After reopen, the new DBImpl has a distinct registry; any attempt to use the
// stale handle through Get() or NewIterator() must return InvalidArgument.
// ---------------------------------------------------------------------------
TEST_F(ManifestRecoveryTest, SnapshotIsEphemeralAndRejectedAfterReopen)
{
	// Open and write a known value so we have something to read back later.
	ASSERT_TRUE(OpenDB().has_value());
	ASSERT_TRUE(db().Put("snap_key", "snap_val").ok());

	// Capture a snapshot from the current (about-to-be-closed) DB instance.
	// The snapshot is bound to that instance's SnapshotRegistry.
	Snapshot stale = db().CaptureSnapshot();

	// Close the DB – destroys the old DBImpl (and its SnapshotRegistry reference).
	// The stale Snapshot still holds a shared_ptr to the old registry, keeping it
	// alive, but the new open will produce a *different* registry pointer.
	CloseDB();
	ASSERT_TRUE(OpenDB().has_value());

	// ── Rejection via Get ─────────────────────────────────────────────────────
	// ResolveSnapshotSequence compares stale.state_->registry with the new
	// snapshot_registry_; they differ → InvalidSnapshotOwnerStatus (InvalidArgument).
	ReadOptions ro;
	ro.snapshot_handle = stale;

	auto get_res = db().Get(ro, "snap_key");
	EXPECT_FALSE(get_res.has_value())
	    << "Get with stale snapshot should fail, but returned: " << get_res.value();
	EXPECT_TRUE(get_res.error().IsInvalidArgument())
	    << "expected InvalidArgument from Get, got: " << get_res.error().ToString();

	// ── Rejection via NewIterator ─────────────────────────────────────────────
	// NewIterator wraps the error in a NewErrorIterator; Valid() is false and
	// status() carries the same InvalidArgument.
	auto iter = db().NewIterator(ro);
	EXPECT_FALSE(iter->Valid())
	    << "iterator with stale snapshot should not be valid";
	EXPECT_TRUE(iter->status().IsInvalidArgument())
	    << "expected InvalidArgument on iterator status, got: " << iter->status().ToString();

	// ── Normal read without snapshot succeeds ─────────────────────────────────
	// The key was persisted before the close; reopening recovers it via WAL/SST.
	auto plain_res = db().Get("snap_key");
	ASSERT_TRUE(plain_res.has_value())
	    << "plain Get (no snapshot) should find snap_key after reopen";
	EXPECT_EQ("snap_val", plain_res.value());

	// stale goes out of scope here; SnapshotState::~SnapshotState calls
	// registry->Release(node), cleanly unlinking the node from the old registry.
	// LSAN verifies no leaks on exit.
}

// ─────────────────────────────────────────────────────────────────────────────
// Invariant guard: reopen rejects ALL stale handles deterministically
//
// Opens a DB, writes data, captures a snapshot, closes the DB, reopens a new
// instance, and verifies that the stale snapshot is rejected for both Get()
// and NewIterator() with the correct error code (InvalidArgument).  Also
// verifies that a plain read (no snapshot) succeeds after reopen, confirming
// data persistence.  This is a stronger version of the existing test because
// it exercises multiple stale-handle paths in a single test and verifies
// determinism (same error code every time, not undefined behavior).
// ─────────────────────────────────────────────────────────────────────────────
TEST_F(ManifestRecoveryTest, ReopenRejectsAllStaleHandlesDeterministically)
{
	// Phase 1: open, write, capture snapshot, close.
	ASSERT_TRUE(OpenDB().has_value());
	ASSERT_TRUE(db().Put("stale_key", "original").ok());
	ASSERT_TRUE(db().Put("stale_key2", "original2").ok());

	Snapshot stale = db().CaptureSnapshot();

	ASSERT_TRUE(db().Put("stale_key", "overwritten").ok());

	CloseDB();

	// Phase 2: reopen and verify stale snapshot rejection.
	ASSERT_TRUE(OpenDB().has_value());

	ReadOptions ro;
	ro.snapshot_handle = stale;

	// Get with stale snapshot → InvalidArgument.
	auto get_res = db().Get(ro, "stale_key");
	EXPECT_FALSE(get_res.has_value()) << "Get with stale snapshot should fail";
	EXPECT_TRUE(get_res.error().IsInvalidArgument())
	    << "expected InvalidArgument, got: " << get_res.error().ToString();

	// Second Get with same stale snapshot → same error (deterministic).
	auto get_res2 = db().Get(ro, "stale_key2");
	EXPECT_FALSE(get_res2.has_value());
	EXPECT_TRUE(get_res2.error().IsInvalidArgument())
	    << "repeated stale-snapshot Get must return same error code";

	// NewIterator with stale snapshot → error iterator.
	auto iter = db().NewIterator(ro);
	EXPECT_FALSE(iter->Valid()) << "iterator with stale snapshot should not be valid";
	EXPECT_TRUE(iter->status().IsInvalidArgument())
	    << "expected InvalidArgument on iterator, got: " << iter->status().ToString();

	// Plain read (no snapshot) must succeed – data persisted before close.
	auto plain = db().Get("stale_key");
	ASSERT_TRUE(plain.has_value()) << "plain Get should find stale_key after reopen";
	EXPECT_EQ("overwritten", plain.value()) << "plain Get should see the latest value";

	auto plain2 = db().Get("stale_key2");
	ASSERT_TRUE(plain2.has_value()) << "plain Get should find stale_key2 after reopen";
	EXPECT_EQ("original2", plain2.value());
}
