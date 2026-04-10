// fault_injection_test.cpp
// Provides FaultInjectionEnv – an EnvWrapper that can deterministically fail
// individual file operations.  The wrapper is self-contained in this file so
// later tasks can include it as a helper header if needed; for now it lives
// here to keep the footprint minimal.

#include "db.h"
#include "env.h"
#include "filename.h"
#include "comparator.h"
#include "options.h"
#include "status.h"
#include "slice.h"
#include "table_cache.h"
#include "version_edit.h"
#include "version_set.h"

#include <gtest/gtest.h>
#include <atomic>
#include <memory>
#include <shared_mutex>
#include <string>
#include <filesystem>

using namespace prism;
namespace fs = std::filesystem;

// ===========================================================================
// FaultInjectionWritableFile – wraps a real WritableFile and can fail any of
// Append, Sync, Close on demand.
// ===========================================================================
class FaultInjectionWritableFile: public WritableFile
{
public:
	FaultInjectionWritableFile(std::unique_ptr<WritableFile> real, std::string filename, std::atomic<bool>* fail_append,
	    std::atomic<bool>* fail_sync, std::atomic<bool>* fail_close)
	    : real_(std::move(real))
	    , filename_(std::move(filename))
	    , fail_append_(fail_append)
	    , fail_sync_(fail_sync)
	    , fail_close_(fail_close)
	{
	}

	Status Append(const Slice& data) override
	{
		if (fail_append_ && fail_append_->load(std::memory_order_acquire))
			return Status::IOError(filename_, "injected Append failure");
		return real_->Append(data);
	}

	Status Flush() override { return real_->Flush(); }

	Status Sync() override
	{
		if (fail_sync_ && fail_sync_->load(std::memory_order_acquire))
			return Status::IOError(filename_, "injected Sync failure");
		return real_->Sync();
	}

	Status Close() override
	{
		if (fail_close_ && fail_close_->load(std::memory_order_acquire))
			return Status::IOError(filename_, "injected Close failure");
		return real_->Close();
	}

private:
	std::unique_ptr<WritableFile> real_;
	std::string filename_;
	std::atomic<bool>* fail_append_;
	std::atomic<bool>* fail_sync_;
	std::atomic<bool>* fail_close_;
};

// ===========================================================================
// FaultInjectionEnv – EnvWrapper with per-operation fault injection flags.
//
// Usage:
//   FaultInjectionEnv env(Env::Default());
//   env.SetFailNewWritableFile(true);   // next NewWritableFile returns IOError
//   auto res = env.NewWritableFile("/tmp/foo");
//   EXPECT_FALSE(res.has_value());
//   env.SetFailNewWritableFile(false);  // back to normal
//
// All flags are std::atomic<bool> so tests can toggle them from any thread
// without a mutex.  Each flag applies globally (to all files); for per-file
// targeting, extend with an allowed-list (see SetTargetFile).
// ===========================================================================
class FaultInjectionEnv: public EnvWrapper
{
public:
	explicit FaultInjectionEnv(Env* base)
	    : EnvWrapper(base)
	    , fail_new_writable_{ false }
	    , fail_new_appendable_{ false }
	    , fail_append_{ false }
	    , fail_sync_{ false }
	    , fail_close_{ false }
	    , fail_rename_{ false }
	    , fail_remove_{ false }
	    , fail_get_children_{ false }
	{
	}

	// ── Fault toggles ───────────────────────────────────────────────────────

	void SetFailNewWritableFile(bool v) { fail_new_writable_.store(v, std::memory_order_release); }
	void SetFailNewAppendableFile(bool v) { fail_new_appendable_.store(v, std::memory_order_release); }
	void SetFailAppend(bool v) { fail_append_.store(v, std::memory_order_release); }
	void SetFailSync(bool v) { fail_sync_.store(v, std::memory_order_release); }
	void SetFailClose(bool v) { fail_close_.store(v, std::memory_order_release); }
	void SetFailRename(bool v) { fail_rename_.store(v, std::memory_order_release); }
	void SetFailRemoveFile(bool v) { fail_remove_.store(v, std::memory_order_release); }
	void SetFailGetChildren(bool v) { fail_get_children_.store(v, std::memory_order_release); }

	// Reset all flags to non-failing.
	void ResetFaults()
	{
		fail_new_writable_.store(false, std::memory_order_release);
		fail_new_appendable_.store(false, std::memory_order_release);
		fail_append_.store(false, std::memory_order_release);
		fail_sync_.store(false, std::memory_order_release);
		fail_close_.store(false, std::memory_order_release);
		fail_rename_.store(false, std::memory_order_release);
		fail_remove_.store(false, std::memory_order_release);
		fail_get_children_.store(false, std::memory_order_release);
	}

	// ── Overridden Env methods ───────────────────────────────────────────────

	Result<std::unique_ptr<WritableFile>> NewWritableFile(const std::string& f) override
	{
		if (fail_new_writable_.load(std::memory_order_acquire))
			return std::unexpected(Status::IOError(f, "injected NewWritableFile failure"));

		auto res = target()->NewWritableFile(f);
		if (!res.has_value())
			return res;

		auto wrapped = std::make_unique<FaultInjectionWritableFile>(std::move(res.value()), f, &fail_append_, &fail_sync_, &fail_close_);
		return wrapped;
	}

	Result<std::unique_ptr<WritableFile>> NewAppendableFile(const std::string& f) override
	{
		if (fail_new_appendable_.load(std::memory_order_acquire))
			return std::unexpected(Status::IOError(f, "injected NewAppendableFile failure"));

		auto res = target()->NewAppendableFile(f);
		if (!res.has_value())
			return res;

		auto wrapped = std::make_unique<FaultInjectionWritableFile>(std::move(res.value()), f, &fail_append_, &fail_sync_, &fail_close_);
		return wrapped;
	}

	Status RenameFile(const std::string& src, const std::string& tgt) override
	{
		if (fail_rename_.load(std::memory_order_acquire))
			return Status::IOError(src, "injected RenameFile failure");
		return target()->RenameFile(src, tgt);
	}

	Status RemoveFile(const std::string& f) override
	{
		if (fail_remove_.load(std::memory_order_acquire))
			return Status::IOError(f, "injected RemoveFile failure");
		return target()->RemoveFile(f);
	}

	Result<std::vector<std::string>> GetChildren(const std::string& dir) override
	{
		if (fail_get_children_.load(std::memory_order_acquire))
			return std::unexpected(Status::IOError(dir, "injected GetChildren failure"));
		return target()->GetChildren(dir);
	}

private:
	std::atomic<bool> fail_new_writable_;
	std::atomic<bool> fail_new_appendable_;
	std::atomic<bool> fail_append_;
	std::atomic<bool> fail_sync_;
	std::atomic<bool> fail_close_;
	std::atomic<bool> fail_rename_;
	std::atomic<bool> fail_remove_;
	std::atomic<bool> fail_get_children_;
};

// ===========================================================================
// Test fixture
// ===========================================================================
class FaultInjectionTest: public ::testing::Test
{
protected:
	void SetUp() override
	{
		env_ = std::make_unique<FaultInjectionEnv>(Env::Default());

		auto test_dir_res = Env::Default()->GetTestDirectory();
		ASSERT_TRUE(test_dir_res.has_value());

		const ::testing::TestInfo* info = ::testing::UnitTest::GetInstance()->current_test_info();
		tmp_path_ = test_dir_res.value() + "/fault_injection_test_" + info->test_suite_name() + "_" + info->name();

		std::error_code ec;
		fs::remove_all(tmp_path_, ec);
		fs::create_directories(tmp_path_, ec);
		ASSERT_FALSE(ec) << ec.message();
	}

	void TearDown() override
	{
		env_.reset();
		std::error_code ec;
		fs::remove_all(tmp_path_, ec);
	}

	std::string TmpFile(const std::string& name) const { return tmp_path_ + "/" + name; }

	std::unique_ptr<FaultInjectionEnv> env_;
	std::string tmp_path_;
};

namespace
{
	std::string ReadCurrentManifest(const std::string& db_path)
	{
		std::string current_target;
		Status s = ReadFileToString(Env::Default(), CurrentFileName(db_path), &current_target);
		if (!s.ok())
		{
			return "";
		}
		if (!current_target.empty() && current_target.back() == '\n')
		{
			current_target.pop_back();
		}
		return current_target;
	}

	Status ApplyVersionEdit(VersionSet* version_set, VersionEdit* edit)
	{
		std::shared_mutex mu;
		mu.lock();
		Status s = version_set->LogAndApply(edit, &mu);
		mu.unlock();
		return s;
	}
}

// ===========================================================================
// Tests
// ===========================================================================

// ── NewWritableFile can be failed deterministically. ────────────────────────
TEST_F(FaultInjectionTest, InjectableEnvCanFailWritableFileCreation)
{
	// Without injection: file creation succeeds.
	{
		auto res = env_->NewWritableFile(TmpFile("ok.txt"));
		ASSERT_TRUE(res.has_value()) << "Expected success without fault injection";
		// Clean close.
		EXPECT_TRUE(res.value()->Close().ok());
	}

	// Inject failure.
	env_->SetFailNewWritableFile(true);
	{
		auto res = env_->NewWritableFile(TmpFile("fail.txt"));
		ASSERT_FALSE(res.has_value()) << "Expected failure with fault injection";
		EXPECT_TRUE(res.error().IsIOError()) << "Expected IOError, got: " << res.error().ToString();
	}

	// Clear injection: succeeds again.
	env_->SetFailNewWritableFile(false);
	{
		auto res = env_->NewWritableFile(TmpFile("ok2.txt"));
		ASSERT_TRUE(res.has_value()) << "Expected success after clearing fault injection";
		EXPECT_TRUE(res.value()->Close().ok());
	}
}

// ── Append failures surface through the WritableFile interface. ─────────────
TEST_F(FaultInjectionTest, InjectableEnvCanFailAppend)
{
	auto res = env_->NewWritableFile(TmpFile("append_test.txt"));
	ASSERT_TRUE(res.has_value());
	auto& wf = res.value();

	// Normal append succeeds.
	EXPECT_TRUE(wf->Append(Slice("hello")).ok());

	// Inject append failure.
	env_->SetFailAppend(true);
	Status s = wf->Append(Slice("world"));
	EXPECT_FALSE(s.ok()) << "Expected Append to fail with injection";
	EXPECT_TRUE(s.IsIOError());

	// Clear injection.
	env_->SetFailAppend(false);
	EXPECT_TRUE(wf->Append(Slice(" again")).ok());

	wf->Close();
}

// ── Sync failures surface correctly. ────────────────────────────────────────
TEST_F(FaultInjectionTest, InjectableEnvCanFailSync)
{
	auto res = env_->NewWritableFile(TmpFile("sync_test.txt"));
	ASSERT_TRUE(res.has_value());
	auto& wf = res.value();

	EXPECT_TRUE(wf->Append(Slice("data")).ok());

	env_->SetFailSync(true);
	Status s = wf->Sync();
	EXPECT_FALSE(s.ok());
	EXPECT_TRUE(s.IsIOError());

	env_->SetFailSync(false);
	EXPECT_TRUE(wf->Sync().ok());
	wf->Close();
}

// ── RenameFile failures surface correctly. ───────────────────────────────────
TEST_F(FaultInjectionTest, InjectableEnvCanFailRename)
{
	// Create a source file.
	{
		auto res = env_->NewWritableFile(TmpFile("src.txt"));
		ASSERT_TRUE(res.has_value());
		res.value()->Append(Slice("x"));
		res.value()->Close();
	}

	env_->SetFailRename(true);
	Status s = env_->RenameFile(TmpFile("src.txt"), TmpFile("dst.txt"));
	EXPECT_FALSE(s.ok());
	EXPECT_TRUE(s.IsIOError());

	env_->SetFailRename(false);
	EXPECT_TRUE(env_->RenameFile(TmpFile("src.txt"), TmpFile("dst.txt")).ok());
}

// ── RemoveFile failures surface correctly. ───────────────────────────────────
TEST_F(FaultInjectionTest, InjectableEnvCanFailRemoveFile)
{
	// Create a file to remove.
	{
		auto res = env_->NewWritableFile(TmpFile("to_remove.txt"));
		ASSERT_TRUE(res.has_value());
		res.value()->Close();
	}

	env_->SetFailRemoveFile(true);
	Status s = env_->RemoveFile(TmpFile("to_remove.txt"));
	EXPECT_FALSE(s.ok());
	EXPECT_TRUE(s.IsIOError());

	env_->SetFailRemoveFile(false);
	EXPECT_TRUE(env_->RemoveFile(TmpFile("to_remove.txt")).ok());
}

// ── ResetFaults clears all injection flags at once. ──────────────────────────
TEST_F(FaultInjectionTest, ResetFaultsClearsAllFlags)
{
	env_->SetFailNewWritableFile(true);
	env_->SetFailAppend(true);
	env_->SetFailSync(true);
	env_->SetFailRename(true);
	env_->SetFailRemoveFile(true);

	env_->ResetFaults();

	// NewWritableFile should work again.
	auto res = env_->NewWritableFile(TmpFile("reset_test.txt"));
	ASSERT_TRUE(res.has_value()) << "Expected success after ResetFaults";

	auto& wf = res.value();
	EXPECT_TRUE(wf->Append(Slice("data")).ok());
	EXPECT_TRUE(wf->Sync().ok());
	wf->Close();
}

TEST_F(FaultInjectionTest, CrashAfterManifestSyncBeforeCurrent)
{
	Options options;
	options.env = env_.get();
	options.comparator = BytewiseComparator();
	options.create_if_missing = true;

	TableCache table_cache(tmp_path_, options, 16);
	InternalKeyComparator icmp(options.comparator);
	VersionSet version_set(tmp_path_, &options, &table_cache, &icmp);

	VersionEdit edit;
	edit.AddFile(0, 101, 4096, InternalKey("a", 100, kTypeValue), InternalKey("z", 100, kTypeValue));

	std::shared_mutex mu;
	mu.lock();
	env_->SetFailRename(true);
	Status s = version_set.LogAndApply(&edit, &mu);
	env_->SetFailRename(false);
	mu.unlock();

	EXPECT_FALSE(s.ok());
	EXPECT_TRUE(s.IsIOError());
	EXPECT_FALSE(Env::Default()->FileExists(CurrentFileName(tmp_path_)));
	EXPECT_EQ(0u, version_set.current()->files(0).size());
}

TEST_F(FaultInjectionTest, FailedManifestRewritePreservesCurrentManifestAfterRecover)
{
	Options options;
	options.env = env_.get();
	options.comparator = BytewiseComparator();
	options.create_if_missing = true;

	TableCache table_cache(tmp_path_, options, 16);
	InternalKeyComparator icmp(options.comparator);

	VersionSet writer(tmp_path_, &options, &table_cache, &icmp);
	VersionEdit initial;
	initial.AddFile(0, 101, 4096, InternalKey("a", 100, kTypeValue), InternalKey("m", 100, kTypeValue));
	ASSERT_TRUE(ApplyVersionEdit(&writer, &initial).ok());

	const std::string original_manifest_name = ReadCurrentManifest(tmp_path_);
	ASSERT_FALSE(original_manifest_name.empty());
	ASSERT_TRUE(Env::Default()->FileExists(tmp_path_ + "/" + original_manifest_name));

	VersionSet recovered(tmp_path_, &options, &table_cache, &icmp);
	bool save_manifest = false;
	ASSERT_TRUE(recovered.Recover(&save_manifest).ok());

	VersionEdit failing_edit;
	failing_edit.AddFile(0, 202, 4096, InternalKey("n", 100, kTypeValue), InternalKey("z", 100, kTypeValue));

	env_->SetFailSync(true);
	Status s = ApplyVersionEdit(&recovered, &failing_edit);
	env_->SetFailSync(false);

	EXPECT_FALSE(s.ok());
	EXPECT_TRUE(s.IsIOError());
	EXPECT_EQ(original_manifest_name, ReadCurrentManifest(tmp_path_));
	EXPECT_TRUE(Env::Default()->FileExists(tmp_path_ + "/" + original_manifest_name));

	VersionSet reopen(tmp_path_, &options, &table_cache, &icmp);
	bool reopen_save_manifest = false;
	ASSERT_TRUE(reopen.Recover(&reopen_save_manifest).ok());

	ASSERT_EQ(1u, reopen.current()->files(0).size());
	EXPECT_EQ(101u, reopen.current()->files(0)[0]->number);
}

// ── WAL Append failure propagates cleanly through write path. ────────────────
// Characterizes: When WAL append fails, the write returns the error, and the DB
// remains stable (no crash, no ownership corruption). The DB can be reopened.
TEST_F(FaultInjectionTest, WalAppendFailurePropagatesCleanly)
{
	Options options;
	options.env = env_.get();
	options.comparator = BytewiseComparator();
	options.create_if_missing = true;

	// Open DB and write some data successfully.
	auto db_result = DB::Open(options, tmp_path_);
	ASSERT_TRUE(db_result.has_value()) << db_result.error().ToString();
	auto db = std::move(db_result.value());

	Status s = db->Put(WriteOptions(), "key1", "value1");
	ASSERT_TRUE(s.ok()) << s.ToString();

	// Inject append failure on the WAL file.
	env_->SetFailAppend(true);

	// Attempt a write - should fail with IOError.
	s = db->Put(WriteOptions(), "key2", "value2");
	EXPECT_FALSE(s.ok()) << "Expected write to fail with injected append error";
	EXPECT_TRUE(s.IsIOError()) << "Expected IOError, got: " << s.ToString();

	// Clear fault injection.
	env_->SetFailAppend(false);

	// Close the DB.
	db.reset();

	// Reopen the DB - should succeed and recover the first write.
	auto reopen_result = DB::Open(options, tmp_path_);
	ASSERT_TRUE(reopen_result.has_value()) << reopen_result.error().ToString();
	auto reopened_db = std::move(reopen_result.value());

	// Verify the first write is present.
	auto get_result = reopened_db->Get(ReadOptions(), "key1");
	EXPECT_TRUE(get_result.has_value()) << get_result.error().ToString();
	EXPECT_EQ(get_result.value(), "value1");

	// Verify the failed write was not persisted.
	auto get2_result = reopened_db->Get(ReadOptions(), "key2");
	EXPECT_FALSE(get2_result.has_value()) << "Expected key2 to not exist";
}

// ── WAL Close failure surfaces but doesn't poison recovery. ───────────────────
// Characterizes: When WAL close fails, the error is surfaced, and subsequent
// recovery/open path remains deterministic per characterized behavior.
TEST_F(FaultInjectionTest, WalCloseFailureDoesNotPoisonReopen)
{
	Options options;
	options.env = env_.get();
	options.comparator = BytewiseComparator();
	options.create_if_missing = true;

	// Open DB and write some data successfully.
	auto db_result = DB::Open(options, tmp_path_);
	ASSERT_TRUE(db_result.has_value()) << db_result.error().ToString();
	auto db = std::move(db_result.value());

	Status s = db->Put(WriteOptions(), "key1", "value1");
	ASSERT_TRUE(s.ok()) << s.ToString();

	// Inject close failure on the WAL file.
	env_->SetFailClose(true);

	// Close the DB - destructor calls CloseLogFile which should fail.
	// The error is logged/recorded but the DB should still be destructible.
	db.reset();

	// Clear fault injection for reopen.
	env_->SetFailClose(false);

	// Reopen the DB - should succeed and recover the write.
	auto reopen_result = DB::Open(options, tmp_path_);
	ASSERT_TRUE(reopen_result.has_value()) << reopen_result.error().ToString();
	auto reopened_db = std::move(reopen_result.value());

	// Verify the write is present.
	auto get_result = reopened_db->Get(ReadOptions(), "key1");
	EXPECT_TRUE(get_result.has_value()) << get_result.error().ToString();
	EXPECT_EQ(get_result.value(), "value1");
}
