#include "db.h"
#include "comparator.h"
#include "db_impl.h"
#include "env.h"
#include "filename.h"
#include "table/table_builder.h"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <gtest/gtest.h>
#include <shared_mutex>
#include <string>
#include <thread>

using namespace prism;

namespace
{
	class ControlledEnv final: public EnvWrapper
	{
	public:
		explicit ControlledEnv(Env* target)
		    : EnvWrapper(target)
		{
		}

		int SleepCalls() const { return sleep_calls_.load(std::memory_order_acquire); }

		void SleepForMicroseconds(int micros) override
		{
			if (micros == 1000)
			{
				sleep_calls_.fetch_add(1, std::memory_order_release);
			}
			target()->SleepForMicroseconds(micros);
		}

	private:
		std::atomic<int> sleep_calls_{ 0 };
	};

	void CreateLegacyL0Files(const std::string& dbname, int count, uint64_t first_number)
	{
		std::filesystem::create_directories(dbname);
		Options table_options;
		InternalKeyComparator icmp(BytewiseComparator());
		table_options.comparator = &icmp;
		table_options.env = Env::Default();
		table_options.compression = CompressionType::kNoCompression;
		for (int i = 0; i < count; ++i)
		{
			const std::string filename = TableFileName(dbname, first_number + static_cast<uint64_t>(i));
			auto writable = Env::Default()->NewWritableFile(filename);
			ASSERT_TRUE(writable.has_value()) << writable.error().ToString();

			TableBuilder builder(table_options, writable.value().get());
			const std::string user_key = "legacy_l0_" + std::to_string(i);
			builder.Add(InternalKey(user_key, 1, kTypeValue).Encode(), Slice("value"));
			ASSERT_TRUE(builder.Finish().ok());
			ASSERT_TRUE(writable.value()->Close().ok());
		}
	}

	bool WaitUntil(const std::function<bool()>& condition, std::chrono::milliseconds timeout)
	{
		const auto deadline = std::chrono::steady_clock::now() + timeout;
		while (std::chrono::steady_clock::now() < deadline)
		{
			if (condition())
			{
				return true;
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
		}
		return condition();
	}

	FileMetaData* MakeFileMeta(uint64_t number, uint64_t file_size, const std::string& smallest, const std::string& largest)
	{
		auto* file = new FileMetaData();
		file->number = number;
		file->file_size = file_size;
		file->smallest = InternalKey(smallest, 100, kTypeValue);
		file->largest = InternalKey(largest, 100, kTypeValue);
		return file;
	}
}

// ===========================================================================
// TODO(wal-rotation): Test coverage for obsolete-file lifecycle guard
// - WAL rotation + flush/compaction lifecycle integration
// - retired WAL not deleted before memtable flush + VersionSet advance
// - grouped append/sync within rotation/obsolescence boundaries
// ===========================================================================

class FlushCompactionTest: public ::testing::Test
{
protected:
	void TearDown() override
	{
		std::error_code ec;
		std::filesystem::remove_all("test_flush_compaction", ec);
		std::filesystem::remove_all("test_flush_stall", ec);
		std::filesystem::remove_all("test_l0_slowdown", ec);
		std::filesystem::remove_all("test_l0_stop", ec);
		std::filesystem::remove_all("test_bg_error_stall", ec);
		std::filesystem::remove_all("test_single_flight", ec);
		std::filesystem::remove_all("test_obsolete_files", ec);
	}
};

TEST_F(FlushCompactionTest, WriteRotatesMemtableAndSchedulesBackgroundFlush)
{
	ControlledEnv env(Env::Default());

	Options options;
	options.env = &env;
	options.create_if_missing = true;
	options.write_buffer_size = 128;

	auto open = DBImpl::OpenInternal(options, "test_flush_compaction");
	ASSERT_TRUE(open.has_value()) << open.error().ToString();
	auto db = std::move(open.value());
	auto* impl = db.get();
	impl->TEST_HoldBackgroundCompaction(true);

	for (int i = 0; i < 32; ++i)
	{
		ASSERT_TRUE(db->Put("k" + std::to_string(i), std::string(64, 'v')).ok());
		if (impl->TEST_HasImmutableMemTable())
		{
			break;
		}
	}

	EXPECT_TRUE(impl->TEST_HasImmutableMemTable());
	EXPECT_TRUE(WaitUntil([impl] { return impl->TEST_BackgroundCompactionStartCount() >= 1; }, std::chrono::milliseconds(2000)));

	impl->TEST_HoldBackgroundCompaction(false);
	EXPECT_TRUE(WaitUntil([impl] { return !impl->TEST_HasImmutableMemTable(); }, std::chrono::milliseconds(2000)));
}

TEST_F(FlushCompactionTest, WriterWaitsWhenImmutableMemtableExists)
{
	ControlledEnv env(Env::Default());

	Options options;
	options.env = &env;
	options.create_if_missing = true;
	options.write_buffer_size = 128;

	auto open = DBImpl::OpenInternal(options, "test_flush_stall");
	ASSERT_TRUE(open.has_value()) << open.error().ToString();
	auto db = std::move(open.value());
	auto* impl = db.get();
	impl->TEST_HoldBackgroundCompaction(true);

	for (int i = 0; i < 32; ++i)
	{
		ASSERT_TRUE(db->Put("seed" + std::to_string(i), std::string(64, 'x')).ok());
		if (impl->TEST_HasImmutableMemTable())
		{
			break;
		}
	}
	ASSERT_TRUE(impl->TEST_HasImmutableMemTable());
	ASSERT_TRUE(WaitUntil([impl] { return impl->TEST_BackgroundCompactionStartCount() >= 1; }, std::chrono::milliseconds(2000)));

	std::atomic<bool> writer_done{ false };
	Status writer_status;
	std::thread writer([&] {
		writer_status = db->Put("blocked", "value");
		writer_done.store(true, std::memory_order_release);
	});

	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	EXPECT_FALSE(writer_done.load(std::memory_order_acquire));

	impl->TEST_HoldBackgroundCompaction(false);
	writer.join();
	EXPECT_TRUE(writer_status.ok()) << writer_status.ToString();
}

TEST_F(FlushCompactionTest, LevelZeroPressureTriggersStallPolicy)
{
	{
		ControlledEnv env(Env::Default());
		CreateLegacyL0Files("test_l0_slowdown", 8, 1000);

		Options options;
		options.env = &env;
		options.create_if_missing = true;

		auto open = DBImpl::OpenInternal(options, "test_l0_slowdown");
		ASSERT_TRUE(open.has_value()) << open.error().ToString();
		auto db = std::move(open.value());

		Status s = db->Put("slow", "path");
		EXPECT_TRUE(s.ok()) << s.ToString();
		EXPECT_GE(env.SleepCalls(), 1);
	}

	{
		ControlledEnv env(Env::Default());
		CreateLegacyL0Files("test_l0_stop", 12, 2000);

		Options options;
		options.env = &env;
		options.create_if_missing = true;

		auto open = DBImpl::OpenInternal(options, "test_l0_stop");
		ASSERT_TRUE(open.has_value()) << open.error().ToString();
		auto db = std::move(open.value());
		auto* impl = db.get();

		std::atomic<bool> writer_done{ false };
		Status writer_status;
		std::thread writer([&] {
			writer_status = db->Put("stop", "write");
			writer_done.store(true, std::memory_order_release);
		});

		std::this_thread::sleep_for(std::chrono::milliseconds(50));
		EXPECT_FALSE(writer_done.load(std::memory_order_acquire));

		impl->TEST_SetBackgroundError(Status::IOError("forced stop-unblock"));
		impl->TEST_SignalBackgroundWorkFinished();
		writer.join();
		EXPECT_FALSE(writer_status.ok());
	}
}

TEST_F(FlushCompactionTest, FlushInstallsLevelZeroTableViaVersionEdit)
{
	const std::string dbname = "test_flush_compaction";
	std::filesystem::create_directories(dbname);

	Options options;
	InternalKeyComparator icmp(BytewiseComparator());
	VersionSet vset(dbname, &options, nullptr, &icmp);

	const int l0_before = static_cast<int>(vset.current()->files(0).size());
	VersionEdit edit;
	edit.AddFile(0, 7, 4096, InternalKey("a", 100, kTypeValue), InternalKey("z", 100, kTypeValue));

	std::shared_mutex mu;
	mu.lock();
	ASSERT_TRUE(vset.LogAndApply(&edit, &mu).ok());
	mu.unlock();

	EXPECT_GT(static_cast<int>(vset.current()->files(0).size()), l0_before);
}

TEST_F(FlushCompactionTest, PickLevelForMemTableOutputStopsAtConfiguredMaxLevel)
{
	Options options;
	InternalKeyComparator icmp(BytewiseComparator());
	VersionSet vset(&options, icmp);

	Version* v = vset.NewVersion();
	v->Ref();
	v->AddFile(4, MakeFileMeta(101, 1024, "m", "n"));

	EXPECT_EQ(v->PickLevelForMemTableOutput("a", "b"), config::kMaxMemCompactLevel);

	v->Unref();
}

TEST_F(FlushCompactionTest, EmptyMemtableDoesNotAddManifestFileEntry)
{
	Options options;
	options.create_if_missing = true;

	auto open = DBImpl::OpenInternal(options, "test_flush_compaction");
	ASSERT_TRUE(open.has_value()) << open.error().ToString();
	auto db = std::move(open.value());
	auto* impl = db.get();

	EXPECT_FALSE(impl->TEST_HasImmutableMemTable());
	const int l0_before = impl->TEST_NumLevelFiles(0);
	impl->TEST_SignalBackgroundWorkFinished();
	std::this_thread::sleep_for(std::chrono::milliseconds(20));
	EXPECT_EQ(impl->TEST_NumLevelFiles(0), l0_before);

	int table_files = 0;
	for (const auto& entry : std::filesystem::directory_iterator("test_flush_compaction"))
	{
		uint64_t number = 0;
		FileType type;
		if (ParseFileName(entry.path().filename().string(), &number, &type) && type == FileType::kTableFile)
		{
			++table_files;
		}
	}
	EXPECT_EQ(table_files, 0);
}

TEST_F(FlushCompactionTest, BackgroundErrorPreventsCompactionScheduling)
{
	ControlledEnv env(Env::Default());

	Options options;
	options.env = &env;
	options.create_if_missing = true;
	options.write_buffer_size = 128;

	auto open = DBImpl::OpenInternal(options, "test_bg_error_stall");
	ASSERT_TRUE(open.has_value()) << open.error().ToString();
	auto db = std::move(open.value());
	auto* impl = db.get();

	// Inject background error before any writes.
	// MaybeScheduleCompaction checks bg_error_ before scheduling.
	impl->TEST_SetBackgroundError(Status::IOError("injected compaction stall"));

	for (int i = 0; i < 32; ++i)
	{
		Status s = db->Put("k" + std::to_string(i), std::string(64, 'v'));
		// Writes will fail once the memtable fills and bg_error_ blocks rotation.
		if (!s.ok())
		{
			EXPECT_TRUE(s.IsIOError()) << s.ToString();
			break;
		}
	}

	// bg_error_ gates MaybeScheduleCompaction — no background lane should have started.
	EXPECT_EQ(impl->TEST_BackgroundCompactionStartCount(), 0);
}

TEST_F(FlushCompactionTest, CompactionSingleFlightPreservedWithStructController)
{
	ControlledEnv env(Env::Default());

	Options options;
	options.env = &env;
	options.create_if_missing = true;
	options.write_buffer_size = 128;

	auto open = DBImpl::OpenInternal(options, "test_single_flight");
	ASSERT_TRUE(open.has_value()) << open.error().ToString();
	auto db = std::move(open.value());
	auto* impl = db.get();
	impl->TEST_HoldBackgroundCompaction(true);

	// Write until the memtable fills and an imm_ is created.
	// The controller should launch exactly one background flush lane.
	for (int i = 0; i < 32; ++i)
	{
		ASSERT_TRUE(db->Put("k" + std::to_string(i), std::string(64, 'v')).ok());
		if (impl->TEST_HasImmutableMemTable())
		{
			break;
		}
	}
	ASSERT_TRUE(impl->TEST_HasImmutableMemTable());
	ASSERT_TRUE(WaitUntil([impl] { return impl->TEST_HasInFlightCompaction(); }, std::chrono::milliseconds(2000)));
	ASSERT_TRUE(WaitUntil([impl] { return impl->TEST_BackgroundCompactionStartCount() >= 1; }, std::chrono::milliseconds(2000)));
	const int scheduled_first = impl->TEST_BackgroundCompactionStartCount();
	EXPECT_TRUE(impl->TEST_HasInFlightCompaction());

	impl->TEST_ScheduleCompaction();
	impl->TEST_ScheduleCompaction();
	impl->TEST_ScheduleCompaction();
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	EXPECT_EQ(impl->TEST_BackgroundCompactionStartCount(), scheduled_first);
	EXPECT_TRUE(impl->TEST_HasInFlightCompaction());

	// A second writer will block on the CV in MakeRoomForWrite (imm_ is set).
	// The controller lane is still active, so MaybeScheduleCompaction should
	// bail without a second background flush attempt.
	std::atomic<bool> writer_done{ false };
	Status writer_status;
	std::thread writer([&] {
		writer_status = db->Put("blocked", std::string(64, 'v'));
		writer_done.store(true, std::memory_order_release);
	});

	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	EXPECT_FALSE(writer_done.load(std::memory_order_acquire));

	// Single-flight invariant: no additional background flush attempt.
	EXPECT_EQ(impl->TEST_BackgroundCompactionStartCount(), scheduled_first);

	impl->TEST_HoldBackgroundCompaction(false);
	EXPECT_TRUE(WaitUntil([impl] { return !impl->TEST_HasInFlightCompaction(); }, std::chrono::milliseconds(2000)));
	EXPECT_FALSE(impl->TEST_HasImmutableMemTable());
	writer.join();
	EXPECT_TRUE(writer_status.ok()) << writer_status.ToString();
}

TEST_F(FlushCompactionTest, ObsoleteFileLifecycleGuard)
{
	ControlledEnv env(Env::Default());

	Options options;
	options.env = &env;
	options.create_if_missing = true;
	options.write_buffer_size = 128;

	auto open = DBImpl::OpenInternal(options, "test_obsolete_files");
	ASSERT_TRUE(open.has_value()) << open.error().ToString();
	auto db = std::move(open.value());
	auto* impl = db.get();

	// Write enough entries to fill the memtable, trigger a WAL rotation,
	// and flush the imm_ to an L0 SST file.  Small write_buffer_size ensures
	// the rotation happens within a few writes.  Wait for the background
	// flush to drain before proceeding.
	for (int i = 0; i < 32; ++i)
	{
		ASSERT_TRUE(db->Put("k" + std::to_string(i), std::string(64, 'v')).ok());
	}
	ASSERT_TRUE(WaitUntil([impl] { return !impl->TEST_HasImmutableMemTable(); }, std::chrono::milliseconds(5000)));

	// After the flush completes, the version set should reference at least
	// one file in level 0 — proving the flush output was correctly installed.
	EXPECT_GE(impl->TEST_NumLevelFiles(0), 1);

	// Reopen the DB to verify the flushed data survives and the
	// version-set metadata is consistent with on-disk state.
	db.reset();
	auto reopen = DBImpl::OpenInternal(options, "test_obsolete_files");
	ASSERT_TRUE(reopen.has_value()) << reopen.error().ToString();
	db = std::move(reopen.value());
	const auto get = db->Get("k0");
	ASSERT_TRUE(get.has_value()) << get.error().ToString();
	EXPECT_EQ(get.value(), std::string(64, 'v'));
}
