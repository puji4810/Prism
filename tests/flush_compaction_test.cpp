#include "db.h"
#include "comparator.h"
#include "db_impl.h"
#include "env.h"
#include "filename.h"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <gtest/gtest.h>
#include <mutex>
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

		void HoldScheduledWork(bool hold)
		{
			std::lock_guard<std::mutex> lock(mu_);
			hold_scheduled_work_ = hold;
			if (!hold_scheduled_work_)
			{
				cv_.notify_all();
			}
		}

		void ReleaseScheduledWork() { HoldScheduledWork(false); }

		int ScheduledCalls() const { return scheduled_calls_.load(std::memory_order_acquire); }
		int SleepCalls() const { return sleep_calls_.load(std::memory_order_acquire); }

		void Schedule(void (*function)(void*), void* arg) override
		{
			scheduled_calls_.fetch_add(1, std::memory_order_release);
			std::thread([this, function, arg] {
				std::unique_lock<std::mutex> lock(mu_);
				cv_.wait(lock, [this] { return !hold_scheduled_work_; });
				lock.unlock();
				function(arg);
			}).detach();
		}

		void SleepForMicroseconds(int micros) override
		{
			if (micros == 1000)
			{
				sleep_calls_.fetch_add(1, std::memory_order_release);
			}
			target()->SleepForMicroseconds(micros);
		}

	private:
		mutable std::mutex mu_;
		std::condition_variable cv_;
		bool hold_scheduled_work_ = true;
		std::atomic<int> scheduled_calls_{ 0 };
		std::atomic<int> sleep_calls_{ 0 };
	};

	void CreateLegacyL0Files(const std::string& dbname, int count, uint64_t first_number)
	{
		std::filesystem::create_directories(dbname);
		for (int i = 0; i < count; ++i)
		{
			const std::string filename = TableFileName(dbname, first_number + static_cast<uint64_t>(i));
			std::ofstream out(filename, std::ios::binary);
			ASSERT_TRUE(out.is_open());
			out << "legacy_l0";
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
	}
};

TEST_F(FlushCompactionTest, WriteRotatesMemtableAndSchedulesBackgroundFlush)
{
	ControlledEnv env(Env::Default());
	env.HoldScheduledWork(true);

	Options options;
	options.env = &env;
	options.create_if_missing = true;
	options.write_buffer_size = 128;

	auto open = DB::Open(options, "test_flush_compaction");
	ASSERT_TRUE(open.has_value()) << open.error().ToString();
	auto db = std::move(open.value());
	auto* impl = static_cast<DBImpl*>(db.get());

	for (int i = 0; i < 32; ++i)
	{
		ASSERT_TRUE(db->Put("k" + std::to_string(i), std::string(64, 'v')).ok());
		if (impl->TEST_HasImmutableMemTable())
		{
			break;
		}
	}

	EXPECT_TRUE(impl->TEST_HasImmutableMemTable());
	EXPECT_GE(env.ScheduledCalls(), 1);

	env.ReleaseScheduledWork();
	EXPECT_TRUE(WaitUntil([impl] { return !impl->TEST_HasImmutableMemTable(); }, std::chrono::milliseconds(2000)));
}

TEST_F(FlushCompactionTest, WriterWaitsWhenImmutableMemtableExists)
{
	ControlledEnv env(Env::Default());
	env.HoldScheduledWork(true);

	Options options;
	options.env = &env;
	options.create_if_missing = true;
	options.write_buffer_size = 128;

	auto open = DB::Open(options, "test_flush_stall");
	ASSERT_TRUE(open.has_value()) << open.error().ToString();
	auto db = std::move(open.value());
	auto* impl = static_cast<DBImpl*>(db.get());

	for (int i = 0; i < 32; ++i)
	{
		ASSERT_TRUE(db->Put("seed" + std::to_string(i), std::string(64, 'x')).ok());
		if (impl->TEST_HasImmutableMemTable())
		{
			break;
		}
	}
	ASSERT_TRUE(impl->TEST_HasImmutableMemTable());

	std::atomic<bool> writer_done{ false };
	Status writer_status;
	std::thread writer([&] {
		writer_status = db->Put("blocked", "value");
		writer_done.store(true, std::memory_order_release);
	});

	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	EXPECT_FALSE(writer_done.load(std::memory_order_acquire));

	env.ReleaseScheduledWork();
	writer.join();
	EXPECT_TRUE(writer_status.ok()) << writer_status.ToString();
}

TEST_F(FlushCompactionTest, LevelZeroPressureTriggersStallPolicy)
{
	{
		ControlledEnv env(Env::Default());
		env.HoldScheduledWork(false);
		CreateLegacyL0Files("test_l0_slowdown", 8, 1000);

		Options options;
		options.env = &env;
		options.create_if_missing = true;

		auto open = DB::Open(options, "test_l0_slowdown");
		ASSERT_TRUE(open.has_value()) << open.error().ToString();
		auto db = std::move(open.value());

		Status s = db->Put("slow", "path");
		EXPECT_TRUE(s.ok()) << s.ToString();
		EXPECT_GE(env.SleepCalls(), 1);
	}

	{
		ControlledEnv env(Env::Default());
		env.HoldScheduledWork(false);
		CreateLegacyL0Files("test_l0_stop", 12, 2000);

		Options options;
		options.env = &env;
		options.create_if_missing = true;

		auto open = DB::Open(options, "test_l0_stop");
		ASSERT_TRUE(open.has_value()) << open.error().ToString();
		auto db = std::move(open.value());
		auto* impl = static_cast<DBImpl*>(db.get());

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

	auto open = DB::Open(options, "test_flush_compaction");
	ASSERT_TRUE(open.has_value()) << open.error().ToString();
	auto db = std::move(open.value());
	auto* impl = static_cast<DBImpl*>(db.get());

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
