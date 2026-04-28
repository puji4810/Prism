#include "comparator.h"
#include "db_impl.h"
#include "env.h"
#include "filename.h"
#include "table/table_builder.h"
#include "runtime_metrics.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <gtest/gtest.h>
#include <memory>
#include <semaphore>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace prism;
using namespace std::chrono_literals;

namespace
{
	struct TableEntry
	{
		std::string user_key;
		SequenceNumber sequence;
		ValueType type;
		std::string value;
	};

	bool WaitUntil(const std::function<bool()>& condition, std::chrono::milliseconds timeout)
	{
		const auto deadline = std::chrono::steady_clock::now() + timeout;
		while (std::chrono::steady_clock::now() < deadline)
		{
			if (condition())
			{
				return true;
			}
			std::this_thread::sleep_for(5ms);
		}
		return condition();
	}

	void FillUntilImmutable(DBImpl* db, const std::string& prefix, std::vector<std::pair<std::string, std::string>>* kvs = nullptr)
	{
		for (int i = 0; i < 64; ++i)
		{
			const std::string key = prefix + std::to_string(i);
			const std::string value(64, static_cast<char>('a' + (i % 26)));
			ASSERT_TRUE(db->Put(key, value).ok());
			if (kvs != nullptr)
			{
				kvs->push_back({ key, value });
			}
			if (db->TEST_HasImmutableMemTable())
			{
				return;
			}
		}
		FAIL() << "immutable memtable was not created";
	}

	class BlockingTableFileEnv final: public EnvWrapper
	{
	public:
		explicit BlockingTableFileEnv(Env* target)
		    : EnvWrapper(target)
		{
		}

		void EnableAppendBlock() noexcept
		{
			block_table_appends_.store(true, std::memory_order_release);
			append_block_consumed_.store(false, std::memory_order_release);
		}

		void DisableAppendBlock() noexcept { block_table_appends_.store(false, std::memory_order_release); }

		bool WaitForBlockedAppend(std::chrono::milliseconds timeout) { return append_started_.try_acquire_for(timeout); }

		void ReleaseBlockedAppend() { release_append_.release(); }

		Result<std::unique_ptr<WritableFile>> NewWritableFile(const std::string& fname) override
		{
			auto file = target()->NewWritableFile(fname);
			if (!file.has_value())
			{
				return file;
			}

			uint64_t number = 0;
			FileType type;
			const std::filesystem::path path(fname);
			if (ParseFileName(path.filename().string(), &number, &type) && type == FileType::kTableFile)
			{
				return std::unique_ptr<WritableFile>(new BlockingWritableFile(this, std::move(file.value())));
			}

			return file;
		}

	private:
		class BlockingWritableFile final: public WritableFile
		{
		public:
			BlockingWritableFile(BlockingTableFileEnv* env, std::unique_ptr<WritableFile> target)
			    : env_(env)
			    , target_(std::move(target))
			{
			}

			Status Append(const Slice& data) override
			{
				if (env_->block_table_appends_.load(std::memory_order_acquire))
				{
					bool expected = false;
					if (env_->append_block_consumed_.compare_exchange_strong(expected, true, std::memory_order_acq_rel))
					{
						env_->append_started_.release();
						env_->release_append_.acquire();
					}
				}
				return target_->Append(data);
			}

			Status Close() override { return target_->Close(); }
			Status Flush() override { return target_->Flush(); }
			Status Sync() override { return target_->Sync(); }

		private:
			BlockingTableFileEnv* env_;
			std::unique_ptr<WritableFile> target_;
		};

		std::atomic<bool> block_table_appends_{ false };
		std::atomic<bool> append_block_consumed_{ false };
		std::binary_semaphore append_started_{ 0 };
		std::binary_semaphore release_append_{ 0 };
	};

	void AddTableFile(DBImpl* db, const std::string& dbname, int level, const std::vector<TableEntry>& entries)
	{
		ASSERT_FALSE(entries.empty());
		struct EncodedEntry
		{
			std::string key;
			std::string value;
			InternalKey internal_key;
		};
		std::vector<EncodedEntry> encoded;
		encoded.reserve(entries.size());
		InternalKeyComparator icmp(BytewiseComparator());
		for (const auto& entry : entries)
		{
			InternalKey internal_key(entry.user_key, entry.sequence, entry.type);
			encoded.push_back(EncodedEntry{ internal_key.Encode().ToString(), entry.value, internal_key });
		}
		std::sort(encoded.begin(), encoded.end(), [&icmp](const EncodedEntry& lhs, const EncodedEntry& rhs) {
			return icmp.Compare(lhs.key, rhs.key) < 0;
		});

		const uint64_t file_number = db->TEST_NewFileNumber();
		const std::string filename = TableFileName(dbname, file_number);
		auto file_result = db->TEST_Env()->NewWritableFile(filename);
		ASSERT_TRUE(file_result.has_value()) << file_result.error().ToString();

		TableBuilder builder(db->TEST_Options(), file_result.value().get());
		for (const auto& item : encoded)
		{
			builder.Add(item.key, item.value);
		}
		const InternalKey& smallest = encoded.front().internal_key;
		const InternalKey& largest = encoded.back().internal_key;

		ASSERT_TRUE(builder.Finish().ok());
		ASSERT_TRUE(file_result.value()->Sync().ok());
		ASSERT_TRUE(file_result.value()->Close().ok());
		ASSERT_TRUE(db->TEST_AddFileToVersion(level, file_number, builder.FileSize(), smallest, largest).ok());
	}

	void AddCompactionInputs(DBImpl* db)
	{
		AddTableFile(db, db->TEST_DBName(), 0,
		    { TableEntry{ "a", 1, kTypeValue, "va0" }, TableEntry{ "d", 1, kTypeValue, "vd0" } });
		AddTableFile(db, db->TEST_DBName(), 0,
		    { TableEntry{ "b", 1, kTypeValue, "vb1" }, TableEntry{ "e", 1, kTypeValue, "ve1" } });
		AddTableFile(db, db->TEST_DBName(), 0,
		    { TableEntry{ "c", 1, kTypeValue, "vc2" }, TableEntry{ "f", 1, kTypeValue, "vf2" } });
		AddTableFile(db, db->TEST_DBName(), 0,
		    { TableEntry{ "d", 0, kTypeValue, "vd3" }, TableEntry{ "g", 1, kTypeValue, "vg3" } });
	}
}

class CompactionCancellationTest: public ::testing::Test
{
protected:
	void TearDown() override
	{
		std::error_code ec;
		std::filesystem::remove_all("test_prestart_compaction_cancel", ec);
		std::filesystem::remove_all("test_running_compaction_cancel", ec);
		std::filesystem::remove_all("test_writer_release_after_cancel", ec);
		std::filesystem::remove_all("test_shutdown_quiescence_running_compaction", ec);
	}

	void SetUp() override { RuntimeMetrics::Instance().Reset(); }
};

TEST_F(CompactionCancellationTest, PreStartCompactionCancelledBeforeExecution)
{
	Options options;
	options.create_if_missing = true;
	options.write_buffer_size = 128;

	auto open = DBImpl::OpenInternal(options, "test_prestart_compaction_cancel");
	ASSERT_TRUE(open.has_value()) << open.error().ToString();
	auto db = std::move(open.value());
	auto* impl = db.get();
	impl->TEST_HoldBackgroundCompaction(true);

	std::vector<std::pair<std::string, std::string>> kvs;
	FillUntilImmutable(impl, "seed", &kvs);
	ASSERT_TRUE(WaitUntil([impl] { return impl->TEST_BackgroundCompactionStartCount() >= 1; }, 5s));
	EXPECT_EQ(RuntimeMetrics::Instance().active_compaction_lane.load(std::memory_order_relaxed), 1);

	impl->TEST_RequestCompactionStop();
	impl->TEST_HoldBackgroundCompaction(false);
	ASSERT_TRUE(WaitUntil([impl] { return !impl->TEST_HasInFlightCompaction(); }, 5s));
	EXPECT_EQ(RuntimeMetrics::Instance().active_compaction_lane.load(std::memory_order_relaxed), 0);
	EXPECT_TRUE(impl->TEST_HasImmutableMemTable());
	EXPECT_EQ(impl->TEST_NumLevelFiles(0), 0);

	db.reset();
	open = DBImpl::OpenInternal(options, "test_prestart_compaction_cancel");
	ASSERT_TRUE(open.has_value()) << open.error().ToString();
	db = std::move(open.value());
	for (const auto& entry : { kvs.front(), kvs.back() })
	{
		auto value = db->Get(entry.first);
		ASSERT_TRUE(value.has_value()) << value.error().ToString();
		EXPECT_EQ(value.value(), entry.second);
	}
}

TEST_F(CompactionCancellationTest, RunningCompactionStopsAtCheckpoint)
{
	BlockingTableFileEnv env(Env::Default());
	Options options;
	options.env = &env;
	options.create_if_missing = true;

	auto open = DBImpl::OpenInternal(options, "test_running_compaction_cancel");
	ASSERT_TRUE(open.has_value()) << open.error().ToString();
	auto db = std::move(open.value());
	auto* impl = db.get();
	ASSERT_TRUE(db->Put("sequence_seed", "seed").ok());

	AddCompactionInputs(impl);
	env.EnableAppendBlock();
	impl->TEST_ScheduleCompaction();
	ASSERT_TRUE(env.WaitForBlockedAppend(5s));
	EXPECT_EQ(RuntimeMetrics::Instance().active_compaction_lane.load(std::memory_order_relaxed), 1);

	impl->TEST_RequestCompactionStop();
	env.ReleaseBlockedAppend();
	ASSERT_TRUE(WaitUntil([impl] { return !impl->TEST_HasInFlightCompaction(); }, 5s));
	EXPECT_EQ(RuntimeMetrics::Instance().active_compaction_lane.load(std::memory_order_relaxed), 0);

	EXPECT_TRUE(impl->TEST_PendingOutputsEmpty());
	EXPECT_EQ(impl->TEST_NumLevelFiles(0), 4);
	EXPECT_EQ(impl->TEST_NumLevelFiles(1), 0);

	env.DisableAppendBlock();
	db.reset();
	open = DBImpl::OpenInternal(options, "test_running_compaction_cancel");
	ASSERT_TRUE(open.has_value()) << open.error().ToString();
	db = std::move(open.value());
	for (const auto& key : { std::string("a"), std::string("d") })
	{
		auto value = db->Get(key);
		ASSERT_TRUE(value.has_value()) << value.error().ToString();
		EXPECT_FALSE(value.value().empty());
	}
}

TEST_F(CompactionCancellationTest, MakeRoomForWriteStopRequestedReturnsIoError)
{
	Options options;
	options.create_if_missing = true;
	options.write_buffer_size = 128;

	auto open = DBImpl::OpenInternal(options, "test_writer_release_after_cancel");
	ASSERT_TRUE(open.has_value()) << open.error().ToString();
	auto db = std::move(open.value());
	auto* impl = db.get();
	impl->TEST_HoldBackgroundCompaction(true);

	FillUntilImmutable(impl, "writer_seed");
	ASSERT_TRUE(WaitUntil([impl] { return impl->TEST_BackgroundCompactionStartCount() >= 1; }, 5s));

	std::atomic<bool> writer_done{ false };
	Status writer_status;
	std::thread writer([&] {
		writer_status = db->Put("blocked", "value");
		writer_done.store(true, std::memory_order_release);
	});

	std::this_thread::sleep_for(50ms);
	EXPECT_FALSE(writer_done.load(std::memory_order_acquire));

	impl->TEST_RequestCompactionStop();
	impl->TEST_HoldBackgroundCompaction(false);
	ASSERT_TRUE(WaitUntil([&writer_done] { return writer_done.load(std::memory_order_acquire); }, 5s));
	writer.join();
	EXPECT_FALSE(writer_status.ok());
	EXPECT_TRUE(writer_status.IsIOError()) << writer_status.ToString();

	db.reset();
	open = DBImpl::OpenInternal(options, "test_writer_release_after_cancel");
	ASSERT_TRUE(open.has_value()) << open.error().ToString();
}

TEST_F(CompactionCancellationTest, ShutdownQuiescenceWhileCompactionRunning)
{
	BlockingTableFileEnv env(Env::Default());
	Options options;
	options.env = &env;
	options.create_if_missing = true;

	auto open = DBImpl::OpenInternal(options, "test_shutdown_quiescence_running_compaction");
	ASSERT_TRUE(open.has_value()) << open.error().ToString();
	auto db = std::move(open.value());
	auto* impl = db.get();

	AddCompactionInputs(impl);
	env.EnableAppendBlock();
	impl->TEST_ScheduleCompaction();
	ASSERT_TRUE(env.WaitForBlockedAppend(5s));
	EXPECT_EQ(RuntimeMetrics::Instance().active_compaction_lane.load(std::memory_order_relaxed), 1);

	std::atomic<bool> destroyed{ false };
	std::thread destroy_thread([&] {
		db.reset();
		destroyed.store(true, std::memory_order_release);
	});

	std::this_thread::sleep_for(50ms);
	EXPECT_FALSE(destroyed.load(std::memory_order_acquire));

	env.ReleaseBlockedAppend();
	ASSERT_TRUE(WaitUntil([&destroyed] { return destroyed.load(std::memory_order_acquire); }, 5s));
	destroy_thread.join();
	EXPECT_EQ(RuntimeMetrics::Instance().active_compaction_lane.load(std::memory_order_relaxed), 0);
	EXPECT_GE(RuntimeMetrics::Instance().shutdown_wait_count.load(std::memory_order_relaxed), 1u);
	EXPECT_GT(RuntimeMetrics::Instance().shutdown_wait_duration_us.load(std::memory_order_relaxed), 0u);

	env.DisableAppendBlock();
	open = DBImpl::OpenInternal(options, "test_shutdown_quiescence_running_compaction");
	ASSERT_TRUE(open.has_value()) << open.error().ToString();
}
