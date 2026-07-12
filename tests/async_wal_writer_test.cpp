#include "../src/async_runtime.h"
#include "../src/async_wal_writer.h"

#include "env.h"
#include "log_writer.h"
#include "scheduler.h"

#include "gtest/gtest.h"

#include <condition_variable>
#include <mutex>
#include <string>

using namespace prism;

namespace
{
	class MemoryWritableFile final: public WritableFile
	{
	public:
		Status Append(const Slice& data) override
		{
			if (!append_status_.ok())
			{
				return append_status_;
			}
			contents_.append(data.data(), data.size());
			append_offset_ += data.size();
			return Status::OK();
		}

		Status Close() override { return Status::OK(); }
		Status Flush() override { return Status::OK(); }

		Status Sync() override
		{
			++sync_count_;
			return sync_status_;
		}

		uint64_t AppendOffset() const noexcept override { return append_offset_; }

		const std::string& contents() const noexcept { return contents_; }
		int sync_count() const noexcept { return sync_count_; }
		void set_append_status(Status status) { append_status_ = std::move(status); }
		void set_sync_status(Status status) { sync_status_ = std::move(status); }

	private:
		std::string contents_;
		uint64_t append_offset_ = 0;
		int sync_count_ = 0;
		Status append_status_;
		Status sync_status_;
	};

	Status WriteAndWait(AsyncWalWriter& wal, MemoryWritableFile& file, log::Writer& writer, const std::string& record, bool sync)
	{
		std::mutex mutex;
		std::condition_variable cv;
		bool done = false;
		Status result;
		wal.Write(file, writer, Slice(record), sync, [&](Status status) {
			{
				std::lock_guard lock(mutex);
				result = std::move(status);
				done = true;
			}
			cv.notify_one();
		});

		std::unique_lock lock(mutex);
		cv.wait(lock, [&] { return done; });
		return result;
	}
}

TEST(AsyncWalWriterTest, FallbackAppendWithoutSyncWritesEncodedBytes)
{
	CpuThreadPool scheduler(2);
	AsyncRuntime runtime(scheduler);
	AsyncWalWriter wal(runtime);
	MemoryWritableFile file;
	log::Writer writer(&file);

	const std::string record = "wal helper record";
	Status s = WriteAndWait(wal, file, writer, record, false);
	ASSERT_TRUE(s.ok()) << s.ToString();

	auto encoded = log::EncodeRecordFragments(Slice(record), 0);
	ASSERT_TRUE(encoded.has_value()) << encoded.error().ToString();
	EXPECT_EQ(file.contents(), encoded->bytes);
	EXPECT_EQ(file.sync_count(), 0);
}

TEST(AsyncWalWriterTest, FallbackAppendWithSyncCallsSyncAfterAppend)
{
	CpuThreadPool scheduler(2);
	AsyncRuntime runtime(scheduler);
	AsyncWalWriter wal(runtime);
	MemoryWritableFile file;
	log::Writer writer(&file);

	Status s = WriteAndWait(wal, file, writer, "sync record", true);
	ASSERT_TRUE(s.ok()) << s.ToString();
	EXPECT_EQ(file.sync_count(), 1);
}

TEST(AsyncWalWriterTest, AppendFailureDoesNotCallSync)
{
	CpuThreadPool scheduler(2);
	AsyncRuntime runtime(scheduler);
	AsyncWalWriter wal(runtime);
	MemoryWritableFile file;
	log::Writer writer(&file);
	file.set_append_status(Status::IOError("append failed"));

	Status s = WriteAndWait(wal, file, writer, "will fail", true);
	EXPECT_TRUE(s.IsIOError()) << s.ToString();
	EXPECT_TRUE(file.contents().empty());
	EXPECT_EQ(file.sync_count(), 0);
}

TEST(AsyncWalWriterTest, SyncFailureIsReturned)
{
	CpuThreadPool scheduler(2);
	AsyncRuntime runtime(scheduler);
	AsyncWalWriter wal(runtime);
	MemoryWritableFile file;
	log::Writer writer(&file);
	file.set_sync_status(Status::IOError("sync failed"));

	Status s = WriteAndWait(wal, file, writer, "sync fail", true);
	EXPECT_TRUE(s.IsIOError()) << s.ToString();
	EXPECT_EQ(file.sync_count(), 1);
}

TEST(AsyncWalWriterTest, WriterReservationIsNotRolledBackAfterAppendFailure)
{
	CpuThreadPool scheduler(2);
	AsyncRuntime runtime(scheduler);
	AsyncWalWriter wal(runtime);
	MemoryWritableFile file;
	log::Writer writer(&file);

	const std::string failed_record = "failed reservation";
	auto failed_encoding = log::EncodeRecordFragments(Slice(failed_record), 0);
	ASSERT_TRUE(failed_encoding.has_value()) << failed_encoding.error().ToString();

	file.set_append_status(Status::IOError("append failed"));
	Status failed = WriteAndWait(wal, file, writer, failed_record, false);
	ASSERT_TRUE(failed.IsIOError()) << failed.ToString();

	file.set_append_status(Status::OK());
	const std::string next_record = "next record";
	Status next = WriteAndWait(wal, file, writer, next_record, false);
	ASSERT_TRUE(next.ok()) << next.ToString();

	auto expected = log::EncodeRecordFragments(Slice(next_record), failed_encoding->ending_block_offset);
	ASSERT_TRUE(expected.has_value()) << expected.error().ToString();
	EXPECT_EQ(file.contents(), expected->bytes);
}
