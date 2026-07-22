#include "../src/async_runtime.h"

#include "asyncdb.h"
#include "coro_task.h"
#include "env.h"
#include "filename.h"
#include "runtime_metrics.h"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <semaphore>
#include <stdexcept>
#include <string>
#include <vector>

using namespace prism;
using namespace prism::tests;

namespace
{
	struct WalOverlapControl
	{
		std::atomic<int> append_count{ 0 };
		std::binary_semaphore first_append_started{ 0 };
		std::binary_semaphore allow_first_append{ 0 };
		std::binary_semaphore second_append_started{ 0 };
	};

	class ObservedWalFile final: public WritableFile
	{
	public:
		ObservedWalFile(std::unique_ptr<WritableFile> target, WalOverlapControl& control)
		    : target_(std::move(target))
		    , control_(&control)
		{
		}

		Status Append(const Slice& data) override
		{
			const int append_index = control_->append_count.fetch_add(1, std::memory_order_acq_rel) + 1;
			if (append_index == 1)
			{
				control_->first_append_started.release();
				control_->allow_first_append.acquire();
			}
			else if (append_index == 2)
			{
				control_->second_append_started.release();
			}
			return target_->Append(data);
		}

		Status Close() override { return target_->Close(); }
		Status Flush() override { return target_->Flush(); }
		Status Sync() override { return target_->Sync(); }
		int FileDescriptor() const noexcept override { return -1; }
		uint64_t AppendOffset() const noexcept override { return target_->AppendOffset(); }
		void AdvanceAppendOffset(std::size_t bytes) noexcept override { target_->AdvanceAppendOffset(bytes); }

	private:
		std::unique_ptr<WritableFile> target_;
		WalOverlapControl* control_;
	};

	class WalOverlapEnv final: public EnvWrapper
	{
	public:
		WalOverlapEnv(Env* target, WalOverlapControl& control)
		    : EnvWrapper(target)
		    , control_(&control)
		{
		}

		Result<std::unique_ptr<WritableFile>> NewWritableFile(const std::string& filename) override
		{
			auto file = target()->NewWritableFile(filename);
			if (!file.has_value())
			{
				return std::unexpected(file.error());
			}

			uint64_t number = 0;
			FileType type;
			const std::string basename = std::filesystem::path(filename).filename().string();
			if (ParseFileName(basename, &number, &type) && type == FileType::kLogFile)
			{
				return std::unique_ptr<WritableFile>(new ObservedWalFile(std::move(file.value()), *control_));
			}
			return file;
		}

	private:
		WalOverlapControl* control_;
	};
}

TEST(WriteCoordinatorOverlapTest, OverlapsNextWalWithPreviousGroupContinuation)
{
	constexpr const char* kDbName = "test_write_coordinator_overlap";
	std::error_code cleanup_error;
	std::filesystem::remove_all(kDbName, cleanup_error);

	WalOverlapControl control;
	std::binary_semaphore first_write_resumed{ 0 };
	std::binary_semaphore allow_first_write_return{ 0 };
	Status first_status;
	Status second_status;
	std::optional<Task<AsyncDB>> open_task;
	std::optional<Task<void>> first_task;
	std::optional<Task<void>> second_task;
	bool second_wal_overlapped_completion = false;

	{
		WalOverlapEnv env(Env::Default(), control);
		CpuThreadPool scheduler(4);
		AsyncRuntimeOptions runtime_options;
		runtime_options.db_read_threads = 2;
		runtime_options.blocking_io_threads = 2;
		AsyncRuntime runtime(scheduler, runtime_options);

		Options options;
		options.create_if_missing = true;
		options.env = &env;

		open_task.emplace([](AsyncRuntime& runtime, Options options) -> Task<AsyncDB> {
			auto result = co_await AsyncDB::OpenAsync(runtime, options, kDbName);
			if (!result.has_value())
			{
				throw std::runtime_error(result.error().ToString());
			}
			co_return std::move(result.value());
		}(runtime, options));
		AsyncDB db = open_task->SyncWait();

		first_task.emplace(
		    [](AsyncDB& db, Status& status, std::binary_semaphore& resumed, std::binary_semaphore& allow_return) -> Task<void> {
			    status = co_await db.PutAsync(WriteOptions(), "first", "value-1");
			    resumed.release();
			    allow_return.acquire();
			    co_return;
		    }(db, first_status, first_write_resumed, allow_first_write_return));

		const bool first_append_observed = control.first_append_started.try_acquire_for(std::chrono::seconds(2));
		EXPECT_TRUE(first_append_observed);

		second_task.emplace([](AsyncDB& db, Status& status) -> Task<void> {
			status = co_await db.PutAsync(WriteOptions(), "second", "value-2");
			co_return;
		}(db, second_status));

		control.allow_first_append.release();
		const bool first_resumed = first_write_resumed.try_acquire_for(std::chrono::seconds(2));
		EXPECT_TRUE(first_resumed);
		if (first_resumed)
		{
			second_wal_overlapped_completion = control.second_append_started.try_acquire_for(std::chrono::seconds(2));
		}
		allow_first_write_return.release();

		EXPECT_TRUE(first_task->SyncWaitFor(std::chrono::seconds(5)));
		EXPECT_TRUE(second_task->SyncWaitFor(std::chrono::seconds(5)));
		EXPECT_TRUE(first_status.ok()) << first_status.ToString();
		EXPECT_TRUE(second_status.ok()) << second_status.ToString();
		EXPECT_TRUE(second_wal_overlapped_completion);
	}

	second_task.reset();
	first_task.reset();
	open_task.reset();
	std::filesystem::remove_all(kDbName, cleanup_error);
}

#ifdef PRISM_RUNTIME_METRICS
TEST(WriteCoordinatorOverlapTest, AppliesOneMergedBatchPerWriteGroup)
{
	constexpr const char* kDbName = "test_write_coordinator_group_apply";
	constexpr int kGroupedWrites = 64;
	std::error_code cleanup_error;
	std::filesystem::remove_all(kDbName, cleanup_error);

	WalOverlapControl control;
	std::vector<Status> statuses(static_cast<std::size_t>(kGroupedWrites + 1));
	std::vector<Task<void>> tasks;
	tasks.reserve(static_cast<std::size_t>(kGroupedWrites + 1));

	{
		WalOverlapEnv env(Env::Default(), control);
		CpuThreadPool scheduler(4);
		AsyncRuntimeOptions runtime_options;
		runtime_options.db_read_threads = 2;
		runtime_options.blocking_io_threads = 2;
		AsyncRuntime runtime(scheduler, runtime_options);

		Options options;
		options.create_if_missing = true;
		options.env = &env;

		auto open_task = [](AsyncRuntime& runtime, Options options) -> Task<AsyncDB> {
			auto result = co_await AsyncDB::OpenAsync(runtime, options, kDbName);
			if (!result.has_value())
			{
				throw std::runtime_error(result.error().ToString());
			}
			co_return std::move(result.value());
		}(runtime, options);
		AsyncDB db = open_task.SyncWait();
		RuntimeMetrics::Instance().Reset();

		auto put = [](AsyncDB& db, std::string key, std::string value, Status* status) -> Task<void> {
			*status = co_await db.PutAsync(WriteOptions(), std::move(key), std::move(value));
			co_return;
		};

		tasks.push_back(put(db, "first", "value-0", &statuses[0]));
		ASSERT_TRUE(control.first_append_started.try_acquire_for(std::chrono::seconds(2)));

		for (int i = 1; i <= kGroupedWrites; ++i)
		{
			tasks.push_back(put(db, "key-" + std::to_string(i), "value-" + std::to_string(i), &statuses[i]));
		}

		control.allow_first_append.release();
		ASSERT_TRUE(control.second_append_started.try_acquire_for(std::chrono::seconds(2)));
		for (auto& task : tasks)
		{
			ASSERT_TRUE(task.SyncWaitFor(std::chrono::seconds(5)));
		}

		for (const Status& status : statuses)
		{
			ASSERT_TRUE(status.ok()) << status.ToString();
		}
		EXPECT_EQ(control.append_count.load(std::memory_order_acquire), 2);
		EXPECT_EQ(RuntimeMetrics::Instance().write_groups_completed.load(std::memory_order_relaxed), 2u);
		EXPECT_EQ(RuntimeMetrics::Instance().write_apply_batch_iterations_total.load(std::memory_order_relaxed), 2u);

		auto verify_task = [](AsyncDB& db) -> Task<void> {
			for (int i = 1; i <= kGroupedWrites; ++i)
			{
				auto value = co_await db.GetAsync(ReadOptions(), "key-" + std::to_string(i));
				if (!value.has_value())
				{
					ADD_FAILURE() << value.error().ToString();
					co_return;
				}
				EXPECT_EQ(value.value(), "value-" + std::to_string(i));
			}
			co_return;
		}(db);
		verify_task.SyncWait();
		tasks.clear();
	}

	std::filesystem::remove_all(kDbName, cleanup_error);
}
#endif
