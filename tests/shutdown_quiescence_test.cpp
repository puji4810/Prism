#include "../src/task_scope.h"
#include "../src/runtime_metrics.h"

#include "db_impl.h"

#include <atomic>
#include <filesystem>
#include <functional>
#include <gtest/gtest.h>
#include <memory>
#include <semaphore>
#include <string>
#include <thread>

using namespace prism;
using namespace std::chrono_literals;

namespace
{
	class ShutdownQuiescenceTest: public ::testing::Test
	{
	protected:
		void SetUp() override { RuntimeMetrics::Instance().Reset(); }
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

	void FillUntilImmutable(DBImpl* db, const std::string& prefix)
	{
		for (int i = 0; i < 64; ++i)
		{
			ASSERT_TRUE(db->Put(prefix + std::to_string(i), std::string(64, 'q')).ok());
			if (db->TEST_HasImmutableMemTable())
			{
				return;
			}
		}
		FAIL() << "immutable memtable was not created";
	}
}

TEST_F(ShutdownQuiescenceTest, ScopeDestructorWaitsForAllChildren)
{
	BlockingExecutor executor(2);
	std::binary_semaphore child_a_started(0);
	std::binary_semaphore child_b_started(0);
	std::binary_semaphore release_children(0);
	std::binary_semaphore destroyed(0);
	std::atomic<bool> destroy_done{ false };

	auto scope = std::make_unique<TaskScope>(executor);
	scope->Submit([&](StopToken) {
		child_a_started.release();
		release_children.acquire();
	});
	scope->Submit([&](StopToken) {
		child_b_started.release();
		release_children.acquire();
	});

	ASSERT_TRUE(child_a_started.try_acquire_for(5s));
	ASSERT_TRUE(child_b_started.try_acquire_for(5s));

	std::thread destroy_thread([&] {
		scope.reset();
		destroy_done.store(true, std::memory_order_release);
		destroyed.release();
	});

	std::this_thread::yield();
	EXPECT_FALSE(destroy_done.load(std::memory_order_acquire));

	release_children.release();
	release_children.release();
	ASSERT_TRUE(destroyed.try_acquire_for(5s));
	destroy_thread.join();
	EXPECT_TRUE(destroy_done.load(std::memory_order_acquire));
}

TEST_F(ShutdownQuiescenceTest, ShutdownQuarantinesLateResultsAfterStop)
{
	BlockingExecutor executor;
	std::shared_ptr<OperationState<int>> state;
	std::binary_semaphore started(0);
	std::binary_semaphore allow_complete(0);
	std::atomic<int> applied{ 0 };

	{
		TaskScope scope(executor);
		state = scope.SubmitOperation(
		    [&](StopToken) {
			    started.release();
			    allow_complete.acquire();
			    return 7;
		    },
		    [&](int value) { applied.store(value, std::memory_order_release); });

		ASSERT_TRUE(started.try_acquire_for(5s));
		scope.RequestStop();
		allow_complete.release();
	}

	EXPECT_EQ(applied.load(std::memory_order_acquire), 0);
	EXPECT_TRUE(state->WasQuarantined());
	EXPECT_GE(RuntimeMetrics::Instance().late_completion_quarantined.load(std::memory_order_relaxed), 1u);
}

TEST_F(ShutdownQuiescenceTest, CompactionShutdownQuiescesCleanly)
{
	std::error_code ec;
	std::filesystem::remove_all("test_compaction_shutdown_quiesces_cleanly", ec);

	Options options;
	options.create_if_missing = true;
	options.write_buffer_size = 128;

	auto open = DBImpl::OpenInternal(options, "test_compaction_shutdown_quiesces_cleanly");
	ASSERT_TRUE(open.has_value()) << open.error().ToString();
	auto db = std::move(open.value());
	auto* impl = db.get();
	impl->TEST_HoldBackgroundCompaction(true);

	FillUntilImmutable(impl, "shutdown");
	ASSERT_TRUE(WaitUntil([impl] { return impl->TEST_BackgroundCompactionStartCount() >= 1; }, 5s));

	std::atomic<bool> destroyed{ false };
	std::thread destroy_thread([&] {
		db.reset();
		destroyed.store(true, std::memory_order_release);
	});

	ASSERT_TRUE(WaitUntil([&destroyed] { return destroyed.load(std::memory_order_acquire); }, 5s));
	destroy_thread.join();
	EXPECT_GE(RuntimeMetrics::Instance().shutdown_wait_count.load(std::memory_order_relaxed), 1u);
	EXPECT_GT(RuntimeMetrics::Instance().shutdown_wait_duration_us.load(std::memory_order_relaxed), 0u);

	std::filesystem::remove_all("test_compaction_shutdown_quiesces_cleanly", ec);
}

TEST_F(ShutdownQuiescenceTest, NoCompactionLeakAfterShutdown)
{
	std::error_code ec;
	std::filesystem::remove_all("test_no_compaction_leak_after_shutdown", ec);

	Options options;
	options.create_if_missing = true;
	options.write_buffer_size = 128;

	auto open = DBImpl::OpenInternal(options, "test_no_compaction_leak_after_shutdown");
	ASSERT_TRUE(open.has_value()) << open.error().ToString();
	auto db = std::move(open.value());
	auto* impl = db.get();
	impl->TEST_HoldBackgroundCompaction(true);

	FillUntilImmutable(impl, "leak");
	ASSERT_TRUE(WaitUntil([impl] { return impl->TEST_BackgroundCompactionStartCount() >= 1; }, 5s));
	ASSERT_TRUE(impl->TEST_HasInFlightCompaction());

	db.reset();
	open = DBImpl::OpenInternal(options, "test_no_compaction_leak_after_shutdown");
	ASSERT_TRUE(open.has_value()) << open.error().ToString();
	db = std::move(open.value());
	ASSERT_TRUE(db->Put("after", "shutdown").ok());
	auto value = db->Get("after");
	ASSERT_TRUE(value.has_value()) << value.error().ToString();
	EXPECT_EQ(value.value(), "shutdown");

	std::filesystem::remove_all("test_no_compaction_leak_after_shutdown", ec);
}
