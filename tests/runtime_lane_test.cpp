// runtime_lane_test.cpp -- Regression tests for lane isolation 
//
// Verifies that the separate read and compaction executors are wired
// correctly, foreground reads make progress during compaction, compaction
// remains single-flight, and coroutine wakeups are never lost under the
// split-executor architecture.
//
// These tests are regression guardrails: any future refactoring that would
// silently collapse the read and compaction lanes back into a single executor
// will fail one or more of these tests.

#include "runtime_executor.h"
#include "compaction_controller.h"

#include "db_impl.h"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <gtest/gtest.h>
#include <memory>
#include <semaphore>
#include <string>
#include <thread>

namespace prism::tests
{

using namespace std::chrono_literals;

namespace
{

	template <typename Pred>
	bool WaitUntil(Pred pred, std::chrono::milliseconds timeout = 10s)
	{
		auto start = std::chrono::steady_clock::now();
		while (std::chrono::steady_clock::now() - start < timeout)
		{
			if (pred())
			{
				return true;
			}
			std::this_thread::yield();
		}
		return pred();
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

// ===========================================================================
// Test 1: Read Lane Isolation Verification
//
// Confirms that read_executor and compaction_executor are distinct objects
// and that runtime_scheduler.BlockingScheduler() routes to the read lane,
// NOT the compaction lane.
// ===========================================================================
TEST(RuntimeLaneTest, ReadAndCompactionExecutorsAreSeparate)
{
	ThreadPoolScheduler scheduler(2);
	RuntimeBundle runtime(scheduler);

	EXPECT_NE(&runtime.read_executor, &runtime.compaction_executor);

	IScheduler* blocking = runtime.runtime_scheduler.BlockingScheduler();
	EXPECT_EQ(blocking, &runtime.read_scheduler);
	EXPECT_NE(blocking, &runtime.compaction_scheduler);
	EXPECT_EQ(runtime.runtime_scheduler.ContinuationScheduler(), &runtime.cpu_scheduler);
	EXPECT_EQ(runtime.foreground_db_scheduler.BlockingScheduler(), &runtime.foreground_db_scheduler);
	EXPECT_EQ(runtime.foreground_db_scheduler.ContinuationScheduler(), &runtime.foreground_db_scheduler);
	EXPECT_EQ(runtime.cpu_scheduler.BlockingScheduler(), &runtime.cpu_scheduler);
	EXPECT_EQ(runtime.cpu_scheduler.ContinuationScheduler(), &runtime.cpu_scheduler);
	EXPECT_EQ(runtime.read_scheduler.ContinuationScheduler(), &runtime.read_scheduler);
	EXPECT_EQ(runtime.compaction_scheduler.ContinuationScheduler(), &runtime.compaction_scheduler);
	EXPECT_EQ(runtime.serial_scheduler.ContinuationScheduler(), &runtime.serial_scheduler);

	{
		std::binary_semaphore done(0);
		blocking->Submit([&done] { done.release(); });
		EXPECT_TRUE(done.try_acquire_for(5s));
	}

	{
		std::binary_semaphore done(0);
		runtime.compaction_scheduler.Submit([&done] { done.release(); });
		EXPECT_TRUE(done.try_acquire_for(5s));
	}
}

// ===========================================================================
// Test 2: Foreground Progress During Compaction
//
// Verifies that foreground read work can complete while compaction work is
// occupying the compaction executor.  Without lane isolation the foreground
// work would be serialized behind the long-running compaction job.
// ===========================================================================
TEST(RuntimeLaneTest, ForegroundProgressDuringBackgroundCompaction)
{
	ThreadPoolScheduler scheduler(2);
	RuntimeBundle runtime(scheduler);

	std::binary_semaphore compaction_started(0);
	std::binary_semaphore allow_compaction_finish(0);
	std::atomic<bool> compaction_finished{ false };

	runtime.compaction_executor.Submit([&] {
		compaction_started.release();
		allow_compaction_finish.acquire();
		compaction_finished.store(true, std::memory_order_release);
	});

	ASSERT_TRUE(compaction_started.try_acquire_for(5s));

	std::binary_semaphore read_done(0);
	runtime.runtime_scheduler.BlockingScheduler()->Submit([&read_done] { read_done.release(); });

	EXPECT_TRUE(read_done.try_acquire_for(5s))
	    << "foreground read was blocked behind compaction — lane isolation broken";

	EXPECT_FALSE(compaction_finished.load(std::memory_order_acquire));

	allow_compaction_finish.release();
	EXPECT_TRUE(WaitUntil([&compaction_finished] { return compaction_finished.load(); }, 5s));
}

TEST(RuntimeLaneTest, ForegroundDbSchedulerBypassesBusyReadLane)
{
	ThreadPoolScheduler scheduler(4);
	RuntimeBundle runtime(scheduler);

	constexpr int kReadWorkers = 4;
	std::binary_semaphore read_started(0);
	std::binary_semaphore release_reads(0);

	for (int i = 0; i < kReadWorkers; ++i)
	{
		runtime.read_scheduler.Submit([&] {
			read_started.release();
			release_reads.acquire();
		});
	}

	for (int i = 0; i < kReadWorkers; ++i)
	{
		ASSERT_TRUE(read_started.try_acquire_for(5s));
	}

	std::binary_semaphore foreground_done(0);
	runtime.foreground_db_scheduler.Submit([&] { foreground_done.release(); });
	EXPECT_TRUE(foreground_done.try_acquire_for(5s))
	    << "foreground DB work was blocked behind read-lane work";

	for (int i = 0; i < kReadWorkers; ++i)
	{
		release_reads.release();
	}
}

TEST(RuntimeLaneTest, RuntimeBundleCanReleaseLastReferenceFromReadWorker)
{
	ThreadPoolScheduler scheduler(2);
	auto runtime = AcquireRuntimeBundle(scheduler);

	std::binary_semaphore released_on_worker(0);
	runtime->read_executor.Submit([runtime = std::move(runtime), &released_on_worker]() mutable {
		runtime.reset();
		released_on_worker.release();
	});

	EXPECT_TRUE(released_on_worker.try_acquire_for(5s));
}

TEST(RuntimeLaneTest, DeferredDeletionIsSafeWithActiveCpuBurst)
{
	ThreadPoolScheduler scheduler(4);
	auto runtime = AcquireRuntimeBundle(scheduler);
	std::weak_ptr<RuntimeBundle> weak_runtime = runtime;

	constexpr int kBurstJobs = 128;
	std::binary_semaphore burst_submitted(0);
	std::binary_semaphore released_on_read_worker(0);
	std::atomic<bool> allow_burst{ false };
	std::atomic<int> burst_completed{ 0 };

	runtime->foreground_db_scheduler.Submit([&] {
		for (int i = 0; i < kBurstJobs; ++i)
		{
			scheduler.Submit([&] {
				while (!allow_burst.load(std::memory_order_acquire))
				{
					std::this_thread::yield();
				}
				burst_completed.fetch_add(1, std::memory_order_release);
			});
		}
		burst_submitted.release();
	});

	ASSERT_TRUE(burst_submitted.try_acquire_for(5s));

	runtime->read_executor.Submit([runtime = std::move(runtime), &released_on_read_worker]() mutable {
		runtime.reset();
		released_on_read_worker.release();
	});

	ASSERT_TRUE(released_on_read_worker.try_acquire_for(5s));
	allow_burst.store(true, std::memory_order_release);

	EXPECT_TRUE(WaitUntil([&] { return burst_completed.load(std::memory_order_acquire) == kBurstJobs; }, 5s));
	EXPECT_TRUE(WaitUntil([&] { return weak_runtime.expired(); }, 5s));
}

// ===========================================================================
// Test 3: Compaction Single-Flight Preservation
//
// Verifies that after the lane split, the CompactionController still prevents
// duplicate compaction submissions — only one background compaction may be
// queued or running at any time.
//
// Uses the same DBImpl-based pattern as shutdown_quiescence_test to exercise
// the real CompactionController::TrySubmitLocked / lane_active_ path.
// ===========================================================================
TEST(RuntimeLaneTest, CompactionRemainsSingleFlight)
{
	std::error_code ec;
	std::filesystem::remove_all("test_compaction_single_flight", ec);

	Options options;
	options.create_if_missing = true;
	options.write_buffer_size = 128;

	auto open = DBImpl::OpenInternal(options, "test_compaction_single_flight");
	ASSERT_TRUE(open.has_value()) << open.error().ToString();
	auto db = std::move(open.value());
	auto* impl = db.get();

	impl->TEST_HoldBackgroundCompaction(true);

	FillUntilImmutable(impl, "single");
	ASSERT_TRUE(WaitUntil([impl] { return impl->TEST_BackgroundCompactionStartCount() >= 1; }, 5s));

	ASSERT_TRUE(impl->TEST_HasInFlightCompaction());

	int start_count_before = impl->TEST_BackgroundCompactionStartCount();
	impl->TEST_ScheduleCompaction();

	EXPECT_EQ(impl->TEST_BackgroundCompactionStartCount(), start_count_before)
	    << "a second compaction was started while one was already in-flight — "
	       "single-flight invariant broken";

	impl->TEST_HoldBackgroundCompaction(false);
	ASSERT_TRUE(WaitUntil([impl] { return !impl->TEST_HasInFlightCompaction(); }, 5s));

	EXPECT_FALSE(impl->TEST_HasInFlightCompaction());

	std::filesystem::remove_all("test_compaction_single_flight", ec);
}

// ===========================================================================
// Test 4: No Lost Wakeups Under Lane Isolation
//
// Verifies that work submitted concurrently to the read lane and the
// compaction lane both complete correctly (no lost wakeups).  Uses
// semaphore-based synchronization to avoid ASan false positives from
// coroutine cross-thread stack access.
//
// Without lane isolation, work submitted to the same executor would
// be serialized. With separate lanes, both sets of work must make
// progress concurrently and complete without hang.
// ===========================================================================
TEST(RuntimeLaneTest, NoLostWakeupsWithSeparateLanes)
{
	ThreadPoolScheduler scheduler(4);
	RuntimeBundle runtime(scheduler);

	constexpr int kNumWorkItems = 100;
	std::atomic<int> read_counter{ 0 };
	std::atomic<int> compaction_counter{ 0 };
	std::binary_semaphore read_all_done(0);
	std::binary_semaphore compaction_all_done(0);

	for (int i = 0; i < kNumWorkItems; ++i)
	{
		runtime.runtime_scheduler.BlockingScheduler()->Submit([&read_counter, i, &read_all_done, kNumWorkItems] {
			read_counter.fetch_add(i, std::memory_order_relaxed);
			if (i == kNumWorkItems - 1)
			{
				read_all_done.release();
			}
		});
	}

	for (int i = 0; i < kNumWorkItems; ++i)
	{
		runtime.compaction_scheduler.Submit([&compaction_counter, i, &compaction_all_done, kNumWorkItems] {
			compaction_counter.fetch_add(i * 2, std::memory_order_relaxed);
			if (i == kNumWorkItems - 1)
			{
				compaction_all_done.release();
			}
		});
	}

	EXPECT_TRUE(read_all_done.try_acquire_for(10s))
	    << "read-lane work did not complete — possible lost wakeup";
	EXPECT_TRUE(compaction_all_done.try_acquire_for(10s))
	    << "compaction-lane work did not complete — possible lost wakeup";

	constexpr int kExpectedReadSum = kNumWorkItems * (kNumWorkItems - 1) / 2;
	constexpr int kExpectedCompactionSum = kNumWorkItems * (kNumWorkItems - 1) / 2 * 2;

	EXPECT_EQ(read_counter.load(), kExpectedReadSum)
	    << "read-lane work produced wrong result — possible lost wakeup or double-execution";
	EXPECT_EQ(compaction_counter.load(), kExpectedCompactionSum)
	    << "compaction-lane work produced wrong result — possible lost wakeup or double-execution";
}

} // namespace prism::tests
