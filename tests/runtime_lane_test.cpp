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

#include "async_runtime.h"
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
	// Confirms that db_read_executor, blocking_io_executor, db_write_executor,
	// and compaction_executor are distinct physical
	// lanes, while their scheduler adapters remain simple submit-only views.
	// ===========================================================================
	TEST(RuntimeLaneTest, ReadAndCompactionExecutorsAreSeparate)
	{
		CpuThreadPool scheduler(2);
		AsyncRuntime runtime(scheduler);

		EXPECT_NE(&runtime.DbReadExecutor(), &runtime.BlockingIoExecutor());
		EXPECT_NE(&runtime.DbReadExecutor(), &runtime.CompactionExecutor());
		EXPECT_NE(&runtime.BlockingIoExecutor(), &runtime.CompactionExecutor());

		{
			std::binary_semaphore done(0);
			runtime.DbReadExecutor().Submit([&done] { done.release(); });
			EXPECT_TRUE(done.try_acquire_for(5s));
		}

		{
			std::binary_semaphore done(0);
			runtime.BlockingIoExecutor().Submit([&done] { done.release(); });
			EXPECT_TRUE(done.try_acquire_for(5s));
		}

		{
			std::binary_semaphore done(0);
			runtime.CompactionExecutor().Submit([&done] { done.release(); });
			EXPECT_TRUE(done.try_acquire_for(5s));
		}

		{
			std::binary_semaphore done(0);
			runtime.DbWriteExecutor().Submit([&] {
				EXPECT_TRUE(runtime.DbWriteExecutor().IsCurrentWorker());
				done.release();
			});
			EXPECT_TRUE(done.try_acquire_for(5s));
		}
	}

	TEST(RuntimeLaneTest, DbReadReentrantWorkCanRunInline)
	{
		CpuThreadPool scheduler(1);
		AsyncRuntimeOptions options;
		options.db_read_threads = 1;
		AsyncRuntime runtime(scheduler, options);

		std::binary_semaphore done(0);
		std::atomic<int> step{ 0 };
		bool external_ran = false;
		EXPECT_FALSE(runtime.DbReadExecutor().TryRunInline([&] { external_ran = true; }));
		EXPECT_FALSE(external_ran);
		runtime.DbReadExecutor().Submit([&] {
			EXPECT_TRUE(runtime.DbReadExecutor().IsCurrentWorker());
			step.store(1, std::memory_order_relaxed);
			EXPECT_TRUE(runtime.DbReadExecutor().TryRunInline([&] {
				EXPECT_TRUE(runtime.DbReadExecutor().IsCurrentWorker());
				EXPECT_EQ(step.load(std::memory_order_relaxed), 1);
				step.store(2, std::memory_order_relaxed);
			}));
			EXPECT_EQ(step.load(std::memory_order_relaxed), 2);
			done.release();
		});

		ASSERT_TRUE(done.try_acquire_for(5s));
		EXPECT_EQ(step.load(std::memory_order_relaxed), 2);
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
		CpuThreadPool scheduler(2);
		AsyncRuntime runtime(scheduler);

		std::binary_semaphore compaction_started(0);
		std::binary_semaphore allow_compaction_finish(0);
		std::atomic<bool> compaction_finished{ false };

		runtime.CompactionExecutor().Submit([&] {
			compaction_started.release();
			allow_compaction_finish.acquire();
			compaction_finished.store(true, std::memory_order_release);
		});

		ASSERT_TRUE(compaction_started.try_acquire_for(5s));

		std::binary_semaphore read_done(0);
		runtime.DbReadExecutor().Submit([&read_done] { read_done.release(); });

		EXPECT_TRUE(read_done.try_acquire_for(5s)) << "foreground read was blocked behind compaction — lane isolation broken";

		EXPECT_FALSE(compaction_finished.load(std::memory_order_acquire));

		allow_compaction_finish.release();
		EXPECT_TRUE(WaitUntil([&compaction_finished] { return compaction_finished.load(); }, 5s));
	}

	TEST(RuntimeLaneTest, CpuExecutorBypassesBusyBlockingIoLane)
	{
		CpuThreadPool scheduler(4);
		AsyncRuntime runtime(scheduler);

		constexpr int kReadWorkers = 4;
		std::counting_semaphore<kReadWorkers> read_started(0);
		std::counting_semaphore<kReadWorkers> release_reads(0);

		for (int i = 0; i < kReadWorkers; ++i)
		{
			runtime.BlockingIoExecutor().Submit([&] {
				read_started.release();
				release_reads.acquire();
			});
		}

		for (int i = 0; i < kReadWorkers; ++i)
		{
			ASSERT_TRUE(read_started.try_acquire_for(5s));
		}

		std::binary_semaphore foreground_done(0);
		runtime.CpuExecutor().Submit([&] { foreground_done.release(); });
		EXPECT_TRUE(foreground_done.try_acquire_for(5s)) << "foreground DB work was blocked behind read-lane work";

		for (int i = 0; i < kReadWorkers; ++i)
		{
			release_reads.release();
		}
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
		CpuThreadPool scheduler(4);
		AsyncRuntime runtime(scheduler);

		constexpr int kNumWorkItems = 100;
		std::atomic<int> read_counter{ 0 };
		std::atomic<int> compaction_counter{ 0 };
		std::binary_semaphore read_all_done(0);
		std::binary_semaphore compaction_all_done(0);

		for (int i = 0; i < kNumWorkItems; ++i)
		{
			runtime.BlockingIoExecutor().Submit([&read_counter, i, &read_all_done, kNumWorkItems] {
				read_counter.fetch_add(i, std::memory_order_relaxed);
				if (i == kNumWorkItems - 1)
				{
					read_all_done.release();
				}
			});
		}

		for (int i = 0; i < kNumWorkItems; ++i)
		{
			runtime.CompactionExecutor().Submit([&compaction_counter, i, &compaction_all_done, kNumWorkItems] {
				compaction_counter.fetch_add(i * 2, std::memory_order_relaxed);
				if (i == kNumWorkItems - 1)
				{
					compaction_all_done.release();
				}
			});
		}

		EXPECT_TRUE(read_all_done.try_acquire_for(10s)) << "read-lane work did not complete — possible lost wakeup";
		EXPECT_TRUE(compaction_all_done.try_acquire_for(10s)) << "compaction-lane work did not complete — possible lost wakeup";

		constexpr int kExpectedReadSum = kNumWorkItems * (kNumWorkItems - 1) / 2;
		constexpr int kExpectedCompactionSum = kNumWorkItems * (kNumWorkItems - 1) / 2 * 2;

		EXPECT_EQ(read_counter.load(), kExpectedReadSum)
		    << "read-lane work produced wrong result — possible lost wakeup or double-execution";
		EXPECT_EQ(compaction_counter.load(), kExpectedCompactionSum)
		    << "compaction-lane work produced wrong result — possible lost wakeup or double-execution";
	}

} // namespace prism::tests
