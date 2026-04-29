// scheduler_invariant_test.cpp
//
// Targeted regression tests that freeze correctness invariants most at risk
// during scheduler surgery: self-submit affinity, SubmitIn pinning, wakeup
// correctness, and zero-wakeup edge cases.

#include "scheduler.h"

#include <atomic>
#include <chrono>
#include <gtest/gtest.h>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

using namespace prism;
using namespace std::chrono_literals;

// ---------------------------------------------------------------------------
// Helper: poll until predicate or timeout
// ---------------------------------------------------------------------------
template <typename Pred>
static bool PollUntil(Pred pred, std::chrono::milliseconds timeout = 5000ms)
{
	auto start = std::chrono::steady_clock::now();
	while (std::chrono::steady_clock::now() - start < timeout)
	{
		if (pred())
			return true;
		std::this_thread::yield();
	}
	return pred();
}

// ---------------------------------------------------------------------------
// INVARIANT 1: SelfSubmitRunsOnOwnWorker
//
// A worker thread calling Submit() from within its own job must cause the
// submitted job to run on the SAME worker thread (no dispatcher mediation).
// This is the worker-local fast-path invariant that the redesign must preserve.
// ---------------------------------------------------------------------------
TEST(SchedulerInvariantTest, SelfSubmitRunsOnOwnWorker)
{
	ThreadPoolScheduler scheduler(4);

	std::atomic<bool> done{ false };
	std::thread::id outer_tid;
	std::thread::id inner_tid;

	scheduler.Submit([&scheduler, &done, &outer_tid, &inner_tid]() {
		outer_tid = std::this_thread::get_id();
		// Self-submit: worker submits a job from within its own execution.
		scheduler.Submit([&done, &inner_tid]() {
			inner_tid = std::this_thread::get_id();
			done.store(true, std::memory_order_release);
		});
	});

	ASSERT_TRUE(PollUntil([&] { return done.load(std::memory_order_acquire); }))
	    << "Timed out waiting for self-submitted job";

	// The inner job must run on the same worker as the outer job.
	EXPECT_EQ(outer_tid, inner_tid)
	    << "Self-submit from worker must run on same worker thread (no dispatcher mediation)";
}

// ---------------------------------------------------------------------------
// INVARIANT 2: SubmitInPinnedTaskStaysOnWorker
//
// SubmitIn() with a valid context must pin ALL submitted jobs to the exact
// worker that captured the context, even when multiple workers are idle.
// This is the affinity-pinning invariant.
// ---------------------------------------------------------------------------
TEST(SchedulerInvariantTest, SubmitInPinnedTaskStaysOnWorker)
{
	ThreadPoolScheduler scheduler(4);

	std::atomic<int> done_count{ 0 };
	std::thread::id outer_tid;
	std::vector<std::thread::id> inner_tids;
	std::mutex tids_mutex;
	constexpr int kAffinityJobs = 10;

	scheduler.Submit([&scheduler, &done_count, &outer_tid, &inner_tids, &tids_mutex]() {
		outer_tid = std::this_thread::get_id();
		auto ctx = scheduler.CaptureContext();
		ASSERT_TRUE(ctx.IsValid());

		// Submit multiple affinity-pinned jobs.
		for (int i = 0; i < kAffinityJobs; ++i)
		{
			scheduler.SubmitIn(ctx, [&done_count, &inner_tids, &tids_mutex]() {
				{
					std::lock_guard lock(tids_mutex);
					inner_tids.push_back(std::this_thread::get_id());
				}
				done_count.fetch_add(1, std::memory_order_release);
			});
		}
	});

	ASSERT_TRUE(PollUntil([&] { return done_count.load(std::memory_order_acquire) == kAffinityJobs; }))
	    << "Timed out waiting for affinity jobs";

	// Every single affinity job must have run on the same worker.
	std::lock_guard lock(tids_mutex);
	ASSERT_EQ(inner_tids.size(), static_cast<std::size_t>(kAffinityJobs));
	for (std::size_t i = 0; i < inner_tids.size(); ++i)
	{
		EXPECT_EQ(inner_tids[i], outer_tid)
		    << "Affinity job " << i << " ran on wrong worker — pinning invariant violated";
	}
}

// ---------------------------------------------------------------------------
// INVARIANT 3: NoLostWakeupsAcrossFallbackPaths
//
// Under high-volume mixed submission (Submit + SubmitIn), every job must
// complete exactly once. No wakeups may be lost across the fallback/dispatch
// paths. This stress-tests semaphore/futex correctness.
// ---------------------------------------------------------------------------
TEST(SchedulerInvariantTest, NoLostWakeupsAcrossFallbackPaths)
{
	ThreadPoolScheduler scheduler(4);

	auto submit_counter = std::make_shared<std::atomic<int>>(0);
	auto affinity_counter = std::make_shared<std::atomic<int>>(0);
	constexpr int kNumTasks = 500;

	// Submit a mix of dispatched and affinity jobs.
	for (int i = 0; i < kNumTasks; ++i)
	{
		scheduler.Submit([submit_counter]() {
			submit_counter->fetch_add(1, std::memory_order_relaxed);
		});

		scheduler.Submit([&scheduler, affinity_counter]() {
			auto ctx = scheduler.CaptureContext();
			if (ctx.IsValid())
			{
				scheduler.SubmitIn(ctx, [affinity_counter]() {
					affinity_counter->fetch_add(1, std::memory_order_relaxed);
				});
			}
			else
			{
				// Fallback: context invalid (shouldn't happen from worker)
				affinity_counter->fetch_add(1, std::memory_order_relaxed);
			}
		});
	}

	ASSERT_TRUE(PollUntil(
	    [&] {
		    return submit_counter->load(std::memory_order_acquire) == kNumTasks
		        && affinity_counter->load(std::memory_order_acquire) == kNumTasks;
	    },
	    10s))
	    << "Lost wakeups: submit=" << submit_counter->load() << " affinity=" << affinity_counter->load()
	    << " expected=" << kNumTasks;

	EXPECT_EQ(submit_counter->load(), kNumTasks);
	EXPECT_EQ(affinity_counter->load(), kNumTasks);
}

// ---------------------------------------------------------------------------
// INVARIANT 4: ZeroWakeupEmptySchedulerMainSubmit
//
// Submitting a single job from the main thread to an otherwise idle scheduler
// must complete without any prior wakeup activity. This is the zero-wakeup
// cold-start edge case.
// ---------------------------------------------------------------------------
TEST(SchedulerInvariantTest, ZeroWakeupEmptySchedulerMainSubmit)
{
	ThreadPoolScheduler scheduler(4);

	std::atomic<bool> done{ false };

	// Single job, no other work in the system.
	scheduler.Submit([&done]() {
		done.store(true, std::memory_order_release);
	});

	ASSERT_TRUE(PollUntil([&] { return done.load(std::memory_order_acquire); }))
	    << "Single main-thread submit to idle scheduler never completed";

	EXPECT_TRUE(done.load());
}
