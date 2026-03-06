#include "scheduler.h"

#include <atomic>
#include <chrono>
#include <gtest/gtest.h>
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
// TEST 1: CaptureContext from main thread is invalid
// ---------------------------------------------------------------------------
TEST(SchedulerContextTest, MainThreadCaptureIsInvalid)
{
	ThreadPoolScheduler scheduler(2);
	auto ctx = scheduler.CaptureContext();
	EXPECT_FALSE(ctx.IsValid()) << "CaptureContext() called on main thread must return an invalid Context";
}

// ---------------------------------------------------------------------------
// TEST 2: CaptureContext from a worker thread is valid
// ---------------------------------------------------------------------------
TEST(SchedulerContextTest, WorkerThreadCaptureIsValid)
{
	ThreadPoolScheduler scheduler(2);
	std::atomic<bool> valid_captured{ false };

	scheduler.Submit([&scheduler, &valid_captured]() {
		auto ctx = scheduler.CaptureContext();
		valid_captured.store(ctx.IsValid(), std::memory_order_release);
	});

	ASSERT_TRUE(PollUntil([&] { return valid_captured.load(std::memory_order_acquire); }))
	    << "Timed out waiting for worker task to set valid_captured";
	EXPECT_TRUE(valid_captured.load());
}

// ---------------------------------------------------------------------------
// TEST 3: Context captured on a different scheduler's worker is invalid
//         when tested against the first scheduler
// ---------------------------------------------------------------------------
TEST(SchedulerContextTest, ForeignSchedulerCaptureIsInvalid)
{
	ThreadPoolScheduler sched_a(2);
	ThreadPoolScheduler sched_b(2);

	// Capture a context from sched_b's worker
	ThreadPoolScheduler::Context foreign_ctx;
	std::atomic<bool> done{ false };

	sched_b.Submit([&sched_b, &foreign_ctx, &done]() {
		foreign_ctx = sched_b.CaptureContext();
		done.store(true, std::memory_order_release);
	});
	ASSERT_TRUE(PollUntil([&] { return done.load(std::memory_order_acquire); }));

	// foreign_ctx is valid for sched_b
	EXPECT_TRUE(foreign_ctx.IsValid());

	// But when used with sched_a::SubmitIn it should fall back (not crash),
	// and the job should still execute.
	std::atomic<bool> job_ran{ false };
	sched_a.SubmitIn(foreign_ctx, [&job_ran]() { job_ran.store(true, std::memory_order_release); });
	EXPECT_TRUE(PollUntil([&] { return job_ran.load(std::memory_order_acquire); }))
	    << "Job submitted with foreign context must still execute via fallback Submit";
}

// ---------------------------------------------------------------------------
// TEST 4: Default-constructed Context is invalid
// ---------------------------------------------------------------------------
TEST(SchedulerContextTest, DefaultContextIsInvalid)
{
	ThreadPoolScheduler::Context ctx;
	EXPECT_FALSE(ctx.IsValid());
}

// ---------------------------------------------------------------------------
// TEST 5: SubmitIn with valid context — job executes on the same worker thread
// ---------------------------------------------------------------------------
TEST(SchedulerContextTest, SubmitInSameThread)
{
	// Use a single-worker pool so the captured context unambiguously pins to that worker.
	// kMinThreads is 2, so 2 workers — we still capture the exact worker and verify
	// it self-submits back to the same thread.
	ThreadPoolScheduler scheduler(2);

	std::atomic<bool> done{ false };
	std::thread::id outer_tid;
	std::thread::id inner_tid;

	scheduler.Submit([&scheduler, &done, &outer_tid, &inner_tid]() {
		outer_tid = std::this_thread::get_id();
		auto ctx = scheduler.CaptureContext();
		EXPECT_TRUE(ctx.IsValid());

		// Submit back to the same worker via affinity context
		scheduler.SubmitIn(ctx, [&done, &inner_tid]() {
			inner_tid = std::this_thread::get_id();
			done.store(true, std::memory_order_release);
		});
	});

	ASSERT_TRUE(PollUntil([&] { return done.load(std::memory_order_acquire); })) << "Timed out waiting for inner affinity task";

	EXPECT_EQ(outer_tid, inner_tid) << "SubmitIn with valid context must pin the job to the same worker thread";
}

// ---------------------------------------------------------------------------
// TEST 6: SubmitIn with invalid context falls back — job still runs
// ---------------------------------------------------------------------------
TEST(SchedulerContextTest, SubmitInInvalidContextFallsBack)
{
	ThreadPoolScheduler scheduler(2);

	ThreadPoolScheduler::Context invalid_ctx; // default = invalid
	ASSERT_FALSE(invalid_ctx.IsValid());

	std::atomic<bool> ran{ false };
	scheduler.SubmitIn(invalid_ctx, [&ran]() { ran.store(true, std::memory_order_release); });

	EXPECT_TRUE(PollUntil([&] { return ran.load(std::memory_order_acquire); }))
	    << "SubmitIn with invalid context must fall back to Submit and still run the job";
}

// ---------------------------------------------------------------------------
// TEST 7: Multiple workers each produce a valid, distinct context
// ---------------------------------------------------------------------------
TEST(SchedulerContextTest, AllWorkersProduceDistinctValidContexts)
{
	constexpr std::size_t kWorkers = 4;
	ThreadPoolScheduler scheduler(kWorkers);

	std::atomic<int> valid_count{ 0 };
	std::atomic<int> task_count{ 0 };

	// Submit one task per worker slot; each should see a valid context.
	for (std::size_t i = 0; i < kWorkers * 2; ++i)
	{
		scheduler.Submit([&scheduler, &valid_count, &task_count]() {
			auto ctx = scheduler.CaptureContext();
			if (ctx.IsValid())
				valid_count.fetch_add(1, std::memory_order_relaxed);
			task_count.fetch_add(1, std::memory_order_relaxed);
		});
	}

	ASSERT_TRUE(PollUntil([&] { return task_count.load() == static_cast<int>(kWorkers * 2); }));
	EXPECT_EQ(valid_count.load(), static_cast<int>(kWorkers * 2)) << "Every task submitted to worker threads must see a valid Context";
}

// ---------------------------------------------------------------------------
// TEST 8: Two schedulers — contexts do not cross-validate
// ---------------------------------------------------------------------------
TEST(SchedulerContextTest, ContextsDoNotCrossValidate)
{
	ThreadPoolScheduler sched_a(2);
	ThreadPoolScheduler sched_b(2);

	// Capture a context on sched_a's worker and verify it is NOT valid on sched_b
	ThreadPoolScheduler::Context ctx_a;
	std::atomic<bool> captured{ false };

	sched_a.Submit([&sched_a, &ctx_a, &captured]() {
		ctx_a = sched_a.CaptureContext();
		captured.store(true, std::memory_order_release);
	});
	ASSERT_TRUE(PollUntil([&] { return captured.load(std::memory_order_acquire); }));

	// ctx_a.IsValid() is an instance-agnostic check (non-null scheduler ptr); it is true.
	EXPECT_TRUE(ctx_a.IsValid());

	// SubmitIn on sched_b with ctx_a: should fall back (job still executes).
	std::atomic<bool> ran{ false };
	sched_b.SubmitIn(ctx_a, [&ran]() { ran.store(true, std::memory_order_release); });
	EXPECT_TRUE(PollUntil([&] { return ran.load(std::memory_order_acquire); }))
	    << "Cross-scheduler SubmitIn must still execute the job via fallback";
}

// ---------------------------------------------------------------------------
int main(int argc, char** argv)
{
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
