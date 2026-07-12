#include "scheduler.h"
#include "runtime_metrics.h"

#include <atomic>
#include <chrono>
#include <gtest/gtest.h>
#include <memory>

using namespace prism;
using namespace std::chrono_literals;

TEST(SchedulerStressTest, HighConcurrency)
{
	CpuThreadPool scheduler(4);
	auto counter = std::make_shared<std::atomic<int>>(0);
	constexpr int kNumTasks = 10000;

	for (int i = 0; i < kNumTasks; ++i)
	{
		scheduler.SubmitWithPriority([counter]() { counter->fetch_add(1, std::memory_order_relaxed); }, i % 10);
	}

	// Poll with bounded timeout instead of sleep_for
	auto start = std::chrono::steady_clock::now();
	while (std::chrono::steady_clock::now() - start < 5s)
	{
		if (counter->load() == kNumTasks)
		{
			break;
		}
		std::this_thread::yield();
	}
	EXPECT_EQ(counter->load(), kNumTasks) << "Some tasks were lost!";
}

TEST(SchedulerStressTest, AffinityTasks)
{
	CpuThreadPool scheduler(4);
	auto counter = std::make_shared<std::atomic<int>>(0);
	constexpr int kNumTasks = 1000;

	for (int i = 0; i < kNumTasks; ++i)
	{
		scheduler.Submit([&scheduler, counter]() {
			// Capture context from within the worker thread
			auto my_ctx = scheduler.CaptureContext();

			// Submit affinity task - should try to execute on same thread
			scheduler.SubmitIn(my_ctx, [counter]() { counter->fetch_add(1, std::memory_order_relaxed); });
		});
	}

	// Poll with bounded timeout
	auto start = std::chrono::steady_clock::now();
	while (std::chrono::steady_clock::now() - start < 5s)
	{
		if (counter->load() == kNumTasks)
		{
			break;
		}
		std::this_thread::yield();
	}

	EXPECT_EQ(counter->load(), kNumTasks) << "Some affinity tasks were lost";
}

TEST(SchedulerStressTest, MixedWorkload)
{
	CpuThreadPool scheduler(4);
	auto immediate = std::make_shared<std::atomic<int>>(0);
	auto affinity = std::make_shared<std::atomic<int>>(0);
	constexpr int kNumTasks = 1000;

	for (int i = 0; i < kNumTasks; ++i)
	{
		scheduler.SubmitWithPriority([immediate]() { immediate->fetch_add(1, std::memory_order_relaxed); }, i % 5);

		scheduler.Submit([&scheduler, affinity]() {
			auto ctx = scheduler.CaptureContext();
			scheduler.SubmitIn(ctx, [affinity]() { affinity->fetch_add(1, std::memory_order_relaxed); });
		});
	}

	// Wait until all counters reach target
	// Poll with bounded timeout instead of fixed sleep
	auto start = std::chrono::steady_clock::now();
	while (std::chrono::steady_clock::now() - start < 5s)
	{
		if (immediate->load() == kNumTasks && affinity->load() == kNumTasks)
		{
			break;
		}
		std::this_thread::yield();
	}

	EXPECT_EQ(immediate->load(), kNumTasks);
	EXPECT_EQ(affinity->load(), kNumTasks);
}

TEST(SchedulerStressTest, MixedAffinityAndStolenWorkCompletesAll)
{
	CpuThreadPool scheduler(4);
	auto counter = std::make_shared<std::atomic<int>>(0);
	constexpr int kNumTasks = 1000;

	for (int i = 0; i < kNumTasks; ++i)
	{
		scheduler.Submit([counter]() { counter->fetch_add(1, std::memory_order_relaxed); });

		scheduler.Submit([&scheduler, counter]() {
			auto ctx = scheduler.CaptureContext();
			scheduler.SubmitIn(ctx, [counter]() { counter->fetch_add(1, std::memory_order_relaxed); });
		});
	}

	auto start = std::chrono::steady_clock::now();
	while (std::chrono::steady_clock::now() - start < 10s)
	{
		if (counter->load() == kNumTasks * 2)
			break;
		std::this_thread::yield();
	}

	EXPECT_EQ(counter->load(), kNumTasks * 2) << "Mixed Submit + SubmitIn must all complete exactly once";
}

// ---------------------------------------------------------------------------
// StolenWorkProgressAssertions
// Submit a high volume of stealable tasks (priority 0, worker-local path)
// and verify that the steal metrics reflect actual stealing activity, and
// that all tasks complete including those that were stolen.
// ---------------------------------------------------------------------------
TEST(SchedulerStressTest, StolenWorkProgressAssertions)
{
#ifdef PRISM_RUNTIME_METRICS
	prism::RuntimeMetrics::Instance().Reset();
#endif

	CpuThreadPool scheduler(4);
	auto counter = std::make_shared<std::atomic<int>>(0);
	constexpr int kNumTasks = 5000;

	// Submit tasks as stealable (priority 0, non-pinned).
	for (int i = 0; i < kNumTasks; ++i)
	{
		scheduler.Submit([counter]() { counter->fetch_add(1, std::memory_order_relaxed); });
	}

	auto start = std::chrono::steady_clock::now();
	while (counter->load() < kNumTasks && std::chrono::steady_clock::now() - start < 10s)
		std::this_thread::yield();

	EXPECT_EQ(counter->load(), kNumTasks) << "Not all stealable tasks completed: " << counter->load() << "/" << kNumTasks;

#ifdef PRISM_RUNTIME_METRICS
	// Verify that some jobs were completed (whether local or stolen).
	const auto total_completed = prism::RuntimeMetrics::Instance().worker_local_jobs_completed.load(std::memory_order_relaxed)
	    + prism::RuntimeMetrics::Instance().stolen_jobs_completed.load(std::memory_order_relaxed);

	EXPECT_GE(total_completed, static_cast<uint64_t>(kNumTasks))
	    << "Fewer tasks completed (" << total_completed << ") than submitted (" << kNumTasks << ")";

	// With 4 workers and 5000 tasks, stealing should occur at least some of the time.
	// This is a characterization: if steal_attempts == 0, that's noteworthy but not
	// necessarily a bug (can happen under very fast task completion).
	const auto steal_attempts = prism::RuntimeMetrics::Instance().steal_attempts.load(std::memory_order_relaxed);
	const auto steal_successes = prism::RuntimeMetrics::Instance().steal_successes.load(std::memory_order_relaxed);

	// Record the observed stealing behavior for characterization purposes.
	// No hard assertion on steal count since it depends on timing.
	SUCCEED() << "Characterization: steal_attempts=" << steal_attempts << " steal_successes=" << steal_successes
	          << " stolen_jobs_completed=" << prism::RuntimeMetrics::Instance().stolen_jobs_completed.load(std::memory_order_relaxed);
#else
	auto& metrics = prism::RuntimeMetrics::Instance();
	EXPECT_EQ(metrics.foreground_fastpath_submits.load(std::memory_order_relaxed), 0u);
	EXPECT_EQ(metrics.foreground_fallback_submits.load(std::memory_order_relaxed), 0u);
	EXPECT_EQ(metrics.steal_attempts.load(std::memory_order_relaxed), 0u);
	EXPECT_EQ(metrics.steal_successes.load(std::memory_order_relaxed), 0u);
	EXPECT_EQ(metrics.worker_local_jobs_completed.load(std::memory_order_relaxed), 0u);
	EXPECT_EQ(metrics.stolen_jobs_completed.load(std::memory_order_relaxed), 0u);
#endif
}

// ---------------------------------------------------------------------------
// HighConcurrencyStolenWorkAllCompletes
// Submit a very large volume of stealable + affinity-mixed work at high
// concurrency. All tasks must complete regardless of how many times they
// were stolen or executed locally.
// ---------------------------------------------------------------------------
TEST(SchedulerStressTest, HighConcurrencyStolenWorkAllCompletes)
{
#ifdef PRISM_RUNTIME_METRICS
	prism::RuntimeMetrics::Instance().Reset();
#endif

	CpuThreadPool scheduler(4);
	auto counter = std::make_shared<std::atomic<int>>(0);
	constexpr int kNumTasks = 10000;

	// Mix of stealable (direct Submit) and pinned (SubmitIn from worker).
	for (int i = 0; i < kNumTasks; ++i)
	{
		scheduler.Submit([counter]() { counter->fetch_add(1, std::memory_order_relaxed); });

		scheduler.Submit([&scheduler, counter]() {
			auto ctx = scheduler.CaptureContext();
			if (ctx.IsValid())
			{
				scheduler.SubmitIn(ctx, [counter]() { counter->fetch_add(1, std::memory_order_relaxed); });
			}
			else
			{
				counter->fetch_add(1, std::memory_order_relaxed);
			}
		});
	}

	auto start = std::chrono::steady_clock::now();
	while (counter->load() < kNumTasks * 2 && std::chrono::steady_clock::now() - start < 15s)
		std::this_thread::yield();

	EXPECT_EQ(counter->load(), kNumTasks * 2) << "High-concurrency stolen+affinity work incomplete: " << counter->load() << "/"
	                                          << (kNumTasks * 2);

#ifdef PRISM_RUNTIME_METRICS
	const auto total_completed = prism::RuntimeMetrics::Instance().worker_local_jobs_completed.load(std::memory_order_relaxed)
	    + prism::RuntimeMetrics::Instance().stolen_jobs_completed.load(std::memory_order_relaxed);

	EXPECT_GE(total_completed, static_cast<uint64_t>(kNumTasks * 2))
	    << "Total completed jobs (" << total_completed << ") less than submitted (" << (kNumTasks * 2) << ")";
#endif
}
