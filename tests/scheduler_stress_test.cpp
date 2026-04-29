#include "scheduler.h"

#include <atomic>
#include <chrono>
#include <gtest/gtest.h>
#include <memory>

using namespace prism;
using namespace std::chrono_literals;

TEST(SchedulerStressTest, HighConcurrency)
{
	ThreadPoolScheduler scheduler(4);
	auto counter = std::make_shared<std::atomic<int>>(0);
	constexpr int kNumTasks = 10000;

	for (int i = 0; i < kNumTasks; ++i)
	{
		scheduler.Submit([counter]() { counter->fetch_add(1, std::memory_order_relaxed); }, i % 10);
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
	ThreadPoolScheduler scheduler(4);
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
	ThreadPoolScheduler scheduler(4);
	auto immediate = std::make_shared<std::atomic<int>>(0);
	auto affinity = std::make_shared<std::atomic<int>>(0);
	constexpr int kNumTasks = 1000;

	for (int i = 0; i < kNumTasks; ++i)
	{
		scheduler.Submit([immediate]() { immediate->fetch_add(1, std::memory_order_relaxed); }, i % 5);

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
	ThreadPoolScheduler scheduler(4);
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

	EXPECT_EQ(counter->load(), kNumTasks * 2)
	    << "Mixed Submit + SubmitIn must all complete exactly once";
}
