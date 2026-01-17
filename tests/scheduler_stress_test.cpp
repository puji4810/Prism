#include "scheduler.h"

#include <atomic>
#include <chrono>
#include <gtest/gtest.h>
#include <vector>

using namespace prism;
using namespace std::chrono_literals;

TEST(SchedulerStressTest, HighConcurrency)
{
	ThreadPoolScheduler scheduler(4);
	std::atomic<int> counter{ 0 };
	constexpr int kNumTasks = 10000;

	for (int i = 0; i < kNumTasks; ++i)
	{
		scheduler.Submit([&counter] { counter.fetch_add(1, std::memory_order_relaxed); }, i % 10);
	}

	std::this_thread::sleep_for(500ms);
	EXPECT_EQ(counter.load(), kNumTasks) << "Some tasks were lost!";
}

TEST(SchedulerStressTest, DelayedTasks)
{
	ThreadPoolScheduler scheduler(2);
	std::atomic<int> completed{ 0 };
	constexpr int kNumTasks = 100;

	auto start = std::chrono::steady_clock::now();
	for (int i = 0; i < kNumTasks; ++i)
	{
		scheduler.SubmitAfter(100ms, [&completed] { completed.fetch_add(1, std::memory_order_relaxed); });
	}

	std::this_thread::sleep_for(200ms);
	EXPECT_EQ(completed.load(), kNumTasks) << "Not all delayed tasks completed";

	auto elapsed = std::chrono::steady_clock::now() - start;
	EXPECT_GE(elapsed, 100ms) << "Tasks completed too early";
}

TEST(SchedulerStressTest, AffinityTasks)
{
	ThreadPoolScheduler scheduler(4);
	auto ctx = ThreadPoolScheduler::CaptureContext();

	std::atomic<int> counter{ 0 };
	constexpr int kNumTasks = 1000;

	for (int i = 0; i < kNumTasks; ++i)
	{
		scheduler.Submit([&scheduler, &counter] {
			auto my_ctx = ThreadPoolScheduler::CaptureContext();
			scheduler.SubmitIn(my_ctx, [&counter] { counter.fetch_add(1, std::memory_order_relaxed); });
		});
	}

	std::this_thread::sleep_for(500ms);
	EXPECT_EQ(counter.load(), kNumTasks) << "Some affinity tasks were lost";
}

TEST(SchedulerStressTest, MixedWorkload)
{
	ThreadPoolScheduler scheduler(4);
	std::atomic<int> immediate{ 0 }, delayed{ 0 }, affinity{ 0 };

	for (int i = 0; i < 1000; ++i)
	{
		scheduler.Submit([&immediate] { immediate.fetch_add(1, std::memory_order_relaxed); }, i % 5);

		scheduler.SubmitAfter(50ms, [&delayed] { delayed.fetch_add(1, std::memory_order_relaxed); });

		scheduler.Submit([&scheduler, &affinity] {
			auto ctx = ThreadPoolScheduler::CaptureContext();
			scheduler.SubmitIn(ctx, [&affinity] { affinity.fetch_add(1, std::memory_order_relaxed); });
		});
	}

	std::this_thread::sleep_for(500ms);
	EXPECT_EQ(immediate.load(), 1000);
	EXPECT_EQ(delayed.load(), 1000);
	EXPECT_EQ(affinity.load(), 1000);
}

int main(int argc, char** argv)
{
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
