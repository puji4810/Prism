#include "scheduler.h"

#include <atomic>
#include <chrono>
#include <gtest/gtest.h>
#include <memory>
#include <semaphore>
#include <thread>

using namespace prism;
using namespace std::chrono_literals;

// ---------------------------------------------------------------------------
// Helper: bounded poll that waits until predicate is true or timeout expires.
// Returns true if predicate became true within the timeout.
// ---------------------------------------------------------------------------
template <typename Pred>
static bool WaitUntil(Pred pred, std::chrono::milliseconds timeout = 10s)
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

// ===========================================================================
// TODO(wal-rotation): Test coverage for shutdown drain of retired-close work
// - scheduler/background work coordination for WAL rotation
// - exact-once drain/shutdown pattern for retired-close background path
// - shutdown drain for all retired-WAL close background tasks
// ===========================================================================

// ---------------------------------------------------------------------------
// ImmediateTasks
// Submit a large batch of immediate tasks, then let the scheduler go out of
// scope (triggering destruction/drain).  All tasks must have executed.
// ---------------------------------------------------------------------------
TEST(SchedulerShutdownTest, ImmediateTasks)
{
	auto counter = std::make_shared<std::atomic<int>>(0);
	constexpr int kNumTasks = 1000;

	{
		ThreadPoolScheduler scheduler(4);
		for (int i = 0; i < kNumTasks; ++i)
		{
			scheduler.Submit([counter]() { counter->fetch_add(1, std::memory_order_relaxed); });
		}
		// Destructor drains all queues before returning.
	}

	EXPECT_EQ(counter->load(), kNumTasks) << "Immediate tasks were dropped on scheduler shutdown";
}

// ---------------------------------------------------------------------------
// ImmediateTasksWithPriority
// Same as ImmediateTasks but tasks are submitted with varying priorities to
// exercise the priority queue path during drain.
// ---------------------------------------------------------------------------
TEST(SchedulerShutdownTest, ImmediateTasksWithPriority)
{
	auto counter = std::make_shared<std::atomic<int>>(0);
	constexpr int kNumTasks = 1000;

	{
		ThreadPoolScheduler scheduler(4);
		for (int i = 0; i < kNumTasks; ++i)
		{
			scheduler.Submit([counter]() { counter->fetch_add(1, std::memory_order_relaxed); }, static_cast<std::size_t>(i % 10));
		}
	}

	EXPECT_EQ(counter->load(), kNumTasks) << "Priority tasks were dropped on scheduler shutdown";
}

// ---------------------------------------------------------------------------
// DelayedTasksShortDeadline
// Submit delayed tasks with very short deadlines (already expired or expiring
// imminently) so the LazyLoop would normally promote them.  On shutdown the
// scheduler must drain the lazy queue and execute them.
// ---------------------------------------------------------------------------
TEST(SchedulerShutdownTest, DelayedTasksShortDeadline)
{
	auto counter = std::make_shared<std::atomic<int>>(0);
	constexpr int kNumTasks = 200;

	{
		ThreadPoolScheduler scheduler(4);

		// Use a deadline in the past so tasks are immediately eligible.
		auto past = std::chrono::steady_clock::now() - 1s;
		for (int i = 0; i < kNumTasks; ++i)
		{
			scheduler.SubmitAfter(past, [counter]() { counter->fetch_add(1, std::memory_order_relaxed); });
		}

		// Give LazyLoop a brief window to pick up tasks that are already past
		// their deadline before we trigger shutdown.  This validates the normal
		// path as well as the shutdown drain path.
		WaitUntil([&counter]() { return counter->load() >= kNumTasks; }, 100ms);
	}

	EXPECT_EQ(counter->load(), kNumTasks) << "Delayed tasks (short deadline) were dropped on scheduler shutdown";
}

// ---------------------------------------------------------------------------
// DelayedTasksPromotedOnShutdown
// Submit delayed tasks with a future deadline so they are still pending in the
// lazy queue when shutdown is triggered.  The scheduler must promote them to
// immediate execution during drain.
// ---------------------------------------------------------------------------
TEST(SchedulerShutdownTest, DelayedTasksPromotedOnShutdown)
{
	auto counter = std::make_shared<std::atomic<int>>(0);
	constexpr int kNumTasks = 200;

	{
		ThreadPoolScheduler scheduler(4);

		// Deadline 30 seconds in the future — will NOT expire naturally before
		// the scheduler destructs, so drain must promote them.
		auto future = std::chrono::steady_clock::now() + 30s;
		for (int i = 0; i < kNumTasks; ++i)
		{
			scheduler.SubmitAfter(future, [counter]() { counter->fetch_add(1, std::memory_order_relaxed); });
		}
		// Destructor must promote pending lazy tasks to immediate execution.
	}

	EXPECT_EQ(counter->load(), kNumTasks) << "Delayed tasks were not promoted to immediate execution on shutdown";
}

// ---------------------------------------------------------------------------
// MixedTasks
// Submit both immediate and delayed tasks, then destroy the scheduler.
// Both sets must complete.
// ---------------------------------------------------------------------------
TEST(SchedulerShutdownTest, MixedTasks)
{
	auto immediate_counter = std::make_shared<std::atomic<int>>(0);
	auto delayed_counter = std::make_shared<std::atomic<int>>(0);
	constexpr int kNumEach = 500;

	{
		ThreadPoolScheduler scheduler(4);

		// Immediate tasks
		for (int i = 0; i < kNumEach; ++i)
		{
			scheduler.Submit(
			    [immediate_counter]() { immediate_counter->fetch_add(1, std::memory_order_relaxed); }, static_cast<std::size_t>(i % 5));
		}

		// Delayed tasks with future deadline (must be promoted on shutdown)
		auto future = std::chrono::steady_clock::now() + 30s;
		for (int i = 0; i < kNumEach; ++i)
		{
			scheduler.SubmitAfter(future, [delayed_counter]() { delayed_counter->fetch_add(1, std::memory_order_relaxed); });
		}
	}

	EXPECT_EQ(immediate_counter->load(), kNumEach) << "Immediate tasks were dropped during mixed-workload shutdown";
	EXPECT_EQ(delayed_counter->load(), kNumEach) << "Delayed tasks were not promoted/executed during mixed-workload shutdown";
}

// ---------------------------------------------------------------------------
// HighBacklogImmediate
// Flood the scheduler with more tasks than workers can service instantly to
// ensure the backlog in the priority queue is fully drained on shutdown.
// ---------------------------------------------------------------------------
TEST(SchedulerShutdownTest, HighBacklogImmediate)
{
	auto counter = std::make_shared<std::atomic<int>>(0);
	constexpr int kNumTasks = 5000;

	{
		// Use 2 threads to maximise backlog pressure.
		ThreadPoolScheduler scheduler(2);
		for (int i = 0; i < kNumTasks; ++i)
		{
			scheduler.Submit([counter]() { counter->fetch_add(1, std::memory_order_relaxed); });
		}
	}

	EXPECT_EQ(counter->load(), kNumTasks) << "High-backlog immediate tasks were dropped on scheduler shutdown";
}

// ---------------------------------------------------------------------------
// LateSubmissionsDuringShutdown
// The header contract states: "Late Submissions: Submissions during shutdown
// are permitted and will be drained."  We approximate this by submitting
// tasks from inside a running task so that some submissions race with shutdown.
// ---------------------------------------------------------------------------
TEST(SchedulerShutdownTest, LateSubmissionsDuringShutdown)
{
	auto outer_counter = std::make_shared<std::atomic<int>>(0);
	auto inner_counter = std::make_shared<std::atomic<int>>(0);
	constexpr int kOuter = 100;

	{
		ThreadPoolScheduler scheduler(4);
		for (int i = 0; i < kOuter; ++i)
		{
			scheduler.Submit([&scheduler, outer_counter, inner_counter]() {
				outer_counter->fetch_add(1, std::memory_order_relaxed);
				// Submit a child task from within a worker — this is a "late"
				// submission that must also be drained.
				scheduler.Submit([inner_counter]() { inner_counter->fetch_add(1, std::memory_order_relaxed); });
			});
		}
	}

	EXPECT_EQ(outer_counter->load(), kOuter) << "Outer tasks were dropped";
	EXPECT_EQ(inner_counter->load(), kOuter) << "Inner (late-submitted) tasks were dropped on shutdown";
}
