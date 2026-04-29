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

// ---------------------------------------------------------------------------
// RetiredCloseWorkDrainedDuringShutdown
// Models the retired-WAL/background-close shutdown path through delayed tasks
// (future deadline, must be promoted on shutdown) + affinity tasks submitted
// from worker context (simulating the coordination path for WAL close).
// Fills the TODO gap at lines 30-35 about shutdown drain of retired-close work.
// ---------------------------------------------------------------------------
TEST(SchedulerShutdownTest, RetiredCloseWorkDrainedDuringShutdown)
{
	auto close_counter = std::make_shared<std::atomic<int>>(0);
	constexpr int kNumCloseTasks = 100;

	{
		ThreadPoolScheduler scheduler(4);

		// Model retired-WAL close as delayed tasks with future deadlines.
		// These simulate scheduled background-close work that must be promoted
		// to immediate execution during shutdown drain.
		auto future = std::chrono::steady_clock::now() + 30s;
		for (int i = 0; i < kNumCloseTasks; ++i)
		{
			scheduler.SubmitAfter(future, [close_counter]() { close_counter->fetch_add(1, std::memory_order_relaxed); });
		}

		// Model the worker-coordination path: a dispatched job captures its
		// context and submits an affinity "close" continuation.
		scheduler.Submit([&scheduler, close_counter]() {
			auto ctx = scheduler.CaptureContext();
			ASSERT_TRUE(ctx.IsValid());
			scheduler.SubmitIn(ctx, [close_counter]() { close_counter->fetch_add(1, std::memory_order_relaxed); });
		});
	}

	// All tasks must be drained: kNumCloseTasks delayed + 1 affinity continuation.
	EXPECT_EQ(close_counter->load(), kNumCloseTasks + 1)
	    << "Retired-close modeled tasks were not fully drained on shutdown";
}

// ---------------------------------------------------------------------------
// AllSubmissionPathsDrainedOnShutdown
// Submit tasks through ALL submission paths (priority, lazy with future
// deadline, affinity, and worker-chained) and verify the scheduler destructor
// drains every queue completely.
// ---------------------------------------------------------------------------
TEST(SchedulerShutdownTest, AllSubmissionPathsDrainedOnShutdown)
{
	auto priority_counter = std::make_shared<std::atomic<int>>(0);
	auto lazy_counter = std::make_shared<std::atomic<int>>(0);
	auto affinity_counter = std::make_shared<std::atomic<int>>(0);
	constexpr int kNumEach = 100;

	{
		ThreadPoolScheduler scheduler(4);

		// 1. Priority queue (immediate tasks with varying priority)
		for (int i = 0; i < kNumEach; ++i)
		{
			scheduler.Submit(
			    [priority_counter]() { priority_counter->fetch_add(1, std::memory_order_relaxed); }, static_cast<std::size_t>(i % 5));
		}

		// 2. Lazy queue (future deadline — must be promoted on drain)
		auto future = std::chrono::steady_clock::now() + 30s;
		for (int i = 0; i < kNumEach; ++i)
		{
			scheduler.SubmitAfter(future, [lazy_counter]() { lazy_counter->fetch_add(1, std::memory_order_relaxed); });
		}

		// 3. Affinity path: from a dispatched worker, submit tasks via SubmitIn
		//    into the same worker's queue. These are NOT dispatcher-tracked and
		//    must be drained from the worker's local queue.
		scheduler.Submit([&scheduler, affinity_counter]() {
			auto ctx = scheduler.CaptureContext();
			ASSERT_TRUE(ctx.IsValid());
			for (int i = 0; i < kNumEach; ++i)
			{
				scheduler.SubmitIn(ctx,
				    [affinity_counter]() { affinity_counter->fetch_add(1, std::memory_order_relaxed); });
			}
		});
	}

	EXPECT_EQ(priority_counter->load(), kNumEach) << "Priority tasks were dropped on shutdown";
	EXPECT_EQ(lazy_counter->load(), kNumEach) << "Lazy tasks were not promoted/drained on shutdown";
	EXPECT_EQ(affinity_counter->load(), kNumEach) << "Affinity tasks were dropped on shutdown";
}

// ---------------------------------------------------------------------------
// NoDoubleExecutionDuringDrain
// Verify that tasks drained during shutdown execute exactly once — not zero
// and not twice. Uses a counter that must equal exactly kNumTasks (not >=).
// Catches double-execution bugs where a task runs during normal operation
// AND again during the sequential drain phase.
// ---------------------------------------------------------------------------
TEST(SchedulerShutdownTest, NoDoubleExecutionDuringDrain)
{
	auto counter = std::make_shared<std::atomic<int>>(0);
	constexpr int kNumTasks = 500;

	{
		ThreadPoolScheduler scheduler(2);

		// Mixed workload:
		// - Immediate tasks that may or may not run before shutdown starts
		// - Delayed tasks with future deadline that will be promoted on drain
		for (int i = 0; i < kNumTasks; ++i)
		{
			scheduler.Submit([counter]() { counter->fetch_add(1, std::memory_order_relaxed); });
		}

		auto future = std::chrono::steady_clock::now() + 30s;
		for (int i = 0; i < kNumTasks; ++i)
		{
			scheduler.SubmitAfter(future, [counter]() { counter->fetch_add(1, std::memory_order_relaxed); });
		}
	}

	// The counter must equal exactly kNumTasks * 2 — no more (double-execution)
	// and no less (dropped tasks).
	const int actual = counter->load();
	EXPECT_EQ(actual, kNumTasks * 2) << "Expected exactly " << (kNumTasks * 2) << " task executions, got " << actual
	                                 << " (tasks were either dropped or double-executed during drain)";
}

// ---------------------------------------------------------------------------
// WorkerLocalReentrantSubmitDuringShutdownDrain
// Worker threads that submit new jobs during shutdown drain must still have
// those late-submitted jobs drained. This is the re-entrant submit invariant:
// draining jobs may spawn more work, and the system must not drop it.
// ---------------------------------------------------------------------------
TEST(SchedulerShutdownTest, WorkerLocalReentrantSubmitDuringShutdownDrain)
{
	auto outer_counter = std::make_shared<std::atomic<int>>(0);
	auto inner_counter = std::make_shared<std::atomic<int>>(0);
	constexpr int kOuter = 200;

	{
		ThreadPoolScheduler scheduler(4);
		for (int i = 0; i < kOuter; ++i)
		{
			scheduler.Submit([&scheduler, outer_counter, inner_counter]() {
				outer_counter->fetch_add(1, std::memory_order_relaxed);
				// Re-entrant submit: worker submits a child job during its own execution.
				// During shutdown drain, this child must also be drained.
				scheduler.Submit([inner_counter]() {
					inner_counter->fetch_add(1, std::memory_order_relaxed);
				});
			});
		}
	}

	EXPECT_EQ(outer_counter->load(), kOuter) << "Outer tasks were dropped";
	EXPECT_EQ(inner_counter->load(), kOuter)
	    << "Inner re-entrant tasks were dropped during shutdown drain";
}

// ---------------------------------------------------------------------------
// DelayedTaskPromotesDuringShutdownWithFallback
// SubmitAfter with a far-future deadline that will NOT expire naturally.
// On scheduler destruction, these must be promoted to immediate execution
// via the fallback/drain path. Tests the delayed-task promotion invariant.
// ---------------------------------------------------------------------------
TEST(SchedulerShutdownTest, DelayedTaskPromotesDuringShutdownWithFallback)
{
	auto counter = std::make_shared<std::atomic<int>>(0);
	constexpr int kNumTasks = 200;

	{
		ThreadPoolScheduler scheduler(4);

		auto future = std::chrono::steady_clock::now() + 30s;
		for (int i = 0; i < kNumTasks; ++i)
		{
			scheduler.SubmitAfter(future, [counter]() {
				counter->fetch_add(1, std::memory_order_relaxed);
			});
		}
	}

	EXPECT_EQ(counter->load(), kNumTasks)
	    << "Delayed tasks with future deadline were not promoted/executed during shutdown";
}
