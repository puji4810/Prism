#include "../src/runtime_executor.h"
#include "../src/task_scope.h"

#include <atomic>
#include <chrono>
#include <gtest/gtest.h>
#include <semaphore>
#include <thread>
#include <vector>

using namespace prism;
using namespace std::chrono_literals;

namespace
{
	template <typename Pred>
	bool WaitUntil(Pred pred, std::chrono::milliseconds timeout = 5s)
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

	class InlineExecutor: public IContinuationExecutor
	{
	public:
		void Submit(std::function<void()> work) override { work(); }
	};
}

TEST(StructuredTaskTest, SerialLanePreservesSubmissionOrder)
{
	SerialLane lane;
	std::mutex mutex;
	std::vector<int> observed;
	std::atomic<int> done{ 0 };

	for (int i = 0; i < 8; ++i)
	{
		lane.Submit([&mutex, &observed, &done, i] {
			std::lock_guard lock(mutex);
			observed.push_back(i);
			done.fetch_add(1, std::memory_order_release);
		});
	}

	ASSERT_TRUE(WaitUntil([&done] { return done.load(std::memory_order_acquire) == 8; }));
	EXPECT_TRUE(lane.Done());
	EXPECT_EQ(observed, (std::vector<int>{ 0, 1, 2, 3, 4, 5, 6, 7 }));
}

TEST(StructuredTaskTest, BlockingExecutorRunsSubmittedWork)
{
	BlockingExecutor executor;
	std::binary_semaphore finished(0);
	std::atomic<int> value{ 0 };

	executor.Submit([&] {
		value.store(7, std::memory_order_release);
		finished.release();
	});

	ASSERT_TRUE(finished.try_acquire_for(5s));
	EXPECT_EQ(value.load(std::memory_order_acquire), 7);
}

TEST(StructuredTaskTest, ThreadPoolExecutorForwardsToWrappedScheduler)
{
	ThreadPoolScheduler scheduler(2);
	ThreadPoolExecutor executor(scheduler);
	std::binary_semaphore finished(0);
	std::atomic<int> value{ 0 };

	executor.Submit([&] {
		value.store(11, std::memory_order_release);
		finished.release();
	});

	ASSERT_TRUE(finished.try_acquire_for(5s));
	EXPECT_EQ(value.load(std::memory_order_acquire), 11);
}

TEST(StructuredTaskTest, RuntimeBundleConstructsAndDrainsResources)
{
	std::binary_semaphore blocking_done(0);
	std::binary_semaphore serial_done(0);
	std::binary_semaphore cpu_done(0);

	{
		ThreadPoolScheduler scheduler(2);
		RuntimeBundle runtime(scheduler);

		runtime.read_executor.Submit([&] { blocking_done.release(); });
		runtime.serial_lane.Submit([&] { serial_done.release(); });
		runtime.cpu_executor->Submit([&] { cpu_done.release(); });

		ASSERT_TRUE(blocking_done.try_acquire_for(5s));
		ASSERT_TRUE(serial_done.try_acquire_for(5s));
		ASSERT_TRUE(cpu_done.try_acquire_for(5s));
		EXPECT_TRUE(runtime.read_executor.Empty());
		EXPECT_TRUE(runtime.compaction_executor.Empty());
		EXPECT_TRUE(runtime.serial_lane.Done());
	}
}

TEST(StructuredTaskTest, SchedulerAdapterRoutesBlockingAndContinuationSeparately)
{
	InlineExecutor continuation_executor;
	InlineExecutor blocking_executor;
	ExecutorSchedulerAdapter blocking_scheduler(blocking_executor);
	ExecutorSchedulerAdapter continuation_scheduler(continuation_executor);
	ExecutorSchedulerAdapter runtime_scheduler(continuation_executor, &blocking_scheduler, &continuation_scheduler);

	EXPECT_EQ(runtime_scheduler.BlockingScheduler(), &blocking_scheduler);
	EXPECT_EQ(runtime_scheduler.ContinuationScheduler(), &continuation_scheduler);
}

TEST(StructuredTaskTest, TaskScopeSubmitsAndJoinsChildren)
{
	BlockingExecutor executor;
	std::binary_semaphore entered(0);
	std::binary_semaphore release(0);
	std::binary_semaphore destroyed(0);
	std::atomic<bool> destroy_done{ false };

	auto scope = std::make_unique<TaskScope>(executor);
	scope->Submit([&](StopToken) {
		entered.release();
		release.acquire();
	});

	ASSERT_TRUE(entered.try_acquire_for(5s));

	std::thread destroy_thread([&] {
		scope.reset();
		destroy_done.store(true, std::memory_order_release);
		destroyed.release();
	});

	std::this_thread::yield();
	EXPECT_FALSE(destroy_done.load(std::memory_order_acquire));

	release.release();
	ASSERT_TRUE(destroyed.try_acquire_for(5s));
	destroy_thread.join();
	EXPECT_TRUE(destroy_done.load(std::memory_order_acquire));
}

TEST(StructuredTaskTest, NestedScopesObserveParentStopPropagation)
{
	InlineExecutor executor;
	StopSource parent_source;
	TaskScope parent_scope(executor, parent_source);
	TaskScope child_scope(executor, parent_scope.GetStopToken());
	std::atomic<bool> child_saw_stop{ false };

	parent_source.RequestStop();
	child_scope.Submit([&](StopToken token) {
		child_saw_stop.store(token.CheckStop(), std::memory_order_release);
	});

	child_scope.Join();
	EXPECT_TRUE(child_scope.StopRequested());
	EXPECT_FALSE(child_saw_stop.load(std::memory_order_acquire));
	EXPECT_EQ(child_scope.QuarantineSink().Count(Quarantine::EntryKind::kCancelled), 1);
}

TEST(StructuredTaskTest, ChildScopeDestructorCleansUpBeforeParentReturns)
{
	BlockingExecutor executor;
	std::binary_semaphore child_started(0);
	std::binary_semaphore unblock_child(0);
	std::binary_semaphore child_destroyed(0);
	std::atomic<bool> child_destroy_done{ false };

	TaskScope parent_scope(executor);
	auto child_scope = std::make_unique<TaskScope>(executor, parent_scope.GetStopToken());
	child_scope->Submit([&](StopToken) {
		child_started.release();
		unblock_child.acquire();
	});

	ASSERT_TRUE(child_started.try_acquire_for(5s));

	std::thread child_destroy_thread([&] {
		child_scope.reset();
		child_destroy_done.store(true, std::memory_order_release);
		child_destroyed.release();
	});

	std::this_thread::yield();
	EXPECT_FALSE(child_destroy_done.load(std::memory_order_acquire));

	unblock_child.release();
	ASSERT_TRUE(child_destroyed.try_acquire_for(5s));
	child_destroy_thread.join();
	EXPECT_TRUE(child_destroy_done.load(std::memory_order_acquire));
}
