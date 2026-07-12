#include "../src/async_runtime.h"
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

	class InlineExecutor
	{
	public:
		using Job = InlineJob;

		void Submit(Job work) { work(); }
	};
}

TEST(StructuredTaskTest, SerialExecutorPreservesSubmissionOrder)
{
	SerialExecutor lane;
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

TEST(StructuredTaskTest, CpuThreadPoolRunsSubmittedWork)
{
	CpuThreadPool scheduler(2);
	std::binary_semaphore finished(0);
	std::atomic<int> value{ 0 };

	scheduler.Submit([&] {
		value.store(11, std::memory_order_release);
		finished.release();
	});

	ASSERT_TRUE(finished.try_acquire_for(5s));
	EXPECT_EQ(value.load(std::memory_order_acquire), 11);
}

TEST(StructuredTaskTest, AsyncRuntimeConstructsAndDrainsResources)
{
	std::binary_semaphore blocking_done(0);
	std::binary_semaphore serial_done(0);
	std::binary_semaphore cpu_done(0);

	{
		CpuThreadPool scheduler(2);
		AsyncRuntime runtime(scheduler);

		runtime.BlockingIoExecutor().Submit([&] { blocking_done.release(); });
		runtime.SerialFileExecutor().Submit([&] { serial_done.release(); });
		runtime.CpuExecutor().Submit([&] { cpu_done.release(); });

		ASSERT_TRUE(blocking_done.try_acquire_for(5s));
		ASSERT_TRUE(serial_done.try_acquire_for(5s));
		ASSERT_TRUE(cpu_done.try_acquire_for(5s));
		EXPECT_TRUE(runtime.BlockingIoExecutor().Empty());
		EXPECT_TRUE(runtime.CompactionExecutor().Empty());
		EXPECT_TRUE(runtime.SerialFileExecutor().Done());
	}
}

TEST(StructuredTaskTest, ExecutorRefSubmitsToWrappedExecutor)
{
	InlineExecutor executor;
	ExecutorRef scheduler(executor);
	std::atomic<int> submitted{ 0 };

	scheduler.Submit([&submitted] { submitted.fetch_add(1, std::memory_order_relaxed); });

	EXPECT_EQ(submitted.load(std::memory_order_relaxed), 1);
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
	child_scope.Submit([&](StopToken token) { child_saw_stop.store(token.CheckStop(), std::memory_order_release); });

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
