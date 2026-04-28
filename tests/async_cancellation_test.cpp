#include "../src/task_scope.h"
#include "../src/runtime_metrics.h"

#include <atomic>
#include <functional>
#include <gtest/gtest.h>
#include <mutex>
#include <queue>
#include <semaphore>

using namespace prism;
using namespace std::chrono_literals;

namespace
{
	class AsyncCancellationTest: public ::testing::Test
	{
	protected:
		void SetUp() override { RuntimeMetrics::Instance().Reset(); }
	};

	class ManualExecutor: public IContinuationExecutor
	{
	public:
		void Submit(std::function<void()> work) override
		{
			std::lock_guard lock(mutex_);
			queue_.push(std::move(work));
		}

		void DrainOne()
		{
			std::function<void()> work;
			{
				std::lock_guard lock(mutex_);
				ASSERT_FALSE(queue_.empty());
				work = std::move(queue_.front());
				queue_.pop();
			}
			work();
		}

	private:
		std::mutex mutex_;
		std::queue<std::function<void()>> queue_;
	};
}

TEST_F(AsyncCancellationTest, PreStartCancelledPayloadNeverExecutes)
{
	ManualExecutor executor;
	TaskScope scope(executor);
	std::atomic<int> ran{ 0 };

	scope.Submit([&] { ran.fetch_add(1, std::memory_order_relaxed); });
	scope.RequestStop();
	executor.DrainOne();
	scope.Join();

	EXPECT_EQ(ran.load(std::memory_order_acquire), 0);
	EXPECT_EQ(scope.QuarantineSink().Count(Quarantine::EntryKind::kCancelled), 1);
	EXPECT_GE(RuntimeMetrics::Instance().cancelled_before_start.load(std::memory_order_relaxed), 1u);
}

TEST_F(AsyncCancellationTest, RunningCpuTaskStopsAtCheckpoint)
{
	BlockingExecutor executor;
	TaskScope scope(executor);
	std::binary_semaphore entered(0);
	std::binary_semaphore release_checkpoint(0);
	std::atomic<int> checkpoints{ 0 };
	std::atomic<bool> stopped{ false };

	scope.Submit([&](StopToken token) {
		checkpoints.fetch_add(1, std::memory_order_relaxed);
		entered.release();
		release_checkpoint.acquire();
		if (token.CheckStop())
		{
			stopped.store(true, std::memory_order_release);
			return;
		}
		checkpoints.fetch_add(1, std::memory_order_relaxed);
	});

	ASSERT_TRUE(entered.try_acquire_for(5s));
	scope.RequestStop();
	release_checkpoint.release();
	scope.Join();

	EXPECT_TRUE(stopped.load(std::memory_order_acquire));
	EXPECT_EQ(checkpoints.load(std::memory_order_acquire), 1);
	EXPECT_EQ(scope.QuarantineSink().Size(), 0);
	EXPECT_GE(RuntimeMetrics::Instance().cooperative_checkpoint_cancel.load(std::memory_order_relaxed), 1u);
}

TEST_F(AsyncCancellationTest, LateCompletionIsQuarantinedInsteadOfApplied)
{
	BlockingExecutor executor;
	TaskScope scope(executor);
	std::binary_semaphore started(0);
	std::binary_semaphore allow_complete(0);
	std::atomic<int> applied{ 0 };

	auto state = scope.SubmitOperation(
	    [&](StopToken) {
		    started.release();
		    allow_complete.acquire();
		    return 99;
	    },
	    [&](int value) { applied.store(value, std::memory_order_release); });

	ASSERT_TRUE(started.try_acquire_for(5s));
	state->Cancel();
	allow_complete.release();
	scope.Join();

	EXPECT_EQ(applied.load(std::memory_order_acquire), 0);
	EXPECT_TRUE(state->WasQuarantined());
	EXPECT_FALSE(state->WasApplied());
	EXPECT_EQ(scope.QuarantineSink().Count(Quarantine::EntryKind::kValue), 1);
	EXPECT_GE(RuntimeMetrics::Instance().late_completion_quarantined.load(std::memory_order_relaxed), 1u);
}
