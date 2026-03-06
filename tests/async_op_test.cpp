// async_op_test.cpp
//
// Deterministic regression test for the AsyncOp<T> completion-before-suspension handshake.
//
// Design:
//   InlineScheduler::Submit(job) executes the job IMMEDIATELY on the calling thread, inline,
//   before returning.  Because Submit is called from inside await_suspend(), the worker lambda
//   therefore runs to completion – including its CAS kSuspending→kCompleted – *before*
//   await_suspend() attempts its own CAS kSuspending→kSuspended.
//
//   This deterministically exercises the "worker wins" fast-path every single time:
//     1. await_suspend() calls scheduler_.Submit(lambda).
//     2. InlineScheduler::Submit runs lambda immediately: stores result, CAS kSuspending→kCompleted.
//     3. Submit returns.  await_suspend() now tries CAS kSuspending→kSuspended — fails (state is kCompleted).
//     4. await_suspend() returns false → coroutine is NOT suspended, continues immediately.
//     5. await_resume() retrieves the result normally.
//
//   Prior to the three-state handshake fix, step 4 would call handle.resume() while the coroutine
//   was still inside await_suspend(), causing resume-before-suspension UB / guaranteed hang.
//
//   The loop (10 000 iterations) proves the fix is stable: any regression would reliably manifest
//   as a crash, assertion failure, or infinite hang under ASan/LSan.

#include "async_op.h"
#include "coro_task.h"

#include <atomic>
#include <chrono>
#include <stdexcept>
#include <string>

#include <gtest/gtest.h>

namespace prism::tests
{

	// ---------------------------------------------------------------------------
	// InlineScheduler: executes every job synchronously on the calling thread.
	// This is the minimal IScheduler implementation needed for deterministic tests.
	// ---------------------------------------------------------------------------
	class InlineScheduler: public IScheduler
	{
	public:
		// Submit executes `job()` immediately before returning.
		// priority is intentionally ignored (single-threaded inline execution has no ordering concept).
		void Submit(Job job, std::size_t /*priority*/ = 0) override { job(); }
	};

	// ---------------------------------------------------------------------------
	// Helper: build an AsyncOp<int> that returns a fixed value via an inline scheduler.
	// ---------------------------------------------------------------------------
	static AsyncOp<int> MakeIntOp(IScheduler& sched, int value)
	{
		return AsyncOp<int>(sched, [value] { return value; });
	}

	// ---------------------------------------------------------------------------
	// Helper: build an AsyncOp<void> that increments an atomic counter.
	// ---------------------------------------------------------------------------
	static AsyncOp<void> MakeVoidOp(IScheduler& sched, std::atomic<int>& counter)
	{
		return AsyncOp<void>(sched, [&counter] { counter.fetch_add(1, std::memory_order_relaxed); });
	}

	// ---------------------------------------------------------------------------
	// Test 1: single co_await of AsyncOp<int> completes without hang
	// ---------------------------------------------------------------------------
	TEST(AsyncOpTest, SingleIntOpCompletesInline)
	{
		InlineScheduler sched;

		auto task = [&]() -> Task<int> {
			int result = co_await MakeIntOp(sched, 42);
			co_return result;
		}();

		int value = task.SyncWait();
		EXPECT_EQ(value, 42);
	}

	// ---------------------------------------------------------------------------
	// Test 2: single co_await of AsyncOp<void> increments counter exactly once
	// ---------------------------------------------------------------------------
	TEST(AsyncOpTest, SingleVoidOpIncrementsCounterExactlyOnce)
	{
		InlineScheduler sched;
		std::atomic<int> counter{ 0 };

		auto task = [&]() -> Task<void> { co_await MakeVoidOp(sched, counter); }();

		task.SyncWait();
		EXPECT_EQ(counter.load(), 1);
	}

	// ---------------------------------------------------------------------------
	// Test 3: AsyncOp<int> propagates exceptions through co_await
	// ---------------------------------------------------------------------------
	TEST(AsyncOpTest, ExceptionPropagatesOnInt)
	{
		InlineScheduler sched;

		auto task = [&]() -> Task<int> {
			int result = co_await AsyncOp<int>(sched, [] -> int { throw std::runtime_error("test-error"); });
			co_return result;
		}();

		EXPECT_THROW(task.SyncWait(), std::runtime_error);
	}

	// ---------------------------------------------------------------------------
	// Test 4: AsyncOp<void> propagates exceptions through co_await
	// ---------------------------------------------------------------------------
	TEST(AsyncOpTest, ExceptionPropagatesOnVoid)
	{
		InlineScheduler sched;

		auto task = [&]() -> Task<void> { co_await AsyncOp<void>(sched, [] { throw std::runtime_error("void-test-error"); }); }();

		EXPECT_THROW(task.SyncWait(), std::runtime_error);
	}

	// ---------------------------------------------------------------------------
	// Test 5: AsyncOp<int> returns correct value for different inputs
	// ---------------------------------------------------------------------------
	TEST(AsyncOpTest, IntOpReturnsCorrectValue)
	{
		InlineScheduler sched;
		constexpr int kExpected = -7;

		auto task = [&]() -> Task<int> { co_return co_await MakeIntOp(sched, kExpected); }();

		EXPECT_EQ(task.SyncWait(), kExpected);
	}

	// ---------------------------------------------------------------------------
	// Test 6: Repeated regression — 10 000 iterations stress the worker-wins path.
	//
	// Under the OLD (broken) handshake, at least some iterations would race into
	// resume-before-suspend UB. The inline scheduler makes EVERY iteration hit the
	// worker-wins fast-path deterministically; any regression will reliably crash
	// or hang under ASan/LSan rather than being a flaky data race.
	// ---------------------------------------------------------------------------
	TEST(AsyncOpTest, RepeatedIntOpHandshakeNeverHangs)
	{
		constexpr int kIterations = 10'000;
		InlineScheduler sched;

		auto task = [&]() -> Task<int> {
			int sum = 0;
			for (int i = 0; i < kIterations; ++i)
			{
				sum += co_await MakeIntOp(sched, 1);
			}
			co_return sum;
		}();

		int result = task.SyncWait();
		EXPECT_EQ(result, kIterations);
	}

	// ---------------------------------------------------------------------------
	// Test 7: Repeated void op — counter must equal exactly kIterations
	// ---------------------------------------------------------------------------
	TEST(AsyncOpTest, RepeatedVoidOpIncrementsPrecisely)
	{
		constexpr int kIterations = 10'000;
		InlineScheduler sched;
		std::atomic<int> counter{ 0 };

		auto task = [&]() -> Task<void> {
			for (int i = 0; i < kIterations; ++i)
			{
				co_await MakeVoidOp(sched, counter);
			}
		}();

		task.SyncWait();
		EXPECT_EQ(counter.load(), kIterations);
	}

	// ---------------------------------------------------------------------------
	// Test 8: Multiple sequential AsyncOp<int> in one coroutine, verify each result
	// ---------------------------------------------------------------------------
	TEST(AsyncOpTest, MultipleSequentialOpsReturnDistinctValues)
	{
		InlineScheduler sched;

		auto task = [&]() -> Task<void> {
			int a = co_await MakeIntOp(sched, 10);
			int b = co_await MakeIntOp(sched, 20);
			int c = co_await MakeIntOp(sched, 30);
			EXPECT_EQ(a, 10);
			EXPECT_EQ(b, 20);
			EXPECT_EQ(c, 30);
		}();

		task.SyncWait();
	}

	// ---------------------------------------------------------------------------
	// Test 9: AsyncOp<std::string> — non-trivial return type moves correctly
	// ---------------------------------------------------------------------------
	TEST(AsyncOpTest, StringOpMovesResultCorrectly)
	{
		InlineScheduler sched;

		auto task = [&]() -> Task<std::string> {
			std::string result = co_await AsyncOp<std::string>(sched, [] { return std::string("hello-prism"); });
			co_return result;
		}();

		EXPECT_EQ(task.SyncWait(), "hello-prism");
	}

	// ---------------------------------------------------------------------------
	// Test 10: Mixed int and void ops in one coroutine — both paths work together
	// ---------------------------------------------------------------------------
	TEST(AsyncOpTest, MixedIntAndVoidOpsInSingleCoroutine)
	{
		InlineScheduler sched;
		std::atomic<int> side_effects{ 0 };

		auto task = [&]() -> Task<int> {
			co_await MakeVoidOp(sched, side_effects);
			int v = co_await MakeIntOp(sched, 7);
			co_await MakeVoidOp(sched, side_effects);
			co_return v;
		}();

		int result = task.SyncWait();
		EXPECT_EQ(result, 7);
		EXPECT_EQ(side_effects.load(), 2);
	}

} // namespace prism::tests

int main(int argc, char** argv)
{
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}