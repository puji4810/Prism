#include "async_op.h"
#include "runtime_executor.h"
#include "runtime_metrics.h"
#include "scheduler.h"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

// ---------------------------------------------------------------------------
// Microbenchmark: measure std::function overhead in the Prism hot path
//
// We measure five operations that correspond to the real GetAsync hot path:
//
//   1. Empty lambda → std::function wrap (SBO, small capture)
//   2. Work lambda → std::function wrap (heap, large capture: shared_ptr +
//      ReadOptions + string)
//   3. Cont lambda → std::function wrap (SBO, single shared_ptr capture)
//   4. std::function<void()>::operator() invocation
//   5. BlockingExecutor::Submit + execute round-trip with metrics-like wrapper
//
// Each test runs N iterations and reports avg ns.
// ---------------------------------------------------------------------------

namespace prism::bench
{
	using Clock = std::chrono::steady_clock;

	// 24 bytes on libstdc++: shared_ptr is 16 bytes + vtable/ref overhead
	struct SmallCapture
	{
		std::shared_ptr<int> p;
	};

	// ~72-80 bytes: shared_ptr (16) + string (32) + ReadOptions (~24)
	// This DOES NOT fit in std::function's SBO (typically 24-32 bytes)
	struct LargeCapture
	{
		std::shared_ptr<int> p;
		std::string key;
		int opts_field1{ 0 };
		int opts_field2{ 0 };
		int opts_field3{ 0 };
	};

	// Helper: measure avg ns for a given operation
	template <typename F>
	static uint64_t Measure(const char* name, int iterations, F&& fn, bool warmup = true)
	{
		if (warmup)
		{
			for (int i = 0; i < 10000; ++i)
				fn();
		}

		auto start = Clock::now();
		for (int i = 0; i < iterations; ++i)
			fn();
		auto end = Clock::now();

		auto total_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
		auto avg_ns = total_ns / static_cast<uint64_t>(iterations);

		std::printf("  %-45s %8lu ns/op  (total %lu ns over %d iters)\n", name,
		    static_cast<unsigned long>(avg_ns), static_cast<unsigned long>(total_ns), iterations);

		return avg_ns;
	}

	// RAII helper to create + drain a single-thread thread-pool scheduler
	struct ScopedScheduler
	{
		ScopedScheduler(std::size_t n)
		    : sched(n)
		{
		}
		~ScopedScheduler() = default;
		ThreadPoolScheduler sched;
	};

	void RunFunctionOverheadBench()
	{
		std::printf("=== std::function Overhead Microbenchmark ===\n");
		std::printf("Simulating GetAsync hot-path callable operations\n\n");

		constexpr int kIterations = 1'000'000;

		// ---------------------------------------------------------------
		// 1. std::function construction from empty lambda (SBO baseline)
		// ---------------------------------------------------------------
		{
			std::function<void()> f;
			Measure("1a. std::function() default ctor", kIterations, [&]() {
				f = std::function<void()>();
			});
		}

		{
			Measure("1b. std::function from empty lambda (SBO: []{})", kIterations, []() {
				auto f = std::function<void()>([] { /* no-op */ });
				(void)f;
			});
		}

		// ---------------------------------------------------------------
		// 2. std::function construction from small-capture lambda (SBO)
		//    Models: continuation lambda [st]{ ... }
		// ---------------------------------------------------------------
		{
			auto p = std::make_shared<int>(42);
			Measure("2a. small-capture lambda -> function (SBO: [sp])", kIterations, [&]() {
				auto f = std::function<void()>([p] { (void)*p; });
				(void)f;
			});
		}

		{
			auto p = std::make_shared<int>(42);
			Measure("2b. small capture + construct + call + destruct", kIterations, [&]() {
				auto f = std::function<void()>([p] { (void)*p; });
				f();
			});
		}

		// ---------------------------------------------------------------
		// 3. std::function from large-capture lambda (heap allocates)
		//    Models: work lambda [state, opts, key]{ return db.Get(opts, key); }
		// ---------------------------------------------------------------
		{
			auto p = std::make_shared<int>(42);
			std::string big_key(1000, 'x');
			LargeCapture cap{ p, big_key, 1, 2, 3 };

			Measure("3a. large-capture lambda -> function (HEAP: [sp,str,opts])", kIterations, [&]() {
				auto f = std::function<void()>([cap] { (void)*cap.p; });
				(void)f;
			});
		}

		{
			auto p = std::make_shared<int>(42);
			std::string big_key(1000, 'x');
			LargeCapture cap{ p, big_key, 1, 2, 3 };

			Measure("3b. large capture + construct + call + destruct", kIterations / 10, [&]() {
				auto f = std::function<void()>([cap] { (void)*cap.p; });
				f();
			});
		}

		// ---------------------------------------------------------------
		// 4. std::function invocation overhead (virtual dispatch)
		// ---------------------------------------------------------------
		{
			auto p = std::make_shared<int>(42);
			std::function<void()> f = [p] { (void)*p; };
			// warm up
			for (int i = 0; i < 10000; ++i)
				f();

			Measure("4. std::function::operator() virtual dispatch", kIterations, [&]() {
				f();
			});
		}

		{
			auto p = std::make_shared<int>(42);
			auto big_key = std::string(1000, 'x');
			LargeCapture cap{ p, big_key, 1, 2, 3 };
			std::function<void()> f = [cap] { (void)*cap.p; };
			for (int i = 0; i < 10000; ++i)
				f();

			Measure("5. large-capture function::operator() (heap-backed)", kIterations, [&]() {
				f();
			});
		}

		// ---------------------------------------------------------------
		// 6. Simulate BlockingExecutor::Submit round-trip
		//    (construct + move to queue + pop + invoke + destruct)
		// ---------------------------------------------------------------
		{
			ScopedScheduler scoped(1);
			auto& sched = scoped.sched;
			BlockingExecutor exec(1, BlockingExecutorLane::kRead);

			// Small-capture case: like continuation
			auto p = std::make_shared<int>(42);
			std::atomic<uint64_t> count{ 0 };

			for (int i = 0; i < 10000; ++i)
			{
				exec.Submit([p, &count] {
					count.fetch_add(1, std::memory_order_relaxed);
				});
			}

			// Drain
			while (count.load() < 10000)
				std::this_thread::yield();

			count.store(0);
			auto start = Clock::now();

			constexpr int kSubmitIters = 100000;
			for (int i = 0; i < kSubmitIters; ++i)
			{
				exec.Submit([p, &count] {
					count.fetch_add(1, std::memory_order_relaxed);
				});
			}

			// Wait for completion
			while (count.load() < kSubmitIters)
				std::this_thread::yield();

			auto end = Clock::now();
			auto total_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
			auto avg_ns = total_ns / kSubmitIters;

			std::printf("  %-45s %8lu ns/op  (SBO capture, submit + exec round-trip)\n",
			    "6a. BlockingExecutor submit-exec (small cap.)",
			    static_cast<unsigned long>(avg_ns));
		}

		{
			ScopedScheduler scoped(1);
			auto& sched = scoped.sched;
			BlockingExecutor exec(1, BlockingExecutorLane::kRead);

			auto p = std::make_shared<int>(42);
			auto big_key = std::string(1000, 'x');
			LargeCapture cap{ p, big_key, 1, 2, 3 };
			std::atomic<uint64_t> count{ 0 };

			for (int i = 0; i < 10000; ++i)
			{
				exec.Submit([cap, &count] {
					count.fetch_add(1, std::memory_order_relaxed);
				});
			}
			while (count.load() < 10000)
				std::this_thread::yield();

			count.store(0);
			auto start = Clock::now();

			constexpr int kSubmitIters = 50000;
			for (int i = 0; i < kSubmitIters; ++i)
			{
				exec.Submit([cap, &count] {
					count.fetch_add(1, std::memory_order_relaxed);
				});
			}

			while (count.load() < kSubmitIters)
				std::this_thread::yield();

			auto end = Clock::now();
			auto total_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
			auto avg_ns = total_ns / kSubmitIters;

			std::printf("  %-45s %8lu ns/op  (HEAP capture, submit + exec round-trip)\n",
			    "6b. BlockingExecutor submit-exec (large cap.)",
			    static_cast<unsigned long>(avg_ns));
		}

		// ---------------------------------------------------------------
		// 7. Full GetAsync hot-path simulation
		//    1 large-capture function + 1 small-capture function + submit
		// ---------------------------------------------------------------
		{
			ScopedScheduler scoped(1);
			auto& sched = scoped.sched;
			BlockingExecutor exec(1, BlockingExecutorLane::kRead);

			auto p = std::make_shared<int>(42);
			auto big_key = std::string(1000, 'x');
			LargeCapture cap{ p, big_key, 1, 2, 3 };

			std::atomic<uint64_t> count{ 0 };
			constexpr int kHotIters = 50000;

			for (int i = 0; i < 10000; ++i)
			{
				exec.Submit([cap, &count, &exec] {
					// Simulate: work executes, then submits continuation
					exec.Submit([&count] {
						count.fetch_add(1, std::memory_order_relaxed);
					});
				});
			}
			while (count.load() < 10000)
				std::this_thread::yield();

			count.store(0);
			auto start = Clock::now();

			for (int i = 0; i < kHotIters; ++i)
			{
				exec.Submit([cap, &count, &exec] {
					exec.Submit([&count] {
						count.fetch_add(1, std::memory_order_relaxed);
					});
				});
			}

			while (count.load() < kHotIters)
				std::this_thread::yield();

			auto end = Clock::now();
			auto total_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
			auto avg_ns = total_ns / kHotIters;

			std::printf("  %-45s %8lu ns/op  (work(HEAP) + cont(SBO) full cycle)\n",
			    "7.  Full GetAsync cycle (2 functions)", static_cast<unsigned long>(avg_ns));
		}

		std::printf("\n=== End of std::function Overhead Microbenchmark ===\n");
	}
}

int main()
{
	prism::bench::RunFunctionOverheadBench();
	return 0;
}
