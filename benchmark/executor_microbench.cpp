#include "runtime_executor.h"
#include "scheduler.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>

// ---------------------------------------------------------------------------
// Microbenchmark: BlockingExecutor isolation — pure submit-to-execute overhead
//
// Measures the BlockingExecutor single-queue behaviour without any DB work:
//   - Submit count (how many work items submitted)
//   - Execution count (how many completed)
//   - Queue-depth stats (min / max / avg across the run)
//   - Submit-to-execute delay (min / max / avg in microseconds)
//
// All instrumentation is external — the BlockingExecutor class is UNMODIFIED.
//
// Usage:
//   ./executor_microbench --workers=4 --tasks=100000
// ---------------------------------------------------------------------------

namespace prism::bench
{
	using Clock = std::chrono::steady_clock;

	struct ExecutorBenchStats
	{
		std::atomic<uint64_t> submit_count{ 0 };
		std::atomic<uint64_t> exec_count{ 0 };
		std::atomic<uint64_t> queue_depth_min{ UINT64_MAX };
		std::atomic<uint64_t> queue_depth_max{ 0 };
		std::atomic<uint64_t> queue_depth_sum{ 0 };
		std::atomic<uint64_t> delay_min_us{ UINT64_MAX };
		std::atomic<uint64_t> delay_max_us{ 0 };
		std::atomic<uint64_t> delay_sum_us{ 0 };
	};

	// Lightweight atomic min via CAS loop.
	static void AtomicMin(std::atomic<uint64_t>& target, uint64_t val)
	{
		uint64_t cur = target.load(std::memory_order_relaxed);
		while (val < cur && !target.compare_exchange_weak(cur, val, std::memory_order_relaxed))
		{
		}
	}

	// Lightweight atomic max via CAS loop.
	static void AtomicMax(std::atomic<uint64_t>& target, uint64_t val)
	{
		uint64_t cur = target.load(std::memory_order_relaxed);
		while (val > cur && !target.compare_exchange_weak(cur, val, std::memory_order_relaxed))
		{
		}
	}

	void RunExecutorMicrobench(int num_workers, int num_tasks)
	{
		BlockingExecutor executor(static_cast<std::size_t>(num_workers),
		    BlockingExecutorLane::kGeneric);
		ExecutorBenchStats stats;
		std::atomic<uint64_t> counter{ 0 };
		std::atomic<uint64_t> in_flight{ 0 };

		std::printf("=== Executor Isolation Microbenchmark ===\n");

		auto bench_start = Clock::now();

		for (int i = 0; i < num_tasks; ++i)
		{
			auto submit_time = Clock::now();

			uint64_t qd = in_flight.fetch_add(1, std::memory_order_relaxed);
			stats.submit_count.fetch_add(1, std::memory_order_relaxed);

			AtomicMin(stats.queue_depth_min, qd);
			AtomicMax(stats.queue_depth_max, qd);
			stats.queue_depth_sum.fetch_add(qd, std::memory_order_relaxed);

			executor.Submit([submit_time, &stats, &in_flight, &counter] {
				auto exec_time = Clock::now();
				auto delay_us = static_cast<uint64_t>(
				    std::chrono::duration_cast<std::chrono::microseconds>(
				        exec_time - submit_time)
				        .count());

				AtomicMin(stats.delay_min_us, delay_us);
				AtomicMax(stats.delay_max_us, delay_us);
				stats.delay_sum_us.fetch_add(delay_us, std::memory_order_relaxed);

				in_flight.fetch_sub(1, std::memory_order_relaxed);
				counter.fetch_add(1, std::memory_order_relaxed);
				stats.exec_count.fetch_add(1, std::memory_order_relaxed);
			});
		}

		while (stats.exec_count.load(std::memory_order_relaxed) <
		       static_cast<uint64_t>(num_tasks))
		{
			std::this_thread::yield();
		}

		auto bench_end = Clock::now();
		auto total_us = std::chrono::duration_cast<std::chrono::microseconds>(
		                    bench_end - bench_start)
		                    .count();

		uint64_t submits = stats.submit_count.load(std::memory_order_relaxed);
		uint64_t execs = stats.exec_count.load(std::memory_order_relaxed);

		uint64_t qd_min = stats.queue_depth_min.load(std::memory_order_relaxed);
		uint64_t qd_max = stats.queue_depth_max.load(std::memory_order_relaxed);
		uint64_t qd_sum = stats.queue_depth_sum.load(std::memory_order_relaxed);
		double qd_avg =
		    submits > 0 ? static_cast<double>(qd_sum) / static_cast<double>(submits) : 0.0;

		uint64_t d_min = stats.delay_min_us.load(std::memory_order_relaxed);
		uint64_t d_max = stats.delay_max_us.load(std::memory_order_relaxed);
		uint64_t d_sum = stats.delay_sum_us.load(std::memory_order_relaxed);
		double d_avg =
		    execs > 0 ? static_cast<double>(d_sum) / static_cast<double>(execs) : 0.0;

		std::printf(
		    "workers=%d tasks=%d | submits=%lu execs=%lu | qd_min=%lu qd_max=%lu "
		    "qd_avg=%.1f | delay_min=%lu delay_max=%lu delay_avg=%.1f\n",
		    num_workers, num_tasks, static_cast<unsigned long>(submits),
		    static_cast<unsigned long>(execs), static_cast<unsigned long>(qd_min),
		    static_cast<unsigned long>(qd_max), qd_avg,
		    static_cast<unsigned long>(d_min), static_cast<unsigned long>(d_max),
		    d_avg);

		std::printf("counter=%lu  total_us=%lu  tasks/us=%.3f\n",
		    static_cast<unsigned long>(counter.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(total_us),
		    total_us > 0 ? static_cast<double>(num_tasks) / static_cast<double>(total_us)
		                 : 0.0);
	}

} // namespace prism::bench

// ---------------------------------------------------------------------------
// main — simple CLI parsing
// ---------------------------------------------------------------------------
int main(int argc, char** argv)
{
	int num_workers = 4;
	int num_tasks = 100000;

	for (int i = 1; i < argc; ++i)
	{
		if (std::strncmp(argv[i], "--workers=", 10) == 0)
		{
			num_workers = std::atoi(argv[i] + 10);
		}
		else if (std::strncmp(argv[i], "--tasks=", 8) == 0)
		{
			num_tasks = std::atoi(argv[i] + 8);
		}
	}

	if (num_workers < 1)
		num_workers = 1;
	if (num_tasks < 1)
		num_tasks = 1;

	prism::bench::RunExecutorMicrobench(num_workers, num_tasks);
	return 0;
}
