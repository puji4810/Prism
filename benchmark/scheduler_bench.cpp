#include "scheduler.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <latch>
#include <string_view>
#include <thread>
#include <vector>

namespace
{
	using Clock = std::chrono::steady_clock;

	struct alignas(64) Completion
	{
		std::atomic<std::size_t> count{ 0 };
	};

	void RunFanout(std::size_t workers, std::size_t tasks, std::size_t work)
	{
		Completion completed;
		std::vector<std::uint64_t> results(tasks);
		prism::CpuThreadPool scheduler(workers);
		const auto begin = Clock::now();
		scheduler.Submit([&] {
			for (std::size_t i = 0; i < tasks; ++i)
				scheduler.Submit([&, i] {
					std::uint64_t value = i + 1;
					for (std::size_t j = 0; j < work; ++j)
					{
						value ^= value << 13;
						value ^= value >> 7;
						value ^= value << 17;
					}
					results[i] = value;
					completed.count.fetch_add(1, std::memory_order_release);
				});
		});
		while (completed.count.load(std::memory_order_acquire) < tasks)
		{
			if (Clock::now() - begin > std::chrono::seconds(30))
				std::abort();
			std::this_thread::sleep_for(std::chrono::microseconds(50));
		}
		const auto seconds = std::chrono::duration<double>(Clock::now() - begin).count();
		std::uint64_t checksum = 0;
		for (auto result : results)
			checksum ^= result;
		std::printf("mode=fanout workers=%zu tasks=%zu work=%zu checksum=%llu seconds=%.6f tasks_per_second=%.0f\n",
		    scheduler.WorkerCount(), tasks, work, static_cast<unsigned long long>(checksum), seconds, static_cast<double>(tasks) / seconds);
	}

	// Hold the owner inside a job while its peer drains the entire backlog.
	// Two workers make the victim deterministic, including for the old scheduler.
	void RunSteal(std::size_t tasks)
	{
		Completion completed;
		std::binary_semaphore queued(0), release_owner(0), peer_started(0), release_peer(0);
		prism::CpuThreadPool scheduler(2);
		scheduler.Submit([&] {
			peer_started.release();
			release_peer.acquire();
		});
		peer_started.acquire();
		scheduler.Submit([&] {
			for (std::size_t i = 0; i < tasks; ++i)
				scheduler.Submit([&] { completed.count.fetch_add(1, std::memory_order_release); });
			queued.release();
			release_owner.acquire();
		});
		queued.acquire();
		const auto begin = Clock::now();
		release_peer.release();
		while (completed.count.load(std::memory_order_acquire) < tasks)
		{
			if (Clock::now() - begin > std::chrono::seconds(30))
				std::abort();
			std::this_thread::sleep_for(std::chrono::microseconds(50));
		}
		const auto seconds = std::chrono::duration<double>(Clock::now() - begin).count();
		release_owner.release();
		std::printf("mode=steal workers=2 producers=1 tasks=%zu seconds=%.6f tasks_per_second=%.0f\n", tasks, seconds,
		    static_cast<double>(tasks) / seconds);
	}

	void Run(std::string_view mode, std::size_t workers, std::size_t producers, std::size_t tasks)
	{
		std::vector<Completion> completed(producers);
		std::latch ready(producers), start(1);
		std::vector<std::jthread> threads;
		prism::CpuThreadPool scheduler(workers);
		auto submit = [&](std::size_t p) {
			for (std::size_t i = 0; i < tasks; ++i)
			{
				auto job = [&, p] { completed[p].count.fetch_add(1, std::memory_order_release); };
				if (mode == "priority")
					scheduler.SubmitWithPriority(std::move(job), 1 + i % 8);
				else
					scheduler.Submit(std::move(job));
			}
		};
		for (std::size_t p = 0; p < producers; ++p)
		{
			threads.emplace_back([&, p] {
				ready.count_down();
				start.wait();
				if (mode == "local")
					scheduler.Submit([&, p] { submit(p); });
				else
					submit(p);
			});
		}
		ready.wait();
		const auto begin = Clock::now();
		start.count_down();
		for (auto& thread : threads)
			thread.join();
		for (auto& completion : completed)
		{
			while (completion.count.load(std::memory_order_acquire) < tasks)
			{
				if (Clock::now() - begin > std::chrono::seconds(30))
				{
					std::fprintf(stderr, "scheduler benchmark timed out\n");
					std::exit(1);
				}
				std::this_thread::sleep_for(std::chrono::microseconds(50));
			}
			if (completion.count.load(std::memory_order_acquire) != tasks)
				std::abort();
		}
		const auto seconds = std::chrono::duration<double>(Clock::now() - begin).count();
		std::printf("mode=%.*s workers=%zu producers=%zu tasks=%zu seconds=%.6f tasks_per_second=%.0f\n", static_cast<int>(mode.size()),
		    mode.data(), scheduler.WorkerCount(), producers, tasks * producers, seconds, static_cast<double>(tasks * producers) / seconds);
	}
}

// Positional arguments: external|local|priority|steal|fanout workers producers tasks-per-producer rounds [work].
// Use a release build with runtime_metrics disabled for timing.
int main(int argc, char** argv)
{
	const std::string_view mode = argc > 1 ? argv[1] : "external";
	const auto workers = argc > 2 ? std::strtoul(argv[2], nullptr, 10) : 4;
	const auto producers = argc > 3 ? std::strtoul(argv[3], nullptr, 10) : 1;
	const auto tasks = argc > 4 ? std::strtoul(argv[4], nullptr, 10) : 100000;
	const auto rounds = argc > 5 ? std::strtoul(argv[5], nullptr, 10) : 5;
	const auto work = argc > 6 ? std::strtoul(argv[6], nullptr, 10) : 10000;
	if ((mode != "external" && mode != "local" && mode != "priority" && mode != "steal" && mode != "fanout") || !workers || !producers
	    || !tasks || !rounds || (mode == "steal" && (workers != 2 || producers != 1)) || (mode == "fanout" && producers != 1))
	{
		std::fprintf(stderr,
		    "usage: scheduler_bench external|local|priority|steal|fanout workers producers tasks-per-producer rounds [work]\n"
		    "steal requires workers=2 producers=1; fanout requires producers=1\n");
		return 1;
	}
	for (std::size_t round = 0; round < rounds; ++round)
	{
		if (mode == "fanout")
			RunFanout(workers, tasks, work);
		else if (mode == "steal")
			RunSteal(tasks);
		else
			Run(mode, workers, producers, tasks);
	}
}
