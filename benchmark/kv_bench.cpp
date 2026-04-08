#include "kv_bench_lib.h"

#include "asyncdb.h"
#include "bench_env_wrapper.h"
#include "db.h"
#include "scheduler.h"

#include <cstdio>
#include <filesystem>
#include <memory>
#include <semaphore>

#include <unistd.h>

namespace prism::bench
{
	static void RunSyncBenchmark(const Config& cfg, const std::vector<std::vector<std::string>>& keys)
	{
		if (cfg.mode == BenchMode::kSstReadPipeline)
		{
			std::fprintf(stderr, "error: sst_read_pipeline benchmark is async-only. Use --async instead of --sync.\n");
			exit(1);
		}
		if (cfg.mode == BenchMode::kCompactionOverlap)
		{
			std::fprintf(stderr, "error: compaction_overlap benchmark is async-only. Use --async instead of --sync.\n");
			exit(1);
		}

		const std::string dir = MakeTempDir("bench_sync");
		Options options;
		options.create_if_missing = true;
		options.write_buffer_size = cfg.write_buffer_size;

		if (cfg.mode == BenchMode::kDiskRead)
		{
			options.write_buffer_size = std::min<std::size_t>(options.write_buffer_size, 4 * 1024);
		}

		auto db_res = DB::Open(options, dir);
		if (!db_res.has_value())
		{
			std::fprintf(stderr, "sync open failed: %s\n", db_res.error().ToString().c_str());
			return;
		}
		auto db = std::move(db_res.value());

		const bool should_prefill_sync
		    = (cfg.prefill == 1) || (cfg.prefill == -1 && (cfg.mode == BenchMode::kDiskRead || cfg.read_ratio > 0));
		if (should_prefill_sync)
		{
			Prefill(*db, keys, cfg.ops_per_client, cfg.value_size);
		}

		if (cfg.mode == BenchMode::kDiskRead)
		{
			db.reset();
			auto reopened = DB::Open(options, dir);
			if (!reopened.has_value())
			{
				std::fprintf(stderr, "sync reopen failed: %s\n", reopened.error().ToString().c_str());
				return;
			}
			db = std::move(reopened.value());
		}

		std::vector<uint64_t> all_lat;
		double total_seconds = 0;
		std::size_t total_max_client_inflight = 0;
		int total_write_sync = 0;

		for (int r = 1; r <= cfg.rounds; ++r)
		{
			Stats stats;
			if (cfg.mode == BenchMode::kDiskRead)
			{
				stats = RunSyncDiskRead(*db, cfg, keys);
				PrintLine("sync_disk", cfg, r, stats);
			}
			else if (cfg.mode == BenchMode::kDurabilityWrite)
			{
				stats = RunSyncDurabilityWrite(*db, cfg, keys);
				PrintLine("sync_durability_write", cfg, r, stats);
			}
			else
			{
				stats = RunSyncMixed(*db, cfg, keys);
				PrintLine("sync", cfg, r, stats);
			}

			total_seconds += stats.seconds;
			if (stats.max_client_inflight > total_max_client_inflight)
			{
				total_max_client_inflight = stats.max_client_inflight;
			}
			total_write_sync = stats.write_sync;
			if (!cfg.no_latency)
			{
				auto it = all_lat.insert(all_lat.end(), stats.latency_ns.begin(), stats.latency_ns.end());
				if (it == all_lat.end())
				{
					std::terminate();
				}
			}
		}

		Stats total;
		total.seconds = total_seconds / static_cast<double>(cfg.rounds);
		total.latency_ns = std::move(all_lat);
		total.max_client_inflight = total_max_client_inflight;
		total.write_sync = total_write_sync;
		if (cfg.mode == BenchMode::kDiskRead)
		{
			PrintLine("sync_disk_total", cfg, 0, total);
		}
		else if (cfg.mode == BenchMode::kDurabilityWrite)
		{
			PrintLine("sync_durability_write_total", cfg, 0, total);
		}
		else
		{
			PrintLine("sync_total", cfg, 0, total);
		}

		(void)std::filesystem::remove_all(dir);
	}

	static Detached RunAsyncOpen(AsyncDB*& out_db, ThreadPoolScheduler& scheduler, const Options& options, const std::string& dir,
	    std::binary_semaphore& sem, std::exception_ptr& exc)
	{
		try
		{
			auto result = co_await AsyncDB::OpenAsync(scheduler, options, dir);
			if (result.has_value())
			{
				out_db = result.value().release();
			}
		}
		catch (...)
		{
			exc = std::current_exception();
		}
		sem.release();
		co_return;
	}

	static void RunAsyncBenchmark(const Config& cfg, const std::vector<std::vector<std::string>>& keys)
	{
		const std::string dir = cfg.db_dir.empty() ? MakeTempDir("bench_async") : cfg.db_dir;
		ThreadPoolScheduler scheduler(static_cast<std::size_t>(cfg.workers));
		Options options;
		options.create_if_missing = true;
		options.write_buffer_size = cfg.write_buffer_size;

		if (cfg.mode == BenchMode::kDiskRead)
		{
			options.write_buffer_size = std::min<std::size_t>(options.write_buffer_size, 4 * 1024);
		}

		BenchEnvWrapper env_wrapper(Env::Default());
		Env* env_to_use = Env::Default();
		if (cfg.mode == BenchMode::kCompactionOverlap)
		{
			env_to_use = &env_wrapper;
			options.env = &env_wrapper;
		}

		const bool should_prefill = (cfg.mode == BenchMode::kSstReadPipeline) || (cfg.prefill == 1)
		    || (cfg.mode == BenchMode::kCompactionOverlap)
		    || (cfg.prefill == -1 && (cfg.mode == BenchMode::kDiskRead || cfg.read_ratio > 0));

		if (should_prefill)
		{
			Options prefill_options = options;
			prefill_options.env = env_to_use;
			auto pre = DB::Open(prefill_options, dir);
			if (!pre.has_value())
			{
				std::fprintf(stderr, "async prefill open failed: %s\n", pre.error().ToString().c_str());
				return;
			}
			auto pre_db = std::move(pre.value());
			Prefill(*pre_db, keys, cfg.ops_per_client, cfg.value_size);
		}

		auto open_sem = std::binary_semaphore(0);
		AsyncDB* adb_ptr = nullptr;
		std::exception_ptr open_exc;
		RunAsyncOpen(adb_ptr, scheduler, options, dir, open_sem, open_exc);

		open_sem.acquire();
		if (open_exc)
		{
			std::rethrow_exception(open_exc);
		}
		if (!adb_ptr)
		{
			std::fprintf(stderr, "async open failed\n");
			return;
		}
		std::unique_ptr<AsyncDB> adb(adb_ptr);

		std::printf("scheduler_threads=%zu inflight_per_client=%d\n", scheduler.WorkerCount(), cfg.inflight_per_client);

		for (int w = 0; w < cfg.warmup_rounds; ++w)
		{
			if (cfg.mode == BenchMode::kDiskRead)
			{
				(void)RunAsyncDiskRead(*adb, scheduler, cfg, keys);
			}
			else if (cfg.mode == BenchMode::kSstReadPipeline)
			{
				(void)RunAsyncSstReadPipeline(*adb, scheduler, cfg, keys);
			}
			else if (cfg.mode == BenchMode::kDurabilityWrite)
			{
				(void)RunAsyncDurabilityWrite(*adb, scheduler, cfg, keys);
			}
			else if (cfg.mode == BenchMode::kCompactionOverlap)
			{
				(void)RunAsyncCompactionOverlap(*adb, scheduler, cfg, keys);
			}
			else
			{
				(void)RunAsyncMixed(*adb, scheduler, cfg, keys);
			}
		}

		std::vector<uint64_t> all_lat;
		double total_seconds = 0;
		std::size_t total_max_inflight_observed = 0;
		std::size_t total_max_client_inflight = 0;
		int total_write_sync = 0;
		int total_bg_scheduled = 0;
		int total_bg_sleeps = 0;

		for (int r = 1; r <= cfg.rounds; ++r)
		{
			if (cfg.mode == BenchMode::kCompactionOverlap)
			{
				env_wrapper.Reset();
			}

			Stats stats;
			if (cfg.mode == BenchMode::kDiskRead)
			{
				stats = RunAsyncDiskRead(*adb, scheduler, cfg, keys);
				PrintLine("async_disk", cfg, r, stats, stats.max_inflight_observed);
			}
			else if (cfg.mode == BenchMode::kSstReadPipeline)
			{
				stats = RunAsyncSstReadPipeline(*adb, scheduler, cfg, keys);
				PrintLine("async_sst_read_pipeline", cfg, r, stats, stats.max_inflight_observed);
			}
			else if (cfg.mode == BenchMode::kDurabilityWrite)
			{
				stats = RunAsyncDurabilityWrite(*adb, scheduler, cfg, keys);
				PrintLine("async_durability_write", cfg, r, stats, stats.max_inflight_observed);
			}
			else if (cfg.mode == BenchMode::kCompactionOverlap)
			{
				stats = RunAsyncCompactionOverlap(*adb, scheduler, cfg, keys);
				stats.bg_scheduled = env_wrapper.ScheduledCalls();
				stats.bg_sleeps = env_wrapper.SleepCalls();
				PrintLine("async_compaction_overlap", cfg, r, stats, stats.max_inflight_observed);
			}
			else
			{
				stats = RunAsyncMixed(*adb, scheduler, cfg, keys);
				PrintLine("async", cfg, r, stats, stats.max_inflight_observed);
			}

			total_seconds += stats.seconds;
			if (stats.max_inflight_observed > total_max_inflight_observed)
			{
				total_max_inflight_observed = stats.max_inflight_observed;
			}
			if (stats.max_client_inflight > total_max_client_inflight)
			{
				total_max_client_inflight = stats.max_client_inflight;
			}
			total_write_sync = stats.write_sync;
			total_bg_scheduled += stats.bg_scheduled;
			total_bg_sleeps += stats.bg_sleeps;
			if (!cfg.no_latency)
			{
				auto it = all_lat.insert(all_lat.end(), stats.latency_ns.begin(), stats.latency_ns.end());
				if (it == all_lat.end())
				{
					std::terminate();
				}
			}
		}

		Stats total;
		total.seconds = total_seconds / static_cast<double>(cfg.rounds);
		total.latency_ns = std::move(all_lat);
		total.max_inflight_observed = total_max_inflight_observed;
		total.max_client_inflight = total_max_client_inflight;
		total.write_sync = total_write_sync;
		total.bg_scheduled = total_bg_scheduled;
		total.bg_sleeps = total_bg_sleeps;
		if (cfg.mode == BenchMode::kDiskRead)
		{
			PrintLine("async_disk_total", cfg, 0, total, total.max_inflight_observed);
		}
		else if (cfg.mode == BenchMode::kSstReadPipeline)
		{
			PrintLine("async_sst_read_pipeline_total", cfg, 0, total, total.max_inflight_observed);
		}
		else if (cfg.mode == BenchMode::kDurabilityWrite)
		{
			PrintLine("async_durability_write_total", cfg, 0, total, total.max_inflight_observed);
		}
		else if (cfg.mode == BenchMode::kCompactionOverlap)
		{
			PrintLine("async_compaction_overlap_total", cfg, 0, total, total.max_inflight_observed);
		}
		else
		{
			PrintLine("async_total", cfg, 0, total, total.max_inflight_observed);
		}

		if (!cfg.keep_db)
		{
			(void)std::filesystem::remove_all(dir);
		}
	}

} // namespace prism::bench

int main(int argc, char** argv)
{
	using namespace prism;
	using namespace prism::bench;

	Config cfg = ParseArgs(argc, argv);
	const auto keys = MakeKeys(cfg.clients, cfg.ops_per_client);

	std::printf("config: run=%s bench=%s clients=%d workers=%d ops=%zu value_size=%zu read_ratio=%d\n", RunName(cfg).c_str(),
	    BenchName(cfg.mode).c_str(), cfg.clients, cfg.workers, cfg.ops_per_client, cfg.value_size, cfg.read_ratio);

	if (cfg.do_sync)
	{
		RunSyncBenchmark(cfg, keys);
	}

	if (cfg.do_async)
	{
		RunAsyncBenchmark(cfg, keys);
	}

	return 0;
}