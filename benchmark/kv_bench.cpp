#include "asyncdb.h"
#include "db.h"
#include "scheduler.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <filesystem>
#include <optional>
#include <random>
#include <semaphore>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <unistd.h>

namespace prism::bench
{
	using Clock = std::chrono::steady_clock;

	static uint64_t NowNs() { return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count(); }

	static std::string MakeTempDir(std::string_view tag)
	{
		const auto base = std::filesystem::temp_directory_path();
		const auto pid = static_cast<unsigned long>(::getpid());
		const auto t = static_cast<unsigned long long>(NowNs());
		std::string name;
		name.reserve(128);
		name.append("prism_");
		name.append(tag);
		name.push_back('_');
		name.append(std::to_string(pid));
		name.push_back('_');
		name.append(std::to_string(t));
		auto dir = base / name;
		(void)std::filesystem::create_directories(dir);
		return dir.string();
	}

	static std::string MakeValue(std::size_t n)
	{
		std::string v;
		v.resize(n);
		std::fill(v.begin(), v.end(), 'v');
		return v;
	}

	static std::string MakeKey(int worker, std::size_t i)
	{
		char buf[64];
		const int n = std::snprintf(buf, sizeof(buf), "k_%d_%zu", worker, i);
		return std::string(buf, static_cast<std::size_t>(n));
	}

	static std::vector<std::vector<std::string>> MakeKeys(int clients, std::size_t ops_per_client)
	{
		std::vector<std::vector<std::string>> keys;
		keys.resize(static_cast<std::size_t>(clients));
		for (int t = 0; t < clients; ++t)
		{
			auto& v = keys[static_cast<std::size_t>(t)];
			v.reserve(ops_per_client);
			for (std::size_t i = 0; i < ops_per_client; ++i)
			{
				v.push_back(MakeKey(t, i));
			}
		}
		return keys;
	}

	struct StartGate
	{
		struct Awaiter
		{
			StartGate* gate;

			bool await_ready() const noexcept { return gate->open.load(std::memory_order_acquire); }

			bool await_suspend(std::coroutine_handle<> handle)
			{
				if (gate->open.load(std::memory_order_acquire))
				{
					return false;
				}
				std::lock_guard lock(gate->mutex);
				if (gate->open.load(std::memory_order_relaxed))
				{
					return false;
				}
				gate->waiters.push_back(handle);
				return true;
			}

			void await_resume() const noexcept {}
		};

		Awaiter operator co_await() noexcept { return Awaiter{ this }; }

		void Open(ThreadPoolScheduler& scheduler)
		{
			std::vector<std::coroutine_handle<>> to_resume;
			{
				std::lock_guard lock(mutex);
				open.store(true, std::memory_order_release);
				to_resume.swap(waiters);
			}
			for (auto h : to_resume)
			{
				scheduler.Submit([h] { h.resume(); });
			}
		}

		std::mutex mutex;
		std::vector<std::coroutine_handle<>> waiters;
		std::atomic<bool> open{ false };
	};

	struct DoneState
	{
		explicit DoneState(int count)
		    : remaining(count)
		{
		}

		void NotifyDone()
		{
			if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1)
			{
				done.release();
			}
		}

		std::binary_semaphore done{ 0 };
		std::atomic<int> remaining;
		std::exception_ptr exception;
	};

	struct Detached
	{
		struct promise_type
		{
			Detached get_return_object() noexcept { return {}; }
			std::suspend_never initial_suspend() noexcept { return {}; }
			std::suspend_never final_suspend() noexcept { return {}; }
			void return_void() noexcept {}
			void unhandled_exception() noexcept { std::terminate(); }
		};
	};

	enum class BenchMode
	{
		kMixed,
		kDiskRead
	};

	struct Config
	{
		int clients = 4;
		int workers = 4;
		std::size_t ops_per_client = 10000;
		std::size_t value_size = 100;
		int read_ratio = 0;
		int rounds = 3;
		BenchMode mode = BenchMode::kMixed;
		std::size_t write_buffer_size = 4 * 1024 * 1024;
		bool do_sync = true;
		bool do_async = true;
	};

	struct Stats
	{
		double seconds = 0;
		std::vector<uint64_t> latency_ns;
	};

	static uint64_t PercentileNs(std::vector<uint64_t> v, double p)
	{
		if (v.empty())
		{
			return 0;
		}
		std::sort(v.begin(), v.end());
		const std::size_t idx = static_cast<std::size_t>(p * static_cast<double>(v.size() - 1));
		return v[idx];
	}

	static void Prefill(DB& db, const std::vector<std::vector<std::string>>& keys, std::size_t ops_per_client, std::size_t value_size)
	{
		const std::string value = MakeValue(value_size);
		for (std::size_t t = 0; t < keys.size(); ++t)
		{
			for (std::size_t i = 0; i < ops_per_client; ++i)
			{
				Status s = db.Put(WriteOptions(), Slice(keys[t][i]), Slice(value));
				if (!s.ok())
				{
					throw std::runtime_error(s.ToString());
				}
			}
		}
	}

	static Stats RunSyncMixed(DB& db, const Config& cfg, const std::vector<std::vector<std::string>>& keys)
	{
		Stats out;
		out.latency_ns.reserve(static_cast<std::size_t>(cfg.clients) * cfg.ops_per_client);

		std::atomic<bool> start{ false };
		std::atomic<int> ready_count{ 0 };
		std::atomic<bool> all_ready{ false };
		std::atomic<uint64_t> start_ns{ 0 };
		std::atomic<uint64_t> sink{ 0 };

		std::mutex lat_mutex;

		std::vector<std::thread> threads;
		threads.reserve(cfg.clients);

		const std::string value = MakeValue(cfg.value_size);

		for (int t = 0; t < cfg.clients; ++t)
		{
			threads.emplace_back([&, t] {
				std::mt19937_64 rng(static_cast<uint64_t>(t + 1));
				std::vector<uint64_t> local_lat;
				local_lat.reserve(cfg.ops_per_client);
				uint64_t local_sink = 0;

				const int prev_ready = ready_count.fetch_add(1, std::memory_order_acq_rel);
				if (prev_ready + 1 == cfg.clients)
				{
					all_ready.store(true, std::memory_order_release);
				}

				while (!start.load(std::memory_order_acquire))
				{
					std::this_thread::yield();
				}

				for (std::size_t i = 0; i < cfg.ops_per_client; ++i)
				{
					const bool do_read = (cfg.read_ratio > 0) && (static_cast<int>(rng() % 100) < cfg.read_ratio);
					const uint64_t begin = NowNs();
					if (do_read)
					{
						auto r = db.Get(ReadOptions(), Slice(keys[static_cast<std::size_t>(t)][i]));
						if (r.has_value())
						{
							local_sink += r.value().size();
						}
					}
					else
					{
						Status s = db.Put(WriteOptions(), Slice(keys[static_cast<std::size_t>(t)][i]), Slice(value));
						if (s.ok())
						{
							local_sink += 1;
						}
					}
					const uint64_t end = NowNs();
					local_lat.push_back(end - begin);
				}

				(void)sink.fetch_add(local_sink, std::memory_order_relaxed);
				std::lock_guard lock(lat_mutex);
				auto it = out.latency_ns.insert(out.latency_ns.end(), local_lat.begin(), local_lat.end());
				if (it == out.latency_ns.end())
				{
					std::terminate();
				}
			});
		}

		while (!all_ready.load(std::memory_order_acquire))
		{
			std::this_thread::yield();
		}

		start_ns.store(NowNs(), std::memory_order_release);
		start.store(true, std::memory_order_release);

		for (auto& th : threads)
		{
			th.join();
		}

		(void)sink.load(std::memory_order_relaxed);
		const uint64_t end_ns = NowNs();

		const uint64_t begin_ns = start_ns.load(std::memory_order_acquire);
		out.seconds = static_cast<double>(end_ns - begin_ns) / 1e9;
		return out;
	}

	static Stats RunSyncDiskRead(DB& db, const Config& cfg, const std::vector<std::vector<std::string>>& keys)
	{
		Stats out;
		out.latency_ns.reserve(static_cast<std::size_t>(cfg.clients) * cfg.ops_per_client);

		std::atomic<bool> start{ false };
		std::atomic<int> ready_count{ 0 };
		std::atomic<bool> all_ready{ false };
		std::atomic<uint64_t> start_ns{ 0 };
		std::atomic<uint64_t> sink{ 0 };

		std::mutex lat_mutex;
		std::vector<std::thread> threads;
		threads.reserve(cfg.clients);

		for (int t = 0; t < cfg.clients; ++t)
		{
			threads.emplace_back([&, t] {
				std::vector<uint64_t> local_lat;
				local_lat.reserve(cfg.ops_per_client);
				uint64_t local_sink = 0;
				const int prev_ready = ready_count.fetch_add(1, std::memory_order_acq_rel);
				if (prev_ready + 1 == cfg.clients)
				{
					all_ready.store(true, std::memory_order_release);
				}

				while (!start.load(std::memory_order_acquire))
				{
					std::this_thread::yield();
				}

				for (std::size_t i = 0; i < cfg.ops_per_client; ++i)
				{
					const uint64_t begin = NowNs();
					auto r = db.Get(ReadOptions(), Slice(keys[static_cast<std::size_t>(t)][i]));
					if (r.has_value())
					{
						local_sink += r.value().size();
					}
					const uint64_t end = NowNs();
					local_lat.push_back(end - begin);
				}

				(void)sink.fetch_add(local_sink, std::memory_order_relaxed);
				std::lock_guard lock(lat_mutex);
				auto it = out.latency_ns.insert(out.latency_ns.end(), local_lat.begin(), local_lat.end());
				if (it == out.latency_ns.end())
				{
					std::terminate();
				}
			});
		}

		while (!all_ready.load(std::memory_order_acquire))
		{
			std::this_thread::yield();
		}

		start_ns.store(NowNs(), std::memory_order_release);
		start.store(true, std::memory_order_release);

		for (auto& th : threads)
		{
			th.join();
		}

		(void)sink.load(std::memory_order_relaxed);
		const uint64_t end_ns = NowNs();
		const uint64_t begin_ns = start_ns.load(std::memory_order_acquire);
		out.seconds = static_cast<double>(end_ns - begin_ns) / 1e9;
		return out;
	}

	static Detached RunAsyncMixedWorker(AsyncDB& db, StartGate& gate, DoneState& done, const Config& cfg,
	    const std::vector<std::vector<std::string>>& keys, int id, std::string value, std::vector<uint64_t>& lat)

	{
		try
		{
			std::mt19937_64 rng(static_cast<uint64_t>(id + 1));
			co_await gate;
			lat.reserve(cfg.ops_per_client);
			for (std::size_t i = 0; i < cfg.ops_per_client; ++i)
			{
				const bool do_read = (cfg.read_ratio > 0) && (static_cast<int>(rng() % 100) < cfg.read_ratio);
				const uint64_t begin = NowNs();
				if (do_read)
				{
					(void)co_await db.GetAsync(ReadOptions(), keys[static_cast<std::size_t>(id)][i]);
				}
				else
				{
					(void)co_await db.PutAsync(WriteOptions(), keys[static_cast<std::size_t>(id)][i], value);
				}
				const uint64_t end = NowNs();
				lat.push_back(end - begin);
			}
		}
		catch (...)
		{
			done.exception = std::current_exception();
		}
		done.NotifyDone();
		co_return;
	}

	static Detached RunAsyncDiskReadWorker(AsyncDB& db, StartGate& gate, DoneState& done, const Config& cfg,
	    const std::vector<std::vector<std::string>>& keys, int id, std::vector<uint64_t>& lat)
	{
		try
		{
			co_await gate;
			lat.reserve(cfg.ops_per_client);
			for (std::size_t i = 0; i < cfg.ops_per_client; ++i)
			{
				const uint64_t begin = NowNs();
				(void)co_await db.GetAsync(ReadOptions(), keys[static_cast<std::size_t>(id)][i]);
				const uint64_t end = NowNs();
				lat.push_back(end - begin);
			}
		}
		catch (...)
		{
			done.exception = std::current_exception();
		}
		done.NotifyDone();
		co_return;
	}

	static Stats RunAsyncMixed(
	    AsyncDB& db, ThreadPoolScheduler& scheduler, const Config& cfg, const std::vector<std::vector<std::string>>& keys)
	{
		Stats out;
		StartGate gate;
		DoneState done(cfg.clients);
		std::vector<std::vector<uint64_t>> lat;
		lat.resize(static_cast<std::size_t>(cfg.clients));
		const std::string value = MakeValue(cfg.value_size);

		for (int t = 0; t < cfg.clients; ++t)
		{
			RunAsyncMixedWorker(db, gate, done, cfg, keys, t, value, lat[static_cast<std::size_t>(t)]);
		}

		const uint64_t begin_ns = NowNs();
		gate.Open(scheduler);
		done.done.acquire();
		const uint64_t end_ns = NowNs();

		if (done.exception)
		{
			std::rethrow_exception(done.exception);
		}

		out.seconds = static_cast<double>(end_ns - begin_ns) / 1e9;
		out.latency_ns.reserve(static_cast<std::size_t>(cfg.clients) * cfg.ops_per_client);
		for (auto& v : lat)
		{
			out.latency_ns.insert(out.latency_ns.end(), v.begin(), v.end());
		}
		return out;
	}

	static Stats RunAsyncDiskRead(
	    AsyncDB& db, ThreadPoolScheduler& scheduler, const Config& cfg, const std::vector<std::vector<std::string>>& keys)
	{
		Stats out;
		StartGate gate;
		DoneState done(cfg.clients);
		std::vector<std::vector<uint64_t>> lat;
		lat.resize(static_cast<std::size_t>(cfg.clients));

		for (int t = 0; t < cfg.clients; ++t)
		{
			RunAsyncDiskReadWorker(db, gate, done, cfg, keys, t, lat[static_cast<std::size_t>(t)]);
		}

		const uint64_t begin_ns = NowNs();
		gate.Open(scheduler);
		done.done.acquire();
		const uint64_t end_ns = NowNs();

		if (done.exception)
		{
			std::rethrow_exception(done.exception);
		}

		out.seconds = static_cast<double>(end_ns - begin_ns) / 1e9;
		out.latency_ns.reserve(static_cast<std::size_t>(cfg.clients) * cfg.ops_per_client);
		for (auto& v : lat)
		{
			out.latency_ns.insert(out.latency_ns.end(), v.begin(), v.end());
		}
		return out;
	}

	static void PrintLine(std::string_view name, const Config& cfg, int round, const Stats& stats)
	{
		const double total_ops = static_cast<double>(cfg.clients) * static_cast<double>(cfg.ops_per_client);
		const double ops_per_sec = total_ops / stats.seconds;
		const uint64_t p50_ns = PercentileNs(stats.latency_ns, 0.50);
		const uint64_t p95_ns = PercentileNs(stats.latency_ns, 0.95);
		std::printf("%s r=%d clients=%d workers=%d ops=%zu value=%zu read_ratio=%d time=%.3fs ops/s=%.0f p50_us=%.2f p95_us=%.2f\n",
		    std::string(name).c_str(), round, cfg.clients, cfg.workers, cfg.ops_per_client, cfg.value_size, cfg.read_ratio, stats.seconds,
		    ops_per_sec, static_cast<double>(p50_ns) / 1000.0, static_cast<double>(p95_ns) / 1000.0);
	}

	static Config ParseArgs(int argc, char** argv)
	{
		Config cfg;
		for (int i = 1; i < argc; ++i)
		{
			std::string_view arg(argv[i]);
			auto parse_int = [&](std::string_view key, int& out) {
				if (!arg.starts_with(key))
				{
					return;
				}
				out = std::stoi(std::string(arg.substr(key.size())));
			};

			auto parse_size = [&](std::string_view key, std::size_t& out) {
				if (!arg.starts_with(key))
				{
					return;
				}
				out = static_cast<std::size_t>(std::stoull(std::string(arg.substr(key.size()))));
			};

			parse_int("--clients=", cfg.clients);
			parse_int("--workers=", cfg.workers);
			parse_int("--rounds=", cfg.rounds);
			parse_size("--ops=", cfg.ops_per_client);
			parse_size("--value_size=", cfg.value_size);
			parse_size("--write_buffer_size=", cfg.write_buffer_size);
			parse_int("--read_ratio=", cfg.read_ratio);

			if (arg == "--sync")
			{
				cfg.do_sync = true;
				cfg.do_async = false;
			}
			if (arg == "--async")
			{
				cfg.do_sync = false;
				cfg.do_async = true;
			}
			if (arg == "--bench=mixed")
			{
				cfg.mode = BenchMode::kMixed;
			}
			if (arg == "--bench=disk_read")
			{
				cfg.mode = BenchMode::kDiskRead;
			}
		}

		if (cfg.clients <= 0)
		{
			cfg.clients = 1;
		}
		if (cfg.workers <= 0)
		{
			cfg.workers = 1;
		}
		if (cfg.rounds <= 0)
		{
			cfg.rounds = 1;
		}
		if (cfg.read_ratio < 0)
		{
			cfg.read_ratio = 0;
		}
		if (cfg.read_ratio > 100)
		{
			cfg.read_ratio = 100;
		}

		return cfg;
	}
}

int main(int argc, char** argv)
{
	using namespace prism;
	using namespace prism::bench;

	Config cfg = ParseArgs(argc, argv);
	const auto keys = MakeKeys(cfg.clients, cfg.ops_per_client);

	if (cfg.do_sync)
	{
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
			return 1;
		}
		auto db = std::move(db_res.value());

		if (cfg.mode == BenchMode::kDiskRead || cfg.read_ratio > 0)
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
				return 1;
			}
			db = std::move(reopened.value());
		}

		std::vector<uint64_t> all_lat;
		double total_seconds = 0;

		for (int r = 1; r <= cfg.rounds; ++r)
		{
			Stats stats;
			if (cfg.mode == BenchMode::kDiskRead)
			{
				stats = RunSyncDiskRead(*db, cfg, keys);
				PrintLine("sync_disk", cfg, r, stats);
			}
			else
			{
				stats = RunSyncMixed(*db, cfg, keys);
				PrintLine("sync", cfg, r, stats);
			}

			total_seconds += stats.seconds;
			auto it = all_lat.insert(all_lat.end(), stats.latency_ns.begin(), stats.latency_ns.end());
			if (it == all_lat.end())
			{
				std::terminate();
			}
		}

		Stats total;
		total.seconds = total_seconds / static_cast<double>(cfg.rounds);
		total.latency_ns = std::move(all_lat);
		PrintLine(cfg.mode == BenchMode::kDiskRead ? "sync_disk_total" : "sync_total", cfg, 0, total);

		(void)std::filesystem::remove_all(dir);
	}

	if (cfg.do_async)
	{
		const std::string dir = MakeTempDir("bench_async");
		ThreadPoolScheduler scheduler(static_cast<std::size_t>(cfg.workers));
		Options options;
		options.create_if_missing = true;
		options.write_buffer_size = cfg.write_buffer_size;

		if (cfg.mode == BenchMode::kDiskRead)
		{
			options.write_buffer_size = std::min<std::size_t>(options.write_buffer_size, 4 * 1024);
		}

		{
			auto pre = DB::Open(options, dir);
			if (!pre.has_value())
			{
				std::fprintf(stderr, "async prefill open failed: %s\n", pre.error().ToString().c_str());
				return 1;
			}
			auto pre_db = std::move(pre.value());
			if (cfg.mode == BenchMode::kDiskRead || cfg.read_ratio > 0)
			{
				Prefill(*pre_db, keys, cfg.ops_per_client, cfg.value_size);
			}
		}

		auto open_sem = std::binary_semaphore(0);
		std::optional<Result<std::unique_ptr<AsyncDB>>> opened;
		std::exception_ptr open_exc;
		auto open_fn = [&]() -> Detached {
			try
			{
				opened = co_await AsyncDB::OpenAsync(scheduler, options, dir);
			}
			catch (...)
			{
				open_exc = std::current_exception();
			}
			open_sem.release();
			co_return;
		};
		open_fn();

		open_sem.acquire();
		if (open_exc)
		{
			std::rethrow_exception(open_exc);
		}
		if (!opened.has_value() || !opened->has_value())
		{
			const Status s = opened.has_value() ? opened->error() : Status::InvalidArgument("async open failed");
			std::fprintf(stderr, "async open failed: %s\n", s.ToString().c_str());
			return 1;
		}

		auto adb = std::move(opened->value());

		std::vector<uint64_t> all_lat;
		double total_seconds = 0;

		for (int r = 1; r <= cfg.rounds; ++r)
		{
			Stats stats;
			if (cfg.mode == BenchMode::kDiskRead)
			{
				stats = RunAsyncDiskRead(*adb, scheduler, cfg, keys);
				PrintLine("async_disk", cfg, r, stats);
			}
			else
			{
				stats = RunAsyncMixed(*adb, scheduler, cfg, keys);
				PrintLine("async", cfg, r, stats);
			}

			total_seconds += stats.seconds;
			auto it = all_lat.insert(all_lat.end(), stats.latency_ns.begin(), stats.latency_ns.end());
			if (it == all_lat.end())
			{
				std::terminate();
			}
		}

		Stats total;
		total.seconds = total_seconds / static_cast<double>(cfg.rounds);
		total.latency_ns = std::move(all_lat);
		PrintLine(cfg.mode == BenchMode::kDiskRead ? "async_disk_total" : "async_total", cfg, 0, total);

		(void)std::filesystem::remove_all(dir);
	}

	return 0;
}
