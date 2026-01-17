#include "asyncdb.h"
#include "db.h"
#include "scheduler.h"

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

	struct Config
	{
		int clients = 4;
		int workers = 4;
		std::size_t ops_per_client = 10000;
		std::size_t value_size = 100;
		int read_ratio = 0;
		bool do_sync = true;
		bool do_async = true;
	};

	static void Prefill(DB& db, int clients, std::size_t per_client, std::size_t value_size)
	{
		const std::string value = MakeValue(value_size);
		for (int t = 0; t < clients; ++t)
		{
			for (std::size_t i = 0; i < per_client; ++i)
			{
				Status s = db.Put(WriteOptions(), Slice(MakeKey(t, i)), Slice(value));
				if (!s.ok())
				{
					throw std::runtime_error(s.ToString());
				}
			}
		}
	}

	static double RunSync(DB& db, const Config& cfg)
	{
		std::atomic<bool> start{ false };
		std::atomic<int> ready_count{ 0 };
		std::atomic<uint64_t> start_ns{ 0 };
		std::atomic<uint64_t> sink{ 0 };

		const std::string value = MakeValue(cfg.value_size);
		std::vector<std::thread> threads;
		threads.reserve(cfg.clients);

		for (int t = 0; t < cfg.clients; ++t)
		{
			threads.emplace_back([&, t] {
				std::mt19937_64 rng(static_cast<uint64_t>(t + 1));
				uint64_t local_sink = 0;
				(void)ready_count.fetch_add(1, std::memory_order_release);
				while (!start.load(std::memory_order_acquire))
				{
					std::this_thread::yield();
				}

				for (std::size_t i = 0; i < cfg.ops_per_client; ++i)
				{
					const bool do_read = (cfg.read_ratio > 0) && (static_cast<int>(rng() % 100) < cfg.read_ratio);
					if (do_read)
					{
						auto r = db.Get(ReadOptions(), Slice(MakeKey(t, i)));
						if (r.has_value())
						{
							local_sink += r.value().size();
						}
					}
					else
					{
						Status s = db.Put(WriteOptions(), Slice(MakeKey(t, i)), Slice(value));
						if (s.ok())
						{
							local_sink += 1;
						}
					}
				}

				(void)sink.fetch_add(local_sink, std::memory_order_relaxed);
			});
		}

		while (ready_count.load(std::memory_order_acquire) != cfg.clients)
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
		return static_cast<double>(end_ns - begin_ns) / 1e9;
	}

	static Detached RunAsyncWorker(AsyncDB& db, StartGate& gate, DoneState& done, const Config& cfg, int id, std::string value)
	{
		try
		{
			std::mt19937_64 rng(static_cast<uint64_t>(id + 1));
			co_await gate;
			for (std::size_t i = 0; i < cfg.ops_per_client; ++i)
			{
				const bool do_read = (cfg.read_ratio > 0) && (static_cast<int>(rng() % 100) < cfg.read_ratio);
				if (do_read)
				{
					auto r = co_await db.GetAsync(ReadOptions(), MakeKey(id, i));
					if (r.has_value())
					{
						(void)r.value().size();
					}
				}
				else
				{
					Status s = co_await db.PutAsync(WriteOptions(), MakeKey(id, i), value);
					(void)s.ok();
				}
			}
		}
		catch (...)
		{
			done.exception = std::current_exception();
		}
		done.NotifyDone();
		co_return;
	}

	static double RunAsync(AsyncDB& db, ThreadPoolScheduler& scheduler, const Config& cfg)
	{
		StartGate gate;
		DoneState done(cfg.clients);
		const std::string value = MakeValue(cfg.value_size);

		for (int t = 0; t < cfg.clients; ++t)
		{
			RunAsyncWorker(db, gate, done, cfg, t, value);
		}

		const uint64_t begin_ns = NowNs();
		gate.Open(scheduler);
		done.done.acquire();
		const uint64_t end_ns = NowNs();

		if (done.exception)
		{
			std::rethrow_exception(done.exception);
		}

		return static_cast<double>(end_ns - begin_ns) / 1e9;
	}

	static void PrintResult(const char* name, const Config& cfg, double seconds)
	{
		const double total_ops = static_cast<double>(cfg.clients) * static_cast<double>(cfg.ops_per_client);
		const double ops_per_sec = total_ops / seconds;
		std::printf("%s: clients=%d workers=%d ops=%zu value=%zu read_ratio=%d time=%.3fs ops/s=%.0f\n", name, cfg.clients, cfg.workers,
		    cfg.ops_per_client, cfg.value_size, cfg.read_ratio, seconds, ops_per_sec);
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
			parse_size("--ops=", cfg.ops_per_client);
			parse_size("--value_size=", cfg.value_size);
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
		}
		return cfg;
	}
}

int main(int argc, char** argv)
{
	using namespace prism;
	using namespace prism::bench;

	Config cfg = ParseArgs(argc, argv);

	if (cfg.clients <= 0)
	{
		cfg.clients = 1;
	}
	if (cfg.workers <= 0)
	{
		cfg.workers = 1;
	}

	if (cfg.do_sync)
	{
		const std::string dir = MakeTempDir("bench_sync");
		Options options;
		options.create_if_missing = true;
		auto db_res = DB::Open(options, dir);
		if (!db_res.has_value())
		{
			std::fprintf(stderr, "sync open failed: %s\n", db_res.error().ToString().c_str());
			return 1;
		}
		auto db = std::move(db_res.value());
		if (cfg.read_ratio > 0)
		{
			Prefill(*db, cfg.clients, cfg.ops_per_client, cfg.value_size);
		}
		double seconds = RunSync(*db, cfg);
		PrintResult("sync", cfg, seconds);
		(void)std::filesystem::remove_all(dir);
	}

	if (cfg.do_async)
	{
		const std::string dir = MakeTempDir("bench_async");
		ThreadPoolScheduler scheduler(static_cast<std::size_t>(cfg.workers));
		Options options;
		options.create_if_missing = true;

		std::binary_semaphore open_sem(0);
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
		if (cfg.read_ratio > 0)
		{
			Prefill(*adb->SyncDB(), cfg.clients, cfg.ops_per_client, cfg.value_size);
		}

		double seconds = RunAsync(*adb, scheduler, cfg);
		PrintResult("async", cfg, seconds);
		(void)std::filesystem::remove_all(dir);
	}

	return 0;
}
