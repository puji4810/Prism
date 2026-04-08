#include "kv_bench_lib.h"

#include "asyncdb.h"
#include "db.h"
#include "scheduler.h"

#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <random>
#include <stdexcept>
#include <unistd.h>

namespace prism::bench
{
	uint64_t NowNs() { return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count(); }

	std::string MakeTempDir(std::string_view tag)
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

	std::string MakeValue(std::size_t n)
	{
		std::string v;
		v.resize(n);
		std::fill(v.begin(), v.end(), 'v');
		return v;
	}

	std::string MakeKey(int worker, std::size_t i)
	{
		char buf[64];
		const int n = std::snprintf(buf, sizeof(buf), "k_%d_%zu", worker, i);
		return std::string(buf, static_cast<std::size_t>(n));
	}

	std::vector<std::vector<std::string>> MakeKeys(int clients, std::size_t ops_per_client)
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

	bool StartGate::Awaiter::await_ready() const noexcept { return gate->open.load(std::memory_order_acquire); }

	bool StartGate::Awaiter::await_suspend(std::coroutine_handle<> handle)
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

	StartGate::Awaiter StartGate::operator co_await() noexcept { return Awaiter{ this }; }

	void StartGate::Open(ThreadPoolScheduler& scheduler)
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

	DoneState::DoneState(int count)
	    : remaining(count)
	{
	}

	void DoneState::NotifyDone()
	{
		if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1)
		{
			done.release();
		}
	}

	Detached Detached::promise_type::get_return_object() noexcept { return { }; }
	std::suspend_never Detached::promise_type::initial_suspend() noexcept { return { }; }
	std::suspend_never Detached::promise_type::final_suspend() noexcept { return { }; }
	void Detached::promise_type::unhandled_exception() noexcept { std::terminate(); }

	uint64_t PercentileNs(std::vector<uint64_t> v, double p)
	{
		if (v.empty())
		{
			return 0;
		}
		std::sort(v.begin(), v.end());
		const std::size_t idx = static_cast<std::size_t>(p * static_cast<double>(v.size() - 1));
		return v[idx];
	}

	void Prefill(DB& db, const std::vector<std::vector<std::string>>& keys, std::size_t ops_per_client, std::size_t value_size)
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

	Stats RunSyncMixed(DB& db, const Config& cfg, const std::vector<std::vector<std::string>>& keys)
	{
		Stats out;
		if (!cfg.no_latency)
		{
			out.latency_ns.reserve(static_cast<std::size_t>(cfg.clients) * cfg.ops_per_client);
		}

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
				if (!cfg.no_latency)
				{
					local_lat.reserve(cfg.ops_per_client);
				}
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
					if (!cfg.no_latency)
					{
						local_lat.push_back(end - begin);
					}
				}

				(void)sink.fetch_add(local_sink, std::memory_order_relaxed);
				if (!cfg.no_latency)
				{
					std::lock_guard lock(lat_mutex);
					auto it = out.latency_ns.insert(out.latency_ns.end(), local_lat.begin(), local_lat.end());
					if (it == out.latency_ns.end())
					{
						std::terminate();
					}
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
		out.write_sync = 0;
		return out;
	}

	Stats RunSyncDiskRead(DB& db, const Config& cfg, const std::vector<std::vector<std::string>>& keys)
	{
		Stats out;
		if (!cfg.no_latency)
		{
			out.latency_ns.reserve(static_cast<std::size_t>(cfg.clients) * cfg.ops_per_client);
		}

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
				if (!cfg.no_latency)
				{
					local_lat.reserve(cfg.ops_per_client);
				}
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
					if (!cfg.no_latency)
					{
						local_lat.push_back(end - begin);
					}
				}

				(void)sink.fetch_add(local_sink, std::memory_order_relaxed);
				if (!cfg.no_latency)
				{
					std::lock_guard lock(lat_mutex);
					auto it = out.latency_ns.insert(out.latency_ns.end(), local_lat.begin(), local_lat.end());
					if (it == out.latency_ns.end())
					{
						std::terminate();
					}
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
		out.write_sync = 0;
		return out;
	}

	Stats RunSyncDurabilityWrite(DB& db, const Config& cfg, const std::vector<std::vector<std::string>>& keys)
	{
		Stats out;
		if (!cfg.no_latency)
		{
			out.latency_ns.reserve(static_cast<std::size_t>(cfg.clients) * cfg.ops_per_client);
		}

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
				std::vector<uint64_t> local_lat;
				if (!cfg.no_latency)
				{
					local_lat.reserve(cfg.ops_per_client);
				}
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

				WriteOptions write_opts;
				write_opts.sync = true;

				for (std::size_t i = 0; i < cfg.ops_per_client; ++i)
				{
					const uint64_t begin = NowNs();
					Status s = db.Put(write_opts, Slice(keys[static_cast<std::size_t>(t)][i]), Slice(value));
					if (s.ok())
					{
						local_sink += 1;
					}
					const uint64_t end = NowNs();
					if (!cfg.no_latency)
					{
						local_lat.push_back(end - begin);
					}
				}

				(void)sink.fetch_add(local_sink, std::memory_order_relaxed);
				if (!cfg.no_latency)
				{
					std::lock_guard lock(lat_mutex);
					auto it = out.latency_ns.insert(out.latency_ns.end(), local_lat.begin(), local_lat.end());
					if (it == out.latency_ns.end())
					{
						std::terminate();
					}
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
		out.write_sync = 1;
		return out;
	}

	Detached RunAsyncMixedLane(AsyncDB& db, StartGate& gate, DoneState& done, const Config& cfg,
	    const std::vector<std::vector<std::string>>& keys, int client_id, int lane_id, int num_lanes, std::string value,
	    std::vector<uint64_t>& lat, std::atomic<std::size_t>& global_inflight, std::atomic<std::size_t>& global_max_inflight,
	    std::atomic<std::size_t>& client_inflight, std::atomic<std::size_t>& client_max_inflight)
	{
		try
		{
			std::mt19937_64 rng(static_cast<uint64_t>(client_id * 1000 + lane_id + 1));
			co_await gate;

			const std::size_t ops_per_lane = cfg.ops_per_client / static_cast<std::size_t>(num_lanes);
			const std::size_t start_idx = static_cast<std::size_t>(lane_id) * ops_per_lane;
			const std::size_t end_idx = (lane_id == num_lanes - 1) ? cfg.ops_per_client : start_idx + ops_per_lane;

			if (!cfg.no_latency)
			{
				lat.reserve(end_idx - start_idx);
			}

			for (std::size_t i = start_idx; i < end_idx; ++i)
			{
				const bool do_read = (cfg.read_ratio > 0) && (static_cast<int>(rng() % 100) < cfg.read_ratio);
				const uint64_t begin = NowNs();

				const std::size_t cur_global = global_inflight.fetch_add(1, std::memory_order_relaxed) + 1;
				std::size_t observed_global = global_max_inflight.load(std::memory_order_relaxed);
				while (cur_global > observed_global)
				{
					if (global_max_inflight.compare_exchange_weak(observed_global, cur_global, std::memory_order_relaxed))
					{
						break;
					}
				}

				const std::size_t cur_client = client_inflight.fetch_add(1, std::memory_order_relaxed) + 1;
				std::size_t observed_client = client_max_inflight.load(std::memory_order_relaxed);
				while (cur_client > observed_client)
				{
					if (client_max_inflight.compare_exchange_weak(observed_client, cur_client, std::memory_order_relaxed))
					{
						break;
					}
				}

				if (do_read)
				{
					(void)co_await db.GetAsync(ReadOptions(), keys[static_cast<std::size_t>(client_id)][i]);
				}
				else
				{
					(void)co_await db.PutAsync(WriteOptions(), keys[static_cast<std::size_t>(client_id)][i], value);
				}

				global_inflight.fetch_sub(1, std::memory_order_relaxed);
				client_inflight.fetch_sub(1, std::memory_order_relaxed);

				const uint64_t end = NowNs();
				if (!cfg.no_latency)
				{
					lat.push_back(end - begin);
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

	Detached RunAsyncDiskReadLane(AsyncDB& db, StartGate& gate, DoneState& done, const Config& cfg,
	    const std::vector<std::vector<std::string>>& keys, int client_id, int lane_id, int num_lanes, std::vector<uint64_t>& lat,
	    std::atomic<std::size_t>& global_inflight, std::atomic<std::size_t>& global_max_inflight, std::atomic<std::size_t>& client_inflight,
	    std::atomic<std::size_t>& client_max_inflight)
	{
		try
		{
			co_await gate;

			const std::size_t ops_per_lane = cfg.ops_per_client / static_cast<std::size_t>(num_lanes);
			const std::size_t start_idx = static_cast<std::size_t>(lane_id) * ops_per_lane;
			const std::size_t end_idx = (lane_id == num_lanes - 1) ? cfg.ops_per_client : start_idx + ops_per_lane;

			if (!cfg.no_latency)
			{
				lat.reserve(end_idx - start_idx);
			}

			for (std::size_t i = start_idx; i < end_idx; ++i)
			{
				const uint64_t begin = NowNs();

				const std::size_t cur_global = global_inflight.fetch_add(1, std::memory_order_relaxed) + 1;
				std::size_t observed_global = global_max_inflight.load(std::memory_order_relaxed);
				while (cur_global > observed_global)
				{
					if (global_max_inflight.compare_exchange_weak(observed_global, cur_global, std::memory_order_relaxed))
					{
						break;
					}
				}

				const std::size_t cur_client = client_inflight.fetch_add(1, std::memory_order_relaxed) + 1;
				std::size_t observed_client = client_max_inflight.load(std::memory_order_relaxed);
				while (cur_client > observed_client)
				{
					if (client_max_inflight.compare_exchange_weak(observed_client, cur_client, std::memory_order_relaxed))
					{
						break;
					}
				}

				(void)co_await db.GetAsync(ReadOptions(), keys[static_cast<std::size_t>(client_id)][i]);

				global_inflight.fetch_sub(1, std::memory_order_relaxed);
				client_inflight.fetch_sub(1, std::memory_order_relaxed);

				const uint64_t end = NowNs();
				if (!cfg.no_latency)
				{
					lat.push_back(end - begin);
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

	Stats RunAsyncMixed(AsyncDB& db, ThreadPoolScheduler& scheduler, const Config& cfg, const std::vector<std::vector<std::string>>& keys)
	{
		Stats out;
		StartGate gate;

		const int effective_inflight = (cfg.inflight_per_client < 1) ? 1 : cfg.inflight_per_client;
		const int total_lanes = cfg.clients * effective_inflight;
		DoneState done(total_lanes);

		std::vector<std::vector<uint64_t>> lat;
		lat.resize(static_cast<std::size_t>(total_lanes));
		const std::string value = MakeValue(cfg.value_size);

		std::atomic<std::size_t> global_inflight{ 0 };
		std::atomic<std::size_t> global_max_inflight{ 0 };
		auto client_inflight = std::make_unique<std::atomic<std::size_t>[]>(static_cast<std::size_t>(cfg.clients));
		auto client_max_inflight = std::make_unique<std::atomic<std::size_t>[]>(static_cast<std::size_t>(cfg.clients));
		for (int c = 0; c < cfg.clients; ++c)
		{
			client_inflight[static_cast<std::size_t>(c)].store(0, std::memory_order_relaxed);
			client_max_inflight[static_cast<std::size_t>(c)].store(0, std::memory_order_relaxed);
		}

		int lane_idx = 0;
		for (int c = 0; c < cfg.clients; ++c)
		{
			for (int l = 0; l < effective_inflight; ++l)
			{
				RunAsyncMixedLane(db, gate, done, cfg, keys, c, l, effective_inflight, value, lat[static_cast<std::size_t>(lane_idx)],
				    global_inflight, global_max_inflight, client_inflight[static_cast<std::size_t>(c)],
				    client_max_inflight[static_cast<std::size_t>(c)]);
				++lane_idx;
			}
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
		out.max_inflight_observed = global_max_inflight.load(std::memory_order_relaxed);

		std::size_t max_client_inflight = 0;
		for (int c = 0; c < cfg.clients; ++c)
		{
			const std::size_t client_max = client_max_inflight[static_cast<std::size_t>(c)].load(std::memory_order_relaxed);
			if (client_max > max_client_inflight)
			{
				max_client_inflight = client_max;
			}
		}
		out.max_client_inflight = max_client_inflight;

		out.write_sync = 0;
		if (!cfg.no_latency)
		{
			out.latency_ns.reserve(static_cast<std::size_t>(cfg.clients) * cfg.ops_per_client);
			for (auto& v : lat)
			{
				out.latency_ns.insert(out.latency_ns.end(), v.begin(), v.end());
			}
		}
		return out;
	}

	Stats RunAsyncDiskRead(
	    AsyncDB& db, ThreadPoolScheduler& scheduler, const Config& cfg, const std::vector<std::vector<std::string>>& keys)
	{
		Stats out;
		StartGate gate;

		const int effective_inflight = (cfg.inflight_per_client < 1) ? 1 : cfg.inflight_per_client;
		const int total_lanes = cfg.clients * effective_inflight;
		DoneState done(total_lanes);

		std::vector<std::vector<uint64_t>> lat;
		lat.resize(static_cast<std::size_t>(total_lanes));

		std::atomic<std::size_t> global_inflight{ 0 };
		std::atomic<std::size_t> global_max_inflight{ 0 };
		auto client_inflight = std::make_unique<std::atomic<std::size_t>[]>(static_cast<std::size_t>(cfg.clients));
		auto client_max_inflight = std::make_unique<std::atomic<std::size_t>[]>(static_cast<std::size_t>(cfg.clients));
		for (int c = 0; c < cfg.clients; ++c)
		{
			client_inflight[static_cast<std::size_t>(c)].store(0, std::memory_order_relaxed);
			client_max_inflight[static_cast<std::size_t>(c)].store(0, std::memory_order_relaxed);
		}

		int lane_idx = 0;
		for (int c = 0; c < cfg.clients; ++c)
		{
			for (int l = 0; l < effective_inflight; ++l)
			{
				RunAsyncDiskReadLane(db, gate, done, cfg, keys, c, l, effective_inflight, lat[static_cast<std::size_t>(lane_idx)],
				    global_inflight, global_max_inflight, client_inflight[static_cast<std::size_t>(c)],
				    client_max_inflight[static_cast<std::size_t>(c)]);
				++lane_idx;
			}
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
		out.max_inflight_observed = global_max_inflight.load(std::memory_order_relaxed);

		std::size_t max_client_inflight = 0;
		for (int c = 0; c < cfg.clients; ++c)
		{
			const std::size_t client_max = client_max_inflight[static_cast<std::size_t>(c)].load(std::memory_order_relaxed);
			if (client_max > max_client_inflight)
			{
				max_client_inflight = client_max;
			}
		}
		out.max_client_inflight = max_client_inflight;

		out.write_sync = 0;
		if (!cfg.no_latency)
		{
			out.latency_ns.reserve(static_cast<std::size_t>(cfg.clients) * cfg.ops_per_client);
			for (auto& v : lat)
			{
				out.latency_ns.insert(out.latency_ns.end(), v.begin(), v.end());
			}
		}
		return out;
	}

	Detached RunAsyncSstReadPipelineLane(AsyncDB& db, StartGate& gate, DoneState& done, const Config& cfg,
	    const std::vector<std::vector<std::string>>& keys, int client_id, int lane_id, int num_lanes, std::vector<uint64_t>& lat,
	    std::atomic<std::size_t>& global_inflight, std::atomic<std::size_t>& global_max_inflight, std::atomic<std::size_t>& client_inflight,
	    std::atomic<std::size_t>& client_max_inflight)
	{
		try
		{
			co_await gate;

			const std::size_t ops_per_lane = cfg.ops_per_client / static_cast<std::size_t>(num_lanes);
			const std::size_t start_idx = static_cast<std::size_t>(lane_id) * ops_per_lane;
			const std::size_t end_idx = (lane_id == num_lanes - 1) ? cfg.ops_per_client : start_idx + ops_per_lane;

			if (!cfg.no_latency)
			{
				lat.reserve(end_idx - start_idx);
			}

			// Use fill_cache=false for SST read pipeline benchmark
			ReadOptions read_opts;
			read_opts.fill_cache = false;

			for (std::size_t i = start_idx; i < end_idx; ++i)
			{
				const uint64_t begin = NowNs();

				const std::size_t cur_global = global_inflight.fetch_add(1, std::memory_order_relaxed) + 1;
				std::size_t observed_global = global_max_inflight.load(std::memory_order_relaxed);
				while (cur_global > observed_global)
				{
					if (global_max_inflight.compare_exchange_weak(observed_global, cur_global, std::memory_order_relaxed))
					{
						break;
					}
				}

				const std::size_t cur_client = client_inflight.fetch_add(1, std::memory_order_relaxed) + 1;
				std::size_t observed_client = client_max_inflight.load(std::memory_order_relaxed);
				while (cur_client > observed_client)
				{
					if (client_max_inflight.compare_exchange_weak(observed_client, cur_client, std::memory_order_relaxed))
					{
						break;
					}
				}

				(void)co_await db.GetAsync(read_opts, keys[static_cast<std::size_t>(client_id)][i]);

				global_inflight.fetch_sub(1, std::memory_order_relaxed);
				client_inflight.fetch_sub(1, std::memory_order_relaxed);

				const uint64_t end = NowNs();
				if (!cfg.no_latency)
				{
					lat.push_back(end - begin);
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

	Stats RunAsyncSstReadPipeline(
	    AsyncDB& db, ThreadPoolScheduler& scheduler, const Config& cfg, const std::vector<std::vector<std::string>>& keys)
	{
		Stats out;
		StartGate gate;

		const int effective_inflight = (cfg.inflight_per_client < 1) ? 1 : cfg.inflight_per_client;
		const int total_lanes = cfg.clients * effective_inflight;
		DoneState done(total_lanes);

		std::vector<std::vector<uint64_t>> lat;
		lat.resize(static_cast<std::size_t>(total_lanes));

		std::atomic<std::size_t> global_inflight{ 0 };
		std::atomic<std::size_t> global_max_inflight{ 0 };
		auto client_inflight = std::make_unique<std::atomic<std::size_t>[]>(static_cast<std::size_t>(cfg.clients));
		auto client_max_inflight = std::make_unique<std::atomic<std::size_t>[]>(static_cast<std::size_t>(cfg.clients));
		for (int c = 0; c < cfg.clients; ++c)
		{
			client_inflight[static_cast<std::size_t>(c)].store(0, std::memory_order_relaxed);
			client_max_inflight[static_cast<std::size_t>(c)].store(0, std::memory_order_relaxed);
		}

		int lane_idx = 0;
		for (int c = 0; c < cfg.clients; ++c)
		{
			for (int l = 0; l < effective_inflight; ++l)
			{
				RunAsyncSstReadPipelineLane(db, gate, done, cfg, keys, c, l, effective_inflight, lat[static_cast<std::size_t>(lane_idx)],
				    global_inflight, global_max_inflight, client_inflight[static_cast<std::size_t>(c)],
				    client_max_inflight[static_cast<std::size_t>(c)]);
				++lane_idx;
			}
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
		out.max_inflight_observed = global_max_inflight.load(std::memory_order_relaxed);

		std::size_t max_client_inflight = 0;
		for (int c = 0; c < cfg.clients; ++c)
		{
			const std::size_t client_max = client_max_inflight[static_cast<std::size_t>(c)].load(std::memory_order_relaxed);
			if (client_max > max_client_inflight)
			{
				max_client_inflight = client_max;
			}
		}
		out.max_client_inflight = max_client_inflight;

		out.write_sync = 0;
		if (!cfg.no_latency)
		{
			out.latency_ns.reserve(static_cast<std::size_t>(cfg.clients) * cfg.ops_per_client);
			for (auto& v : lat)
			{
				out.latency_ns.insert(out.latency_ns.end(), v.begin(), v.end());
			}
		}
		return out;
	}

	Detached RunAsyncCompactionOverlapReaderLane(AsyncDB& db, StartGate& gate, DoneState& done, const Config& cfg,
	    const std::vector<std::vector<std::string>>& keys, int client_id, int lane_id, int num_lanes, std::vector<uint64_t>& lat,
	    std::atomic<std::size_t>& global_inflight, std::atomic<std::size_t>& global_max_inflight, std::atomic<std::size_t>& client_inflight,
	    std::atomic<std::size_t>& client_max_inflight)
	{
		try
		{
			co_await gate;

			const std::size_t ops_per_lane = cfg.ops_per_client / static_cast<std::size_t>(num_lanes);
			const std::size_t start_idx = static_cast<std::size_t>(lane_id) * ops_per_lane;
			const std::size_t end_idx = (lane_id == num_lanes - 1) ? cfg.ops_per_client : start_idx + ops_per_lane;

			if (!cfg.no_latency)
			{
				lat.reserve(end_idx - start_idx);
			}

			ReadOptions read_opts;
			read_opts.fill_cache = false;

			for (std::size_t i = start_idx; i < end_idx; ++i)
			{
				const uint64_t begin = NowNs();

				const std::size_t cur_global = global_inflight.fetch_add(1, std::memory_order_relaxed) + 1;
				std::size_t observed_global = global_max_inflight.load(std::memory_order_relaxed);
				while (cur_global > observed_global)
				{
					if (global_max_inflight.compare_exchange_weak(observed_global, cur_global, std::memory_order_relaxed))
					{
						break;
					}
				}

				const std::size_t cur_client = client_inflight.fetch_add(1, std::memory_order_relaxed) + 1;
				std::size_t observed_client = client_max_inflight.load(std::memory_order_relaxed);
				while (cur_client > observed_client)
				{
					if (client_max_inflight.compare_exchange_weak(observed_client, cur_client, std::memory_order_relaxed))
					{
						break;
					}
				}

				(void)co_await db.GetAsync(read_opts, keys[static_cast<std::size_t>(client_id)][i]);

				global_inflight.fetch_sub(1, std::memory_order_relaxed);
				client_inflight.fetch_sub(1, std::memory_order_relaxed);

				const uint64_t end = NowNs();
				if (!cfg.no_latency)
				{
					lat.push_back(end - begin);
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

	Detached RunAsyncCompactionOverlapWriterLane(AsyncDB& db, StartGate& gate, DoneState& done, const Config& cfg, int client_id,
	    int lane_id, int num_lanes, std::string value, std::vector<uint64_t>& lat, std::atomic<std::size_t>& global_inflight,
	    std::atomic<std::size_t>& global_max_inflight, std::atomic<std::size_t>& client_inflight,
	    std::atomic<std::size_t>& client_max_inflight)
	{
		try
		{
			co_await gate;

			const std::size_t ops_per_lane = cfg.ops_per_client / static_cast<std::size_t>(num_lanes);
			const std::size_t start_idx = static_cast<std::size_t>(lane_id) * ops_per_lane;
			const std::size_t end_idx = (lane_id == num_lanes - 1) ? cfg.ops_per_client : start_idx + ops_per_lane;

			if (!cfg.no_latency)
			{
				lat.reserve(end_idx - start_idx);
			}

			WriteOptions write_opts;
			write_opts.sync = false;

			for (std::size_t i = start_idx; i < end_idx; ++i)
			{
				const uint64_t begin = NowNs();

				const std::size_t cur_global = global_inflight.fetch_add(1, std::memory_order_relaxed) + 1;
				std::size_t observed_global = global_max_inflight.load(std::memory_order_relaxed);
				while (cur_global > observed_global)
				{
					if (global_max_inflight.compare_exchange_weak(observed_global, cur_global, std::memory_order_relaxed))
					{
						break;
					}
				}

				const std::size_t cur_client = client_inflight.fetch_add(1, std::memory_order_relaxed) + 1;
				std::size_t observed_client = client_max_inflight.load(std::memory_order_relaxed);
				while (cur_client > observed_client)
				{
					if (client_max_inflight.compare_exchange_weak(observed_client, cur_client, std::memory_order_relaxed))
					{
						break;
					}
				}

				char key_buf[64];
				const int n = std::snprintf(key_buf, sizeof(key_buf), "cw_%d_%zu", client_id, i);
				const std::string key(key_buf, static_cast<std::size_t>(n));

				(void)co_await db.PutAsync(write_opts, key, value);

				global_inflight.fetch_sub(1, std::memory_order_relaxed);
				client_inflight.fetch_sub(1, std::memory_order_relaxed);

				const uint64_t end = NowNs();
				if (!cfg.no_latency)
				{
					lat.push_back(end - begin);
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

	Stats RunAsyncCompactionOverlap(
	    AsyncDB& db, ThreadPoolScheduler& scheduler, const Config& cfg, const std::vector<std::vector<std::string>>& keys)
	{
		Stats out;
		StartGate gate;

		const int effective_inflight = (cfg.inflight_per_client < 1) ? 1 : cfg.inflight_per_client;
		const int half_clients = cfg.clients / 2;
		const int num_readers = (half_clients < 1) ? 1 : half_clients;
		const int num_writers = cfg.clients - num_readers;
		const int total_reader_lanes = num_readers * effective_inflight;
		const int total_writer_lanes = num_writers * effective_inflight;
		const int total_lanes = total_reader_lanes + total_writer_lanes;
		DoneState done(total_lanes);

		std::vector<std::vector<uint64_t>> lat;
		lat.resize(static_cast<std::size_t>(total_lanes));
		const std::string value = MakeValue(cfg.value_size);

		std::atomic<std::size_t> global_inflight{ 0 };
		std::atomic<std::size_t> global_max_inflight{ 0 };
		auto client_inflight = std::make_unique<std::atomic<std::size_t>[]>(static_cast<std::size_t>(cfg.clients));
		auto client_max_inflight = std::make_unique<std::atomic<std::size_t>[]>(static_cast<std::size_t>(cfg.clients));
		for (int c = 0; c < cfg.clients; ++c)
		{
			client_inflight[static_cast<std::size_t>(c)].store(0, std::memory_order_relaxed);
			client_max_inflight[static_cast<std::size_t>(c)].store(0, std::memory_order_relaxed);
		}

		int lane_idx = 0;
		for (int c = 0; c < num_readers; ++c)
		{
			for (int l = 0; l < effective_inflight; ++l)
			{
				RunAsyncCompactionOverlapReaderLane(db, gate, done, cfg, keys, c, l, effective_inflight,
				    lat[static_cast<std::size_t>(lane_idx)], global_inflight, global_max_inflight,
				    client_inflight[static_cast<std::size_t>(c)], client_max_inflight[static_cast<std::size_t>(c)]);
				++lane_idx;
			}
		}

		for (int c = num_readers; c < cfg.clients; ++c)
		{
			for (int l = 0; l < effective_inflight; ++l)
			{
				RunAsyncCompactionOverlapWriterLane(db, gate, done, cfg, c, l, effective_inflight, value,
				    lat[static_cast<std::size_t>(lane_idx)], global_inflight, global_max_inflight,
				    client_inflight[static_cast<std::size_t>(c)], client_max_inflight[static_cast<std::size_t>(c)]);
				++lane_idx;
			}
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
		out.max_inflight_observed = global_max_inflight.load(std::memory_order_relaxed);

		std::size_t max_client_inflight = 0;
		for (int c = 0; c < cfg.clients; ++c)
		{
			const std::size_t client_max = client_max_inflight[static_cast<std::size_t>(c)].load(std::memory_order_relaxed);
			if (client_max > max_client_inflight)
			{
				max_client_inflight = client_max;
			}
		}
		out.max_client_inflight = max_client_inflight;

		out.write_sync = 0;
		if (!cfg.no_latency)
		{
			out.latency_ns.reserve(static_cast<std::size_t>(cfg.clients) * cfg.ops_per_client);
			for (auto& v : lat)
			{
				out.latency_ns.insert(out.latency_ns.end(), v.begin(), v.end());
			}
		}
		return out;
	}

	Detached RunAsyncDurabilityWriteLane(AsyncDB& db, StartGate& gate, DoneState& done, const Config& cfg,
	    const std::vector<std::vector<std::string>>& keys, int client_id, int lane_id, int num_lanes, std::string value,
	    std::vector<uint64_t>& lat, std::atomic<std::size_t>& global_inflight, std::atomic<std::size_t>& global_max_inflight,
	    std::atomic<std::size_t>& client_inflight, std::atomic<std::size_t>& client_max_inflight)
	{
		try
		{
			co_await gate;

			const std::size_t ops_per_lane = cfg.ops_per_client / static_cast<std::size_t>(num_lanes);
			const std::size_t start_idx = static_cast<std::size_t>(lane_id) * ops_per_lane;
			const std::size_t end_idx = (lane_id == num_lanes - 1) ? cfg.ops_per_client : start_idx + ops_per_lane;

			if (!cfg.no_latency)
			{
				lat.reserve(end_idx - start_idx);
			}

			WriteOptions write_opts;
			write_opts.sync = true;

			for (std::size_t i = start_idx; i < end_idx; ++i)
			{
				const uint64_t begin = NowNs();

				const std::size_t cur_global = global_inflight.fetch_add(1, std::memory_order_relaxed) + 1;
				std::size_t observed_global = global_max_inflight.load(std::memory_order_relaxed);
				while (cur_global > observed_global)
				{
					if (global_max_inflight.compare_exchange_weak(observed_global, cur_global, std::memory_order_relaxed))
					{
						break;
					}
				}

				const std::size_t cur_client = client_inflight.fetch_add(1, std::memory_order_relaxed) + 1;
				std::size_t observed_client = client_max_inflight.load(std::memory_order_relaxed);
				while (cur_client > observed_client)
				{
					if (client_max_inflight.compare_exchange_weak(observed_client, cur_client, std::memory_order_relaxed))
					{
						break;
					}
				}

				(void)co_await db.PutAsync(write_opts, keys[static_cast<std::size_t>(client_id)][i], value);

				global_inflight.fetch_sub(1, std::memory_order_relaxed);
				client_inflight.fetch_sub(1, std::memory_order_relaxed);

				const uint64_t end = NowNs();
				if (!cfg.no_latency)
				{
					lat.push_back(end - begin);
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

	Stats RunAsyncDurabilityWrite(
	    AsyncDB& db, ThreadPoolScheduler& scheduler, const Config& cfg, const std::vector<std::vector<std::string>>& keys)
	{
		Stats out;
		StartGate gate;

		const int effective_inflight = (cfg.inflight_per_client < 1) ? 1 : cfg.inflight_per_client;
		const int total_lanes = cfg.clients * effective_inflight;
		DoneState done(total_lanes);

		std::vector<std::vector<uint64_t>> lat;
		lat.resize(static_cast<std::size_t>(total_lanes));
		const std::string value = MakeValue(cfg.value_size);

		std::atomic<std::size_t> global_inflight{ 0 };
		std::atomic<std::size_t> global_max_inflight{ 0 };
		auto client_inflight = std::make_unique<std::atomic<std::size_t>[]>(static_cast<std::size_t>(cfg.clients));
		auto client_max_inflight = std::make_unique<std::atomic<std::size_t>[]>(static_cast<std::size_t>(cfg.clients));
		for (int c = 0; c < cfg.clients; ++c)
		{
			client_inflight[static_cast<std::size_t>(c)].store(0, std::memory_order_relaxed);
			client_max_inflight[static_cast<std::size_t>(c)].store(0, std::memory_order_relaxed);
		}

		int lane_idx = 0;
		for (int c = 0; c < cfg.clients; ++c)
		{
			for (int l = 0; l < effective_inflight; ++l)
			{
				RunAsyncDurabilityWriteLane(db, gate, done, cfg, keys, c, l, effective_inflight, value,
				    lat[static_cast<std::size_t>(lane_idx)], global_inflight, global_max_inflight,
				    client_inflight[static_cast<std::size_t>(c)], client_max_inflight[static_cast<std::size_t>(c)]);
				++lane_idx;
			}
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
		out.max_inflight_observed = global_max_inflight.load(std::memory_order_relaxed);

		std::size_t max_client_inflight = 0;
		for (int c = 0; c < cfg.clients; ++c)
		{
			const std::size_t client_max = client_max_inflight[static_cast<std::size_t>(c)].load(std::memory_order_relaxed);
			if (client_max > max_client_inflight)
			{
				max_client_inflight = client_max;
			}
		}
		out.max_client_inflight = max_client_inflight;

		out.write_sync = 1;
		if (!cfg.no_latency)
		{
			out.latency_ns.reserve(static_cast<std::size_t>(cfg.clients) * cfg.ops_per_client);
			for (auto& v : lat)
			{
				out.latency_ns.insert(out.latency_ns.end(), v.begin(), v.end());
			}
		}
		return out;
	}

	void PrintLine(std::string_view name, const Config& cfg, int round, const Stats& stats, std::size_t max_inflight)
	{
		const double total_ops = static_cast<double>(cfg.clients) * static_cast<double>(cfg.ops_per_client);
		const double ops_per_sec = total_ops / stats.seconds;
		const std::string scenario = BenchName(cfg.mode);

		// Determine if read_ratio is meaningful for this benchmark mode
		const bool read_ratio_meaningful = (cfg.mode == BenchMode::kMixed || cfg.mode == BenchMode::kDiskRead);

		if (cfg.no_latency)
		{
			if (read_ratio_meaningful)
			{
				std::printf("%s r=%d clients=%d workers=%d ops=%zu value=%zu read_ratio=%d time=%.3fs ops/s=%.0f "
				            "scenario=%s inflight_cfg=%d max_inflight=%zu max_client_inflight=%zu write_sync=%d "
				            "bg_scheduled=%d bg_sleeps=%d\n",
				    std::string(name).c_str(), round, cfg.clients, cfg.workers, cfg.ops_per_client, cfg.value_size, cfg.read_ratio,
				    stats.seconds, ops_per_sec, scenario.c_str(), cfg.inflight_per_client, max_inflight, stats.max_client_inflight,
				    stats.write_sync, stats.bg_scheduled, stats.bg_sleeps);
			}
			else
			{
				std::printf("%s r=%d clients=%d workers=%d ops=%zu value=%zu read_ratio=ignored time=%.3fs ops/s=%.0f "
				            "scenario=%s inflight_cfg=%d max_inflight=%zu max_client_inflight=%zu write_sync=%d "
				            "bg_scheduled=%d bg_sleeps=%d\n",
				    std::string(name).c_str(), round, cfg.clients, cfg.workers, cfg.ops_per_client, cfg.value_size, stats.seconds,
				    ops_per_sec, scenario.c_str(), cfg.inflight_per_client, max_inflight, stats.max_client_inflight, stats.write_sync,
				    stats.bg_scheduled, stats.bg_sleeps);
			}
		}
		else
		{
			const uint64_t p50_ns = PercentileNs(stats.latency_ns, 0.50);
			const uint64_t p95_ns = PercentileNs(stats.latency_ns, 0.95);
			if (read_ratio_meaningful)
			{
				std::printf("%s r=%d clients=%d workers=%d ops=%zu value=%zu read_ratio=%d time=%.3fs ops/s=%.0f max_inflight=%zu "
				            "p50_us=%.2f p95_us=%.2f scenario=%s inflight_cfg=%d max_client_inflight=%zu write_sync=%d "
				            "bg_scheduled=%d bg_sleeps=%d\n",
				    std::string(name).c_str(), round, cfg.clients, cfg.workers, cfg.ops_per_client, cfg.value_size, cfg.read_ratio,
				    stats.seconds, ops_per_sec, max_inflight, static_cast<double>(p50_ns) / 1000.0, static_cast<double>(p95_ns) / 1000.0,
				    scenario.c_str(), cfg.inflight_per_client, stats.max_client_inflight, stats.write_sync, stats.bg_scheduled,
				    stats.bg_sleeps);
			}
			else
			{
				std::printf("%s r=%d clients=%d workers=%d ops=%zu value=%zu read_ratio=ignored time=%.3fs ops/s=%.0f max_inflight=%zu "
				            "p50_us=%.2f p95_us=%.2f scenario=%s inflight_cfg=%d max_client_inflight=%zu write_sync=%d "
				            "bg_scheduled=%d bg_sleeps=%d\n",
				    std::string(name).c_str(), round, cfg.clients, cfg.workers, cfg.ops_per_client, cfg.value_size, stats.seconds,
				    ops_per_sec, max_inflight, static_cast<double>(p50_ns) / 1000.0, static_cast<double>(p95_ns) / 1000.0, scenario.c_str(),
				    cfg.inflight_per_client, stats.max_client_inflight, stats.write_sync, stats.bg_scheduled, stats.bg_sleeps);
			}
		}
	}

	std::string RunName(const Config& cfg)
	{
		if (cfg.do_sync && cfg.do_async)
			return "both";
		if (cfg.do_sync)
			return "sync";
		return "async";
	}

	std::string BenchName(BenchMode m)
	{
		switch (m)
		{
		case BenchMode::kMixed:
			return "mixed";
		case BenchMode::kDiskRead:
			return "disk_read";
		case BenchMode::kSstReadPipeline:
			return "sst_read_pipeline";
		case BenchMode::kDurabilityWrite:
			return "durability_write";
		case BenchMode::kCompactionOverlap:
			return "compaction_overlap";
		}
		return "unknown";
	}

	Config ParseArgs(int argc, char** argv)
	{
		Config cfg;
		for (int i = 1; i < argc; ++i)
		{
			std::string_view arg(argv[i]);

			if (arg == "--help")
			{
				std::printf("Usage: kv_bench [OPTIONS]\n");
				std::printf("Options:\n");
				std::printf("  --run=<sync|async|both>      Set run mode (default: both)\n");
				std::printf("  --bench=<mode>               Set benchmark mode (default: mixed)\n");
				std::printf("                               Modes: mixed, disk_read, sst_read_pipeline,\n");
				std::printf("                                      durability_write, compaction_overlap\n");
				std::printf("  --clients=<n>                Number of concurrent clients (default: 4)\n");
				std::printf("  --workers=<n>                Number of worker threads (default: 4)\n");
				std::printf("  --ops=<n>                    Operations per client (default: 10000)\n");
				std::printf("  --value_size=<n>             Value size in bytes (default: 100)\n");
				std::printf("  --read_ratio=<n>             Read ratio 0-100 (default: 0)\n");
				std::printf("  --rounds=<n>                 Number of rounds (default: 3)\n");
				std::printf("  --write_buffer_size=<n>      Write buffer size (default: 4MB)\n");
				std::printf("  --sync                       Run only sync benchmark\n");
				std::printf("  --async                      Run only async benchmark\n");
				std::printf("  --inflight_per_client=<n>    Inflight ops per client (default: 1)\n");
				std::printf("  --warmup_rounds=<n>          Warmup rounds before measurement (default: 0)\n");
				std::printf("  --no_latency                 Skip p50/p95 latency collection (default: off)\n");
				std::printf("  --prefill=<-1|0|1>           Prefill: -1=auto, 0=off, 1=force (default: -1)\n");
				std::printf("  --db_dir=<path>              Use existing dir for async DB (default: temp)\n");
				std::printf("  --keep_db=<0|1>              Keep DB dir after run (default: 0)\n");
				std::printf("  --help                       Show this message\n");
				exit(0);
			}

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
			parse_int("--inflight_per_client=", cfg.inflight_per_client);
			parse_int("--warmup_rounds=", cfg.warmup_rounds);
			parse_int("--prefill=", cfg.prefill);

			if (arg == "--no_latency")
			{
				cfg.no_latency = true;
			}

			if (arg.starts_with("--keep_db="))
			{
				int tmp = 0;
				parse_int("--keep_db=", tmp);
				cfg.keep_db = (tmp != 0);
			}

			if (arg.starts_with("--db_dir="))
			{
				cfg.db_dir = std::string(arg.substr(9));
			}

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

			if (arg.starts_with("--run="))
			{
				std::string_view run_val = arg.substr(6);
				if (run_val == "sync")
				{
					cfg.do_sync = true;
					cfg.do_async = false;
				}
				else if (run_val == "async")
				{
					cfg.do_sync = false;
					cfg.do_async = true;
				}
				else if (run_val == "both")
				{
					cfg.do_sync = true;
					cfg.do_async = true;
				}
				else
				{
					std::fprintf(stderr, "invalid --run value: %s; allowed: sync|async|both\n", std::string(run_val).c_str());
					exit(1);
				}
			}

			bool bench_flag_handled = false;
			if (arg == "--bench=mixed")
			{
				cfg.mode = BenchMode::kMixed;
				bench_flag_handled = true;
			}
			if (arg == "--bench=disk_read")
			{
				cfg.mode = BenchMode::kDiskRead;
				bench_flag_handled = true;
			}
			if (arg == "--bench=sst_read_pipeline")
			{
				cfg.mode = BenchMode::kSstReadPipeline;
				bench_flag_handled = true;
			}
			if (arg == "--bench=durability_write")
			{
				cfg.mode = BenchMode::kDurabilityWrite;
				bench_flag_handled = true;
			}
			if (arg == "--bench=compaction_overlap")
			{
				cfg.mode = BenchMode::kCompactionOverlap;
				bench_flag_handled = true;
			}

			if (arg.starts_with("--bench=") && !bench_flag_handled)
			{
				std::string_view bench_val = arg.substr(8);
				std::fprintf(stderr,
				    "unknown --bench value: %s; allowed: mixed|disk_read|sst_read_pipeline|"
				    "durability_write|compaction_overlap\n",
				    std::string(bench_val).c_str());
				exit(1);
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
		if (cfg.inflight_per_client < 1)
		{
			cfg.inflight_per_client = 1;
		}
		if (cfg.warmup_rounds < 0)
		{
			cfg.warmup_rounds = 0;
		}
		if (cfg.prefill < -1 || cfg.prefill > 1)
		{
			cfg.prefill = -1;
		}

		// sst_read_pipeline is async-only
		if (cfg.mode == BenchMode::kSstReadPipeline && cfg.do_sync && !cfg.do_async)
		{
			std::fprintf(stderr, "error: sst_read_pipeline benchmark is async-only. Use --async instead of --sync.\n");
			exit(1);
		}

		return cfg;
	}

} // namespace prism::bench