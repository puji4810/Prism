#ifndef PRISM_BENCH_KV_BENCH_LIB_H_
#define PRISM_BENCH_KV_BENCH_LIB_H_

#include <atomic>
#include <chrono>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <mutex>
#include <semaphore>
#include <string>
#include <string_view>
#include <vector>

namespace prism
{
	class DB;
	class AsyncDB;
	class ThreadPoolScheduler;
	struct ReadOptions;
	struct WriteOptions;
}

namespace prism::bench
{
	using Clock = std::chrono::steady_clock;

	// Time utilities
	uint64_t NowNs();

	// Temp directory creation
	std::string MakeTempDir(std::string_view tag);

	// Value/Key generation
	std::string MakeValue(std::size_t n);
	std::string MakeKey(int worker, std::size_t i);
	std::vector<std::vector<std::string>> MakeKeys(int clients, std::size_t ops_per_client);

	// Coroutine helper structs
	struct StartGate
	{
		struct Awaiter
		{
			StartGate* gate;

			bool await_ready() const noexcept;
			bool await_suspend(std::coroutine_handle<> handle);
			void await_resume() const noexcept { }
		};

		Awaiter operator co_await() noexcept;

		void Open(ThreadPoolScheduler& scheduler);

		std::mutex mutex;
		std::vector<std::coroutine_handle<>> waiters;
		std::atomic<bool> open{ false };
	};

	struct DoneState
	{
		explicit DoneState(int count);

		void NotifyDone();

		std::binary_semaphore done{ 0 };
		std::atomic<int> remaining;
		std::exception_ptr exception;
	};

	struct Detached
	{
		struct promise_type
		{
			Detached get_return_object() noexcept;
			std::suspend_never initial_suspend() noexcept;
			std::suspend_never final_suspend() noexcept;
			void return_void() noexcept { }
			void unhandled_exception() noexcept;
		};
	};

	// Benchmark modes
	enum class BenchMode
	{
		kMixed,
		kDiskRead,
		kSstReadPipeline,
		kDurabilityWrite,
		kCompactionOverlap
	};

	// Configuration
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
		int inflight_per_client = 1;
		int warmup_rounds = 0;
		bool no_latency = false;
		int prefill = -1; // -1=auto, 0=off, 1=force
		std::string db_dir = "";
		bool keep_db = false;
		int max_client_inflight = 0; // populated by runners
	};

	// Statistics
	struct Stats
	{
		double seconds = 0;
		std::vector<uint64_t> latency_ns;
		std::size_t max_inflight_observed = 0;
		std::size_t max_client_inflight = 0;
		int write_sync = 0;
		int bg_scheduled = 0;
		int bg_sleeps = 0;
	};

	// Percentile calculation
	uint64_t PercentileNs(std::vector<uint64_t> v, double p);

	// Prefill helper
	void Prefill(DB& db, const std::vector<std::vector<std::string>>& keys, std::size_t ops_per_client, std::size_t value_size);

	// Sync benchmark runners
	Stats RunSyncMixed(DB& db, const Config& cfg, const std::vector<std::vector<std::string>>& keys);
	Stats RunSyncDiskRead(DB& db, const Config& cfg, const std::vector<std::vector<std::string>>& keys);
	Stats RunSyncDurabilityWrite(DB& db, const Config& cfg, const std::vector<std::vector<std::string>>& keys);

	// Async benchmark lane workers (per-client lane with inflight tracking)
	Detached RunAsyncMixedLane(AsyncDB& db, StartGate& gate, DoneState& done, const Config& cfg,
	    const std::vector<std::vector<std::string>>& keys, int client_id, int lane_id, int num_lanes, std::string value,
	    std::vector<uint64_t>& lat, std::atomic<std::size_t>& global_inflight, std::atomic<std::size_t>& global_max_inflight,
	    std::atomic<std::size_t>& client_inflight, std::atomic<std::size_t>& client_max_inflight);

	Detached RunAsyncDiskReadLane(AsyncDB& db, StartGate& gate, DoneState& done, const Config& cfg,
	    const std::vector<std::vector<std::string>>& keys, int client_id, int lane_id, int num_lanes, std::vector<uint64_t>& lat,
	    std::atomic<std::size_t>& global_inflight, std::atomic<std::size_t>& global_max_inflight, std::atomic<std::size_t>& client_inflight,
	    std::atomic<std::size_t>& client_max_inflight);

	Detached RunAsyncSstReadPipelineLane(AsyncDB& db, StartGate& gate, DoneState& done, const Config& cfg,
	    const std::vector<std::vector<std::string>>& keys, int client_id, int lane_id, int num_lanes, std::vector<uint64_t>& lat,
	    std::atomic<std::size_t>& global_inflight, std::atomic<std::size_t>& global_max_inflight, std::atomic<std::size_t>& client_inflight,
	    std::atomic<std::size_t>& client_max_inflight);

	Detached RunAsyncDurabilityWriteLane(AsyncDB& db, StartGate& gate, DoneState& done, const Config& cfg,
	    const std::vector<std::vector<std::string>>& keys, int client_id, int lane_id, int num_lanes, std::string value,
	    std::vector<uint64_t>& lat, std::atomic<std::size_t>& global_inflight, std::atomic<std::size_t>& global_max_inflight,
	    std::atomic<std::size_t>& client_inflight, std::atomic<std::size_t>& client_max_inflight);

	// Compaction overlap benchmark lane workers
	Detached RunAsyncCompactionOverlapReaderLane(AsyncDB& db, StartGate& gate, DoneState& done, const Config& cfg,
	    const std::vector<std::vector<std::string>>& keys, int client_id, int lane_id, int num_lanes, std::vector<uint64_t>& lat,
	    std::atomic<std::size_t>& global_inflight, std::atomic<std::size_t>& global_max_inflight, std::atomic<std::size_t>& client_inflight,
	    std::atomic<std::size_t>& client_max_inflight);

	Detached RunAsyncCompactionOverlapWriterLane(AsyncDB& db, StartGate& gate, DoneState& done, const Config& cfg, int client_id,
	    int lane_id, int num_lanes, std::string value, std::vector<uint64_t>& lat, std::atomic<std::size_t>& global_inflight,
	    std::atomic<std::size_t>& global_max_inflight, std::atomic<std::size_t>& client_inflight,
	    std::atomic<std::size_t>& client_max_inflight);

	// Async benchmark runners
	Stats RunAsyncMixed(AsyncDB& db, ThreadPoolScheduler& scheduler, const Config& cfg, const std::vector<std::vector<std::string>>& keys);
	Stats RunAsyncDiskRead(
	    AsyncDB& db, ThreadPoolScheduler& scheduler, const Config& cfg, const std::vector<std::vector<std::string>>& keys);
	Stats RunAsyncSstReadPipeline(
	    AsyncDB& db, ThreadPoolScheduler& scheduler, const Config& cfg, const std::vector<std::vector<std::string>>& keys);
	Stats RunAsyncDurabilityWrite(
	    AsyncDB& db, ThreadPoolScheduler& scheduler, const Config& cfg, const std::vector<std::vector<std::string>>& keys);
	Stats RunAsyncCompactionOverlap(
	    AsyncDB& db, ThreadPoolScheduler& scheduler, const Config& cfg, const std::vector<std::vector<std::string>>& keys);

	// Output formatting
	void PrintLine(std::string_view name, const Config& cfg, int round, const Stats& stats, std::size_t max_inflight = 0);

	// Naming helpers
	std::string RunName(const Config& cfg);
	std::string BenchName(BenchMode m);

	// Argument parsing
	Config ParseArgs(int argc, char** argv);

} // namespace prism::bench

#endif // PRISM_BENCH_KV_BENCH_LIB_H_