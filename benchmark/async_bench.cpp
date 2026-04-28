#include "async_env.h"
#include "env.h"
#include "kv_bench_lib.h"
#include "runtime_executor.h"
#include "scheduler.h"
#include "slice.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <coroutine>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <random>
#include <semaphore>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace prism::bench
{
	namespace
	{
		constexpr std::size_t kWorkerCount = 4;
		constexpr std::size_t kFileCount = 10;
		constexpr std::size_t kRecordsPerFile = 100;
		constexpr std::size_t kRecordSize = 4096;
		constexpr std::uint64_t kWorkloadSeed = 0xC0FFEEULL;

		enum class Backend
		{
			kThreadPool,
			kBlockingLane,
		};

		struct BenchConfig
		{
			Backend backend = Backend::kThreadPool;
			std::string backend_name = "thread_pool";
			std::string workload = "random_read";
			std::size_t ops = 10000;
		};

		struct ReadRequest
		{
			std::size_t file_index;
			std::uint64_t offset;
		};

		template <typename T>
		class Task
		{
		public:
			struct promise_type
			{
				std::binary_semaphore done{ 0 };
				std::optional<T> value;
				std::exception_ptr exception;

				Task get_return_object() { return Task(std::coroutine_handle<promise_type>::from_promise(*this)); }
				std::suspend_never initial_suspend() noexcept { return {}; }
				auto final_suspend() noexcept
				{
					struct Awaiter
					{
						promise_type* promise;
						bool await_ready() noexcept { return false; }
						void await_suspend(std::coroutine_handle<>) noexcept { promise->done.release(); }
						void await_resume() noexcept { }
					};
					return Awaiter{ this };
				}
				void unhandled_exception() { exception = std::current_exception(); }
				void return_value(T v) { value = std::move(v); }
			};

			explicit Task(std::coroutine_handle<promise_type> handle)
			    : handle_(handle)
			{
			}

			Task(Task&& other) noexcept
			    : handle_(other.handle_)
			{
				other.handle_ = {};
			}

			Task(const Task&) = delete;
			Task& operator=(const Task&) = delete;

			~Task()
			{
				if (handle_)
				{
					handle_.destroy();
				}
			}

			T SyncWait()
			{
				handle_.promise().done.acquire();
				if (handle_.promise().exception)
				{
					std::rethrow_exception(handle_.promise().exception);
				}
				return std::move(*handle_.promise().value);
			}

		private:
			std::coroutine_handle<promise_type> handle_;
		};

		[[noreturn]] void Usage(const char* argv0, std::string_view error)
		{
			if (!error.empty())
			{
				std::fprintf(stderr, "error: %.*s\n", static_cast<int>(error.size()), error.data());
			}
			std::fprintf(stderr,
			    "usage: %s [--backend=thread_pool|blocking_lane] [--workload=random_read] [--ops=N]\n",
			    argv0);
			std::exit(1);
		}

		std::size_t ParseSize(std::string_view text, const char* argv0)
		{
			if (text.empty())
			{
				Usage(argv0, "missing numeric value");
			}

			char* end = nullptr;
			auto value = std::strtoull(text.data(), &end, 10);
			if (end != text.data() + text.size())
			{
				Usage(argv0, "invalid numeric value");
			}
			return static_cast<std::size_t>(value);
		}

		BenchConfig ParseBenchArgs(int argc, char** argv)
		{
			BenchConfig cfg;
			for (int i = 1; i < argc; ++i)
			{
				const std::string_view arg(argv[i]);
				if (arg.starts_with("--backend="))
				{
					const auto value = arg.substr(std::string_view("--backend=").size());
					if (value == "thread_pool")
					{
						cfg.backend = Backend::kThreadPool;
						cfg.backend_name = "thread_pool";
					}
					else if (value == "blocking_lane")
					{
						cfg.backend = Backend::kBlockingLane;
						cfg.backend_name = "blocking_lane";
					}
					else
					{
						Usage(argv[0], "unsupported backend");
					}
				}
				else if (arg.starts_with("--workload="))
				{
					cfg.workload = std::string(arg.substr(std::string_view("--workload=").size()));
					if (cfg.workload != "random_read")
					{
						Usage(argv[0], "unsupported workload");
					}
				}
				else if (arg.starts_with("--ops="))
				{
					cfg.ops = ParseSize(arg.substr(std::string_view("--ops=").size()), argv[0]);
				}
				else
				{
					Usage(argv[0], "unknown argument");
				}
			}
			return cfg;
		}

		AsyncEnvBackendMode ToBackendMode(Backend backend)
		{
			switch (backend)
			{
			case Backend::kThreadPool:
				return AsyncEnvBackendMode::kThreadPool;
			case Backend::kBlockingLane:
				return AsyncEnvBackendMode::kBlockingLane;
			}
			std::terminate();
		}

		std::vector<std::string> MakeFilePaths(const std::filesystem::path& dir)
		{
			std::vector<std::string> paths;
			paths.reserve(kFileCount);
			for (std::size_t i = 0; i < kFileCount; ++i)
			{
				paths.push_back((dir / ("bench_file_" + std::to_string(i) + ".dat")).string());
			}
			return paths;
		}

		std::string MakeRecord(std::size_t file_index, std::size_t record_index)
		{
			std::string record(kRecordSize, 'a' + static_cast<char>(file_index % 26));
			const auto header = "file=" + std::to_string(file_index) + " record=" + std::to_string(record_index) + "\n";
			std::copy(header.begin(), header.end(), record.begin());
			return record;
		}

		void CreateDataset(Env* env, const std::vector<std::string>& file_paths)
		{
			for (std::size_t file_index = 0; file_index < file_paths.size(); ++file_index)
			{
				auto writable_result = env->NewWritableFile(file_paths[file_index]);
				if (!writable_result.has_value())
				{
					throw std::runtime_error(writable_result.error().ToString());
				}

				auto writable = std::move(writable_result.value());
				for (std::size_t record_index = 0; record_index < kRecordsPerFile; ++record_index)
				{
					const auto record = MakeRecord(file_index, record_index);
					Status s = writable->Append(Slice(record));
					if (!s.ok())
					{
						throw std::runtime_error(s.ToString());
					}
				}
				Status s = writable->Close();
				if (!s.ok())
				{
					throw std::runtime_error(s.ToString());
				}
			}
		}

		std::vector<ReadRequest> BuildWorkload(std::size_t ops)
		{
			std::mt19937_64 rng(kWorkloadSeed);
			std::uniform_int_distribution<std::size_t> file_dist(0, kFileCount - 1);
			std::uniform_int_distribution<std::size_t> record_dist(0, kRecordsPerFile - 1);

			std::vector<ReadRequest> requests;
			requests.reserve(ops);
			for (std::size_t i = 0; i < ops; ++i)
			{
				const auto record_index = record_dist(rng);
				requests.push_back({ file_dist(rng), static_cast<std::uint64_t>(record_index * kRecordSize) });
			}
			return requests;
		}

		std::vector<AsyncRandomAccessFile> OpenAsyncFiles(AsyncEnv& async_env, const std::vector<std::string>& file_paths)
		{
			auto task = [&]() -> Task<std::vector<AsyncRandomAccessFile>> {
				std::vector<AsyncRandomAccessFile> files;
				files.reserve(file_paths.size());
				for (const auto& path : file_paths)
				{
					auto file_result = co_await async_env.NewRandomAccessFileAsync(path);
					if (!file_result.has_value())
					{
						throw std::runtime_error(file_result.error().ToString());
					}
					files.push_back(std::move(file_result.value()));
				}
				co_return files;
			}();

			return task.SyncWait();
		}

		std::vector<double> RunRandomReadBenchmark(
		    const std::vector<AsyncRandomAccessFile>& files, const std::vector<ReadRequest>& requests)
		{
			auto task = [&](const std::vector<AsyncRandomAccessFile>* p_files,
			                const std::vector<ReadRequest>* p_requests) -> Task<std::vector<double>> {
				std::vector<double> latencies_us;
				latencies_us.reserve(p_requests->size());
				std::array<std::byte, kRecordSize> scratch{ };
				std::size_t bytes_read = 0;

				for (const auto& request : *p_requests)
				{
					const auto start = Clock::now();
					auto read_result = co_await (*p_files)[request.file_index].ReadAtAsync(request.offset, std::span<std::byte>(scratch));
					const auto end = Clock::now();
					if (!read_result.has_value())
					{
						throw std::runtime_error(read_result.error().ToString());
					}
					bytes_read += read_result.value();
					latencies_us.push_back(
					    std::chrono::duration<double, std::micro>(end - start).count());
				}

				if (bytes_read == 0)
				{
					throw std::runtime_error("benchmark read zero bytes");
				}
				co_return latencies_us;
			}(std::addressof(files), std::addressof(requests));

			return task.SyncWait();
		}

		double PercentileUs(std::vector<double> latencies, double p)
		{
			if (latencies.empty())
			{
				return 0.0;
			}
			std::sort(latencies.begin(), latencies.end());
			const auto idx = static_cast<std::size_t>(p * static_cast<double>(latencies.size() - 1));
			return latencies[idx];
		}
	} // namespace

} // namespace prism::bench

int main(int argc, char** argv)
{
	using namespace prism;
	using namespace prism::bench;

	const BenchConfig cfg = ParseBenchArgs(argc, argv);
	Env* env = Env::Default();
	if (env == nullptr)
	{
		std::fprintf(stderr, "error: Env::Default() returned null\n");
		return 1;
	}

	const std::filesystem::path bench_dir = MakeTempDir("async_bench");
	const auto cleanup = [&bench_dir]() { (void)std::filesystem::remove_all(bench_dir); };

	try
	{
		const auto file_paths = MakeFilePaths(bench_dir);
		CreateDataset(env, file_paths);
		const auto requests = BuildWorkload(cfg.ops);

		ThreadPoolScheduler scheduler(kWorkerCount);
		auto runtime = AcquireRuntimeBundle(scheduler);
		runtime->async_env_backend = ToBackendMode(cfg.backend);

		AsyncEnv async_env(scheduler, env);
		const auto files = OpenAsyncFiles(async_env, file_paths);

		const auto start = Clock::now();
		auto latencies_us = RunRandomReadBenchmark(files, requests);
		const auto end = Clock::now();

		const double elapsed_seconds = std::chrono::duration<double>(end - start).count();
		const double throughput = elapsed_seconds > 0.0 ? static_cast<double>(cfg.ops) / elapsed_seconds : 0.0;

		std::printf("backend: %s\n", cfg.backend_name.c_str());
		std::printf("workload: %s\n", cfg.workload.c_str());
		std::printf("ops: %zu\n", cfg.ops);
		std::printf("throughput: %.2f ops/sec\n", throughput);
		std::printf("p50: %.2f us\n", PercentileUs(latencies_us, 0.50));
		std::printf("p95: %.2f us\n", PercentileUs(latencies_us, 0.95));
		std::printf("p99: %.2f us\n", PercentileUs(latencies_us, 0.99));

		cleanup();
		return 0;
	}
	catch (const std::exception& ex)
	{
		cleanup();
		std::fprintf(stderr, "error: %s\n", ex.what());
		return 1;
	}
}
