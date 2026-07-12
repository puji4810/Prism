#include "async_wal_writer.h"

#include "async_runtime.h"
#include "env.h"
#include "io_reactor.h"
#include "log_writer.h"
#include "runtime_metrics.h"

#include <cerrno>
#include <chrono>
#include <cstring>
#include <memory>
#include <string>
#include <utility>

namespace prism
{
	namespace
	{
		Status ResultToStatus(const char* context, int result, int expected_success)
		{
			if (result == expected_success)
			{
				return Status::OK();
			}
			if (result >= 0)
			{
				return Status::IOError(context, "short I/O");
			}
			return Status::IOError(context, std::strerror(-result));
		}

		struct WalIoState
		{
			WritableFile* file = nullptr;
			AsyncRuntime* runtime = nullptr;
			std::string bytes;
			bool sync = false;
			std::uint64_t append_user_data = 0;
			std::uint64_t sync_user_data = 0;
#ifdef PRISM_RUNTIME_METRICS
			std::chrono::steady_clock::time_point start_time;
			std::chrono::steady_clock::time_point append_start_time;
			std::chrono::steady_clock::time_point fsync_start_time;
#endif
			AsyncWalWriter::Completion completion;
		};

#ifdef PRISM_RUNTIME_METRICS
		uint64_t ElapsedUs(std::chrono::steady_clock::time_point start)
		{
			return static_cast<uint64_t>(
			    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start).count());
		}
#endif

		void CompleteWalIo(const std::shared_ptr<WalIoState>& state, Status status)
		{
#ifdef PRISM_RUNTIME_METRICS
			RuntimeMetrics::Instance().async_wal_inflight_total_us.fetch_add(ElapsedUs(state->start_time), std::memory_order_relaxed);
			RuntimeMetrics::Instance().async_wal_inflight_count.fetch_add(1, std::memory_order_relaxed);
#endif
			state->completion(std::move(status));
		}
	}

	AsyncWalWriter::AsyncWalWriter(AsyncRuntime& runtime)
	    : runtime_(&runtime)
	{
	}

	void AsyncWalWriter::Write(WritableFile& file, log::Writer& writer, const Slice& record, bool sync, Completion completion)
	{
		auto encoded = writer.ReserveRecord(record);
		if (!encoded.has_value())
		{
			completion(encoded.error());
			return;
		}

		auto state = std::make_shared<WalIoState>();
		state->file = &file;
		state->runtime = runtime_;
		state->bytes = std::move(encoded->bytes);
		state->sync = sync;
		state->append_user_data = next_user_data_++;
		state->sync_user_data = next_user_data_++;
#ifdef PRISM_RUNTIME_METRICS
		state->start_time = std::chrono::steady_clock::now();
#endif
		state->completion = std::move(completion);

		const int fd = file.FileDescriptor();
		if (fd < 0 || !runtime_->Io().HasReactor())
		{
#ifdef PRISM_RUNTIME_METRICS
			RuntimeMetrics::Instance().wal_append_fallback_count.fetch_add(1, std::memory_order_relaxed);
			if (sync)
			{
				RuntimeMetrics::Instance().wal_fsync_fallback_count.fetch_add(1, std::memory_order_relaxed);
			}
#endif
			runtime_->BlockingIoExecutor().Submit([state] {
#ifdef PRISM_RUNTIME_METRICS
				const auto append_start = std::chrono::steady_clock::now();
#endif
				Status s = state->file->Append(Slice(state->bytes));
#ifdef PRISM_RUNTIME_METRICS
				RuntimeMetrics::Instance().wal_append_latency_total_us.fetch_add(ElapsedUs(append_start), std::memory_order_relaxed);
#endif
				if (s.ok() && state->sync)
				{
#ifdef PRISM_RUNTIME_METRICS
					const auto fsync_start = std::chrono::steady_clock::now();
#endif
					s = state->file->Sync();
#ifdef PRISM_RUNTIME_METRICS
					RuntimeMetrics::Instance().wal_fsync_latency_total_us.fetch_add(ElapsedUs(fsync_start), std::memory_order_relaxed);
#endif
				}
				CompleteWalIo(state, std::move(s));
			});
			return;
		}

#ifdef PRISM_RUNTIME_METRICS
		RuntimeMetrics::Instance().wal_append_reactor_count.fetch_add(1, std::memory_order_relaxed);
		if (sync)
		{
			RuntimeMetrics::Instance().wal_fsync_reactor_count.fetch_add(1, std::memory_order_relaxed);
		}
#endif
		const uint64_t offset = file.AppendOffset();
		file.AdvanceAppendOffset(state->bytes.size());
#ifdef PRISM_RUNTIME_METRICS
		state->append_start_time = std::chrono::steady_clock::now();
#endif
		runtime_->Io().SubmitWrite(fd,
		    state->bytes.data(),
		    static_cast<unsigned>(state->bytes.size()),
		    static_cast<off_t>(offset),
		    state->append_user_data,
		    [state, fd](uint64_t, int result) {
#ifdef PRISM_RUNTIME_METRICS
			    RuntimeMetrics::Instance().wal_append_latency_total_us.fetch_add(
			        ElapsedUs(state->append_start_time), std::memory_order_relaxed);
#endif
			    Status s = ResultToStatus("async WAL append", result, static_cast<int>(state->bytes.size()));
			    if (!s.ok() || !state->sync)
			    {
				    CompleteWalIo(state, std::move(s));
				    return;
			    }

#ifdef PRISM_RUNTIME_METRICS
			    state->fsync_start_time = std::chrono::steady_clock::now();
#endif
			    state->runtime->Io().SubmitFsync(fd, state->sync_user_data, [state](uint64_t, int sync_result) {
#ifdef PRISM_RUNTIME_METRICS
				    RuntimeMetrics::Instance().wal_fsync_latency_total_us.fetch_add(
				        ElapsedUs(state->fsync_start_time), std::memory_order_relaxed);
#endif
				    CompleteWalIo(state, ResultToStatus("async WAL sync", sync_result, 0));
			    });
		    });
	}
} // namespace prism
