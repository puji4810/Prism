#include "async_env.h"

#include "runtime_executor.h"
#include <mutex>
#include <utility>

namespace prism
{
	namespace
	{
		struct AsyncEnvBackend
		{
			IScheduler* scheduler;
		};

			// RuntimeBundle& RuntimeFrom(const std::shared_ptr<void>& runtime)
			// {
			// 	return *static_cast<RuntimeBundle*>(runtime.get());
			// }

		AsyncEnvBackend BackendSelect(RuntimeBundle& runtime)
		{
			switch (runtime.async_env_backend)
			{
			case AsyncEnvBackendMode::kThreadPool:
				return { &runtime.runtime_scheduler };
			case AsyncEnvBackendMode::kBlockingLane:
				return { &runtime.read_scheduler };
			case AsyncEnvBackendMode::kDefault:
				return { &runtime.read_scheduler };
			}

			return { &runtime.read_scheduler };
		}
	}

	AsyncRandomAccessFile::AsyncRandomAccessFile(ThreadPoolScheduler& scheduler, std::shared_ptr<RandomAccessFile> file)
	    : runtime_(AcquireRuntimeBundle(scheduler))
	    , file_(std::move(file))
	{
	}

	AsyncRandomAccessFile::~AsyncRandomAccessFile() = default;

	AsyncOp<Result<std::size_t>> AsyncRandomAccessFile::ReadAtAsync(uint64_t offset, std::span<std::byte> dst) const
	{
		auto runtime = runtime_;
		auto backend = BackendSelect(*runtime);
		return AsyncOp<Result<std::size_t>>(
		    *backend.scheduler, [file = file_, offset, dst]() -> Result<std::size_t> {
			    return file->ReadAt(offset, dst);
		    });
	}

	AsyncOp<Result<std::string>> AsyncRandomAccessFile::ReadAtStringAsync(uint64_t offset, std::size_t n) const
	{
		auto runtime = runtime_;
		auto backend = BackendSelect(*runtime);
		return AsyncOp<Result<std::string>>(
		    *backend.scheduler, [file = file_, offset, n]() -> Result<std::string> {
			    std::string buf;
			    buf.resize(n);
			    auto bytes = std::as_writable_bytes(std::span(buf));
			    Result<std::size_t> r = file->ReadAt(offset, bytes);
			    if (!r.has_value())
			    {
				    return std::unexpected(r.error());
			    }
			    buf.resize(r.value());
			    return buf;
		    });
	}

	AsyncWritableFile::AsyncWritableFile(ThreadPoolScheduler& scheduler, std::unique_ptr<WritableFile> file)
	    : runtime_(AcquireRuntimeBundle(scheduler))
	    , file_(std::shared_ptr<WritableFile>(std::move(file)))
	    , write_state_(std::make_shared<WriteState>())
	{
	}

	AsyncWritableFile::~AsyncWritableFile() = default;

	AsyncOp<Status> AsyncWritableFile::AppendAsync(std::string data)
	{
		auto runtime = runtime_;
		auto file = file_;
		auto write_state = write_state_;
		{
			std::lock_guard lock(write_state->mu);
			if (write_state->closed)
				return AsyncOp<Status>((*runtime).cpu_scheduler, [] { return Status::IOError("file closed"); });
		}
		return AsyncOp<Status>((*runtime).serial_scheduler, [file, write_state, data = std::move(data)]() {
			{
				std::lock_guard lock(write_state->mu);
				if (write_state->closed)
				{
					return Status::IOError("file closed");
				}
			}
			return file->Append(Slice(data));
		});
	}

	AsyncOp<Status> AsyncWritableFile::FlushAsync()
	{
		auto runtime = runtime_;
		auto file = file_;
		auto write_state = write_state_;
		{
			std::lock_guard lock(write_state->mu);
			if (write_state->closed)
				return AsyncOp<Status>((*runtime).cpu_scheduler, [] { return Status::IOError("file closed"); });
		}
		return AsyncOp<Status>((*runtime).serial_scheduler, [file, write_state]() {
			{
				std::lock_guard lock(write_state->mu);
				if (write_state->closed)
				{
					return Status::IOError("file closed");
				}
			}
			return file->Flush();
		});
	}

	AsyncOp<Status> AsyncWritableFile::SyncAsync()
	{
		auto runtime = runtime_;
		auto file = file_;
		auto write_state = write_state_;
		{
			std::lock_guard lock(write_state->mu);
			if (write_state->closed)
				return AsyncOp<Status>((*runtime).cpu_scheduler, [] { return Status::IOError("file closed"); });
		}
		return AsyncOp<Status>((*runtime).serial_scheduler, [file, write_state]() {
			{
				std::lock_guard lock(write_state->mu);
				if (write_state->closed)
				{
					return Status::IOError("file closed");
				}
			}
			return file->Sync();
		});
	}

	AsyncOp<Status> AsyncWritableFile::CloseAsync()
	{
		auto runtime = runtime_;
		auto file = file_;
		auto write_state = write_state_;
		{
			std::lock_guard lock(write_state->mu);
			if (write_state->closed)
				return AsyncOp<Status>((*runtime).cpu_scheduler, [] { return Status::IOError("file closed"); });
		}
		return AsyncOp<Status>((*runtime).serial_scheduler, [file, write_state]() {
			{
				std::lock_guard lock(write_state->mu);
				if (write_state->closed)
				{
					return Status::IOError("file closed");
				}
			}
			auto s = file->Close();
			{
				std::lock_guard lock(write_state->mu);
				write_state->closed = true;
			}
			return s;
		});
	}

	AsyncEnv::AsyncEnv(ThreadPoolScheduler& scheduler, Env* env)
	    : runtime_(AcquireRuntimeBundle(scheduler))
	    , env_(env)
	{
	}

	AsyncOp<Result<AsyncRandomAccessFile>> AsyncEnv::NewRandomAccessFileAsync(std::string fname)
	{
		auto runtime = runtime_;
		auto env = env_;
		auto backend = BackendSelect(*runtime);
		return AsyncOp<Result<AsyncRandomAccessFile>>(
		    *backend.scheduler, [env, runtime, fname = std::move(fname)]() -> Result<AsyncRandomAccessFile> {
			    auto f = env->NewRandomAccessFile(fname);
			    if (!f.has_value())
			    {
				    return std::unexpected(f.error());
			    }
			    auto& timer_source = *((*runtime).timer_source);
			    return AsyncRandomAccessFile(timer_source, std::shared_ptr<RandomAccessFile>(std::move(f.value())));
		    });
	}

	AsyncOp<Result<AsyncWritableFile>> AsyncEnv::NewWritableFileAsync(std::string fname)
	{
		auto runtime = runtime_;
		auto env = env_;
		auto backend = BackendSelect(*runtime);
		return AsyncOp<Result<AsyncWritableFile>>(
		    *backend.scheduler, [env, runtime, fname = std::move(fname)]() -> Result<AsyncWritableFile> {
			    auto f = env->NewWritableFile(fname);
			    if (!f.has_value())
			    {
				    return std::unexpected(f.error());
			    }
			    auto& timer_source = *((*runtime).timer_source);
			    return AsyncWritableFile(timer_source, std::move(f.value()));
		    });
	}

	AsyncOp<Result<AsyncWritableFile>> AsyncEnv::NewAppendableFileAsync(std::string fname)
	{
		auto runtime = runtime_;
		auto env = env_;
		auto backend = BackendSelect(*runtime);
		return AsyncOp<Result<AsyncWritableFile>>(
		    *backend.scheduler, [env, runtime, fname = std::move(fname)]() -> Result<AsyncWritableFile> {
			    auto f = env->NewAppendableFile(fname);
			    if (!f.has_value())
			    {
				    return std::unexpected(f.error());
			    }
			    auto& timer_source = *((*runtime).timer_source);
			    return AsyncWritableFile(timer_source, std::move(f.value()));
		    });
	}

	AsyncOp<Status> AsyncEnv::RemoveFileAsync(std::string fname)
	{
		auto runtime = runtime_;
		auto env = env_;
		auto backend = BackendSelect(*runtime);
		return AsyncOp<Status>(*backend.scheduler, [env, fname = std::move(fname)]() { return env->RemoveFile(fname); });
	}

	AsyncOp<Status> AsyncEnv::CreateDirAsync(std::string dirname)
	{
		auto runtime = runtime_;
		auto env = env_;
		auto backend = BackendSelect(*runtime);
		return AsyncOp<Status>(*backend.scheduler, [env, dirname = std::move(dirname)]() { return env->CreateDir(dirname); });
	}

	AsyncOp<Result<std::size_t>> AsyncEnv::GetFileSizeAsync(std::string fname)
	{
		auto runtime = runtime_;
		auto env = env_;
		auto backend = BackendSelect(*runtime);
		return AsyncOp<Result<std::size_t>>(*backend.scheduler, [env, fname = std::move(fname)]() { return env->GetFileSize(fname); });
	}
}
