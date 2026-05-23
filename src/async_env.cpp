#include "async_env.h"

#include "async_runtime.h"
#include "status.h"

#include <cerrno>
#include <cstring>
#include <limits>
#include <mutex>
#include <semaphore>
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
				// Legacy benchmark/config alias. Older callers still use the historical
				// "thread_pool" name; all modes now route directly to
				// read_scheduler for file-I/O isolation.
				return { &runtime.read_scheduler };
			case AsyncEnvBackendMode::kBlockingLane:
				return { &runtime.read_scheduler };
			case AsyncEnvBackendMode::kDefault:
				return { &runtime.read_scheduler };
			}

			return { &runtime.read_scheduler };
		}

		Status ReactorIoError(const char* op, int result)
		{
			const int error_number = result < 0 ? -result : EIO;
			return Status::IOError(op, std::strerror(error_number));
		}

		Result<std::size_t> ReadWithDispatcher(
		    IoDispatcher& dispatcher, const std::shared_ptr<RandomAccessFile>& file, uint64_t offset, std::span<std::byte> dst)
		{
			if (dst.empty())
			{
				return 0;
			}

			const int fd = file->FileDescriptor();
			if (fd < 0 || dst.size() > std::numeric_limits<unsigned>::max())
			{
				return file->ReadAt(offset, dst);
			}

			std::binary_semaphore done(0);
			int read_result = -EIO;
			dispatcher.SubmitRead(fd,
			    dst.data(),
			    static_cast<unsigned>(dst.size()),
			    static_cast<off_t>(offset),
			    0,
			    [&](uint64_t, int completed_result) {
					read_result = completed_result;
					done.release();
			    });
			done.acquire();

			if (read_result < 0)
			{
				return std::unexpected(ReactorIoError("reactor read", read_result));
			}
			return static_cast<std::size_t>(read_result);
		}

		Status WriteWithDispatcher(IoDispatcher& dispatcher, const std::shared_ptr<WritableFile>& file, std::string data)
		{
			if (data.empty())
			{
				return Status::OK();
			}
			if (data.size() > std::numeric_limits<unsigned>::max())
			{
				return file->Append(Slice(data));
			}

			const int fd = file->FileDescriptor();
			if (fd < 0)
			{
				return file->Append(Slice(data));
			}

			const uint64_t offset = file->AppendOffset();
			std::binary_semaphore done(0);
			int write_result = -EIO;
			dispatcher.SubmitWrite(fd,
			    data.data(),
			    static_cast<unsigned>(data.size()),
			    static_cast<off_t>(offset),
			    0,
			    [&](uint64_t, int completed_result) {
				    write_result = completed_result;
				    done.release();
			    });
			done.acquire();

			if (write_result < 0)
			{
				return ReactorIoError("reactor write", write_result);
			}
			if (static_cast<std::size_t>(write_result) != data.size())
			{
				return Status::IOError("reactor write", "short write");
			}

			file->AdvanceAppendOffset(data.size());
			return Status::OK();
		}

	}

	AsyncRandomAccessFile::AsyncRandomAccessFile(ThreadPoolScheduler& scheduler, std::shared_ptr<RandomAccessFile> file)
	    : runtime_(AcquireRuntimeBundle(scheduler))
	    , file_(std::move(file))
	    , dispatcher_(&runtime_->io_dispatcher)
	{
	}

	AsyncRandomAccessFile::~AsyncRandomAccessFile() = default;

	AsyncOp<Result<std::size_t>> AsyncRandomAccessFile::ReadAtAsync(uint64_t offset, std::span<std::byte> dst) const
	{
		auto runtime = runtime_;
		auto dispatcher = dispatcher_;
		if (file_->FileDescriptor() >= 0)
		{
			return AsyncOp<Result<std::size_t>>((*runtime).cpu_scheduler,
			    [dispatcher, file = file_, offset, dst]() -> Result<std::size_t> { return ReadWithDispatcher(*dispatcher, file, offset, dst); });
		}

		auto backend = BackendSelect(*runtime);
		return AsyncOp<Result<std::size_t>>(
		    *backend.scheduler, [file = file_, offset, dst]() -> Result<std::size_t> { return file->ReadAt(offset, dst); });
	}

	AsyncOp<Result<std::string>> AsyncRandomAccessFile::ReadAtStringAsync(uint64_t offset, std::size_t n) const
	{
		auto runtime = runtime_;
		auto dispatcher = dispatcher_;
		if (file_->FileDescriptor() >= 0)
		{
			return AsyncOp<Result<std::string>>((*runtime).cpu_scheduler, [dispatcher, file = file_, offset, n]() -> Result<std::string> {
				std::string buf;
				buf.resize(n);
				auto bytes = std::as_writable_bytes(std::span(buf));
				Result<std::size_t> r = ReadWithDispatcher(*dispatcher, file, offset, bytes);
				if (!r.has_value())
				{
					return std::unexpected(r.error());
				}
				buf.resize(r.value());
				return buf;
			});
		}

		auto backend = BackendSelect(*runtime);
		return AsyncOp<Result<std::string>>(*backend.scheduler, [file = file_, offset, n]() -> Result<std::string> {
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
		auto dispatcher = &runtime->io_dispatcher;
		{
			std::lock_guard lock(write_state->mu);
			if (write_state->closed)
				return AsyncOp<Status>((*runtime).cpu_scheduler, [] { return Status::IOError("file closed"); });
		}
		return AsyncOp<Status>((*runtime).serial_scheduler, [dispatcher, file, write_state, data = std::move(data)]() mutable {
			{
				std::lock_guard lock(write_state->mu);
				if (write_state->closed)
				{
					return Status::IOError("file closed");
				}
			}
			return WriteWithDispatcher(*dispatcher, file, std::move(data));
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
