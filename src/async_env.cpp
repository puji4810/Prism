#include "async_env.h"

#include "async_runtime.h"
#include "status.h"

#include <atomic>
#include <cerrno>
#include <cstring>
#include <functional>
#include <limits>
#include <mutex>
#include <optional>
#include <semaphore>
#include <utility>

namespace prism
{
	namespace
	{
		Status ReactorIoError(const char* op, int result)
		{
			const int error_number = result < 0 ? -result : EIO;
			return Status::IOError(op, std::strerror(error_number));
		}

		void ScheduleReadAtCallback(AsyncRuntime& runtime,
		    RandomAccessFile* file,
		    std::shared_ptr<RandomAccessFile> keep_alive,
		    uint64_t offset,
		    std::span<std::byte> dst,
		    std::move_only_function<void(Result<std::size_t>)> completion)
		{
			const int fd = file->FileDescriptor();
			if (fd >= 0 && dst.size() <= std::numeric_limits<unsigned>::max() && runtime.Io().HasReactor())
			{
				runtime.Io().SubmitRead(fd,
				    dst.data(),
				    static_cast<unsigned>(dst.size()),
				    static_cast<off_t>(offset),
				    0,
				    [&runtime, keep_alive = std::move(keep_alive), completion = std::move(completion)](uint64_t, int completed_result) mutable {
					    Result<std::size_t> result = completed_result < 0
					        ? Result<std::size_t>(std::unexpected(ReactorIoError("reactor read", completed_result)))
					        : Result<std::size_t>(static_cast<std::size_t>(completed_result));
					    runtime.CpuExecutor().Submit(
					        [completion = std::move(completion), result = std::move(result)]() mutable { completion(std::move(result)); });
				    });
				return;
			}

			runtime.BlockingIoExecutor().Submit(
			    [&runtime, file, keep_alive = std::move(keep_alive), offset, dst, completion = std::move(completion)]() mutable {
				    Result<std::size_t> result = std::unexpected(Status::IOError("async read callback", "unknown failure"));
				    try
				    {
					    result = file->ReadAt(offset, dst);
				    }
				    catch (...)
				    {
					    result = std::unexpected(Status::IOError("async read callback", "exception during blocking read"));
				    }
				    runtime.CpuExecutor().Submit(
				        [completion = std::move(completion), result = std::move(result)]() mutable { completion(std::move(result)); });
			    });
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

		struct ReadStateBase
		{
			static constexpr int kSuspending = 0;
			static constexpr int kCompleted = 1;
			static constexpr int kSuspended = 2;

			AsyncRuntime* runtime = nullptr;
			std::shared_ptr<RandomAccessFile> file;
			uint64_t offset = 0;
			std::coroutine_handle<> handle;
			std::exception_ptr exception;
			std::atomic<int> status{ kSuspending };

			void Finish(bool resume_on_cpu)
			{
				auto expected = kSuspending;
				if (status.compare_exchange_strong(expected, kCompleted, std::memory_order_acq_rel, std::memory_order_acquire))
				{
					return;
				}
				if (resume_on_cpu)
				{
					runtime->CpuExecutor().Submit([handle = handle] { handle.resume(); });
				}
				else
				{
					handle.resume();
				}
			}

			bool TrySuspend()
			{
				auto expected = kSuspending;
				return status.compare_exchange_strong(expected, kSuspended, std::memory_order_acq_rel, std::memory_order_acquire);
			}
		};
	}

	struct AsyncReadAtOp::State: ReadStateBase
	{
		std::span<std::byte> dst;
		std::optional<Result<std::size_t>> result;

		State(AsyncRuntime& runtime_arg, std::shared_ptr<RandomAccessFile> file_arg, uint64_t offset_arg, std::span<std::byte> dst_arg)
		{
			runtime = &runtime_arg;
			file = std::move(file_arg);
			offset = offset_arg;
			dst = dst_arg;
			if (dst.empty())
			{
				result = 0;
				status.store(kCompleted, std::memory_order_release);
			}
		}

		void Start(std::shared_ptr<State> self)
		{
			const int fd = file->FileDescriptor();
			if (fd >= 0 && dst.size() <= std::numeric_limits<unsigned>::max() && runtime->Io().HasReactor())
			{
				runtime->Io().SubmitRead(fd,
				    dst.data(),
				    static_cast<unsigned>(dst.size()),
				    static_cast<off_t>(offset),
				    0,
				    [self = std::move(self)](uint64_t, int completed_result) {
					    if (completed_result < 0)
					    {
						    self->result = std::unexpected(ReactorIoError("reactor read", completed_result));
					    }
					    else
					    {
						    self->result = static_cast<std::size_t>(completed_result);
					    }
					    self->Finish(/*resume_on_cpu=*/true);
				    });
				return;
			}

			runtime->BlockingIoExecutor().Submit([self = std::move(self)] {
				try
				{
					self->result = self->file->ReadAt(self->offset, self->dst);
				}
				catch (...)
				{
					self->exception = std::current_exception();
				}
				self->Finish(/*resume_on_cpu=*/true);
			});
		}
	};

	AsyncReadAtOp::AsyncReadAtOp(AsyncRuntime& runtime, std::shared_ptr<RandomAccessFile> file, uint64_t offset, std::span<std::byte> dst)
	    : state_(std::make_shared<State>(runtime, std::move(file), offset, dst))
	{
	}

	AsyncReadAtOp::~AsyncReadAtOp() = default;
	AsyncReadAtOp::AsyncReadAtOp(AsyncReadAtOp&&) noexcept = default;
	AsyncReadAtOp& AsyncReadAtOp::operator=(AsyncReadAtOp&&) noexcept = default;
	AsyncReadAtOp::Awaiter::~Awaiter() = default;

	bool AsyncReadAtOp::Awaiter::await_ready() const noexcept
	{
		return state->status.load(std::memory_order_acquire) == State::kCompleted;
	}

	bool AsyncReadAtOp::Awaiter::await_suspend(std::coroutine_handle<> handle) const
	{
		state->handle = handle;
		state->Start(state);
		return state->TrySuspend();
	}

	Result<std::size_t> AsyncReadAtOp::Awaiter::await_resume() const
	{
		if (state->exception)
		{
			std::rethrow_exception(state->exception);
		}
		return std::move(*state->result);
	}

	AsyncReadAtOp::Awaiter AsyncReadAtOp::operator co_await() && noexcept { return Awaiter{ std::move(state_) }; }

	struct AsyncReadAtStringOp::State: ReadStateBase
	{
		std::string buffer;
		std::optional<Result<std::string>> result;

		State(AsyncRuntime& runtime_arg, std::shared_ptr<RandomAccessFile> file_arg, uint64_t offset_arg, std::size_t n)
		{
			runtime = &runtime_arg;
			file = std::move(file_arg);
			offset = offset_arg;
			buffer.resize(n);
			if (n == 0)
			{
				result = std::string();
				status.store(kCompleted, std::memory_order_release);
			}
		}

		void CompleteFromByteCount(Result<std::size_t> bytes)
		{
			if (!bytes.has_value())
			{
				result = std::unexpected(bytes.error());
				return;
			}
			buffer.resize(bytes.value());
			result = std::move(buffer);
		}

		void Start(std::shared_ptr<State> self)
		{
			const int fd = file->FileDescriptor();
			auto dst = std::as_writable_bytes(std::span(buffer));
			if (fd >= 0 && dst.size() <= std::numeric_limits<unsigned>::max() && runtime->Io().HasReactor())
			{
				runtime->Io().SubmitRead(fd,
				    dst.data(),
				    static_cast<unsigned>(dst.size()),
				    static_cast<off_t>(offset),
				    0,
				    [self = std::move(self)](uint64_t, int completed_result) {
					    if (completed_result < 0)
					    {
						    self->CompleteFromByteCount(std::unexpected(ReactorIoError("reactor read", completed_result)));
					    }
					    else
					    {
						    self->CompleteFromByteCount(static_cast<std::size_t>(completed_result));
					    }
					    self->Finish(/*resume_on_cpu=*/true);
				    });
				return;
			}

			runtime->BlockingIoExecutor().Submit([self = std::move(self), dst] {
				try
				{
					self->CompleteFromByteCount(self->file->ReadAt(self->offset, dst));
				}
				catch (...)
				{
					self->exception = std::current_exception();
				}
				self->Finish(/*resume_on_cpu=*/true);
			});
		}
	};

	AsyncReadAtStringOp::AsyncReadAtStringOp(AsyncRuntime& runtime, std::shared_ptr<RandomAccessFile> file, uint64_t offset, std::size_t n)
	    : state_(std::make_shared<State>(runtime, std::move(file), offset, n))
	{
	}

	AsyncReadAtStringOp::~AsyncReadAtStringOp() = default;
	AsyncReadAtStringOp::AsyncReadAtStringOp(AsyncReadAtStringOp&&) noexcept = default;
	AsyncReadAtStringOp& AsyncReadAtStringOp::operator=(AsyncReadAtStringOp&&) noexcept = default;
	AsyncReadAtStringOp::Awaiter::~Awaiter() = default;

	bool AsyncReadAtStringOp::Awaiter::await_ready() const noexcept
	{
		return state->status.load(std::memory_order_acquire) == State::kCompleted;
	}

	bool AsyncReadAtStringOp::Awaiter::await_suspend(std::coroutine_handle<> handle) const
	{
		state->handle = handle;
		state->Start(state);
		return state->TrySuspend();
	}

	Result<std::string> AsyncReadAtStringOp::Awaiter::await_resume() const
	{
		if (state->exception)
		{
			std::rethrow_exception(state->exception);
		}
		return std::move(*state->result);
	}

	AsyncReadAtStringOp::Awaiter AsyncReadAtStringOp::operator co_await() && noexcept { return Awaiter{ std::move(state_) }; }

	AsyncRandomAccessFile::AsyncRandomAccessFile(AsyncRuntime& runtime, std::shared_ptr<RandomAccessFile> file)
	    : runtime_(&runtime)
	    , file_(std::move(file))
	{
	}

	AsyncRandomAccessFile::~AsyncRandomAccessFile() = default;

	AsyncReadAtOp AsyncRandomAccessFile::ReadAtAsync(uint64_t offset, std::span<std::byte> dst) const
	{
		return AsyncReadAtOp(*runtime_, file_, offset, dst);
	}

	void AsyncRandomAccessFile::ReadAtAsyncCallback(
	    uint64_t offset, std::span<std::byte> dst, std::move_only_function<void(Result<std::size_t>)> completion) const
	{
		if (dst.empty())
		{
			runtime_->CpuExecutor().Submit([completion = std::move(completion)]() mutable { completion(0); });
			return;
		}
		ScheduleReadAtCallback(*runtime_, file_.get(), file_, offset, dst, std::move(completion));
	}

	void ReadAtAsyncCallback(AsyncRuntime& runtime,
	    RandomAccessFile& file,
	    uint64_t offset,
	    std::span<std::byte> dst,
	    std::move_only_function<void(Result<std::size_t>)> completion)
	{
		if (dst.empty())
		{
			runtime.CpuExecutor().Submit([completion = std::move(completion)]() mutable { completion(0); });
			return;
		}
		ScheduleReadAtCallback(runtime, &file, nullptr, offset, dst, std::move(completion));
	}

	AsyncReadAtStringOp AsyncRandomAccessFile::ReadAtStringAsync(uint64_t offset, std::size_t n) const
	{
		return AsyncReadAtStringOp(*runtime_, file_, offset, n);
	}

	AsyncWritableFile::AsyncWritableFile(AsyncRuntime& runtime, std::unique_ptr<WritableFile> file)
	    : runtime_(&runtime)
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
		auto dispatcher = &runtime->Io();
		{
			std::lock_guard lock(write_state->mu);
			if (write_state->closed)
				return AsyncOp<Status>(runtime->CpuExecutor(), [] { return Status::IOError("file closed"); });
		}
		return AsyncOp<Status>(runtime->SerialFileExecutor(), [dispatcher, file, write_state, data = std::move(data)]() mutable {
			{
				std::lock_guard lock(write_state->mu);
				if (write_state->closed)
				{
					return Status::IOError("file closed");
				}
			}
			// TODO(async-serial-write): AppendAsync still blocks the serial file executor
			// while waiting for reactor write completion. Replace with an ordered async
			// write queue before treating writable-file reactor I/O as fully non-blocking.
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
				return AsyncOp<Status>(runtime->CpuExecutor(), [] { return Status::IOError("file closed"); });
		}
		return AsyncOp<Status>(runtime->SerialFileExecutor(), [file, write_state]() {
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
				return AsyncOp<Status>(runtime->CpuExecutor(), [] { return Status::IOError("file closed"); });
		}
		return AsyncOp<Status>(runtime->SerialFileExecutor(), [file, write_state]() {
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
				return AsyncOp<Status>(runtime->CpuExecutor(), [] { return Status::IOError("file closed"); });
		}
		return AsyncOp<Status>(runtime->SerialFileExecutor(), [file, write_state]() {
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

	AsyncEnv::AsyncEnv(AsyncRuntime& runtime, Env* env)
	    : runtime_(&runtime)
	    , env_(env)
	{
	}

	AsyncOp<Result<AsyncRandomAccessFile>> AsyncEnv::NewRandomAccessFileAsync(std::string fname)
	{
		auto runtime = runtime_;
		auto env = env_;
		return AsyncOp<Result<AsyncRandomAccessFile>>(
		    runtime->BlockingIoExecutor(), [env, runtime, fname = std::move(fname)]() -> Result<AsyncRandomAccessFile> {
			    auto f = env->NewRandomAccessFile(fname);
			    if (!f.has_value())
			    {
				    return std::unexpected(f.error());
			    }
			    return AsyncRandomAccessFile(*runtime, std::shared_ptr<RandomAccessFile>(std::move(f.value())));
		    });
	}

	AsyncOp<Result<AsyncWritableFile>> AsyncEnv::NewWritableFileAsync(std::string fname)
	{
		auto runtime = runtime_;
		auto env = env_;
		return AsyncOp<Result<AsyncWritableFile>>(
		    runtime->BlockingIoExecutor(), [env, runtime, fname = std::move(fname)]() -> Result<AsyncWritableFile> {
			    auto f = env->NewWritableFile(fname);
			    if (!f.has_value())
			    {
				    return std::unexpected(f.error());
			    }
			    return AsyncWritableFile(*runtime, std::move(f.value()));
		    });
	}

	AsyncOp<Result<AsyncWritableFile>> AsyncEnv::NewAppendableFileAsync(std::string fname)
	{
		auto runtime = runtime_;
		auto env = env_;
		return AsyncOp<Result<AsyncWritableFile>>(
		    runtime->BlockingIoExecutor(), [env, runtime, fname = std::move(fname)]() -> Result<AsyncWritableFile> {
			    auto f = env->NewAppendableFile(fname);
			    if (!f.has_value())
			    {
				    return std::unexpected(f.error());
			    }
			    return AsyncWritableFile(*runtime, std::move(f.value()));
		    });
	}

	AsyncOp<Status> AsyncEnv::RemoveFileAsync(std::string fname)
	{
		auto runtime = runtime_;
		auto env = env_;
		return AsyncOp<Status>(runtime->BlockingIoExecutor(), [env, fname = std::move(fname)]() { return env->RemoveFile(fname); });
	}

	AsyncOp<Status> AsyncEnv::CreateDirAsync(std::string dirname)
	{
		auto runtime = runtime_;
		auto env = env_;
		return AsyncOp<Status>(runtime->BlockingIoExecutor(), [env, dirname = std::move(dirname)]() { return env->CreateDir(dirname); });
	}

	AsyncOp<Result<std::size_t>> AsyncEnv::GetFileSizeAsync(std::string fname)
	{
		auto runtime = runtime_;
		auto env = env_;
		return AsyncOp<Result<std::size_t>>(runtime->BlockingIoExecutor(), [env, fname = std::move(fname)]() { return env->GetFileSize(fname); });
	}
}
