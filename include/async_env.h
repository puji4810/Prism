#ifndef PRISM_ASYNC_ENV_H
#define PRISM_ASYNC_ENV_H

#include "async_op.h"
#include "env.h"

#include <cstddef>
#include <cstdint>
#include <coroutine>
#include <functional>
#include <memory>
#include <mutex>
#include <span>
#include <string>

// Forward-declared to avoid exposing internal runtime types in public header.
// Defined in src/async_runtime.h; included in src/async_env.cpp.
namespace prism { class AsyncRuntime; }

namespace prism
{
	class IoDispatcher;

	void ReadAtAsyncCallback(AsyncRuntime& runtime,
	    RandomAccessFile& file,
	    uint64_t offset,
	    std::span<std::byte> dst,
	    std::move_only_function<void(Result<std::size_t>)> completion);

	class AsyncReadAtOp
	{
	public:
		struct State;
		struct Awaiter
		{
			std::shared_ptr<State> state;

			~Awaiter();
			bool await_ready() const noexcept;
			bool await_suspend(std::coroutine_handle<> handle) const;
			Result<std::size_t> await_resume() const;
		};

		AsyncReadAtOp(AsyncRuntime& runtime, std::shared_ptr<RandomAccessFile> file, uint64_t offset, std::span<std::byte> dst);
		~AsyncReadAtOp();
		AsyncReadAtOp(AsyncReadAtOp&&) noexcept;
		AsyncReadAtOp& operator=(AsyncReadAtOp&&) noexcept;
		AsyncReadAtOp(const AsyncReadAtOp&) = delete;
		AsyncReadAtOp& operator=(const AsyncReadAtOp&) = delete;

		Awaiter operator co_await() && noexcept;

	private:
		std::shared_ptr<State> state_;
	};

	class AsyncReadAtStringOp
	{
	public:
		struct State;
		struct Awaiter
		{
			std::shared_ptr<State> state;

			~Awaiter();
			bool await_ready() const noexcept;
			bool await_suspend(std::coroutine_handle<> handle) const;
			Result<std::string> await_resume() const;
		};

		AsyncReadAtStringOp(AsyncRuntime& runtime, std::shared_ptr<RandomAccessFile> file, uint64_t offset, std::size_t n);
		~AsyncReadAtStringOp();
		AsyncReadAtStringOp(AsyncReadAtStringOp&&) noexcept;
		AsyncReadAtStringOp& operator=(AsyncReadAtStringOp&&) noexcept;
		AsyncReadAtStringOp(const AsyncReadAtStringOp&) = delete;
		AsyncReadAtStringOp& operator=(const AsyncReadAtStringOp&) = delete;

		Awaiter operator co_await() && noexcept;

	private:
		std::shared_ptr<State> state_;
	};

	// AsyncRandomAccessFile: Asynchronous wrapper for random-access file reads.
	//
	// Design:
	// - Wraps a synchronous RandomAccessFile instance
	// - Provides two async read APIs:
	//   * ReadAtAsync(span): Caller-owned buffer (zero-copy, for performance)
	//   * ReadAtStringAsync(size): Owning buffer (convenience, allocates std::string)
	//
	// Lifecycle Note:
	// - The synchronous Slice/scratch pattern from Env cannot cross co_await boundaries
	//   (scratch buffer may be invalidated after suspension)
	// - AsyncEnv uses owning buffers (std::string) or caller-owned buffers (std::span)
	//   to ensure lifetime safety across async boundaries
	class AsyncRandomAccessFile
	{
	public:
		AsyncRandomAccessFile(AsyncRuntime& runtime, std::shared_ptr<RandomAccessFile> file);
		~AsyncRandomAccessFile();

		AsyncRandomAccessFile(const AsyncRandomAccessFile&) = delete;
		AsyncRandomAccessFile& operator=(const AsyncRandomAccessFile&) = delete;
		AsyncRandomAccessFile(AsyncRandomAccessFile&&) = default;
		AsyncRandomAccessFile& operator=(AsyncRandomAccessFile&&) = default;

		// ReadAtAsync: Zero-copy read into caller-owned buffer.
		// Precondition: dst buffer must remain valid until the AsyncOp completes.
		// (i.e., until the awaiting coroutine resumes).
		// Returns number of bytes actually read.
		AsyncReadAtOp ReadAtAsync(uint64_t offset, std::span<std::byte> dst) const;
		void ReadAtAsyncCallback(
		    uint64_t offset, std::span<std::byte> dst, std::move_only_function<void(Result<std::size_t>)> completion) const;

		// ReadAtStringAsync: Convenience method that allocates and returns std::string.
		// Simpler to use but involves heap allocation.
		AsyncReadAtStringOp ReadAtStringAsync(uint64_t offset, std::size_t n) const;

	private:
		AsyncRuntime* runtime_;
		std::shared_ptr<RandomAccessFile> file_;
	};

	// AsyncWritableFile: Asynchronous wrapper for writable files (append-only).
	//
	// Serialization Contract:
	// - All async operations execute in FIFO (submission) order regardless of thread scheduling.
	// - AsyncRuntime::SerialFileExecutor() provides one-at-a-time execution without blocking shared workers.
	// - After CloseAsync completes, subsequent Append/Flush/Sync ops return IOError("file closed").
	//
	// Lifetime: file_ is held via shared_ptr; wrapper may be destroyed while ops are in-flight.
	class AsyncWritableFile
	{
	public:
		AsyncWritableFile(AsyncRuntime& runtime, std::unique_ptr<WritableFile> file);
		~AsyncWritableFile();

		AsyncWritableFile(const AsyncWritableFile&) = delete;
		AsyncWritableFile& operator=(const AsyncWritableFile&) = delete;
		AsyncWritableFile(AsyncWritableFile&&) = default;
		AsyncWritableFile& operator=(AsyncWritableFile&&) = default;

		// All write operations copy data (std::string ownership) to avoid lifetime issues.
		AsyncOp<Status> AppendAsync(std::string data);
		AsyncOp<Status> FlushAsync();
		AsyncOp<Status> SyncAsync();
		AsyncOp<Status> CloseAsync();

	private:
		struct WriteState
		{
			std::mutex mu;
			bool closed{ false };
		};

		AsyncRuntime* runtime_;
		std::shared_ptr<WritableFile> file_;
		std::shared_ptr<WriteState> write_state_;
	};

	// AsyncEnv: Asynchronous filesystem operations.
	//
	// Current Implementation:
	// - Wraps synchronous Env operations in thread pool tasks
	// - Random-access reads use the reactor-backed path when a permanent file descriptor
	//   and io_uring are available, otherwise they fall back to the blocking read lane
	// - Write-side file creation plus metadata ops stay on blocking workers; AppendAsync
	//   uses the reactor path when possible while Flush/Sync/Close remain serialized.
	//
	// Future Migration Path:
	// - Replace with io_uring (Linux) / kqueue (BSD) / IOCP (Windows)
	// - AsyncOp interface remains unchanged, only internal implementation changes
	class AsyncEnv
	{
	public:
		AsyncEnv(AsyncRuntime& runtime, Env* env);

		// All file factory methods return by-value handles (e.g., Result<AsyncWritableFile>).
		// The previous unique_ptr-returning signatures are deprecated.
		AsyncOp<Result<AsyncRandomAccessFile>> NewRandomAccessFileAsync(std::string fname);
		AsyncOp<Result<AsyncWritableFile>> NewWritableFileAsync(std::string fname);
		AsyncOp<Result<AsyncWritableFile>> NewAppendableFileAsync(std::string fname);

		AsyncOp<Status> RemoveFileAsync(std::string fname);
		AsyncOp<Status> CreateDirAsync(std::string dirname);
		AsyncOp<Result<std::size_t>> GetFileSizeAsync(std::string fname);

	private:
		AsyncRuntime* runtime_;
		Env* env_;
	};
}

#endif
