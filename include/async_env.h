#ifndef PRISM_ASYNC_ENV_H
#define PRISM_ASYNC_ENV_H

#include "async_op.h"
#include "env.h"

#include <cstddef>
#include <cstdint>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <span>
#include <string>

namespace prism
{
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
		AsyncRandomAccessFile(ThreadPoolScheduler& scheduler, std::shared_ptr<RandomAccessFile> file);
		~AsyncRandomAccessFile();

		AsyncRandomAccessFile(const AsyncRandomAccessFile&) = delete;
		AsyncRandomAccessFile& operator=(const AsyncRandomAccessFile&) = delete;
		AsyncRandomAccessFile(AsyncRandomAccessFile&&) = default;
		AsyncRandomAccessFile& operator=(AsyncRandomAccessFile&&) = default;

		// ReadAtAsync: Zero-copy read into caller-owned buffer.
		// Precondition: dst buffer must remain valid until the AsyncOp completes.
		// (i.e., until the awaiting coroutine resumes).
		// Returns number of bytes actually read.
		AsyncOp<Result<std::size_t>> ReadAtAsync(uint64_t offset, std::span<std::byte> dst) const;

		// ReadAtStringAsync: Convenience method that allocates and returns std::string.
		// Simpler to use but involves heap allocation.
		AsyncOp<Result<std::string>> ReadAtStringAsync(uint64_t offset, std::size_t n) const;

	private:
		ThreadPoolScheduler* scheduler_;
		std::shared_ptr<RandomAccessFile> file_;
	};

	// AsyncWritableFile: Asynchronous wrapper for writable files (append-only).
	//
	// Serialization Contract:
	// - All async operations execute in FIFO (submission) order regardless of thread scheduling.
	// - A ticket-based SerialState ensures each lambda waits until prior ops complete before running.
	// - After CloseAsync completes, subsequent Append/Flush/Sync ops return IOError("file closed").
	//
	// Lifetime: file_ is held via shared_ptr; wrapper may be destroyed while ops are in-flight.
	class AsyncWritableFile
	{
	public:
		AsyncWritableFile(ThreadPoolScheduler& scheduler, std::unique_ptr<WritableFile> file);
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
		// SerialState: Ticket-queue for FIFO ordering. Shared by all lambdas submitted from this wrapper.
		struct SerialState
		{
			std::mutex mu;
			std::condition_variable cv;
			uint64_t next_ticket{ 0 }; // incremented at call-time (under mu)
			uint64_t now_serving{ 0 }; // incremented after each op completes
			bool closed{ false };
		};

		ThreadPoolScheduler* scheduler_;
		std::shared_ptr<WritableFile> file_;
		std::shared_ptr<SerialState> serial_;
	};

	// AsyncEnv: Asynchronous filesystem operations.
	//
	// Current Implementation:
	// - Wraps synchronous Env operations in thread pool tasks
	// - All operations block a thread pool thread during execution
	//
	// Future Migration Path:
	// - Replace with io_uring (Linux) / kqueue (BSD) / IOCP (Windows)
	// - AsyncOp interface remains unchanged, only internal implementation changes
	class AsyncEnv
	{
	public:
		AsyncEnv(ThreadPoolScheduler& scheduler, Env* env);

		// All file factory methods return by-value handles (e.g., Result<AsyncWritableFile>).
		// The previous unique_ptr-returning signatures are deprecated.
		AsyncOp<Result<AsyncRandomAccessFile>> NewRandomAccessFileAsync(std::string fname);
		AsyncOp<Result<AsyncWritableFile>> NewWritableFileAsync(std::string fname);
		AsyncOp<Result<AsyncWritableFile>> NewAppendableFileAsync(std::string fname);

		AsyncOp<Status> RemoveFileAsync(std::string fname);
		AsyncOp<Status> CreateDirAsync(std::string dirname);
		AsyncOp<Result<std::size_t>> GetFileSizeAsync(std::string fname);

	private:
		ThreadPoolScheduler* scheduler_;
		Env* env_;
	};
}

#endif
