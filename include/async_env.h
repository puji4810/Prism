#ifndef PRISM_ASYNC_ENV_H
#define PRISM_ASYNC_ENV_H

#include "async_op.h"
#include "env.h"

#include <cstddef>
#include <memory>
#include <span>
#include <string>

namespace prism
{
	class AsyncRandomAccessFile
	{
	public:
		AsyncRandomAccessFile(ThreadPoolScheduler& scheduler, std::unique_ptr<RandomAccessFile> file);
		~AsyncRandomAccessFile();

		AsyncRandomAccessFile(const AsyncRandomAccessFile&) = delete;
		AsyncRandomAccessFile& operator=(const AsyncRandomAccessFile&) = delete;
		AsyncRandomAccessFile(AsyncRandomAccessFile&&) = default;
		AsyncRandomAccessFile& operator=(AsyncRandomAccessFile&&) = default;

		AsyncOp<Result<std::size_t>> ReadAtAsync(uint64_t offset, std::span<std::byte> dst) const;
		AsyncOp<Result<std::string>> ReadAtStringAsync(uint64_t offset, std::size_t n) const;

	private:
		ThreadPoolScheduler* scheduler_;
		std::unique_ptr<RandomAccessFile> file_;
	};

	class AsyncWritableFile
	{
	public:
		AsyncWritableFile(ThreadPoolScheduler& scheduler, std::unique_ptr<WritableFile> file);
		~AsyncWritableFile();

		AsyncWritableFile(const AsyncWritableFile&) = delete;
		AsyncWritableFile& operator=(const AsyncWritableFile&) = delete;
		AsyncWritableFile(AsyncWritableFile&&) = default;
		AsyncWritableFile& operator=(AsyncWritableFile&&) = default;

		AsyncOp<Status> AppendAsync(std::string data);
		AsyncOp<Status> FlushAsync();
		AsyncOp<Status> SyncAsync();
		AsyncOp<Status> CloseAsync();

	private:
		ThreadPoolScheduler* scheduler_;
		std::unique_ptr<WritableFile> file_;
	};

	class AsyncEnv
	{
	public:
		AsyncEnv(ThreadPoolScheduler& scheduler, Env* env);

		AsyncOp<Result<std::unique_ptr<AsyncRandomAccessFile>>> NewRandomAccessFileAsync(std::string fname);
		AsyncOp<Result<std::unique_ptr<AsyncWritableFile>>> NewWritableFileAsync(std::string fname);
		AsyncOp<Result<std::unique_ptr<AsyncWritableFile>>> NewAppendableFileAsync(std::string fname);

		AsyncOp<Status> RemoveFileAsync(std::string fname);
		AsyncOp<Status> CreateDirAsync(std::string dirname);
		AsyncOp<Result<std::size_t>> GetFileSizeAsync(std::string fname);

	private:
		ThreadPoolScheduler* scheduler_;
		Env* env_;
	};
}

#endif
