#include "async_env.h"

#include <utility>

namespace prism
{
	AsyncRandomAccessFile::AsyncRandomAccessFile(ThreadPoolScheduler& scheduler, std::unique_ptr<RandomAccessFile> file)
	    : scheduler_(&scheduler)
	    , file_(std::move(file))
	{
	}

	AsyncRandomAccessFile::~AsyncRandomAccessFile() = default;

	AsyncOp<Result<std::size_t>> AsyncRandomAccessFile::ReadAtAsync(uint64_t offset, std::span<std::byte> dst) const
	{
		return AsyncOp<Result<std::size_t>>(*scheduler_, [this, offset, dst]() mutable { return file_->ReadAt(offset, dst); });
	}

	AsyncOp<Result<std::string>> AsyncRandomAccessFile::ReadAtStringAsync(uint64_t offset, std::size_t n) const
	{
		return AsyncOp<Result<std::string>>(*scheduler_, [this, offset, n]() mutable -> Result<std::string> {
			std::string buf;
			buf.resize(n);
			auto r = file_->ReadAt(offset, std::as_writable_bytes(std::span(buf)));
			if (!r.has_value())
			{
				return std::unexpected(r.error());
			}
			buf.resize(r.value());
			return buf;
		});
	}

	AsyncWritableFile::AsyncWritableFile(ThreadPoolScheduler& scheduler, std::unique_ptr<WritableFile> file)
	    : scheduler_(&scheduler)
	    , file_(std::move(file))
	{
	}

	AsyncWritableFile::~AsyncWritableFile() = default;

	AsyncOp<Status> AsyncWritableFile::AppendAsync(std::string data)
	{
		return AsyncOp<Status>(*scheduler_, [this, data = std::move(data)]() mutable { return file_->Append(Slice(data)); });
	}

	AsyncOp<Status> AsyncWritableFile::FlushAsync()
	{
		return AsyncOp<Status>(*scheduler_, [this] { return file_->Flush(); });
	}

	AsyncOp<Status> AsyncWritableFile::SyncAsync()
	{
		return AsyncOp<Status>(*scheduler_, [this] { return file_->Sync(); });
	}

	AsyncOp<Status> AsyncWritableFile::CloseAsync()
	{
		return AsyncOp<Status>(*scheduler_, [this] { return file_->Close(); });
	}

	AsyncEnv::AsyncEnv(ThreadPoolScheduler& scheduler, Env* env)
	    : scheduler_(&scheduler)
	    , env_(env)
	{
	}

	AsyncOp<Result<std::unique_ptr<AsyncRandomAccessFile>>> AsyncEnv::NewRandomAccessFileAsync(std::string fname)
	{
		return AsyncOp<Result<std::unique_ptr<AsyncRandomAccessFile>>>(
		    *scheduler_, [this, fname = std::move(fname)]() mutable -> Result<std::unique_ptr<AsyncRandomAccessFile>> {
			    auto f = env_->NewRandomAccessFile(fname);
			    if (!f.has_value())
			    {
				    return std::unexpected(f.error());
			    }
			    return std::make_unique<AsyncRandomAccessFile>(*scheduler_, std::move(f.value()));
		    });
	}

	AsyncOp<Result<std::unique_ptr<AsyncWritableFile>>> AsyncEnv::NewWritableFileAsync(std::string fname)
	{
		return AsyncOp<Result<std::unique_ptr<AsyncWritableFile>>>(
		    *scheduler_, [this, fname = std::move(fname)]() mutable -> Result<std::unique_ptr<AsyncWritableFile>> {
			    auto f = env_->NewWritableFile(fname);
			    if (!f.has_value())
			    {
				    return std::unexpected(f.error());
			    }
			    return std::make_unique<AsyncWritableFile>(*scheduler_, std::move(f.value()));
		    });
	}

	AsyncOp<Result<std::unique_ptr<AsyncWritableFile>>> AsyncEnv::NewAppendableFileAsync(std::string fname)
	{
		return AsyncOp<Result<std::unique_ptr<AsyncWritableFile>>>(
		    *scheduler_, [this, fname = std::move(fname)]() mutable -> Result<std::unique_ptr<AsyncWritableFile>> {
			    auto f = env_->NewAppendableFile(fname);
			    if (!f.has_value())
			    {
				    return std::unexpected(f.error());
			    }
			    return std::make_unique<AsyncWritableFile>(*scheduler_, std::move(f.value()));
		    });
	}

	AsyncOp<Status> AsyncEnv::RemoveFileAsync(std::string fname)
	{
		return AsyncOp<Status>(*scheduler_, [this, fname = std::move(fname)]() mutable { return env_->RemoveFile(fname); });
	}

	AsyncOp<Status> AsyncEnv::CreateDirAsync(std::string dirname)
	{
		return AsyncOp<Status>(*scheduler_, [this, dirname = std::move(dirname)]() mutable { return env_->CreateDir(dirname); });
	}

	AsyncOp<Result<std::size_t>> AsyncEnv::GetFileSizeAsync(std::string fname)
	{
		return AsyncOp<Result<std::size_t>>(*scheduler_, [this, fname = std::move(fname)]() mutable { return env_->GetFileSize(fname); });
	}
}
