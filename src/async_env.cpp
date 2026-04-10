#include "async_env.h"

#include <condition_variable>
#include <mutex>
#include <utility>

namespace prism
{
	AsyncRandomAccessFile::AsyncRandomAccessFile(ThreadPoolScheduler& scheduler, std::shared_ptr<RandomAccessFile> file)
	    : scheduler_(&scheduler)
	    , file_(std::move(file))
	{
	}

	AsyncRandomAccessFile::~AsyncRandomAccessFile() = default;

	AsyncOp<Result<std::size_t>> AsyncRandomAccessFile::ReadAtAsync(uint64_t offset, std::span<std::byte> dst) const
	{
		return AsyncOp<Result<std::size_t>>(*scheduler_, [file = file_, offset, dst]() { return file->ReadAt(offset, dst); });
	}

	AsyncOp<Result<std::string>> AsyncRandomAccessFile::ReadAtStringAsync(uint64_t offset, std::size_t n) const
	{
		return AsyncOp<Result<std::string>>(*scheduler_, [file = file_, offset, n]() -> Result<std::string> {
			std::string buf;
			buf.resize(n);
			auto r = file->ReadAt(offset, std::as_writable_bytes(std::span(buf)));
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
	    , file_(std::shared_ptr<WritableFile>(std::move(file)))
	    , serial_(std::make_shared<SerialState>())
	{
	}

	AsyncWritableFile::~AsyncWritableFile() = default;

	AsyncOp<Status> AsyncWritableFile::AppendAsync(std::string data)
	{
		auto file = file_;
		auto state = serial_;
		uint64_t my_ticket;
		{
			std::lock_guard lock(state->mu);
			if (state->closed)
				return AsyncOp<Status>(*scheduler_, [] { return Status::IOError("file closed"); });
			my_ticket = state->next_ticket++;
		}
		return AsyncOp<Status>(*scheduler_, [file, state, my_ticket, data = std::move(data)]() {
			{
				std::unique_lock lock(state->mu);
				state->cv.wait(lock, [&] { return my_ticket == state->now_serving; });
				if (state->closed)
				{
					state->now_serving++;
					state->cv.notify_all();
					return Status::IOError("file closed");
				}
			}
			auto s = file->Append(Slice(data));
			{
				std::lock_guard lock(state->mu);
				state->now_serving++;
				state->cv.notify_all();
			}
			return s;
		});
	}

	AsyncOp<Status> AsyncWritableFile::FlushAsync()
	{
		auto file = file_;
		auto state = serial_;
		uint64_t my_ticket;
		{
			std::lock_guard lock(state->mu);
			if (state->closed)
				return AsyncOp<Status>(*scheduler_, [] { return Status::IOError("file closed"); });
			my_ticket = state->next_ticket++;
		}
		return AsyncOp<Status>(*scheduler_, [file, state, my_ticket]() {
			{
				std::unique_lock lock(state->mu);
				state->cv.wait(lock, [&] { return my_ticket == state->now_serving; });
				if (state->closed)
				{
					state->now_serving++;
					state->cv.notify_all();
					return Status::IOError("file closed");
				}
			}
			auto s = file->Flush();
			{
				std::lock_guard lock(state->mu);
				state->now_serving++;
				state->cv.notify_all();
			}
			return s;
		});
	}

	AsyncOp<Status> AsyncWritableFile::SyncAsync()
	{
		auto file = file_;
		auto state = serial_;
		uint64_t my_ticket;
		{
			std::lock_guard lock(state->mu);
			if (state->closed)
				return AsyncOp<Status>(*scheduler_, [] { return Status::IOError("file closed"); });
			my_ticket = state->next_ticket++;
		}
		return AsyncOp<Status>(*scheduler_, [file, state, my_ticket]() {
			{
				std::unique_lock lock(state->mu);
				state->cv.wait(lock, [&] { return my_ticket == state->now_serving; });
				if (state->closed)
				{
					state->now_serving++;
					state->cv.notify_all();
					return Status::IOError("file closed");
				}
			}
			auto s = file->Sync();
			{
				std::lock_guard lock(state->mu);
				state->now_serving++;
				state->cv.notify_all();
			}
			return s;
		});
	}

	AsyncOp<Status> AsyncWritableFile::CloseAsync()
	{
		auto file = file_;
		auto state = serial_;
		uint64_t my_ticket;
		{
			std::lock_guard lock(state->mu);
			if (state->closed)
				return AsyncOp<Status>(*scheduler_, [] { return Status::IOError("file closed"); });
			my_ticket = state->next_ticket++;
		}
		return AsyncOp<Status>(*scheduler_, [file, state, my_ticket]() {
			{
				std::unique_lock lock(state->mu);
				state->cv.wait(lock, [&] { return my_ticket == state->now_serving; });
				if (state->closed)
				{
					state->now_serving++;
					state->cv.notify_all();
					return Status::IOError("file closed");
				}
			}
			auto s = file->Close();
			{
				std::lock_guard lock(state->mu);
				state->closed = true;
				state->now_serving++;
				state->cv.notify_all();
			}
			return s;
		});
	}

	AsyncEnv::AsyncEnv(ThreadPoolScheduler& scheduler, Env* env)
	    : scheduler_(&scheduler)
	    , env_(env)
	{
	}

	AsyncOp<Result<AsyncRandomAccessFile>> AsyncEnv::NewRandomAccessFileAsync(std::string fname)
	{
		auto env = env_;
		auto scheduler = scheduler_;
		return AsyncOp<Result<AsyncRandomAccessFile>>(
		    *scheduler_, [env, scheduler, fname = std::move(fname)]() -> Result<AsyncRandomAccessFile> {
			    auto f = env->NewRandomAccessFile(fname);
			    if (!f.has_value())
			    {
				    return std::unexpected(f.error());
			    }
			    return AsyncRandomAccessFile(*scheduler, std::shared_ptr<RandomAccessFile>(std::move(f.value())));
		    });
	}

	AsyncOp<Result<AsyncWritableFile>> AsyncEnv::NewWritableFileAsync(std::string fname)
	{
		auto env = env_;
		auto scheduler = scheduler_;
		return AsyncOp<Result<AsyncWritableFile>>(*scheduler_, [env, scheduler, fname = std::move(fname)]() -> Result<AsyncWritableFile> {
			auto f = env->NewWritableFile(fname);
			if (!f.has_value())
			{
				return std::unexpected(f.error());
			}
			return AsyncWritableFile(*scheduler, std::move(f.value()));
		});
	}

	AsyncOp<Result<AsyncWritableFile>> AsyncEnv::NewAppendableFileAsync(std::string fname)
	{
		auto env = env_;
		auto scheduler = scheduler_;
		return AsyncOp<Result<AsyncWritableFile>>(*scheduler_, [env, scheduler, fname = std::move(fname)]() -> Result<AsyncWritableFile> {
			auto f = env->NewAppendableFile(fname);
			if (!f.has_value())
			{
				return std::unexpected(f.error());
			}
			return AsyncWritableFile(*scheduler, std::move(f.value()));
		});
	}

	AsyncOp<Status> AsyncEnv::RemoveFileAsync(std::string fname)
	{
		auto env = env_;
		return AsyncOp<Status>(*scheduler_, [env, fname = std::move(fname)]() { return env->RemoveFile(fname); });
	}

	AsyncOp<Status> AsyncEnv::CreateDirAsync(std::string dirname)
	{
		auto env = env_;
		return AsyncOp<Status>(*scheduler_, [env, dirname = std::move(dirname)]() { return env->CreateDir(dirname); });
	}

	AsyncOp<Result<std::size_t>> AsyncEnv::GetFileSizeAsync(std::string fname)
	{
		auto env = env_;
		return AsyncOp<Result<std::size_t>>(*scheduler_, [env, fname = std::move(fname)]() { return env->GetFileSize(fname); });
	}
}
