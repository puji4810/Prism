#include "table/format.h"
#include "async_env.h"
#include "coding.h"
#include <cstddef>
#include "crc32.h"
#include "options.h"

#include <atomic>
#include <cstring>
#include <functional>
#include <limits>
#include <optional>
#include <span>
#include <utility>

namespace prism
{
	namespace
	{
		Status DecodeBlockContents(
		    const ReadOptions& options,
		    const BlockHandle& handle,
		    const char* data,
		    size_t contents_size,
		    char* owned_buf,
		    BlockContents* result)
		{
			result->data = Slice{ };
			result->cachable = false;
			result->heap_allocated = false;

			const size_t n = static_cast<size_t>(handle.size());
			if (contents_size != n + kBlockTrailerSize)
			{
				delete[] owned_buf;
				return Status::Corruption("truncated block read");
			}

			if (options.verify_checksums)
			{
				const uint32_t crc = Unmask(DecodeFixed32(data + n + 1));
				const uint32_t actual = crc32c::Crc32c(data, n + 1);
				if (actual != crc)
				{
					delete[] owned_buf;
					return Status::Corruption("block checksum mismatch");
				}
			}

			switch (static_cast<CompressionType>(data[n]))
			{
			case CompressionType::kNoCompression:
				if (data == owned_buf)
				{
					result->data = Slice(owned_buf, n);
					result->heap_allocated = true;
					result->cachable = true;
				}
				else
				{
					delete[] owned_buf;
					result->data = Slice(data, n);
					result->heap_allocated = false;
					result->cachable = false;
				}
				return Status::OK();

			case CompressionType::kSnappyCompression:
				delete[] owned_buf;
				return Status::NotSupported("Snappy compression not supported");

			case CompressionType::kZstdCompression:
				delete[] owned_buf;
				return Status::NotSupported("Zstd compression not supported");

			default:
				delete[] owned_buf;
				return Status::Corruption("bad block type");
			}
		}
	}

	void BlockHandle::EncodeTo(std::string& dst) const
	{
		// Sanity check that all fields have been set
		assert(offset_ != ~static_cast<uint64_t>(0));
		assert(size_ != ~static_cast<uint64_t>(0));
		PutVarint64(dst, offset_);
		PutVarint64(dst, size_);
	}

	Status BlockHandle::DecodeFrom(Slice& input)
	{
		if (GetVarint64(&input, &offset_) && GetVarint64(&input, &size_))
		{
			return Status::OK();
		}
		else
		{
			return Status::Corruption("bad block handle");
		}
	}

	void Footer::EncodeTo(std::string& dst) const
	{
		const size_t original_size = dst.size();
		metaindex_handle_.EncodeTo(dst);
		index_handle_.EncodeTo(dst);
		dst.resize(2 * BlockHandle::kMaxEncodedLength); // Padding
		PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber & 0xffffffffu));
		PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32));
		assert(dst.size() == original_size + kEncodedLength);
		(void)original_size; // Disable unused variable warning.
	}

	Status Footer::DecodeFrom(Slice& input)
	{
		if (input.size() < kEncodedLength)
		{
			return Status::Corruption("not an sstable (footer too short)");
		}
		const char* magic_ptr = input.data() + kEncodedLength - 8;
		const uint32_t magic_lo = DecodeFixed32(magic_ptr);
		const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
		const uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) | (static_cast<uint64_t>(magic_lo)));
		if (magic != kTableMagicNumber)
		{
			return Status::Corruption("not an sstable (bad magic number)");
		}
		Status result = metaindex_handle_.DecodeFrom(input);
		if (result.ok())
		{
			result = index_handle_.DecodeFrom(input);
		}
		if (result.ok())
		{
			// We skip over any leftover data (just padding for now) in "input"
			const char* end = magic_ptr + 8;
			input = Slice(end, input.data() + input.size() - end);
		}
		return result;
	}

	Status ReadBlock(RandomAccessFile* file, const ReadOptions& options, const BlockHandle& handle, BlockContents* result)
	{
		result->data = Slice{ };
		result->cachable = false;
		result->heap_allocated = false;

		size_t n = static_cast<size_t>(handle.size());
		char* buf = new char[n + kBlockTrailerSize]; // add the size of trailer(1 type + 4 crc)
		Slice contents;
		Status s = file->Read(
		    handle.offset(), n + kBlockTrailerSize, &contents, buf); // read n + 5, instead of just read n for verify, just 1 IO operation
		if (!s.ok())
		{
			delete[] buf;
			return s;
		}
		return DecodeBlockContents(options, handle, contents.data(), contents.size(), buf, result);
	}

	struct AsyncBlockReadOp::State
	{
		static constexpr int kSuspending = 0;
		static constexpr int kCompleted = 1;
		static constexpr int kSuspended = 2;

		const AsyncRandomAccessFile* file = nullptr;
		ReadOptions options;
		BlockHandle handle;
		std::optional<Result<BlockContents>> result;
		std::exception_ptr exception;
		std::coroutine_handle<> continuation;
		std::atomic<int> status{ kSuspending };

		State(const AsyncRandomAccessFile& file_arg, ReadOptions options_arg, BlockHandle handle_arg)
		    : file(&file_arg)
		    , options(std::move(options_arg))
		    , handle(handle_arg)
		{
		}

		void Finish()
		{
			auto expected = kSuspending;
			if (status.compare_exchange_strong(expected, kCompleted, std::memory_order_acq_rel, std::memory_order_acquire))
			{
				return;
			}
			continuation.resume();
		}

		bool TrySuspend()
		{
			auto expected = kSuspending;
			return status.compare_exchange_strong(expected, kSuspended, std::memory_order_acq_rel, std::memory_order_acquire);
		}

		void Start(std::shared_ptr<State> self)
		{
			ReadBlockAsyncCallback(*file, options, handle, [self = std::move(self)](Result<BlockContents> read_result) mutable {
				try
				{
					self->result = std::move(read_result);
				}
				catch (...)
				{
					self->exception = std::current_exception();
				}
				self->Finish();
			});
		}
	};

	AsyncBlockReadOp::AsyncBlockReadOp(const AsyncRandomAccessFile& file, ReadOptions options, BlockHandle handle)
	    : state_(std::make_shared<State>(file, std::move(options), handle))
	{
	}

	AsyncBlockReadOp::~AsyncBlockReadOp() = default;
	AsyncBlockReadOp::AsyncBlockReadOp(AsyncBlockReadOp&&) noexcept = default;
	AsyncBlockReadOp& AsyncBlockReadOp::operator=(AsyncBlockReadOp&&) noexcept = default;
	AsyncBlockReadOp::Awaiter::~Awaiter() = default;

	bool AsyncBlockReadOp::Awaiter::await_ready() const noexcept
	{
		return state->status.load(std::memory_order_acquire) == State::kCompleted;
	}

	bool AsyncBlockReadOp::Awaiter::await_suspend(std::coroutine_handle<> handle) const
	{
		state->continuation = handle;
		state->Start(state);
		return state->TrySuspend();
	}

	Result<BlockContents> AsyncBlockReadOp::Awaiter::await_resume() const
	{
		if (state->exception)
		{
			std::rethrow_exception(state->exception);
		}
		return std::move(*state->result);
	}

	AsyncBlockReadOp::Awaiter AsyncBlockReadOp::operator co_await() && noexcept { return Awaiter{ std::move(state_) }; }

	AsyncBlockReadOp ReadBlockAsync(const AsyncRandomAccessFile& file, const ReadOptions& options, const BlockHandle& handle)
	{
		return AsyncBlockReadOp(file, options, handle);
	}

	namespace
	{
		struct AsyncBlockCallbackState
		{
			ReadOptions options;
			BlockHandle handle;
			std::unique_ptr<char[]> buffer;
			AsyncBlockReadCallback completion;
		};

		void FinishBlockReadCallback(std::shared_ptr<AsyncBlockCallbackState> state, Result<std::size_t> bytes)
		{
			if (!bytes.has_value())
			{
				state->buffer.reset();
				state->completion(std::unexpected(bytes.error()));
				return;
			}

			BlockContents contents;
			char* raw = state->buffer.release();
			Status s = DecodeBlockContents(state->options, state->handle, raw, bytes.value(), raw, &contents);
			if (!s.ok())
			{
				state->completion(std::unexpected(s));
				return;
			}
			state->completion(contents);
		}

		std::shared_ptr<AsyncBlockCallbackState> MakeBlockReadCallbackState(
		    const ReadOptions& options, const BlockHandle& handle, AsyncBlockReadCallback completion)
		{
			const size_t n = static_cast<size_t>(handle.size());
			auto state = std::make_shared<AsyncBlockCallbackState>();
			state->options = options;
			state->handle = handle;
			state->buffer.reset(new char[n + kBlockTrailerSize]);
			state->completion = std::move(completion);
			return state;
		}
	}

	void ReadBlockAsyncCallback(
	    const AsyncRandomAccessFile& file, const ReadOptions& options, const BlockHandle& handle, AsyncBlockReadCallback completion)
	{
		const size_t n = static_cast<size_t>(handle.size());
		auto state = MakeBlockReadCallbackState(options, handle, std::move(completion));

		auto dst = std::as_writable_bytes(std::span(state->buffer.get(), n + kBlockTrailerSize));
		file.ReadAtAsyncCallback(handle.offset(), dst, [state = std::move(state)](Result<std::size_t> bytes) mutable {
			FinishBlockReadCallback(std::move(state), std::move(bytes));
		});
	}

	void ReadBlockAsyncCallback(
	    AsyncRuntime& runtime, RandomAccessFile& file, const ReadOptions& options, const BlockHandle& handle, AsyncBlockReadCallback completion)
	{
		if (handle.size() > std::numeric_limits<size_t>::max() - kBlockTrailerSize)
		{
			completion(std::unexpected(Status::Corruption("block size too large")));
			return;
		}

		const size_t n = static_cast<size_t>(handle.size());
		const size_t read_size = n + kBlockTrailerSize;
		auto view = file.TryReadView(handle.offset(), read_size);
		if (!view.has_value())
		{
			completion(std::unexpected(view.error()));
			return;
		}
		if (view.value().has_value())
		{
			const Slice mapped = *view.value();
			if (mapped.size() != read_size)
			{
				completion(std::unexpected(Status::Corruption("truncated block view")));
				return;
			}
			char* owned = nullptr;
			const char* data = mapped.data();
			if (options.fill_cache)
			{
				owned = new char[read_size];
				std::memcpy(owned, data, read_size);
				data = owned;
			}

			BlockContents contents;
			Status s = DecodeBlockContents(options, handle, data, mapped.size(), owned, &contents);
			if (!s.ok())
			{
				completion(std::unexpected(s));
				return;
			}
			completion(contents);
			return;
		}

		auto state = MakeBlockReadCallbackState(options, handle, std::move(completion));

		auto dst = std::as_writable_bytes(std::span(state->buffer.get(), read_size));
		ReadAtAsyncCallback(runtime, file, handle.offset(), dst, [state = std::move(state)](Result<std::size_t> bytes) mutable {
			FinishBlockReadCallback(std::move(state), std::move(bytes));
		});
	}
}
