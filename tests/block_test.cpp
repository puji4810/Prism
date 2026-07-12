// Block and BlockBuilder iterator tests
#include "async_env.h"
#include "async_runtime.h"
#include "table/block.h"
#include "table/block_builder.h"
#include "table/format.h"
#include "comparator.h"
#include "options.h"
#include "coding.h"
#include "crc32.h"
#include "coro_task.h"
#include "dbformat.h"

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

using namespace prism;
using namespace prism::tests;

namespace
{

	static std::string Key(int i)
	{
		char buf[32];
		snprintf(buf, sizeof(buf), "k%06d", i);
		return std::string(buf);
	}

	static std::string Val(int i)
	{
		char buf[32];
		snprintf(buf, sizeof(buf), "v%06d", i);
		return std::string(buf);
	}

		class MemoryRandomAccessFile final: public RandomAccessFile
	{
	public:
		explicit MemoryRandomAccessFile(std::string contents)
		    : contents_(std::move(contents))
		{
		}

		Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override
		{
			if (offset >= contents_.size())
			{
				*result = Slice();
				return Status::OK();
			}

			const std::size_t read_size = std::min<std::size_t>(n, contents_.size() - static_cast<std::size_t>(offset));
			std::memcpy(scratch, contents_.data() + offset, read_size);
			*result = Slice(scratch, read_size);
			return Status::OK();
		}

	private:
			std::string contents_;
		};

		class ViewRandomAccessFile final: public RandomAccessFile
		{
		public:
			explicit ViewRandomAccessFile(std::string contents)
			    : contents_(std::move(contents))
			{
			}

			Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override
			{
				++read_calls_;
				if (offset >= contents_.size())
				{
					*result = Slice();
					return Status::OK();
				}
				const size_t read_size = std::min(n, contents_.size() - static_cast<size_t>(offset));
				std::memcpy(scratch, contents_.data() + offset, read_size);
				*result = Slice(scratch, read_size);
				return Status::OK();
			}

			Result<std::optional<Slice>> TryReadView(uint64_t offset, size_t n) const override
			{
				++view_calls_;
				if (offset > contents_.size() || n > contents_.size() - static_cast<size_t>(offset))
				{
					return std::unexpected(Status::InvalidArgument("view out of range"));
				}
				return std::optional<Slice>(std::in_place, contents_.data() + offset, n);
			}

			int read_calls() const { return read_calls_; }
			int view_calls() const { return view_calls_; }

		private:
			std::string contents_;
			mutable int read_calls_ = 0;
			mutable int view_calls_ = 0;
		};

	std::string EncodeRawBlock(std::string payload, CompressionType type, bool valid_checksum = true)
	{
		std::string block = std::move(payload);
		const char type_byte = static_cast<char>(type);
		block.push_back(type_byte);

		uint32_t crc = crc32c::Crc32c(reinterpret_cast<const uint8_t*>(block.data()), block.size() - 1);
		crc = crc32c::Extend(crc, reinterpret_cast<const uint8_t*>(&type_byte), 1);
		const std::size_t trailer_crc_offset = block.size();
		block.resize(block.size() + 4);
		EncodeFixed32(block.data() + trailer_crc_offset, Mask(valid_checksum ? crc : crc + 1));
		return block;
	}

	Task<Result<BlockContents>> ReadBlockAsyncForTest(
	    AsyncRandomAccessFile* file, const ReadOptions options, const BlockHandle handle)
	{
		co_return co_await ReadBlockAsync(*file, options, handle);
	}

	void ReleaseBlockContents(BlockContents* contents)
	{
		if (contents->heap_allocated)
		{
			delete[] contents->data.data();
		}
		contents->data = Slice();
		contents->heap_allocated = false;
	}

	struct BuiltBlock
	{
		Options options;
		BlockBuilder builder;
		std::vector<std::string> keys;
		std::vector<std::string> vals;
		Slice data;
		Block* block = nullptr; // owns no memory

		BuiltBlock(int restart_interval, int n)
		    : options()
		    , builder(&options)
		{
			options.comparator = BytewiseComparator();
			options.block_restart_interval = restart_interval;
			keys.reserve(n);
			vals.reserve(n);
			for (int i = 0; i < n; ++i)
			{
				keys.emplace_back(Key(i));
				vals.emplace_back(Val(i));
				builder.Add(Slice(keys.back()), Slice(vals.back()));
			}
			data = builder.Finish();
			BlockContents contents{ data, /*cachable=*/true, /*heap_allocated=*/false };
			block = new Block(contents);
		}

		~BuiltBlock() { delete block; }
	};

	struct BuiltInternalKeyBlock
	{
		InternalKeyComparator comparator{ BytewiseComparator() };
		Options options;
		BlockBuilder builder{ &options };
		std::vector<std::string> keys;
		Slice data;
		std::unique_ptr<Block> block;

		BuiltInternalKeyBlock()
		{
			options.comparator = &comparator;
			options.block_restart_interval = 2;
			auto add = [this](const char* user_key, SequenceNumber sequence) {
				InternalKey key(Slice(user_key), sequence, kTypeValue);
				keys.push_back(key.Encode().ToString());
				builder.Add(Slice(keys.back()), Slice("value"));
			};
			add("a", 100);
			add("a", 90);
			add("b", 100);
			add("c", 100);
			data = builder.Finish();
			BlockContents contents{ data, /*cachable=*/true, /*heap_allocated=*/false };
			block = std::make_unique<Block>(contents);
		}

		std::string Target(const char* user_key, SequenceNumber sequence) const
		{
			InternalKey key(Slice(user_key), sequence, kTypeValue);
			return key.Encode().ToString();
		}
	};

	TEST(BlockIterTest, IterateAllForward)
	{
		BuiltBlock bb(4, 10);
		std::unique_ptr<Iterator> it(bb.block->NewIterator(BytewiseComparator()));
		it->SeekToFirst();
		int i = 0;
		for (; it->Valid(); it->Next(), ++i)
		{
			EXPECT_EQ(it->key().ToString(), bb.keys[i]);
			EXPECT_EQ(it->value().ToString(), bb.vals[i]);
		}
		EXPECT_EQ(i, 10);
	}

	TEST(BlockIterTest, SeekAndPrev)
	{
		BuiltBlock bb(4, 10);
		std::unique_ptr<Iterator> it(bb.block->NewIterator(BytewiseComparator()));

		// Seek to exact key
		it->Seek(Slice(bb.keys[4]));
		ASSERT_TRUE(it->Valid());
		EXPECT_EQ(it->key().ToString(), bb.keys[4]);

		// Seek to between keys -> first >= target
		it->Seek(Slice("k000004a"));
		ASSERT_TRUE(it->Valid());
		EXPECT_EQ(it->key().ToString(), bb.keys[5]);

		// Next then Prev lands back
		it->Seek(Slice(bb.keys[5]));
		ASSERT_TRUE(it->Valid());
		it->Next();
		ASSERT_TRUE(it->Valid());
		it->Prev();
		ASSERT_TRUE(it->Valid());
		EXPECT_EQ(it->key().ToString(), bb.keys[5]);

		// Seek to last
		it->SeekToLast();
		ASSERT_TRUE(it->Valid());
		EXPECT_EQ(it->key().ToString(), bb.keys.back());

		// Seek past last -> invalid after linear scan in block
		it->Seek(Slice("z"));
		// Depending on implementation, seeking past last may leave iterator invalid
		// because there is no key >= "z" in this block
		EXPECT_FALSE(it->Valid());
	}

	TEST(BlockBuilderTest, RestartsEncoded)
	{
		const int N = 17;
		const int R = 4;
		BuiltBlock bb(R, N);

		// Decode num_restarts from the tail
		const char* base = bb.data.data();
		size_t len = bb.data.size();
		ASSERT_GE(len, sizeof(uint32_t));
		uint32_t num_restarts = DecodeFixed32(base + len - sizeof(uint32_t));
		uint32_t expected = (N + R - 1) / R; // ceil(N / R)
		EXPECT_EQ(num_restarts, expected);

		// Check that each restart offset points to an entry with shared=0
		const char* restarts_begin = base + len - (1 + num_restarts) * sizeof(uint32_t);
		const char* data_end = restarts_begin; // entries end where restarts begin
		for (uint32_t i = 0; i < num_restarts; ++i)
		{
			uint32_t off = DecodeFixed32(restarts_begin + i * sizeof(uint32_t));
			ASSERT_LT(off, static_cast<uint32_t>(data_end - base));
			const char* p = base + off;
			// Fast path varint peek
			uint8_t shared = static_cast<uint8_t>(p[0]);
			if (shared >= 128)
			{
				// Slow path: decode varint32
				uint32_t s = 0;
				const char* q = GetVarint32Ptr(p, data_end, &s);
				ASSERT_NE(q, nullptr);
				shared = static_cast<uint8_t>(s);
			}
			EXPECT_EQ(shared, 0) << "restart entry must have shared=0";
		}
	}

	TEST(BlockIterTest, SeekSupportsKeysLargerThanInlineBuffer)
	{
		Options options;
		options.comparator = BytewiseComparator();
		options.block_restart_interval = 2;
		BlockBuilder builder(&options);
		std::vector<std::string> keys{
		    std::string(192, 'a') + "0",
		    std::string(192, 'a') + "1",
		    std::string(192, 'b') + "0",
		};
		for (const auto& key : keys)
		{
			builder.Add(Slice(key), Slice("value"));
		}

		const Slice data = builder.Finish();
		BlockContents contents{ data, /*cachable=*/true, /*heap_allocated=*/false };
		Block block(contents);
		std::unique_ptr<Iterator> iter(block.NewIterator(BytewiseComparator()));
		iter->Seek(Slice(keys[1]));
		ASSERT_TRUE(iter->Valid());
		EXPECT_EQ(iter->key().ToString(), keys[1]);
		iter->Next();
		ASSERT_TRUE(iter->Valid());
		EXPECT_EQ(iter->key().ToString(), keys[2]);
	}

	TEST(BlockIterTest, InternalBytewiseSeekHonorsDescendingSequence)
	{
		BuiltInternalKeyBlock bb;
		std::unique_ptr<Iterator> it(bb.block->NewIterator(&bb.comparator));

		auto target = bb.Target("a", 95);
		it->Seek(Slice(target));
		ASSERT_TRUE(it->Valid());
		EXPECT_EQ(it->key(), Slice(bb.keys[1]));

		target = bb.Target("a", 110);
		it->Seek(Slice(target));
		ASSERT_TRUE(it->Valid());
		EXPECT_EQ(it->key(), Slice(bb.keys[0]));

		target = bb.Target("a", 80);
		it->Seek(Slice(target));
		ASSERT_TRUE(it->Valid());
		EXPECT_EQ(it->key(), Slice(bb.keys[2]));

		target = bb.Target("b", 100);
		it->Seek(Slice(target));
		ASSERT_TRUE(it->Valid());
		EXPECT_EQ(it->key(), Slice(bb.keys[2]));
	}

	TEST(ReadBlockAsyncTest, MatchesReadBlockForNoCompressionBlock)
	{
		const std::string payload = "raw-block-payload";
		const std::string encoded = EncodeRawBlock(payload, CompressionType::kNoCompression);
		auto backing = std::make_shared<MemoryRandomAccessFile>(encoded);

		BlockHandle handle;
		handle.set_offset(0);
		handle.set_size(payload.size());

		ReadOptions options;
		options.verify_checksums = true;

		BlockContents sync_contents;
		Status sync_status = ReadBlock(backing.get(), options, handle, &sync_contents);
		ASSERT_TRUE(sync_status.ok()) << sync_status.ToString();

		CpuThreadPool scheduler(2);
		AsyncRuntime runtime(scheduler);
		AsyncRandomAccessFile async_file(runtime, backing);

		auto async_result = ReadBlockAsyncForTest(&async_file, options, handle).SyncWait();
		ASSERT_TRUE(async_result.has_value()) << async_result.error().ToString();

		EXPECT_EQ(sync_contents.data.ToString(), payload);
		EXPECT_EQ(async_result.value().data.ToString(), payload);
		EXPECT_TRUE(sync_contents.heap_allocated);
		EXPECT_TRUE(async_result.value().heap_allocated);
		EXPECT_TRUE(sync_contents.cachable);
		EXPECT_TRUE(async_result.value().cachable);

		ReleaseBlockContents(&sync_contents);
		BlockContents async_contents = async_result.value();
		ReleaseBlockContents(&async_contents);
	}

	TEST(ReadBlockAsyncTest, ReportsTruncatedBlockRead)
	{
		const std::string payload = "short";
		const std::string encoded = EncodeRawBlock(payload, CompressionType::kNoCompression).substr(0, payload.size() + 2);
		auto backing = std::make_shared<MemoryRandomAccessFile>(encoded);

		BlockHandle handle;
		handle.set_offset(0);
		handle.set_size(payload.size());

		CpuThreadPool scheduler(2);
		AsyncRuntime runtime(scheduler);
		AsyncRandomAccessFile async_file(runtime, backing);

		ReadOptions options;
		auto result = ReadBlockAsyncForTest(&async_file, options, handle).SyncWait();
		ASSERT_FALSE(result.has_value());
		EXPECT_TRUE(result.error().IsCorruption()) << result.error().ToString();
	}

	TEST(ReadBlockAsyncTest, ReportsChecksumMismatch)
	{
		const std::string payload = "bad-checksum";
		auto backing = std::make_shared<MemoryRandomAccessFile>(
		    EncodeRawBlock(payload, CompressionType::kNoCompression, /*valid_checksum=*/false));

		BlockHandle handle;
		handle.set_offset(0);
		handle.set_size(payload.size());

		ReadOptions options;
		options.verify_checksums = true;

		CpuThreadPool scheduler(2);
		AsyncRuntime runtime(scheduler);
		AsyncRandomAccessFile async_file(runtime, backing);

		auto result = ReadBlockAsyncForTest(&async_file, options, handle).SyncWait();
		ASSERT_FALSE(result.has_value());
		EXPECT_TRUE(result.error().IsCorruption()) << result.error().ToString();
	}

	TEST(ReadBlockAsyncTest, ReportsUnsupportedCompression)
	{
		const std::string payload = "compressed";
		auto backing = std::make_shared<MemoryRandomAccessFile>(EncodeRawBlock(payload, CompressionType::kSnappyCompression));

		BlockHandle handle;
		handle.set_offset(0);
		handle.set_size(payload.size());

		ReadOptions options;
		options.verify_checksums = true;

		CpuThreadPool scheduler(2);
		AsyncRuntime runtime(scheduler);
		AsyncRandomAccessFile async_file(runtime, backing);

		auto result = ReadBlockAsyncForTest(&async_file, options, handle).SyncWait();
		ASSERT_FALSE(result.has_value());
		EXPECT_TRUE(result.error().IsNotSupportedError()) << result.error().ToString();
	}

	TEST(ReadBlockAsyncTest, StableViewCompletesInlineWithoutReadFallback)
	{
		const std::string payload = "mapped-block";
		ViewRandomAccessFile file(EncodeRawBlock(payload, CompressionType::kNoCompression));
		BlockHandle handle;
		handle.set_offset(0);
		handle.set_size(payload.size());
		ReadOptions options;
		options.fill_cache = false;

		CpuThreadPool scheduler(1);
		AsyncRuntime runtime(scheduler);
		std::optional<Result<BlockContents>> result;
		ReadBlockAsyncCallback(runtime, file, options, handle,
		    [&result](Result<BlockContents> contents) { result.emplace(std::move(contents)); });

		ASSERT_TRUE(result.has_value());
		ASSERT_TRUE(result->has_value()) << result->error().ToString();
		EXPECT_EQ(result->value().data.ToString(), payload);
		EXPECT_FALSE(result->value().heap_allocated);
		EXPECT_FALSE(result->value().cachable);
		EXPECT_EQ(file.view_calls(), 1);
		EXPECT_EQ(file.read_calls(), 0);
	}

	TEST(ReadBlockAsyncTest, StableViewCopiesInlineWhenCacheFillIsRequested)
	{
		const std::string payload = "cacheable-mapped-block";
		ViewRandomAccessFile file(EncodeRawBlock(payload, CompressionType::kNoCompression));
		BlockHandle handle;
		handle.set_offset(0);
		handle.set_size(payload.size());

		CpuThreadPool scheduler(1);
		AsyncRuntime runtime(scheduler);
		std::optional<Result<BlockContents>> result;
		ReadBlockAsyncCallback(runtime, file, ReadOptions(), handle,
		    [&result](Result<BlockContents> contents) { result.emplace(std::move(contents)); });

		ASSERT_TRUE(result.has_value());
		ASSERT_TRUE(result->has_value()) << result->error().ToString();
		EXPECT_EQ(result->value().data.ToString(), payload);
		EXPECT_TRUE(result->value().heap_allocated);
		EXPECT_TRUE(result->value().cachable);
		EXPECT_EQ(file.read_calls(), 0);
		BlockContents contents = result->value();
		ReleaseBlockContents(&contents);
	}

	TEST(ReadBlockAsyncTest, StableViewErrorDoesNotFallBackToRead)
	{
		ViewRandomAccessFile file("short");
		BlockHandle handle;
		handle.set_offset(0);
		handle.set_size(128);

		CpuThreadPool scheduler(1);
		AsyncRuntime runtime(scheduler);
		std::optional<Result<BlockContents>> result;
		ReadBlockAsyncCallback(runtime, file, ReadOptions(), handle,
		    [&result](Result<BlockContents> contents) { result.emplace(std::move(contents)); });

		ASSERT_TRUE(result.has_value());
		ASSERT_FALSE(result->has_value());
		EXPECT_TRUE(result->error().IsInvalidArgument()) << result->error().ToString();
		EXPECT_EQ(file.read_calls(), 0);
	}

} // namespace
