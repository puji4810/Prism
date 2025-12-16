// Block and BlockBuilder iterator tests
#include "table/block.h"
#include "table/block_builder.h"
#include "table/format.h"
#include "comparator.h"
#include "options.h"
#include "coding.h"

#include <gtest/gtest.h>
#include <string>
#include <vector>

using namespace prism;

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

} // namespace

int main(int argc, char** argv)
{
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
