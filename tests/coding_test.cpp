#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "coding.h"

using prism::DecodeFixed32;
using prism::DecodeFixed64;
using prism::EncodeVarint32;
using prism::EncodeVarint64;
using prism::GetLengthPrefixedSlice;
using prism::GetVarint32Ptr;
using prism::GetVarint64Ptr;
using prism::PutFixed32;
using prism::PutFixed64;
using prism::PutLengthPrefixedSlice;
using prism::PutVarint32;
using prism::PutVarint64;
using prism::Slice;
using prism::VarintLength;

TEST(CodingTest, Fixed32)
{
	std::string s;
	for (uint32_t v = 0; v < 100000; v++)
	{
		PutFixed32(s, v);
	}
	const char* p = s.data();
	for (uint32_t v = 0; v < 100000; v++)
	{
		uint32_t actual = DecodeFixed32(p);
		EXPECT_EQ(v, actual);
		p += sizeof(uint32_t);
	}
}

TEST(CodingTest, Fixed64)
{
	std::string s;
	for (int power = 0; power <= 63; power++)
	{
		uint64_t v = static_cast<uint64_t>(1) << power;
		PutFixed64(s, v - 1);
		PutFixed64(s, v + 0);
		PutFixed64(s, v + 1);
	}

	const char* p = s.data();
	for (int power = 0; power <= 63; power++)
	{
		uint64_t v = static_cast<uint64_t>(1) << power;
		uint64_t actual;
		actual = DecodeFixed64(p);
		EXPECT_EQ(v - 1, actual);
		p += sizeof(uint64_t);

		actual = DecodeFixed64(p);
		EXPECT_EQ(v + 0, actual);
		p += sizeof(uint64_t);

		actual = DecodeFixed64(p);
		EXPECT_EQ(v + 1, actual);
		p += sizeof(uint64_t);
	}
}

TEST(CodingTest, EncodingOutput)
{
	std::string dst;
	PutFixed32(dst, 0x04030201);
	EXPECT_EQ(4u, dst.size());
	EXPECT_EQ(0x01, static_cast<int>(dst[0]));
	EXPECT_EQ(0x02, static_cast<int>(dst[1]));
	EXPECT_EQ(0x03, static_cast<int>(dst[2]));
	EXPECT_EQ(0x04, static_cast<int>(dst[3]));

	dst.clear();
	PutFixed64(dst, 0x0807060504030201ull);
	EXPECT_EQ(8u, dst.size());
	EXPECT_EQ(0x01, static_cast<int>(dst[0]));
	EXPECT_EQ(0x02, static_cast<int>(dst[1]));
	EXPECT_EQ(0x03, static_cast<int>(dst[2]));
	EXPECT_EQ(0x04, static_cast<int>(dst[3]));
	EXPECT_EQ(0x05, static_cast<int>(dst[4]));
	EXPECT_EQ(0x06, static_cast<int>(dst[5]));
	EXPECT_EQ(0x07, static_cast<int>(dst[6]));
	EXPECT_EQ(0x08, static_cast<int>(dst[7]));
}

TEST(CodingTest, Varint32)
{
	std::string s;
	for (uint32_t i = 0; i < (32 * 32); i++)
	{
		uint32_t v = (i / 32) << (i % 32);
		PutVarint32(s, v);
	}

	const char* p = s.data();
	const char* limit = p + s.size();
	for (uint32_t i = 0; i < (32 * 32); i++)
	{
		uint32_t expected = (i / 32) << (i % 32);
		uint32_t actual;
		const char* start = p;
		p = GetVarint32Ptr(p, limit, &actual);
		EXPECT_NE(nullptr, p);
		EXPECT_EQ(expected, actual);
		EXPECT_EQ(static_cast<int>(VarintLength(actual)), p - start);
	}
	EXPECT_EQ(s.data() + s.size(), p);
}

TEST(CodingTest, Varint64)
{
	std::vector<uint64_t> values;
	values.push_back(0);
	values.push_back(100);
	values.push_back(~static_cast<uint64_t>(0));
	values.push_back(~static_cast<uint64_t>(0) - 1);
	for (uint32_t k = 0; k < 64; k++)
	{
		const uint64_t power = 1ull << k;
		values.push_back(power);
		values.push_back(power - 1);
		values.push_back(power + 1);
	}

	std::string s;
	for (size_t i = 0; i < values.size(); i++)
	{
		PutVarint64(s, values[i]);
	}

	const char* p = s.data();
	const char* limit = p + s.size();
	for (size_t i = 0; i < values.size(); i++)
	{
		EXPECT_LT(p, limit);
		uint64_t actual;
		const char* start = p;
		p = GetVarint64Ptr(p, limit, &actual);
		EXPECT_NE(nullptr, p);
		EXPECT_EQ(values[i], actual);
		EXPECT_EQ(static_cast<int>(VarintLength(actual)), p - start);
	}
	EXPECT_EQ(limit, p);
}

TEST(CodingTest, Varint32Overflow)
{
	uint32_t result = 0;
	std::string input("\x81\x82\x83\x84\x85\x11");
	EXPECT_EQ(nullptr, GetVarint32Ptr(input.data(), input.data() + input.size(), &result));
}

TEST(CodingTest, Varint32Truncation)
{
	uint32_t large_value = (1u << 31) + 100;
	std::string s;
	PutVarint32(s, large_value);
	uint32_t result = 0;
	for (size_t len = 0; len < s.size() - 1; len++)
	{
		EXPECT_EQ(nullptr, GetVarint32Ptr(s.data(), s.data() + len, &result));
	}
	EXPECT_NE(nullptr, GetVarint32Ptr(s.data(), s.data() + s.size(), &result));
	EXPECT_EQ(large_value, result);
}

TEST(CodingTest, Varint64Overflow)
{
	uint64_t result = 0;
	std::string input("\x81\x82\x83\x84\x85\x81\x82\x83\x84\x85\x11");
	EXPECT_EQ(nullptr, GetVarint64Ptr(input.data(), input.data() + input.size(), &result));
}

TEST(CodingTest, Varint64Truncation)
{
	uint64_t large_value = (1ull << 63) + 100ull;
	std::string s;
	PutVarint64(s, large_value);
	uint64_t result = 0;
	for (size_t len = 0; len < s.size() - 1; len++)
	{
		EXPECT_EQ(nullptr, GetVarint64Ptr(s.data(), s.data() + len, &result));
	}
	EXPECT_NE(nullptr, GetVarint64Ptr(s.data(), s.data() + s.size(), &result));
	EXPECT_EQ(large_value, result);
}

TEST(CodingTest, Strings)
{
	std::string s;
	PutLengthPrefixedSlice(s, Slice(""));
	PutLengthPrefixedSlice(s, Slice("foo"));
	PutLengthPrefixedSlice(s, Slice("bar"));
	PutLengthPrefixedSlice(s, Slice(std::string(200, 'x')));

	Slice input(s);
	Slice v;
	EXPECT_TRUE(GetLengthPrefixedSlice(input, v));
	EXPECT_EQ("", v.ToString());
	EXPECT_TRUE(GetLengthPrefixedSlice(input, v));
	EXPECT_EQ("foo", v.ToString());
	EXPECT_TRUE(GetLengthPrefixedSlice(input, v));
	EXPECT_EQ("bar", v.ToString());
	EXPECT_TRUE(GetLengthPrefixedSlice(input, v));
	EXPECT_EQ(std::string(200, 'x'), v.ToString());
	EXPECT_EQ("", input.ToString());
}

int main(int argc, char** argv)
{
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
