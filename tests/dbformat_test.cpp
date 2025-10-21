#include "gtest/gtest.h"
#include "dbformat.h"
#include "comparator.h"

using namespace prism;

namespace
{

	// Local bytewise comparator to avoid linking dependencies
	class TestBytewiseComparator: public Comparator
	{
	public:
		~TestBytewiseComparator() override = default;
		const char* Name() const override { return "Prism.TestBytewiseComparator"; }
		int Compare(const Slice& a, const Slice& b) const override { return a.compare(b); }
		void FindShortestSeparator(std::string* start, const Slice& limit) const override
		{
			// same as BytewiseComparatorImpl
			size_t min_length = std::min(start->size(), limit.size());
			size_t diff_index = 0;
			while (diff_index < min_length && (*start)[diff_index] == limit[diff_index])
			{
				++diff_index;
			}
			if (diff_index < min_length)
			{
				const uint8_t diff = static_cast<uint8_t>((*start)[diff_index]);
				const uint8_t lim = static_cast<uint8_t>(limit[diff_index]);
				if (diff < static_cast<uint8_t>(0xff) && diff + 1 < lim)
				{
					(*start)[diff_index] = static_cast<char>(diff + 1);
					start->resize(diff_index + 1);
				}
			}
		}
		void FindShortSuccessor(std::string* key) const override
		{
			for (size_t i = 0; i < key->size(); ++i)
			{
				const uint8_t byte = static_cast<uint8_t>((*key)[i]);
				if (byte != static_cast<uint8_t>(0xff))
				{
					(*key)[i] = static_cast<char>(byte + 1);
					key->resize(i + 1);
					return;
				}
			}
			// leave it unchanged if all 0xff
		}
	};

	static std::string IKey(const std::string& user_key, uint64_t seq, ValueType vt)
	{
		std::string encoded;
		AppendInternalKey(encoded, ParsedInternalKey(user_key, seq, vt));
		return encoded;
	}

	static std::string Shorten(const std::string& s, const std::string& l)
	{
		TestBytewiseComparator user_cmp;
		InternalKeyComparator icmp(&user_cmp);
		std::string result = s;
		icmp.FindShortestSeparator(&result, l);
		return result;
	}

	static std::string ShortSuccessor(const std::string& s)
	{
		TestBytewiseComparator user_cmp;
		InternalKeyComparator icmp(&user_cmp);
		std::string result = s;
		icmp.FindShortSuccessor(&result);
		return result;
	}

	static void TestKey(const std::string& key, uint64_t seq, ValueType vt)
	{
		std::string encoded = IKey(key, seq, vt);
		Slice in(encoded);
		ParsedInternalKey decoded;
		ASSERT_TRUE(ParseInternalKey(in, &decoded));
		ASSERT_EQ(key, decoded.user_key.ToString());
		ASSERT_EQ(seq, decoded.sequence);
		ASSERT_EQ(vt, decoded.type);
		ASSERT_FALSE(ParseInternalKey(Slice("bar"), &decoded));
	}

} // namespace

TEST(FormatTest, InternalKey_EncodeDecode)
{
	const char* keys[] = { "", "k", "hello", "longggggggggggggggggggggg" };
	const uint64_t seq[] = { 1, 2, 3, (1ull << 8) - 1, 1ull << 8, (1ull << 8) + 1, (1ull << 16) - 1, 1ull << 16, (1ull << 16) + 1,
		(1ull << 32) - 1, 1ull << 32, (1ull << 32) + 1 };
	for (size_t k = 0; k < sizeof(keys) / sizeof(keys[0]); ++k)
	{
		for (size_t s = 0; s < sizeof(seq) / sizeof(seq[0]); ++s)
		{
			TestKey(keys[k], seq[s], ValueType::kTypeValue);
			TestKey("hello", 1, ValueType::kTypeDeletion);
		}
	}
}

TEST(FormatTest, InternalKey_DecodeFromEmpty)
{
	InternalKey internal_key;
	ASSERT_FALSE(internal_key.DecodeFrom(""));
}

TEST(FormatTest, InternalKeyShortSeparator)
{
	// When user keys are same
	ASSERT_EQ(IKey("foo", 100, kValueTypeForSeek), Shorten(IKey("foo", 100, kValueTypeForSeek), IKey("foo", 99, kValueTypeForSeek)));
	ASSERT_EQ(IKey("foo", 100, kValueTypeForSeek), Shorten(IKey("foo", 100, kValueTypeForSeek), IKey("foo", 101, kValueTypeForSeek)));
	ASSERT_EQ(IKey("foo", 100, kValueTypeForSeek), Shorten(IKey("foo", 100, kValueTypeForSeek), IKey("foo", 100, kValueTypeForSeek)));

	// When user keys are misordered
	ASSERT_EQ(IKey("foo", 100, kValueTypeForSeek), Shorten(IKey("foo", 100, kValueTypeForSeek), IKey("bar", 99, kValueTypeForSeek)));

	// When user keys are different, but correctly ordered
	ASSERT_EQ(IKey("g", kMaxSequenceNumber, kValueTypeForSeek),
	    Shorten(IKey("foo", 100, kValueTypeForSeek), IKey("hello", 200, kValueTypeForSeek)));

	// When start user key is prefix of limit user key
	ASSERT_EQ(IKey("foo", 100, kValueTypeForSeek), Shorten(IKey("foo", 100, kValueTypeForSeek), IKey("foobar", 200, kValueTypeForSeek)));

	// When limit user key is prefix of start user key
	ASSERT_EQ(IKey("foobar", 100, kValueTypeForSeek), Shorten(IKey("foobar", 100, kValueTypeForSeek), IKey("foo", 200, kValueTypeForSeek)));
}

TEST(FormatTest, InternalKeyShortestSuccessor)
{
	ASSERT_EQ(IKey("g", kMaxSequenceNumber, kValueTypeForSeek), ShortSuccessor(IKey("foo", 100, kValueTypeForSeek)));
	ASSERT_EQ(IKey("\xff\xff", 100, kValueTypeForSeek), ShortSuccessor(IKey("\xff\xff", 100, kValueTypeForSeek)));
}

TEST(FormatTest, ParsedInternalKeyDebugString)
{
	ParsedInternalKey key("The \"key\" in 'single quotes'", 42, kValueTypeForSeek);
	// Prism format: user_key: '...' , sequence: ..., type: Value/Deletion
	const std::string prefix = "user_key: 'The \"key\" in 'single quotes''";
	EXPECT_TRUE(key.DebugString().starts_with(prefix));
	EXPECT_NE(std::string::npos, key.DebugString().find("sequence: 42"));
	EXPECT_NE(std::string::npos, key.DebugString().find("type: Value"));
}

TEST(FormatTest, InternalKeyDebugString)
{
	InternalKey key("The \"key\" in 'single quotes'", 42, kValueTypeForSeek);
	const std::string s = key.DebugString();
	EXPECT_NE(std::string::npos, s.find("user_key: 'The \"key\" in 'single quotes''"));
	EXPECT_NE(std::string::npos, s.find("sequence: 42"));
	EXPECT_NE(std::string::npos, s.find("type: Value"));

	InternalKey invalid_key; // empty rep_
	EXPECT_EQ("(bad)", invalid_key.DebugString());
}

int main()
{
	testing::InitGoogleTest();
	return RUN_ALL_TESTS();
}
