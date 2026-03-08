#include "gtest/gtest.h"
#include "version_edit.h"
#include "dbformat.h"

using namespace prism;

namespace
{

	// Helper: build an InternalKey from user_key + seq + type
	static InternalKey MakeKey(const std::string& user_key, SequenceNumber seq, ValueType type)
	{
		return InternalKey(Slice(user_key), seq, type);
	}

} // namespace

// ──────────────────────────────────────────────────────────────────────────────
// Test 1: comparator name and new-file entries survive a round-trip
// ──────────────────────────────────────────────────────────────────────────────
TEST(VersionEditTest, RoundTripsComparatorAndFiles)
{
	VersionEdit edit;
	edit.SetComparatorName("leveldb.BytewiseComparator");
	edit.SetLogNumber(42);
	edit.SetNextFile(100);
	edit.SetLastSequence(999);

	InternalKey small = MakeKey("aaa", 1, ValueType::kTypeValue);
	InternalKey large = MakeKey("zzz", 2, ValueType::kTypeValue);
	edit.AddFile(/*level=*/0, /*file=*/7, /*file_size=*/1024, small, large);

	InternalKey small2 = MakeKey("mmm", 10, ValueType::kTypeDeletion);
	InternalKey large2 = MakeKey("nnn", 11, ValueType::kTypeValue);
	edit.AddFile(/*level=*/2, /*file=*/13, /*file_size=*/4096, small2, large2);

	// Encode
	std::string encoded;
	edit.EncodeTo(&encoded);
	ASSERT_FALSE(encoded.empty());

	// Decode
	VersionEdit decoded;
	Status s = decoded.DecodeFrom(Slice(encoded));
	ASSERT_TRUE(s.ok()) << s.ToString();

	// Comparator
	ASSERT_TRUE(decoded.HasComparator());
	EXPECT_EQ("leveldb.BytewiseComparator", decoded.GetComparator());

	// Log / next-file / last-sequence
	ASSERT_TRUE(decoded.HasLogNumber());
	EXPECT_EQ(42u, decoded.GetLogNumber());
	ASSERT_TRUE(decoded.HasNextFileNumber());
	EXPECT_EQ(100u, decoded.GetNextFileNumber());
	ASSERT_TRUE(decoded.HasLastSequence());
	EXPECT_EQ(999u, decoded.GetLastSequence());

	// New files
	const auto& new_files = decoded.GetNewFiles();
	ASSERT_EQ(2u, new_files.size());

	EXPECT_EQ(0, new_files[0].first);
	EXPECT_EQ(7u, new_files[0].second.number);
	EXPECT_EQ(1024u, new_files[0].second.file_size);

	EXPECT_EQ(2, new_files[1].first);
	EXPECT_EQ(13u, new_files[1].second.number);
	EXPECT_EQ(4096u, new_files[1].second.file_size);
}

// ──────────────────────────────────────────────────────────────────────────────
// Test 2: compact pointers, deleted files, prev-log, last-sequence survive
// ──────────────────────────────────────────────────────────────────────────────
TEST(VersionEditTest, PreservesCompactPointersAndSequenceFields)
{
	VersionEdit edit;
	edit.SetPrevLogNumber(5);
	edit.SetLogNumber(10);
	edit.SetNextFile(20);
	edit.SetLastSequence(12345678ULL);

	// Compact pointers
	InternalKey cp0 = MakeKey("cp0", 0, ValueType::kTypeValue);
	InternalKey cp1 = MakeKey("cp1", 0, ValueType::kTypeValue);
	edit.SetCompactPointer(0, cp0);
	edit.SetCompactPointer(1, cp1);

	// Deleted files
	edit.RemoveFile(0, 3);
	edit.RemoveFile(1, 7);

	std::string encoded;
	edit.EncodeTo(&encoded);

	VersionEdit decoded;
	Status s = decoded.DecodeFrom(Slice(encoded));
	ASSERT_TRUE(s.ok()) << s.ToString();

	// Prev-log
	ASSERT_TRUE(decoded.HasPrevLogNumber());
	EXPECT_EQ(5u, decoded.GetPrevLogNumber());

	// Last sequence
	ASSERT_TRUE(decoded.HasLastSequence());
	EXPECT_EQ(12345678ULL, decoded.GetLastSequence());

	// Compact pointers
	const auto& ptrs = decoded.GetCompactPointers();
	ASSERT_EQ(2u, ptrs.size());
	EXPECT_EQ(0, ptrs[0].first);
	EXPECT_EQ(1, ptrs[1].first);
	// Encoded key bytes must match
	EXPECT_EQ(cp0.Encode().ToString(), ptrs[0].second.Encode().ToString());
	EXPECT_EQ(cp1.Encode().ToString(), ptrs[1].second.Encode().ToString());

	// Deleted files
	const auto& del = decoded.GetDeletedFiles();
	EXPECT_EQ(1u, del.count({ 0, 3 }));
	EXPECT_EQ(1u, del.count({ 1, 7 }));
}

// ──────────────────────────────────────────────────────────────────────────────
// Test 3: truncated / corrupt tag stream returns Status::Corruption
// ──────────────────────────────────────────────────────────────────────────────
TEST(VersionEditTest, RejectsCorruptTagStream)
{
	// Sub-test A: completely garbage bytes
	{
		VersionEdit e;
		// Tag 0xFF is unknown in the LevelDB wire format
		const char garbage[] = { '\xff', '\xff', '\x01' };
		Status s = e.DecodeFrom(Slice(garbage, sizeof(garbage)));
		EXPECT_FALSE(s.ok());
		EXPECT_TRUE(s.IsCorruption());
	}

	// Sub-test B: valid tag (kComparator=1) but truncated length-prefixed string
	{
		VersionEdit e;
		// varint32 tag=1, then varint32 length=10, but no bytes following
		std::string bad;
		bad += '\x01'; // tag kComparator
		bad += '\x0a'; // length = 10
		// missing 10 bytes of string data
		Status s = e.DecodeFrom(Slice(bad));
		EXPECT_FALSE(s.ok());
		EXPECT_TRUE(s.IsCorruption());
	}

	// Sub-test C: valid new-file tag but level out of range
	{
		VersionEdit e;
		std::string bad;
		bad += '\x07'; // tag kNewFile = 7
		bad += '\x64'; // level = 100 (> kNumLevels)
		// rest intentionally missing - but level check should fail first
		Status s = e.DecodeFrom(Slice(bad));
		EXPECT_FALSE(s.ok());
		EXPECT_TRUE(s.IsCorruption());
	}

	// Sub-test D: empty slice is valid (no fields), must be OK
	{
		VersionEdit e;
		Status s = e.DecodeFrom(Slice());
		EXPECT_TRUE(s.ok());
	}

	// Sub-test E: trailing garbage after valid content
	{
		// Build a valid edit first
		VersionEdit good;
		good.SetLogNumber(1);
		std::string encoded;
		good.EncodeTo(&encoded);
		// Append a partial varint that looks like it starts a new tag but is truncated
		// Tag 0x00 is not a valid tag (none of 1-7,9), decode should yield corruption
		encoded += '\x00';

		VersionEdit e;
		Status s = e.DecodeFrom(Slice(encoded));
		// 0x00 is an invalid tag -> unknown tag -> corruption
		EXPECT_FALSE(s.ok());
		EXPECT_TRUE(s.IsCorruption());
	}
}

int main(int argc, char** argv)
{
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
