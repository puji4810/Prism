#include <vector>
#include <memory>
#include "gtest/gtest.h"

#include "version_set.h"
#include "version_edit.h"
#include "dbformat.h"
#include "comparator.h"

namespace prism
{

	class VersionSetTest: public testing::Test
	{
	public:
		VersionSetTest()
		    : icmp_(BytewiseComparator())
		{
		}

		~VersionSetTest()
		{
			for (auto f : files_)
			{
				delete f;
			}
		}

		// Helper to create a FileMetaData with given smallest/largest keys
		FileMetaData* CreateFile(uint64_t number, const std::string& smallest_user_key, SequenceNumber smallest_seq,
		    const std::string& largest_user_key, SequenceNumber largest_seq)
		{
			FileMetaData* f = new FileMetaData;
			f->number = number;
			f->smallest = InternalKey(smallest_user_key, smallest_seq, kTypeValue);
			f->largest = InternalKey(largest_user_key, largest_seq, kTypeValue);
			files_.push_back(f);
			return f;
		}

		// Helper to call FindFile
		int Find(const std::string& key, SequenceNumber seq = 100)
		{
			InternalKey target(key, seq, kTypeValue);
			return FindFile(icmp_, files_, target.Encode());
		}

		// Helper to check overlap using Slice objects
		bool Overlaps(const Slice* smallest, const Slice* largest, bool disjoint = true)
		{
			return SomeFileOverlapsRange(icmp_, disjoint, files_, smallest, largest);
		}

		// Helper to check level invariant
		bool CheckInvariant() { return CheckLevelInvariant(icmp_, files_); }

	protected:
		InternalKeyComparator icmp_;
		std::vector<FileMetaData*> files_;
	};

	// Test 1: FindFile with sorted ranges and edge cases
	TEST_F(VersionSetTest, FindFileOnSortedRanges)
	{
		// Create a sorted set of files
		CreateFile(1, "100", 100, "200", 100);
		CreateFile(2, "300", 100, "400", 100);
		CreateFile(3, "500", 100, "600", 100);

		// Test: key before all files
		EXPECT_EQ(0, Find("050"));

		// Test: key at/in first file
		EXPECT_EQ(0, Find("100"));
		EXPECT_EQ(0, Find("150"));
		EXPECT_EQ(0, Find("200"));

		// Test: key between first and second file
		EXPECT_EQ(1, Find("250"));
		EXPECT_EQ(1, Find("299"));

		// Test: key at/in second file
		EXPECT_EQ(1, Find("300"));
		EXPECT_EQ(1, Find("350"));
		EXPECT_EQ(1, Find("400"));

		// Test: key between second and third file
		EXPECT_EQ(2, Find("450"));
		EXPECT_EQ(2, Find("499"));

		// Test: key at/in third file
		EXPECT_EQ(2, Find("500"));
		EXPECT_EQ(2, Find("550"));
		EXPECT_EQ(2, Find("600"));

		// Test: key after all files
		EXPECT_EQ(3, Find("650"));
		EXPECT_EQ(3, Find("999"));
	}

	// Test 2: SomeFileOverlapsRange with null bounds
	TEST_F(VersionSetTest, SomeFileOverlapsRangeHandlesNullBounds)
	{
		// Create files with specific ranges
		CreateFile(1, "100", 100, "200", 100);
		CreateFile(2, "300", 100, "400", 100);
		CreateFile(3, "500", 100, "600", 100);

		// Test: null smallest (unbounded from start)
		std::string large_key = "250";
		Slice large_slice(large_key);
		EXPECT_TRUE(Overlaps(nullptr, &large_slice)); // overlaps with file 1

		// Test: null largest (unbounded to end)
		std::string small_key = "350";
		Slice small_slice(small_key);
		EXPECT_TRUE(Overlaps(&small_slice, nullptr)); // overlaps with files 2 and 3

		// Test: both null (entire range)
		EXPECT_TRUE(Overlaps(nullptr, nullptr));

		// Test: range with no overlap (before all files)
		std::string before_start = "050";
		std::string before_end = "099";
		Slice before_start_slice(before_start);
		Slice before_end_slice(before_end);
		EXPECT_FALSE(Overlaps(&before_start_slice, &before_end_slice));

		// Test: range with no overlap (after all files)
		std::string after_start = "650";
		std::string after_end = "999";
		Slice after_start_slice(after_start);
		Slice after_end_slice(after_end);
		EXPECT_FALSE(Overlaps(&after_start_slice, &after_end_slice));

		// Test: range that overlaps multiple files
		std::string multi_start = "150";
		std::string multi_end = "550";
		Slice multi_start_slice(multi_start);
		Slice multi_end_slice(multi_end);
		EXPECT_TRUE(Overlaps(&multi_start_slice, &multi_end_slice));

		// Test: range at boundary (touches file edge)
		std::string bound_start = "200";
		std::string bound_end = "300";
		Slice bound_start_slice(bound_start);
		Slice bound_end_slice(bound_end);
		EXPECT_TRUE(Overlaps(&bound_start_slice, &bound_end_slice));
	}

	// Test 3: CheckLevelInvariant rejects overlapping files
	// Note: CheckLevelInvariant will assert on overlapping files in debug builds,
	// so we test that valid (non-overlapping) files pass the invariant check.
	TEST_F(VersionSetTest, RejectsOverlappingLevelOneFiles)
	{
		// Test 1: Non-overlapping files should pass invariant
		CreateFile(1, "100", 100, "200", 100);
		CreateFile(2, "300", 100, "400", 100);
		CreateFile(3, "500", 100, "600", 100);
		EXPECT_TRUE(CheckInvariant());

		// Clear for next test case
		for (auto f : files_)
		{
			delete f;
		}
		files_.clear();

		// Test 2: Files with proper non-overlapping boundaries
		// File 1: [100, 200]
		// File 2: [300, 400]  (starts after file 1 ends)
		CreateFile(1, "100", 100, "200", 100);
		CreateFile(2, "300", 100, "400", 100);
		EXPECT_TRUE(CheckInvariant());

		// Clear for next test case
		for (auto f : files_)
		{
			delete f;
		}
		files_.clear();

		// Test 3: Verify the invariant check detects when files are too close
		// This tests the boundary condition - files[i-1].largest must be < files[i].smallest
		CreateFile(1, "100", 100, "300", 100);
		CreateFile(2, "300", 100, "400", 100); // Shares same key "300" with file 1's largest
		// This should trigger an assert in CheckLevelInvariant due to >= comparison
		// We use EXPECT_DEATH to verify the assertion happens
		EXPECT_DEATH({ CheckLevelInvariant(icmp_, files_); }, "");
	}

} // namespace prism

int main()
{
	testing::InitGoogleTest();
	return RUN_ALL_TESTS();
}
