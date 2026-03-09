#include <memory>
#include <shared_mutex>
#include <string>
#include <unistd.h>
#include <vector>
#include "gtest/gtest.h"

#include "env.h"
#include "options.h"
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

		FileMetaData* NewVersionFile(
		    uint64_t number, uint64_t file_size, const std::string& smallest_user_key, const std::string& largest_user_key)
		{
			FileMetaData* f = new FileMetaData;
			f->number = number;
			f->file_size = file_size;
			f->smallest = InternalKey(smallest_user_key, 100, kTypeValue);
			f->largest = InternalKey(largest_user_key, 100, kTypeValue);
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

	TEST_F(VersionSetTest, FinalizeUsesFileCountForLevelZero)
	{
		Options options;
		VersionSet vset(&options, icmp_);
		std::unique_ptr<Version> v(vset.NewVersion());

		v->AddFile(0, NewVersionFile(1, 64 * 1024, "a", "b"));
		v->AddFile(0, NewVersionFile(2, 64 * 1024, "c", "d"));
		v->AddFile(0, NewVersionFile(3, 64 * 1024, "e", "f"));
		v->AddFile(0, NewVersionFile(4, 64 * 1024, "g", "h"));
		v->AddFile(0, NewVersionFile(5, 64 * 1024, "i", "j"));

		vset.Finalize(v.get());

		EXPECT_EQ(0, v->compaction_level());
		EXPECT_DOUBLE_EQ(1.25, v->compaction_score());
	}

	TEST_F(VersionSetTest, FinalizeUsesByteBudgetForLevelOnePlus)
	{
		Options options;
		VersionSet vset(&options, icmp_);
		std::unique_ptr<Version> v(vset.NewVersion());

		v->AddFile(1, NewVersionFile(100, 15ull * 1024 * 1024, "a", "z"));

		vset.Finalize(v.get());

		EXPECT_EQ(1, v->compaction_level());
		EXPECT_DOUBLE_EQ(1.5, v->compaction_score());
	}

	TEST_F(VersionSetTest, BuilderAppliesAddDeleteAndCompactPointer)
	{
		Options options;
		VersionSet vset(&options, icmp_);
		Version* base = vset.NewVersion();
		base->Ref();

		base->AddFile(0, NewVersionFile(10, 64 * 1024, "a", "b"));
		base->AddFile(0, NewVersionFile(20, 64 * 1024, "c", "d"));

		base->AddFile(1, NewVersionFile(100, 64 * 1024, "a", "b"));
		base->AddFile(1, NewVersionFile(200, 64 * 1024, "m", "z"));

		VersionEdit edit;
		edit.RemoveFile(0, 20);
		edit.RemoveFile(1, 100);
		edit.AddFile(0, 30, 64 * 1024, InternalKey("e", 100, kTypeValue), InternalKey("f", 100, kTypeValue));
		edit.AddFile(1, 150, 4ull * 1024 * 1024, InternalKey("c", 100, kTypeValue), InternalKey("l", 100, kTypeValue));
		edit.SetCompactPointer(1, InternalKey("k", 100, kTypeValue));

		std::unique_ptr<Version> out(vset.NewVersion());
		{
			VersionSet::Builder builder(&vset, base);
			builder.Apply(&edit);
			builder.SaveTo(out.get());
		}

		ASSERT_EQ(2u, out->files(0).size());
		EXPECT_EQ(30u, out->files(0)[0]->number);
		EXPECT_EQ(10u, out->files(0)[1]->number);

		ASSERT_EQ(2u, out->files(1).size());
		EXPECT_EQ(150u, out->files(1)[0]->number);
		EXPECT_EQ(200u, out->files(1)[1]->number);

		EXPECT_EQ(256, out->files(1)[0]->allowed_seeks);
		EXPECT_EQ(100, out->files(0)[0]->allowed_seeks);

		InternalKey compact_pointer;
		ASSERT_TRUE(compact_pointer.DecodeFrom(vset.compact_pointer(1)));
		EXPECT_EQ(0, icmp_.Compare(compact_pointer, InternalKey("k", 100, kTypeValue)));

		base->Unref();
	}
	TEST_F(VersionSetTest, LogAndApplyReleasesOldCurrentRefAfterSwap)
	{
		// Create a temporary directory for the test database
		std::string tmp_dir = "/tmp/prism_version_set_test_" + std::to_string(::getpid());
		prism::Env::Default()->CreateDir(tmp_dir);

		Options options;
		options.create_if_missing = true;
		VersionSet vset(tmp_dir, &options, nullptr, &icmp_);

		// Get the initial current version (it starts with refs_ == 1 from VersionSet)
		Version* old_current = vset.current();
		EXPECT_EQ(1, old_current->TEST_Refs());

		// Hold an extra ref to the old current
		old_current->Ref();
		EXPECT_EQ(2, old_current->TEST_Refs());

		// Create a minimal edit to trigger LogAndApply
		VersionEdit edit;
		edit.SetLogNumber(0);
		edit.SetNextFile(2);
		edit.SetLastSequence(0);

		// Apply the edit with a mutex (required by LogAndApply)
		std::shared_mutex mu;
		mu.lock();
		Status s = vset.LogAndApply(&edit, &mu);
		mu.unlock();
		ASSERT_TRUE(s.ok()) << s.ToString();

		// After LogAndApply, the old current should have exactly 1 ref (our extra ref)
		// VersionSet::current_ now points to a new version, and AppendVersion() released
		// VersionSet's ref on old_current
		EXPECT_EQ(1, old_current->TEST_Refs());

		// The new current should also have 1 ref (VersionSet's ownership)
		EXPECT_EQ(1, vset.current()->TEST_Refs());
		EXPECT_NE(old_current, vset.current());

		// Release our extra ref - this should delete old_current
		old_current->Unref();

		// Cleanup: remove temporary directory files
		prism::Env::Default()->RemoveFile(tmp_dir + "/CURRENT");
		prism::Env::Default()->RemoveFile(tmp_dir + "/MANIFEST-000001");
		prism::Env::Default()->RemoveDir(tmp_dir);
	}

	// Test: Verify that LogAndApply invariant (log_number < next_file_number) is maintained
	// when applying edits that set log numbers
	TEST_F(VersionSetTest, BootstrapLogNumberStaysBelowNextFileNumber)
	{
		// Create a temporary directory for the test database
		std::string tmp_dir = "/tmp/prism_version_set_lognum_test_" + std::to_string(::getpid());
		prism::Env::Default()->CreateDir(tmp_dir);

		Options options;
		options.create_if_missing = true;
		VersionSet vset(tmp_dir, &options, nullptr, &icmp_);

		// Simulate the bootstrap scenario: advance next_file_number, then apply an edit
		// that sets a log number close to but below next_file_number

		// First, get the initial state
		uint64_t initial_next = vset.NextFileNumber();

		// Create an edit that sets log_number to be just below next_file_number
		// This should succeed because log_number < next_file_number
		VersionEdit edit1;
		edit1.SetLogNumber(initial_next - 1); // Must be < next_file_number
		edit1.SetNextFile(initial_next);
		edit1.SetLastSequence(0);

		std::shared_mutex mu;
		mu.lock();
		Status s = vset.LogAndApply(&edit1, &mu);
		mu.unlock();
		ASSERT_TRUE(s.ok()) << "LogAndApply with valid log_number should succeed: " << s.ToString();

		// Verify the log number was recorded
		EXPECT_EQ(initial_next - 1, vset.LogNumber());

		// Cleanup
		prism::Env::Default()->RemoveFile(tmp_dir + "/CURRENT");
		// Remove the manifest file (number may vary)
		auto children = prism::Env::Default()->GetChildren(tmp_dir);
		if (children.has_value())
		{
			for (const auto& name : children.value())
			{
				if (name.find("MANIFEST") != std::string::npos)
				{
					prism::Env::Default()->RemoveFile(tmp_dir + "/" + name);
				}
			}
		}
		prism::Env::Default()->RemoveDir(tmp_dir);
	}

} // namespace prism

int main()
{
	testing::InitGoogleTest();
	return RUN_ALL_TESTS();
}
