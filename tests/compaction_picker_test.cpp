#include <algorithm>
#include <filesystem>
#include <memory>
#include <shared_mutex>
#include <vector>

#include "gtest/gtest.h"

#include "comparator.h"
#include "options.h"
#include "version_set.h"

namespace prism
{
	class CompactionPickerTest: public testing::Test
	{
	public:
		CompactionPickerTest()
		    : icmp_(BytewiseComparator())
		{
		}

		void SetUp() override
		{
			dbname_ = "test_compaction_picker_db";
			std::error_code ec;
			std::filesystem::remove_all(dbname_, ec);
			std::filesystem::create_directories(dbname_, ec);
		}

		~CompactionPickerTest()
		{
			std::error_code ec;
			std::filesystem::remove_all(dbname_, ec);

			for (FileMetaData* file : files_)
			{
				delete file;
			}
		}

		FileMetaData* NewFile(uint64_t number, uint64_t file_size, const std::string& smallest, const std::string& largest)
		{
			FileMetaData* file = new FileMetaData();
			file->number = number;
			file->file_size = file_size;
			file->smallest = InternalKey(smallest, 100, kTypeValue);
			file->largest = InternalKey(largest, 100, kTypeValue);
			files_.push_back(file);
			return file;
		}

		void InstallFiles(VersionSet* vset, int level, const std::vector<FileMetaData*>& files)
		{
			VersionEdit edit;
			for (FileMetaData* file : files)
			{
				edit.AddFile(level, file->number, file->file_size, file->smallest, file->largest);
			}

			std::shared_mutex mu;
			mu.lock();
			ASSERT_TRUE(vset->LogAndApply(&edit, &mu).ok());
			mu.unlock();
		}

		std::vector<uint64_t> InputNumbers(const Compaction* c, int which)
		{
			std::vector<uint64_t> numbers;
			for (int i = 0; i < c->num_input_files(which); ++i)
			{
				numbers.push_back(c->input(which, i)->number);
			}
			std::sort(numbers.begin(), numbers.end());
			return numbers;
		}

		InternalKeyComparator icmp_;
		Options options_;
		std::string dbname_;
		std::vector<FileMetaData*> files_;
	};

	TEST_F(CompactionPickerTest, AddBoundaryInputsExtendsSameUserKeyRange)
	{
		FileMetaData* a = NewFile(1, 1024, "100", "100");
		FileMetaData* b = NewFile(2, 1024, "100", "200");
		FileMetaData* c = NewFile(3, 1024, "300", "300");
		a->smallest = InternalKey("100", 3, kTypeValue);
		a->largest = InternalKey("100", 2, kTypeValue);
		b->smallest = InternalKey("100", 1, kTypeValue);
		b->largest = InternalKey("200", 3, kTypeValue);

		std::vector<FileMetaData*> level_files{ c, b, a };
		std::vector<FileMetaData*> compaction_files{ a };

		VersionSet::AddBoundaryInputs(icmp_, level_files, &compaction_files);

		ASSERT_EQ(2u, compaction_files.size());
		EXPECT_EQ(1u, compaction_files[0]->number);
		EXPECT_EQ(2u, compaction_files[1]->number);
	}

	TEST_F(CompactionPickerTest, LevelZeroPickIncludesAllOverlappingFiles)
	{
		VersionSet vset(dbname_, &options_, nullptr, &icmp_);

		InstallFiles(&vset, 0,
		    {
		        NewFile(1, 1024, "g", "h"),
		        NewFile(2, 1024, "e", "f"),
		        NewFile(3, 1024, "a", "c"),
		        NewFile(4, 1024, "b", "d"),
		    });

		InstallFiles(&vset, 1, { NewFile(10, 1024, "a", "d") });

		std::unique_ptr<Compaction> c(vset.PickCompaction());
		ASSERT_NE(nullptr, c);
		EXPECT_EQ(0, c->level());

		const std::vector<uint64_t> level0_inputs = InputNumbers(c.get(), 0);
		EXPECT_EQ((std::vector<uint64_t>{ 3, 4 }), level0_inputs);
		EXPECT_EQ(1, c->num_input_files(1));
	}

	TEST_F(CompactionPickerTest, TrivialMoveRejectedWhenGrandparentOverlapTooLarge)
	{
		VersionSet vset(dbname_, &options_, nullptr, &icmp_);

		InstallFiles(&vset, 1, { NewFile(100, 12ULL * 1024 * 1024, "a", "z") });
		InstallFiles(&vset, 3,
		    {
		        NewFile(200, 11ULL * 1024 * 1024, "a", "m"),
		        NewFile(201, 11ULL * 1024 * 1024, "n", "z"),
		    });

		std::unique_ptr<Compaction> c(vset.PickCompaction());
		ASSERT_NE(nullptr, c);
		EXPECT_EQ(1, c->level());
		EXPECT_EQ(1, c->num_input_files(0));
		EXPECT_EQ(0, c->num_input_files(1));
		EXPECT_FALSE(c->IsTrivialMove());
	}

}

int main(int argc, char** argv)
{
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
