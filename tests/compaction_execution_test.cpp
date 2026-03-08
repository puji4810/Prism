#include "db_impl.h"

#include "comparator.h"
#include "dbformat.h"
#include "env.h"
#include "filename.h"
#include "table_cache.h"
#include "table/table_builder.h"

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <gtest/gtest.h>
#include <string>
#include <utility>
#include <vector>

namespace prism
{
	namespace
	{
		struct Entry
		{
			std::string user_key;
			SequenceNumber sequence;
			ValueType type;
			std::string value;
		};

		class FailingTableEnv final: public EnvWrapper
		{
		public:
			explicit FailingTableEnv(Env* target)
			    : EnvWrapper(target)
			{
			}

			void SetFailTableWrites(bool fail) { fail_table_writes_.store(fail, std::memory_order_release); }

			Result<std::unique_ptr<WritableFile>> NewWritableFile(const std::string& fname) override
			{
				if (fail_table_writes_.load(std::memory_order_acquire))
				{
					uint64_t number = 0;
					FileType type;
					const std::filesystem::path path(fname);
					if (ParseFileName(path.filename().string(), &number, &type) && type == FileType::kTableFile)
					{
						return std::unexpected(Status::IOError("injected table writable-file failure"));
					}
				}
				return target()->NewWritableFile(fname);
			}

		private:
			std::atomic<bool> fail_table_writes_{ false };
		};
	}

	class CompactionExecutionTest: public ::testing::Test
	{
	protected:
		void SetUp() override
		{
			std::error_code ec;
			std::filesystem::remove_all(dbname_, ec);
		}

		void TearDown() override
		{
			db_.reset();
			std::error_code ec;
			std::filesystem::remove_all(dbname_, ec);
		}

		void Open(Options options)
		{
			options.create_if_missing = true;
			auto open = DB::Open(options, dbname_);
			ASSERT_TRUE(open.has_value()) << open.error().ToString();
			db_ = std::move(open.value());
			impl_ = static_cast<DBImpl*>(db_.get());
			ASSERT_NE(impl_, nullptr);
		}

		uint64_t AddTableFileLocked(int level, const std::vector<Entry>& entries, uint64_t metadata_file_size = 0)
		{
			assert(entries.size() > 0);

			struct EncodedEntry
			{
				std::string key;
				std::string value;
			};

			std::vector<EncodedEntry> encoded;
			encoded.reserve(entries.size());
			for (const Entry& entry : entries)
			{
				InternalKey internal_key(entry.user_key, entry.sequence, entry.type);
				encoded.push_back(EncodedEntry{ internal_key.Encode().ToString(), entry.value });
			}

			InternalKeyComparator icmp(BytewiseComparator());
			std::sort(encoded.begin(), encoded.end(),
			    [&icmp](const EncodedEntry& lhs, const EncodedEntry& rhs) { return icmp.Compare(lhs.key, rhs.key) < 0; });

			const uint64_t file_number = impl_->TEST_NewFileNumber();
			const std::string filename = TableFileName(dbname_, file_number);
			auto file_result = impl_->TEST_Env()->NewWritableFile(filename);
			if (!file_result.has_value())
			{
				ADD_FAILURE() << file_result.error().ToString();
				return 0;
			}
			auto file = std::move(file_result.value());

			TableBuilder builder(impl_->TEST_Options(), file.get());
			for (const EncodedEntry& item : encoded)
			{
				builder.Add(item.key, item.value);
			}

			Status s = builder.Finish();
			if (!s.ok())
			{
				ADD_FAILURE() << s.ToString();
				return 0;
			}
			s = file->Sync();
			if (!s.ok())
			{
				ADD_FAILURE() << s.ToString();
				return 0;
			}
			s = file->Close();
			if (!s.ok())
			{
				ADD_FAILURE() << s.ToString();
				return 0;
			}

			InternalKey smallest;
			InternalKey largest;
			smallest.DecodeFrom(encoded.front().key);
			largest.DecodeFrom(encoded.back().key);

			const uint64_t file_size = (metadata_file_size == 0 ? builder.FileSize() : metadata_file_size);
			s = impl_->TEST_AddFileToVersion(level, file_number, file_size, smallest, largest);
			if (!s.ok())
			{
				ADD_FAILURE() << s.ToString();
				return 0;
			}
			return file_number;
		}

		Status RunPickedCompaction() { return impl_->TEST_RunPickedCompaction(); }

		struct ParsedLevelEntry
		{
			std::string user_key;
			ValueType type;
		};

		std::vector<ParsedLevelEntry> ReadLevelEntriesForKey(int level, const std::string& key)
		{
			std::vector<std::pair<uint64_t, uint64_t>> files;
			for (const FileMetaData& file : impl_->TEST_LevelFilesCopy(level))
			{
				files.push_back(std::make_pair(file.number, file.file_size));
			}

			std::vector<ParsedLevelEntry> result;
			for (const auto& file : files)
			{
				std::unique_ptr<Iterator> iter(impl_->TEST_TableCache()->NewIterator(ReadOptions(), file.first, file.second));
				for (iter->SeekToFirst(); iter->Valid(); iter->Next())
				{
					ParsedInternalKey parsed;
					if (!ParseInternalKey(iter->key(), &parsed))
					{
						continue;
					}
					if (parsed.user_key.ToString() == key)
					{
						result.push_back(ParsedLevelEntry{ parsed.user_key.ToString(), parsed.type });
					}
				}
				EXPECT_TRUE(iter->status().ok()) << iter->status().ToString();
			}

			return result;
		}

		int CountTableFilesOnDisk() const
		{
			auto children = impl_->TEST_Env()->GetChildren(dbname_);
			if (!children.has_value())
			{
				return 0;
			}
			int count = 0;
			for (const std::string& name : children.value())
			{
				uint64_t number = 0;
				FileType type;
				if (ParseFileName(name, &number, &type) && type == FileType::kTableFile)
				{
					++count;
				}
			}
			return count;
		}

		std::string dbname_ = "test_compaction_execution_db";
		std::unique_ptr<DB> db_;
		DBImpl* impl_ = nullptr;
	};

	TEST_F(CompactionExecutionTest, MergesInputsWithoutDroppingTombstones)
	{
		Options options;
		Open(options);

		AddTableFileLocked(0, { Entry{ "k", 4, kTypeDeletion, "" }, Entry{ "z", 1, kTypeValue, "vz" } });
		AddTableFileLocked(0, { Entry{ "k", 3, kTypeValue, "old" } });
		AddTableFileLocked(0, { Entry{ "k", 2, kTypeValue, "older" } });
		AddTableFileLocked(0, { Entry{ "k", 1, kTypeValue, "oldest" } });

		Status s = RunPickedCompaction();
		EXPECT_TRUE(s.ok()) << s.ToString();

		const std::vector<ParsedLevelEntry> entries = ReadLevelEntriesForKey(1, "k");
		int tombstones = 0;
		int values = 0;
		for (const ParsedLevelEntry& entry : entries)
		{
			if (entry.type == kTypeDeletion)
			{
				++tombstones;
			}
			else if (entry.type == kTypeValue)
			{
				++values;
			}
		}

		EXPECT_GE(tombstones, 1);
		EXPECT_GE(values, 1);
	}

	TEST_F(CompactionExecutionTest, SplitsOutputWhenGrandparentOverlapExceeded)
	{
		Options options;
		options.max_file_size = 1024;
		Open(options);

		AddTableFileLocked(0, { Entry{ "a", 10, kTypeValue, "va" }, Entry{ "u", 9, kTypeValue, "vu" } });
		AddTableFileLocked(0, { Entry{ "b", 10, kTypeValue, "vb" }, Entry{ "v", 9, kTypeValue, "vv" } });
		AddTableFileLocked(0, { Entry{ "c", 10, kTypeValue, "vc" }, Entry{ "w", 9, kTypeValue, "vw" } });
		AddTableFileLocked(0, { Entry{ "d", 10, kTypeValue, "vd" }, Entry{ "x", 9, kTypeValue, "vx" } });

		AddTableFileLocked(2, { Entry{ "a", 1, kTypeValue, "g1" } }, 4096);
		AddTableFileLocked(2, { Entry{ "g", 1, kTypeValue, "g2" } }, 4096);
		AddTableFileLocked(2, { Entry{ "m", 1, kTypeValue, "g3" } }, 4096);
		AddTableFileLocked(2, { Entry{ "s", 1, kTypeValue, "g4" } }, 4096);

		Status s = RunPickedCompaction();
		EXPECT_TRUE(s.ok()) << s.ToString();
		EXPECT_GE(static_cast<int>(impl_->TEST_LevelFilesCopy(1).size()), 2u);
	}

	TEST_F(CompactionExecutionTest, FailedOutputCleansUpTempState)
	{
		FailingTableEnv env(Env::Default());
		Options options;
		options.env = &env;
		Open(options);

		int table_files_before = 0;
		AddTableFileLocked(0, { Entry{ "k1", 5, kTypeValue, "v1" } });
		AddTableFileLocked(0, { Entry{ "k2", 5, kTypeValue, "v2" } });
		AddTableFileLocked(0, { Entry{ "k3", 5, kTypeValue, "v3" } });
		AddTableFileLocked(0, { Entry{ "k4", 5, kTypeValue, "v4" } });
		table_files_before = CountTableFilesOnDisk();

		env.SetFailTableWrites(true);
		Status s = RunPickedCompaction();
		EXPECT_FALSE(s.ok());
		EXPECT_TRUE(impl_->TEST_PendingOutputsEmpty());

		env.SetFailTableWrites(false);
		EXPECT_EQ(CountTableFilesOnDisk(), table_files_before);
	}

}
