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
			auto open = DBImpl::OpenInternal(options, dbname_);
			ASSERT_TRUE(open.has_value()) << open.error().ToString();
			db_ = std::move(open.value());
			impl_ = db_.get();
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

		void AddL0CompactionFillersBeforeTarget(const std::string& low_key, const std::string& high_key)
		{
			AddTableFileLocked(0, { Entry{ low_key, 1, kTypeValue, "low" } });
			AddTableFileLocked(0, { Entry{ high_key, 1, kTypeValue, "high" } });
			AddTableFileLocked(0, { Entry{ "zz_pad_keep", 1, kTypeValue, "pad" } });
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

		// SetSequence lets test bodies mutate DBImpl::sequence_ through the friend
		// access granted to CompactionExecutionTest. Google Test generates each
		// TEST_F body as a *subclass* of the fixture, and friendship is not
		// inherited in C++, so direct `impl_->sequence_ = n` from test bodies is
		// ill-formed. Delegating through this fixture method works correctly.
		void SetSequence(SequenceNumber s) { impl_->sequence_ = s; }

		std::string dbname_ = "test_compaction_execution_db";
		std::unique_ptr<DBImpl> db_;
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

	// ── Snapshot-aware compaction regression tests ────────────────────────────────
	//
	// These three tests exercise the two drop-eligibility rules in DoCompactionWork:
	//   Rule A: drop a superseded entry when last_sequence_for_key <= oldest_snapshot
	//   Rule B: drop a tombstone when its seq <= oldest_snapshot AND it is in the base
	//           level for that user key (no surviving data in deeper levels)
	//
	// Each test injects SST files directly via AddTableFileLocked so that we bypass
	// the write path and control sequence numbers precisely.
	// ─────────────────────────────────────────────────────────────────────────────

	// Test: Both versions of a key are preserved when a snapshot pins the older one.
	//
	// Setup:  oldest_snapshot = 1  (Snapshot captured at seq 1)
	//         L0 file A:  snap_key@2=new_val , snap_key@1=old_val  ← compaction target
	//         L0 file B:  z_anchor@1=z   \
	//         L0 file C:  zz_pad1@1       } fillers; 4 files hits kL0_CompactionTrigger
	//         L0 file D:  zz_pad2@1      /
	//
	// Sorted by smallest user key: A("snap_key") < B("z_anchor") < C("zz_pad1") < D.
	// PickCompaction selects file A first (empty compact_pointer_).
	// GetOverlappingInputs([snap_key,snap_key]) expands only to A; B–D are excluded.
	// During the compaction the iterator visits file A entries only:
	//   snap_key@2 → first occurrence,  last_seq = kMaxSeq  →  kMaxSeq <= 1? No  → keep
	//   snap_key@1 → same key,          last_seq = 2        →  2 <= 1?      No  → keep   ← snapshot pins
	//
	// Expectation: two kTypeValue entries for "snap_key" survive to L1.
	TEST_F(CompactionExecutionTest, CompactionRespectsOldestLiveSnapshotForOverwrittenValues)
	{
		Options options;
		Open(options);

		// Pin snapshot at sequence 1; sequence_ advances to 3 to simulate later writes.
		SetSequence(2);
		Snapshot snap = impl_->CaptureSnapshot(); // captures seq = sequence_ - 1 = 1
		SetSequence(3);

		ASSERT_TRUE(impl_->GetOldestLiveSnapshotSequence().has_value());
		EXPECT_EQ(1u, impl_->GetOldestLiveSnapshotSequence().value());

		// Add filler files FIRST so target file gets highest file number and is picked.
		// Filler keys overlap target range [snap_key, z_anchor] so they expand into the same compaction.
		AddL0CompactionFillersBeforeTarget("snap_mid", "z_anchor");

		// Add target file LAST so L0 picker chooses it first (L0 ordered by descending file number).
		AddTableFileLocked(0, {
			Entry{ "snap_key", 2, kTypeValue, "new_val" },
			Entry{ "snap_key", 1, kTypeValue, "old_val" },
		});

		Status s = RunPickedCompaction();
		ASSERT_TRUE(s.ok()) << s.ToString();

		ReadOptions snap_options;
		snap_options.snapshot_handle = snap;
		auto snap_get = impl_->Get(snap_options, "snap_key");
		ASSERT_TRUE(snap_get.has_value()) << snap_get.error().ToString();
		EXPECT_EQ("old_val", snap_get.value());

		auto plain_get = impl_->Get("snap_key");
		ASSERT_TRUE(plain_get.has_value()) << plain_get.error().ToString();
		EXPECT_EQ("new_val", plain_get.value());

		// Release snapshot; clear ReadOptions copy too, then registry should drain.
		snap_options.snapshot_handle.reset();
		snap = Snapshot();
		EXPECT_EQ(0u, impl_->TEST_ActiveSnapshotCount());
	}

	// Test: A tombstone is preserved when its sequence number exceeds oldest_snapshot.
	//
	// Setup:  oldest_snapshot = 1  (Snapshot captured at seq 1)
	//         L2 file:            del_key@1=base_val  (makes IsBaseLevelForKey("del_key")=false)
	//         L0 file A:          del_key@2=<deletion> , z_anchor@1=z  ← compaction target
	//         L0 files B–D:       zz_pad{1,2,3}@1  (fillers; keys after "z_anchor")
	//
	// File A spans user keys ["del_key","z_anchor"] — the smallest smallest-key in L0.
	// PickCompaction selects file A. GetOverlappingInputs([del_key,z_anchor]) pulls in
	// only A; files B–D start at "zz_pad*" > "z_anchor" and are excluded.
	//
	// Rule B requires: type==Deletion AND ikey.sequence <= oldest_snapshot AND IsBaseLevel.
	// The tombstone has seq 2.  2 <= 1 is false → Rule B does NOT fire → tombstone kept.
	// (IsBaseLevelForKey is also false because del_key@1 lives in L2, a deeper level.)
	//
	// Expectation: at least one kTypeDeletion entry for "del_key" survives to L1.
	TEST_F(CompactionExecutionTest, CompactionRespectsOldestLiveSnapshotForTombstones)
	{
		Options options;
		Open(options);

		// Place base-level data at L2 so a future compaction could invoke IsBaseLevelForKey.
		AddTableFileLocked(2, {
			Entry{ "del_key", 1, kTypeValue, "base_val" },
		});

		// Pin snapshot at sequence 1.
		SetSequence(2);
		Snapshot snap = impl_->CaptureSnapshot(); // captures seq = 1
		SetSequence(3);

		// Add filler files FIRST so target gets picked (highest file number).
		// Keep fillers overlapping the target range [del_key, z_anchor].
		AddL0CompactionFillersBeforeTarget("del_mid", "z_anchor");

		// Add tombstone target LAST.
		AddTableFileLocked(0, {
			Entry{ "del_key", 2, kTypeDeletion, "" },
			Entry{ "z_anchor", 1, kTypeValue, "z" },
		});

		Status s = RunPickedCompaction();
		ASSERT_TRUE(s.ok()) << s.ToString();

		ReadOptions snap_options;
		snap_options.snapshot_handle = snap;
		auto snap_get = impl_->Get(snap_options, "del_key");
		ASSERT_TRUE(snap_get.has_value()) << snap_get.error().ToString();
		EXPECT_EQ("base_val", snap_get.value());

		auto plain_get = impl_->Get("del_key");
		EXPECT_FALSE(plain_get.has_value());
		EXPECT_TRUE(plain_get.error().IsNotFound()) << plain_get.error().ToString();

		snap = Snapshot();
	}

	// Test: After releasing the snapshot a subsequent compaction reclaims the old values.
	//
	// Phase 1 (snapshot active, oldest_snapshot = 1):
	//   Four L0 files: reclaim_key file (the target) + z_anchor + zz_pad1 + zz_pad2.
	//   PickCompaction picks the reclaim_key file (smallest user key).
	//   Both reclaim_key@2 and @1 survive because last_sequence_for_key for @1 is 2,
	//   and 2 <= 1 (oldest_snapshot) is false.
	//   After the compaction L0 retains: z_anchor@1, zz_pad1@1, zz_pad2@1 (3 files).
	//
	// Phase 2 (snapshot released, oldest_snapshot = 3 = sequence_-1 = 4-1):
	//   A new L0 file carries reclaim_key@3="newest_val".
	//   Compaction now sees:  reclaim_key@3 → keep (first),
	//                         reclaim_key@2 → drop (last_seq=3 <= 3, Rule A),
	//                         reclaim_key@1 → drop (last_seq=3 <= 3, Rule A).
	//   Only "newest_val" survives; a plain Get must return it.
	TEST_F(CompactionExecutionTest, ReclamationOccursAfterOldestSnapshotReleaseAndLaterCompaction)
	{
		Options options;
		Open(options);

		// ── Phase 1: snapshot pins both versions ─────────────────────────────────
		SetSequence(2);
		Snapshot snap = impl_->CaptureSnapshot(); // captures seq = 1
		SetSequence(3);

		AddL0CompactionFillersBeforeTarget("reclaim_mid", "z_anchor");
		AddTableFileLocked(0, {
			Entry{ "reclaim_key", 2, kTypeValue, "new_val" },
			Entry{ "reclaim_key", 1, kTypeValue, "old_val" },
		});

		Status s = RunPickedCompaction();
		ASSERT_TRUE(s.ok()) << "phase 1 compaction failed: " << s.ToString();

		ReadOptions phase1_options;
		phase1_options.snapshot_handle = snap;
		auto phase1_snap = impl_->Get(phase1_options, "reclaim_key");
		ASSERT_TRUE(phase1_snap.has_value()) << phase1_snap.error().ToString();
		EXPECT_EQ("old_val", phase1_snap.value());

		auto phase1_plain = impl_->Get("reclaim_key");
		ASSERT_TRUE(phase1_plain.has_value()) << phase1_plain.error().ToString();
		EXPECT_EQ("new_val", phase1_plain.value());

		// ── Phase 2: release snapshot, then compact again ─────────────────────────
		// Releasing the snapshot empties the registry; clear ReadOptions copy too.
		// sequence_ = 4 so oldest_snapshot = sequence_ - 1 = 3 inside DoCompactionWork.
		phase1_options.snapshot_handle.reset();
		snap = Snapshot();
		EXPECT_EQ(0u, impl_->TEST_ActiveSnapshotCount());

		SetSequence(4);

		// Keep enough L0 files to trigger the next compaction, then add latest value target last.
		AddTableFileLocked(0, { Entry{ "reclaim_mid2", 2, kTypeValue, "m2" } });
		AddTableFileLocked(0, { Entry{ "z_anchor", 2, kTypeValue, "z2" } });
		AddTableFileLocked(0, { Entry{ "zz_pad2", 2, kTypeValue, "p2b" } });
		AddTableFileLocked(0, { Entry{ "reclaim_key", 3, kTypeValue, "newest_val" } });

		s = RunPickedCompaction();
		ASSERT_TRUE(s.ok()) << "phase 2 compaction failed: " << s.ToString();

		auto get_result = impl_->Get("reclaim_key");
		ASSERT_TRUE(get_result.has_value())
		    << "reclaim_key should be readable after reclamation; error: "
		    << get_result.error().ToString();
		EXPECT_EQ("newest_val", get_result.value())
		    << "old values at seq 1 and 2 should have been reclaimed";
	}

} // namespace prism
