#ifndef PRISM_VERSION_SET_H
#define PRISM_VERSION_SET_H

#include <cstdint>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "env.h"
#include "dbformat.h"
#include "log_writer.h"
#include "options.h"
#include "version_edit.h"

namespace prism
{
	class TableCache;

	namespace config
	{
		static constexpr int kL0_CompactionTrigger = 4;
		static constexpr int kL0_SlowdownWritesTrigger = 8;
		static constexpr int kL0_StopWritesTrigger = 12;
		static constexpr int kMaxMemCompactLevel = 2;
	}

	class Version
	{
	public:
		Version();
		~Version();

		void Ref();
		void Unref();

		// Test-only: returns the current reference count.
		int TEST_Refs() const { return refs_; }

		void AddFile(int level, FileMetaData* file);
		const std::vector<FileMetaData*>& files(int level) const;
		std::vector<FileMetaData*>& mutable_files(int level);

		double compaction_score() const { return compaction_score_; }
		int compaction_level() const { return compaction_level_; }

	private:
		friend class VersionSet;

		int refs_;
		Version* prev_ = nullptr;
		Version* next_ = nullptr;
		std::vector<FileMetaData*> files_[kNumLevels];

		double compaction_score_;
		int compaction_level_;
	};

	class VersionSet
	{
	public:
		explicit VersionSet(const Options* options, const InternalKeyComparator& icmp)
		    : VersionSet("", options, nullptr, &icmp)
		{
		}
		VersionSet(const std::string& dbname, const Options* options, TableCache* table_cache, const InternalKeyComparator* cmp);
		VersionSet(const VersionSet&) = delete;
		VersionSet& operator=(const VersionSet&) = delete;
		~VersionSet();

		class Builder
		{
		public:
			Builder(VersionSet* vset, Version* base);
			~Builder();

			void Apply(const VersionEdit* edit);
			void SaveTo(Version* v);

		private:
			struct BySmallestKey;
			using FileSet = std::set<FileMetaData*, BySmallestKey>;

			struct LevelState
			{
				std::set<uint64_t> deleted_files;
				FileSet* added_files;
			};

			void MaybeAddFile(Version* v, int level, FileMetaData* f);

			VersionSet* vset_;
			Version* base_;
			LevelState levels_[kNumLevels];
		};

		Version* NewVersion() const;
		Version* current() const { return current_; }

		Status LogAndApply(VersionEdit* edit, std::mutex* mu);
		Status Recover(bool* save_manifest);
		Status WriteSnapshot(log::Writer* log);

		void Finalize(Version* v) const;
		double MaxBytesForLevel(int level) const;

		uint64_t ManifestFileNumber() const { return manifest_file_number_; }
		uint64_t NewFileNumber() { return next_file_number_++; }
		void MarkFileNumberUsed(uint64_t number)
		{
			if (next_file_number_ <= number)
			{
				next_file_number_ = number + 1;
			}
		}
		void ReuseFileNumber(uint64_t file_number)
		{
			if (next_file_number_ == file_number + 1)
			{
				next_file_number_ = file_number;
			}
		}
		uint64_t LastSequence() const { return last_sequence_; }
		void SetLastSequence(uint64_t s)
		{
			assert(s >= last_sequence_);
			last_sequence_ = s;
		}
		uint64_t LogNumber() const { return log_number_; }
		uint64_t PrevLogNumber() const { return prev_log_number_; }
		uint64_t NextFileNumber() const { return next_file_number_; }

		const std::string& compact_pointer(int level) const;
		void AddLiveFiles(std::set<uint64_t>* live);

	private:
		void AppendVersion(Version* v);

		const Options* options_;
		std::string dbname_;
		TableCache* table_cache_;
		const InternalKeyComparator* icmp_;
		Env* env_;
		Version* current_;

		std::unique_ptr<WritableFile> descriptor_file_;
		std::unique_ptr<log::Writer> descriptor_log_;

		uint64_t next_file_number_;
		uint64_t manifest_file_number_;
		uint64_t last_sequence_;
		uint64_t log_number_;
		uint64_t prev_log_number_;

		std::string compact_pointer_[kNumLevels];
	};

	// ─────────────────────────────────────────────────────────────────────────────
	// File-range helpers
	//
	// These free functions operate on sorted, non-overlapping file lists (as found
	// in levels > 0) or on arbitrary file lists (level 0).  They are the building
	// blocks for compaction planning and point-lookup routing.
	// ─────────────────────────────────────────────────────────────────────────────

	// FindFile
	//
	// Binary-search a sorted, non-overlapping file list for the first file whose
	// largest key is >= key.
	//
	// Returns the index i such that files[i]->largest >= key, or files.size() if
	// every file's largest key is strictly less than key.
	//
	// REQUIRES: files is sorted by largest key in ascending InternalKey order and
	//           contains non-overlapping ranges.
	int FindFile(const InternalKeyComparator& icmp, const std::vector<FileMetaData*>& files, const Slice& key);

	// SomeFileOverlapsRange
	//
	// Returns true iff at least one file in files overlaps the user-key range
	// [*smallest_user_key, *largest_user_key] (inclusive).
	//
	//   smallest_user_key == nullptr  ->  treat as "before all keys"
	//   largest_user_key  == nullptr  ->  treat as "after all keys"
	//
	// When disjoint_sorted_files is true the function uses FindFile for an O(log n)
	// fast path; otherwise it scans all files linearly.
	//
	// REQUIRES: if disjoint_sorted_files, files[] must be sorted by largest key
	//           with no overlapping ranges.
	bool SomeFileOverlapsRange(const InternalKeyComparator& icmp, bool disjoint_sorted_files, const std::vector<FileMetaData*>& files,
	    const Slice* smallest_user_key, const Slice* largest_user_key);

	// CheckLevelInvariant
	//
	// DEBUG helper: asserts that every consecutive pair of files in the given
	// level satisfies the non-overlap invariant required for levels > 0.
	//
	// Returns true if the invariant holds, false otherwise.  In debug builds it
	// also triggers an assertion failure on the first violation found.
	//
	// REQUIRES: level > 0  (level 0 may have overlapping files)
	bool CheckLevelInvariant(const InternalKeyComparator& icmp, const std::vector<FileMetaData*>& files);

} // namespace prism

#endif // PRISM_VERSION_SET_H
