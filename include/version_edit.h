#ifndef PRISM_VERSION_EDIT_H
#define PRISM_VERSION_EDIT_H

#include <atomic>
#include <cstdint>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "dbformat.h"
#include "slice.h"
#include "status.h"

namespace prism
{

	// Maximum number of LSM levels.
	// Keep in sync with the rest of the engine when VersionSet is added.
	static constexpr int kNumLevels = 7;

	// ─────────────────────────────────────────────────────────────────────────────
	// FileMetaData
	//   Metadata for a single SSTable file.  Semantics match LevelDB exactly so
	//   VersionEdit wire encoding is cross-compatible.
	// ─────────────────────────────────────────────────────────────────────────────
	struct FileMetaData
	{
	FileMetaData()
	    : refs(0)
	    , allowed_seeks(1 << 30)
	    , number(0)
	    , file_size(0)
	{
	}

	// Copy constructor: new copy starts unowned (refs = 0)
	FileMetaData(const FileMetaData& other)
	    : refs(0)
	    , allowed_seeks(other.allowed_seeks)
	    , number(other.number)
	    , file_size(other.file_size)
	    , smallest(other.smallest)
	    , largest(other.largest)
	{
	}

	// Copy assignment: new copy starts unowned (refs = 0)
	FileMetaData& operator=(const FileMetaData& other)
	{
		if (this != &other)
		{
			refs.store(0, std::memory_order_relaxed);
			allowed_seeks = other.allowed_seeks;
			number = other.number;
			file_size = other.file_size;
			smallest = other.smallest;
			largest = other.largest;
		}
		return *this;
	}

	std::atomic<int> refs;
	int allowed_seeks; // seeks allowed until next compaction
	uint64_t number; // file number
	uint64_t file_size; // file size in bytes
	InternalKey smallest; // smallest internal key in the file
	InternalKey largest; // largest internal key in the file
	};

	// ─────────────────────────────────────────────────────────────────────────────
	// VersionEdit
	//   A set of changes to be applied to a Version to produce the next Version.
	//   Serialization uses LevelDB-compatible varint tag numbers so MANIFEST files
	//   written by Prism can be read by LevelDB and vice-versa.
	//
	//   Wire tags (must never change):
	//     kComparator     = 1
	//     kLogNumber      = 2
	//     kNextFileNumber = 3
	//     kLastSequence   = 4
	//     kCompactPointer = 5
	//     kDeletedFile    = 6
	//     kNewFile        = 7
	//     kPrevLogNumber  = 9   (8 was large-value refs, now unused)
	// ─────────────────────────────────────────────────────────────────────────────
	class VersionEdit
	{
	public:
		VersionEdit() { Clear(); }
		~VersionEdit() = default;

		// Not copyable by default; allow explicit copy/move as needed.
		VersionEdit(const VersionEdit&) = default;
		VersionEdit& operator=(const VersionEdit&) = default;
		VersionEdit(VersionEdit&&) noexcept = default;
		VersionEdit& operator=(VersionEdit&&) noexcept = default;

		// Reset to initial (empty) state.
		void Clear();

		// ── Setters ──────────────────────────────────────────────────────────────

		void SetComparatorName(const Slice& name)
		{
			has_comparator_ = true;
			comparator_ = name.ToString();
		}

		void SetLogNumber(uint64_t num)
		{
			has_log_number_ = true;
			log_number_ = num;
		}

		void SetPrevLogNumber(uint64_t num)
		{
			has_prev_log_number_ = true;
			prev_log_number_ = num;
		}

		void SetNextFile(uint64_t num)
		{
			has_next_file_number_ = true;
			next_file_number_ = num;
		}

		// NOTE: persist last_sequence (LevelDB convention), NOT next_sequence.
		void SetLastSequence(SequenceNumber seq)
		{
			has_last_sequence_ = true;
			last_sequence_ = seq;
		}

		void SetCompactPointer(int level, const InternalKey& key) { compact_pointers_.emplace_back(level, key); }

		// Add a new SSTable file at the given level.
		void AddFile(int level, uint64_t file, uint64_t file_size, const InternalKey& smallest, const InternalKey& largest)
		{
			FileMetaData f;
			f.number = file;
			f.file_size = file_size;
			f.smallest = smallest;
			f.largest = largest;
			new_files_.emplace_back(level, std::move(f));
		}

		// Mark a file as deleted.
		void RemoveFile(int level, uint64_t file) { deleted_files_.insert({ level, file }); }

		// ── Serialization ────────────────────────────────────────────────────────

		void EncodeTo(std::string* dst) const;
		Status DecodeFrom(const Slice& src);

		// ── Accessors (for tests and VersionSet consumers) ────────────────────────

		bool HasComparator() const { return has_comparator_; }
		const std::string& GetComparator() const { return comparator_; }

		bool HasLogNumber() const { return has_log_number_; }
		uint64_t GetLogNumber() const { return log_number_; }

		bool HasPrevLogNumber() const { return has_prev_log_number_; }
		uint64_t GetPrevLogNumber() const { return prev_log_number_; }

		bool HasNextFileNumber() const { return has_next_file_number_; }
		uint64_t GetNextFileNumber() const { return next_file_number_; }

		bool HasLastSequence() const { return has_last_sequence_; }
		SequenceNumber GetLastSequence() const { return last_sequence_; }

		using DeletedFileSet = std::set<std::pair<int, uint64_t>>;

		const std::vector<std::pair<int, InternalKey>>& GetCompactPointers() const { return compact_pointers_; }
		const DeletedFileSet& GetDeletedFiles() const { return deleted_files_; }
		const std::vector<std::pair<int, FileMetaData>>& GetNewFiles() const { return new_files_; }

		std::string DebugString() const;

	private:
		std::string comparator_;
		uint64_t log_number_{ 0 };
		uint64_t prev_log_number_{ 0 };
		uint64_t next_file_number_{ 0 };
		SequenceNumber last_sequence_{ 0 };

		bool has_comparator_{ false };
		bool has_log_number_{ false };
		bool has_prev_log_number_{ false };
		bool has_next_file_number_{ false };
		bool has_last_sequence_{ false };

		std::vector<std::pair<int, InternalKey>> compact_pointers_;
		DeletedFileSet deleted_files_;
		std::vector<std::pair<int, FileMetaData>> new_files_;
	};

} // namespace prism

#endif // PRISM_VERSION_EDIT_H
