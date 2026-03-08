#include "version_edit.h"

#include "coding.h"

namespace prism
{

	// ─────────────────────────────────────────────────────────────────────────────
	// Wire tag numbers.  These are written to disk and MUST NOT change.
	// Identical to LevelDB's enum Tag in db/version_edit.cc.
	// ─────────────────────────────────────────────────────────────────────────────
	enum Tag : uint32_t
	{
		kComparator = 1,
		kLogNumber = 2,
		kNextFileNumber = 3,
		kLastSequence = 4,
		kCompactPointer = 5,
		kDeletedFile = 6,
		kNewFile = 7,
		// 8 was used for large value refs (never shipped), skip it
		kPrevLogNumber = 9,
	};

	// ─────────────────────────────────────────────────────────────────────────────
	// VersionEdit::Clear
	// ─────────────────────────────────────────────────────────────────────────────
	void VersionEdit::Clear()
	{
		comparator_.clear();
		log_number_ = 0;
		prev_log_number_ = 0;
		last_sequence_ = 0;
		next_file_number_ = 0;
		has_comparator_ = false;
		has_log_number_ = false;
		has_prev_log_number_ = false;
		has_next_file_number_ = false;
		has_last_sequence_ = false;
		compact_pointers_.clear();
		deleted_files_.clear();
		new_files_.clear();
	}

	// ─────────────────────────────────────────────────────────────────────────────
	// VersionEdit::EncodeTo
	//   NOTE: Prism coding helpers take std::string& (not std::string*) and use
	//   the same varint/length-prefix layout as LevelDB, so the encoded bytes are
	//   wire-compatible.
	// ─────────────────────────────────────────────────────────────────────────────
	void VersionEdit::EncodeTo(std::string* dst) const
	{
		if (has_comparator_)
		{
			PutVarint32(*dst, kComparator);
			PutLengthPrefixedSlice(*dst, comparator_);
		}
		if (has_log_number_)
		{
			PutVarint32(*dst, kLogNumber);
			PutVarint64(*dst, log_number_);
		}
		if (has_prev_log_number_)
		{
			PutVarint32(*dst, kPrevLogNumber);
			PutVarint64(*dst, prev_log_number_);
		}
		if (has_next_file_number_)
		{
			PutVarint32(*dst, kNextFileNumber);
			PutVarint64(*dst, next_file_number_);
		}
		if (has_last_sequence_)
		{
			PutVarint32(*dst, kLastSequence);
			PutVarint64(*dst, last_sequence_);
		}

		for (const auto& [level, key] : compact_pointers_)
		{
			PutVarint32(*dst, kCompactPointer);
			PutVarint32(*dst, static_cast<uint32_t>(level));
			PutLengthPrefixedSlice(*dst, key.Encode());
		}

		for (const auto& [level, file_num] : deleted_files_)
		{
			PutVarint32(*dst, kDeletedFile);
			PutVarint32(*dst, static_cast<uint32_t>(level));
			PutVarint64(*dst, file_num);
		}

		for (const auto& [level, f] : new_files_)
		{
			PutVarint32(*dst, kNewFile);
			PutVarint32(*dst, static_cast<uint32_t>(level));
			PutVarint64(*dst, f.number);
			PutVarint64(*dst, f.file_size);
			PutLengthPrefixedSlice(*dst, f.smallest.Encode());
			PutLengthPrefixedSlice(*dst, f.largest.Encode());
		}
	}

	// ─────────────────────────────────────────────────────────────────────────────
	// Local helpers (file-scope)
	// ─────────────────────────────────────────────────────────────────────────────
	namespace
	{

		bool GetInternalKey(Slice* input, InternalKey* dst)
		{
			Slice str;
			if (GetLengthPrefixedSlice(input, &str))
			{
				return dst->DecodeFrom(str);
			}
			return false;
		}

		bool GetLevel(Slice* input, int* level)
		{
			uint32_t v;
			if (GetVarint32(input, &v) && static_cast<int>(v) < kNumLevels)
			{
				*level = static_cast<int>(v);
				return true;
			}
			return false;
		}

	} // namespace

	// ─────────────────────────────────────────────────────────────────────────────
	// VersionEdit::DecodeFrom
	// ─────────────────────────────────────────────────────────────────────────────
	Status VersionEdit::DecodeFrom(const Slice& src)
	{
		Clear();
		Slice input = src;
		const char* msg = nullptr;
		uint32_t tag;

		// Temporaries
		int level;
		uint64_t number;
		FileMetaData f;
		Slice str;
		InternalKey key;

		while (msg == nullptr && GetVarint32(&input, &tag))
		{
			switch (tag)
			{
			case kComparator:
				if (GetLengthPrefixedSlice(&input, &str))
				{
					comparator_ = str.ToString();
					has_comparator_ = true;
				}
				else
				{
					msg = "comparator name";
				}
				break;

			case kLogNumber:
				if (GetVarint64(&input, &log_number_))
				{
					has_log_number_ = true;
				}
				else
				{
					msg = "log number";
				}
				break;

			case kPrevLogNumber:
				if (GetVarint64(&input, &prev_log_number_))
				{
					has_prev_log_number_ = true;
				}
				else
				{
					msg = "previous log number";
				}
				break;

			case kNextFileNumber:
				if (GetVarint64(&input, &next_file_number_))
				{
					has_next_file_number_ = true;
				}
				else
				{
					msg = "next file number";
				}
				break;

			case kLastSequence:
				if (GetVarint64(&input, &last_sequence_))
				{
					has_last_sequence_ = true;
				}
				else
				{
					msg = "last sequence number";
				}
				break;

			case kCompactPointer:
				if (GetLevel(&input, &level) && GetInternalKey(&input, &key))
				{
					compact_pointers_.emplace_back(level, key);
				}
				else
				{
					msg = "compaction pointer";
				}
				break;

			case kDeletedFile:
				if (GetLevel(&input, &level) && GetVarint64(&input, &number))
				{
					deleted_files_.insert({ level, number });
				}
				else
				{
					msg = "deleted file";
				}
				break;

			case kNewFile:
				if (GetLevel(&input, &level) && GetVarint64(&input, &f.number) && GetVarint64(&input, &f.file_size)
				    && GetInternalKey(&input, &f.smallest) && GetInternalKey(&input, &f.largest))
				{
					new_files_.emplace_back(level, f);
				}
				else
				{
					msg = "new-file entry";
				}
				break;

			default:
				msg = "unknown tag";
				break;
			}
		}

		if (msg == nullptr && !input.empty())
		{
			msg = "invalid tag";
		}

		if (msg != nullptr)
		{
			return Status::Corruption("VersionEdit", msg);
		}
		return Status::OK();
	}

	// ─────────────────────────────────────────────────────────────────────────────
	// VersionEdit::DebugString
	// ─────────────────────────────────────────────────────────────────────────────
	std::string VersionEdit::DebugString() const
	{
		std::string r;
		r.append("VersionEdit {");
		if (has_comparator_)
		{
			r.append("\n  Comparator: ");
			r.append(comparator_);
		}
		if (has_log_number_)
		{
			r.append("\n  LogNumber: ");
			AppendNumberTo(&r, log_number_);
		}
		if (has_prev_log_number_)
		{
			r.append("\n  PrevLogNumber: ");
			AppendNumberTo(&r, prev_log_number_);
		}
		if (has_next_file_number_)
		{
			r.append("\n  NextFile: ");
			AppendNumberTo(&r, next_file_number_);
		}
		if (has_last_sequence_)
		{
			r.append("\n  LastSeq: ");
			AppendNumberTo(&r, last_sequence_);
		}
		for (const auto& [level, key] : compact_pointers_)
		{
			r.append("\n  CompactPointer: ");
			AppendNumberTo(&r, static_cast<uint64_t>(level));
			r.append(" ");
			r.append(key.DebugString());
		}
		for (const auto& [level, file_num] : deleted_files_)
		{
			r.append("\n  RemoveFile: ");
			AppendNumberTo(&r, static_cast<uint64_t>(level));
			r.append(" ");
			AppendNumberTo(&r, file_num);
		}
		for (const auto& [level, f] : new_files_)
		{
			r.append("\n  AddFile: ");
			AppendNumberTo(&r, static_cast<uint64_t>(level));
			r.append(" ");
			AppendNumberTo(&r, f.number);
			r.append(" ");
			AppendNumberTo(&r, f.file_size);
			r.append(" ");
			r.append(f.smallest.DebugString());
			r.append(" .. ");
			r.append(f.largest.DebugString());
		}
		r.append("\n}\n");
		return r;
	}

} // namespace prism
