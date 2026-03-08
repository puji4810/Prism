#ifndef PRISM_VERSION_SET_H
#define PRISM_VERSION_SET_H

#include <cstdint>
#include <vector>

#include "dbformat.h"
#include "version_edit.h"

namespace prism
{

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
