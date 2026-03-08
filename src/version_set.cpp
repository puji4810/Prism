#include "version_set.h"

#include <cassert>
#include <cstdint>

namespace prism
{

	// ─────────────────────────────────────────────────────────────────────────────
	// Internal helpers
	// ─────────────────────────────────────────────────────────────────────────────

	// Returns true when user_key is strictly after the largest user-key of f.
	// A null user_key is treated as "before all keys", so never after f.
	static bool AfterFile(const Comparator* ucmp, const Slice* user_key, const FileMetaData* f)
	{
		return (user_key != nullptr && ucmp->Compare(*user_key, f->largest.user_key()) > 0);
	}

	// Returns true when user_key is strictly before the smallest user-key of f.
	// A null user_key is treated as "after all keys", so never before f.
	static bool BeforeFile(const Comparator* ucmp, const Slice* user_key, const FileMetaData* f)
	{
		return (user_key != nullptr && ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
	}

	// ─────────────────────────────────────────────────────────────────────────────
	// FindFile
	// ─────────────────────────────────────────────────────────────────────────────

	int FindFile(const InternalKeyComparator& icmp, const std::vector<FileMetaData*>& files, const Slice& key)
	{
		uint32_t left = 0;
		uint32_t right = static_cast<uint32_t>(files.size());

		while (left < right)
		{
			uint32_t mid = (left + right) / 2;
			const FileMetaData* f = files[mid];

			if (icmp.Compare(f->largest.Encode(), key) < 0)
			{
				// files[mid].largest < key  →  all files at or before mid are too small
				left = mid + 1;
			}
			else
			{
				// files[mid].largest >= key  →  answer is at or before mid
				right = mid;
			}
		}

		return static_cast<int>(right);
	}

	// ─────────────────────────────────────────────────────────────────────────────
	// SomeFileOverlapsRange
	// ─────────────────────────────────────────────────────────────────────────────

	bool SomeFileOverlapsRange(const InternalKeyComparator& icmp, bool disjoint_sorted_files, const std::vector<FileMetaData*>& files,
	    const Slice* smallest_user_key, const Slice* largest_user_key)
	{
		const Comparator* ucmp = icmp.user_comparator();

		if (!disjoint_sorted_files)
		{
			// Level 0: files may overlap — must check every file linearly.
			for (const FileMetaData* f : files)
			{
				if (!AfterFile(ucmp, smallest_user_key, f) && !BeforeFile(ucmp, largest_user_key, f))
				{
					return true;
				}
			}
			return false;
		}

		// Levels > 0: files are disjoint and sorted.
		// Use FindFile to jump to the first file that might overlap.
		uint32_t index = 0;

		if (smallest_user_key != nullptr)
		{
			// Build the earliest possible internal key for smallest_user_key.
			// kMaxSequenceNumber with kValueTypeForSeek ensures we land at or
			// before the first user key equal to smallest_user_key.
			InternalKey small_key(*smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
			index = static_cast<uint32_t>(FindFile(icmp, files, small_key.Encode()));
		}

		if (index >= files.size())
		{
			// The start of the query range is after all files.
			return false;
		}

		// Check that the candidate file is not entirely before our range.
		return !BeforeFile(ucmp, largest_user_key, files[index]);
	}

	// ─────────────────────────────────────────────────────────────────────────────
	// CheckLevelInvariant
	// ─────────────────────────────────────────────────────────────────────────────

	bool CheckLevelInvariant(const InternalKeyComparator& icmp, const std::vector<FileMetaData*>& files)
	{
		for (size_t i = 1; i < files.size(); ++i)
		{
			// files[i-1].largest must be strictly less than files[i].smallest
			if (icmp.Compare(files[i - 1]->largest.Encode(), files[i]->smallest.Encode()) >= 0)
			{
				// Invariant violated: files overlap or are out of order.
				assert(false && "Level > 0 contains overlapping or mis-ordered files");
				return false;
			}
		}
		return true;
	}

} // namespace prism
