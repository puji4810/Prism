#include "version_set.h"

#include <algorithm>
#include <cassert>
#include <cstdint>

namespace prism
{
	static int64_t TotalFileSize(const std::vector<FileMetaData*>& files)
	{
		int64_t sum = 0;
		for (const FileMetaData* file : files)
		{
			sum += static_cast<int64_t>(file->file_size);
		}
		return sum;
	}

	Version::Version()
	    : refs_(0)
	    , compaction_score_(-1)
	    , compaction_level_(-1)
	{
	}

	Version::~Version()
	{
		assert(refs_ == 0);
		for (int level = 0; level < kNumLevels; ++level)
		{
			for (FileMetaData* file : files_[level])
			{
				assert(file->refs > 0);
				--file->refs;
				if (file->refs <= 0)
				{
					delete file;
				}
			}
		}
	}

	void Version::Ref() { ++refs_; }

	void Version::Unref()
	{
		assert(refs_ > 0);
		--refs_;
		if (refs_ == 0)
		{
			delete this;
		}
	}

	void Version::AddFile(int level, FileMetaData* file)
	{
		assert(level >= 0 && level < kNumLevels);
		assert(file != nullptr);
		++file->refs;
		files_[level].push_back(file);
	}

	const std::vector<FileMetaData*>& Version::files(int level) const
	{
		assert(level >= 0 && level < kNumLevels);
		return files_[level];
	}

	std::vector<FileMetaData*>& Version::mutable_files(int level)
	{
		assert(level >= 0 && level < kNumLevels);
		return files_[level];
	}

	VersionSet::VersionSet(const Options* options, const InternalKeyComparator& icmp)
	    : options_(options)
	    , icmp_(icmp)
	{
	}

	Version* VersionSet::NewVersion() const { return new Version(); }

	double VersionSet::MaxBytesForLevel(int level) const
	{
		(void)options_;
		double result = 10.0 * 1048576.0;
		while (level > 1)
		{
			result *= 10;
			--level;
		}
		return result;
	}

	void VersionSet::Finalize(Version* v) const
	{
		int best_level = -1;
		double best_score = -1;

		for (int level = 0; level < kNumLevels - 1; ++level)
		{
			double score = 0;
			if (level == 0)
			{
				score = static_cast<double>(v->files(level).size()) / static_cast<double>(config::kL0_CompactionTrigger);
			}
			else
			{
				score = static_cast<double>(TotalFileSize(v->files(level))) / MaxBytesForLevel(level);
			}

			if (score > best_score)
			{
				best_score = score;
				best_level = level;
			}
		}

		v->compaction_level_ = best_level;
		v->compaction_score_ = best_score;
	}

	const std::string& VersionSet::compact_pointer(int level) const
	{
		assert(level >= 0 && level < kNumLevels);
		return compact_pointer_[level];
	}

	struct VersionSet::Builder::BySmallestKey
	{
		const InternalKeyComparator* internal_comparator;

		bool operator()(const FileMetaData* lhs, const FileMetaData* rhs) const
		{
			const int compare = internal_comparator->Compare(lhs->smallest.Encode(), rhs->smallest.Encode());
			if (compare != 0)
			{
				return compare < 0;
			}
			return lhs->number < rhs->number;
		}
	};

	VersionSet::Builder::Builder(VersionSet* vset, Version* base)
	    : vset_(vset)
	    , base_(base)
	{
		base_->Ref();
		BySmallestKey cmp;
		cmp.internal_comparator = &vset_->icmp_;
		for (int level = 0; level < kNumLevels; ++level)
		{
			levels_[level].added_files = new FileSet(cmp);
		}
	}

	VersionSet::Builder::~Builder()
	{
		for (int level = 0; level < kNumLevels; ++level)
		{
			FileSet* added_files = levels_[level].added_files;
			std::vector<FileMetaData*> to_unref;
			to_unref.reserve(added_files->size());
			for (FileMetaData* file : *added_files)
			{
				to_unref.push_back(file);
			}
			delete added_files;
			for (FileMetaData* file : to_unref)
			{
				--file->refs;
				if (file->refs <= 0)
				{
					delete file;
				}
			}
		}

		base_->Unref();
	}

	void VersionSet::Builder::Apply(const VersionEdit* edit)
	{
		for (const auto& compact_pointer : edit->GetCompactPointers())
		{
			vset_->compact_pointer_[compact_pointer.first] = compact_pointer.second.Encode().ToString();
		}

		for (const auto& deleted_file : edit->GetDeletedFiles())
		{
			levels_[deleted_file.first].deleted_files.insert(deleted_file.second);
		}

		for (const auto& new_file : edit->GetNewFiles())
		{
			const int level = new_file.first;
			FileMetaData* file = new FileMetaData(new_file.second);
			file->refs = 1;
			file->allowed_seeks = static_cast<int>(file->file_size / 16384U);
			if (file->allowed_seeks < 100)
			{
				file->allowed_seeks = 100;
			}

			levels_[level].deleted_files.erase(file->number);
			levels_[level].added_files->insert(file);
		}
	}

	void VersionSet::Builder::SaveTo(Version* v)
	{
		for (int level = 0; level < kNumLevels; ++level)
		{
			for (FileMetaData* file : base_->files(level))
			{
				MaybeAddFile(v, level, file);
			}

			for (FileMetaData* file : *levels_[level].added_files)
			{
				MaybeAddFile(v, level, file);
			}

			auto& files = v->mutable_files(level);
			if (level == 0)
			{
				std::sort(
				    files.begin(), files.end(), [](const FileMetaData* lhs, const FileMetaData* rhs) { return lhs->number > rhs->number; });
			}
			else
			{
				std::sort(files.begin(), files.end(), [this](const FileMetaData* lhs, const FileMetaData* rhs) {
					const int compare = vset_->icmp_.Compare(lhs->smallest.Encode(), rhs->smallest.Encode());
					if (compare != 0)
					{
						return compare < 0;
					}
					return lhs->number < rhs->number;
				});

				for (size_t i = 1; i < files.size(); ++i)
				{
					assert(vset_->icmp_.Compare(files[i - 1]->largest.Encode(), files[i]->smallest.Encode()) < 0);
				}
			}
		}
	}

	void VersionSet::Builder::MaybeAddFile(Version* v, int level, FileMetaData* f)
	{
		if (levels_[level].deleted_files.count(f->number) > 0)
		{
			return;
		}
		v->AddFile(level, f);
	}

	static bool AfterFile(const Comparator* ucmp, const Slice* user_key, const FileMetaData* f)
	{
		return (user_key != nullptr && ucmp->Compare(*user_key, f->largest.user_key()) > 0);
	}

	static bool BeforeFile(const Comparator* ucmp, const Slice* user_key, const FileMetaData* f)
	{
		return (user_key != nullptr && ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
	}

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
				left = mid + 1;
			}
			else
			{
				right = mid;
			}
		}

		return static_cast<int>(right);
	}

	bool SomeFileOverlapsRange(const InternalKeyComparator& icmp, bool disjoint_sorted_files, const std::vector<FileMetaData*>& files,
	    const Slice* smallest_user_key, const Slice* largest_user_key)
	{
		const Comparator* ucmp = icmp.user_comparator();

		if (!disjoint_sorted_files)
		{
			for (const FileMetaData* f : files)
			{
				if (!AfterFile(ucmp, smallest_user_key, f) && !BeforeFile(ucmp, largest_user_key, f))
				{
					return true;
				}
			}
			return false;
		}

		uint32_t index = 0;

		if (smallest_user_key != nullptr)
		{
			InternalKey small_key(*smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
			index = static_cast<uint32_t>(FindFile(icmp, files, small_key.Encode()));
		}

		if (index >= files.size())
		{
			return false;
		}

		return !BeforeFile(ucmp, largest_user_key, files[index]);
	}

	bool CheckLevelInvariant(const InternalKeyComparator& icmp, const std::vector<FileMetaData*>& files)
	{
		for (size_t i = 1; i < files.size(); ++i)
		{
			if (icmp.Compare(files[i - 1]->largest.Encode(), files[i]->smallest.Encode()) >= 0)
			{
				assert(false && "Level > 0 contains overlapping or mis-ordered files");
				return false;
			}
		}
		return true;
	}

}
