#include "version_set.h"

#include <algorithm>
#include <cassert>
#include <cstdint>

#include "env.h"
#include "filename.h"
#include "log_reader.h"
#include "table_cache.h"

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

	VersionSet::VersionSet(const std::string& dbname, const Options* options, TableCache* table_cache, const InternalKeyComparator* cmp)
	    : options_(options)
	    , dbname_(dbname)
	    , table_cache_(table_cache)
	    , icmp_(cmp)
	    , env_(options != nullptr ? options->env : nullptr)
	    , current_(nullptr)
	    , next_file_number_(2)
	    , manifest_file_number_(1)
	    , last_sequence_(0)
	    , log_number_(0)
	    , prev_log_number_(0)
	{
		if (env_ == nullptr)
		{
			env_ = Env::Default();
		}
		current_ = new Version();
		current_->Ref();
	}

	VersionSet::~VersionSet()
	{
		descriptor_log_.reset();
		if (descriptor_file_ != nullptr)
		{
			descriptor_file_->Close();
			descriptor_file_.reset();
		}
		if (current_ != nullptr)
		{
			current_->Unref();
			current_ = nullptr;
		}
	}

	Version* VersionSet::NewVersion() const { return new Version(); }

	void VersionSet::AppendVersion(Version* v)
	{
		assert(v != nullptr);
		Version* old = current_;
		current_ = v;
		current_->Ref();
		if (old != nullptr)
		{
			old->Unref();
		}
	}

	Status VersionSet::LogAndApply(VersionEdit* edit, std::mutex* mu)
	{
		assert(edit != nullptr);
		assert(mu != nullptr);

		if (edit->HasLogNumber())
		{
			assert(edit->GetLogNumber() >= log_number_);
			assert(edit->GetLogNumber() < next_file_number_);
		}
		else
		{
			edit->SetLogNumber(log_number_);
		}

		edit->SetNextFile(next_file_number_);
		edit->SetLastSequence(last_sequence_);

		std::unique_ptr<Version> v(NewVersion());
		{
			Builder builder(this, current_);
			builder.Apply(edit);
			builder.SaveTo(v.get());
		}
		Finalize(v.get());

		std::string new_manifest_file;
		Status s;
		if (!descriptor_log_)
		{
			new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
			auto descriptor_result = env_->NewWritableFile(new_manifest_file);
			if (!descriptor_result.has_value())
			{
				s = descriptor_result.error();
			}
			else
			{
				descriptor_file_ = std::move(descriptor_result.value());
				descriptor_log_ = std::make_unique<log::Writer>(descriptor_file_.get());
				s = WriteSnapshot(descriptor_log_.get());
			}
		}

		mu->unlock();

		if (s.ok())
		{
			std::string record;
			edit->EncodeTo(&record);
			s = descriptor_log_->AddRecord(record);
			if (s.ok())
			{
				s = descriptor_file_->Sync();
			}
			if (!s.ok())
			{
				Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
			}
		}

		if (s.ok() && !new_manifest_file.empty())
		{
			s = SetCurrentFile(env_, dbname_, manifest_file_number_);
		}

		mu->lock();

		if (s.ok())
		{
			AppendVersion(v.release());
			log_number_ = edit->GetLogNumber();
			next_file_number_ = edit->GetNextFileNumber();
			last_sequence_ = edit->GetLastSequence();
		}
		else if (!new_manifest_file.empty())
		{
			descriptor_log_.reset();
			if (descriptor_file_)
			{
				descriptor_file_->Close();
				descriptor_file_.reset();
			}
			env_->RemoveFile(new_manifest_file);
		}

		return s;
	}

	Status VersionSet::Recover(bool* save_manifest)
	{
		if (save_manifest == nullptr)
		{
			return Status::InvalidArgument("save_manifest is null");
		}
		*save_manifest = false;

		std::string current;
		Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);
		if (!s.ok())
		{
			if (!s.IsNotFound())
			{
				return s;
			}

			auto children = env_->GetChildren(dbname_);
			if (!children.has_value())
			{
				return children.error();
			}

			bool has_manifest = false;
			bool has_legacy_files = false;
			uint64_t max_file_number = 0;
			uint64_t max_log_number = 0;

			VersionEdit bootstrap_edit;
			bootstrap_edit.SetComparatorName(icmp_->user_comparator()->Name());
			bootstrap_edit.SetPrevLogNumber(0);

			for (const std::string& name : children.value())
			{
				uint64_t number = 0;
				FileType type;
				if (!ParseFileName(name, &number, &type))
				{
					continue;
				}

				if (number > max_file_number)
				{
					max_file_number = number;
				}

				if (type == FileType::kDescriptorFile)
				{
					has_manifest = true;
					continue;
				}

				if (type == FileType::kTableFile)
				{
					has_legacy_files = true;
					auto file_size = env_->GetFileSize(dbname_ + "/" + name);
					if (!file_size.has_value())
					{
						if (file_size.error().IsNotFound())
						{
							continue;
						}
						return file_size.error();
					}

					const InternalKey boundary_key("", kMaxSequenceNumber, kValueTypeForSeek);
					bootstrap_edit.AddFile(0, number, static_cast<uint64_t>(file_size.value()), boundary_key, boundary_key);
					continue;
				}

				if (type == FileType::kLogFile)
				{
					has_legacy_files = true;
					if (number > max_log_number)
					{
						max_log_number = number;
					}
				}
			}

			if (has_manifest)
			{
				return Status::Corruption("CURRENT missing while MANIFEST exists");
			}

			if (!has_legacy_files)
			{
				return s;
			}

			MarkFileNumberUsed(max_file_number);
			bootstrap_edit.SetLogNumber(max_log_number);

			std::mutex mu;
			mu.lock();
			s = LogAndApply(&bootstrap_edit, &mu);
			mu.unlock();
			if (!s.ok())
			{
				return s;
			}

			*save_manifest = true;
			return Status::OK();
		}

		if (current.empty() || current.back() != '\n')
		{
			return Status::Corruption("CURRENT file does not end with newline");
		}
		current.pop_back();

		uint64_t manifest_number = 0;
		FileType manifest_type;
		if (!ParseFileName(current, &manifest_number, &manifest_type) || manifest_type != FileType::kDescriptorFile)
		{
			return Status::Corruption("CURRENT points to invalid MANIFEST");
		}

		auto file_res = env_->NewSequentialFile(dbname_ + "/" + current);
		if (!file_res.has_value())
		{
			if (file_res.error().IsNotFound())
			{
				return Status::Corruption("CURRENT points to a missing MANIFEST");
			}
			return file_res.error();
		}

		struct LogReporter: public log::Reader::Reporter
		{
			Status* status = nullptr;

			void Corruption(size_t, const Status& s) override
			{
				if (status != nullptr && status->ok())
				{
					*status = s;
				}
			}
		};

		bool have_log_number = false;
		bool have_prev_log_number = false;
		bool have_next_file = false;
		bool have_last_sequence = false;
		uint64_t log_number = 0;
		uint64_t prev_log_number = 0;
		uint64_t next_file = 0;
		uint64_t last_sequence = 0;

		Builder builder(this, current_);
		LogReporter reporter;
		reporter.status = &s;
		log::Reader reader(file_res.value().get(), &reporter, true, 0);

		Slice record;
		std::string scratch;
		while (reader.ReadRecord(&record, &scratch) && s.ok())
		{
			VersionEdit edit;
			s = edit.DecodeFrom(record);
			if (!s.ok())
			{
				break;
			}

			if (edit.HasComparator() && edit.GetComparator() != icmp_->user_comparator()->Name())
			{
				s = Status::Corruption("comparator name mismatch");
				break;
			}

			builder.Apply(&edit);

			if (edit.HasLogNumber())
			{
				have_log_number = true;
				log_number = edit.GetLogNumber();
			}
			if (edit.HasPrevLogNumber())
			{
				have_prev_log_number = true;
				prev_log_number = edit.GetPrevLogNumber();
			}
			if (edit.HasNextFileNumber())
			{
				have_next_file = true;
				next_file = edit.GetNextFileNumber();
			}
			if (edit.HasLastSequence())
			{
				have_last_sequence = true;
				last_sequence = edit.GetLastSequence();
			}
		}

		if (s.ok() && !reader.status().ok())
		{
			s = reader.status();
		}

		if (s.ok())
		{
			if (!have_next_file)
			{
				s = Status::Corruption("no next file number in MANIFEST");
			}
			else if (!have_log_number)
			{
				s = Status::Corruption("no log number in MANIFEST");
			}
			else if (!have_last_sequence)
			{
				s = Status::Corruption("no last sequence in MANIFEST");
			}
		}

		if (!s.ok())
		{
			return s;
		}

		if (!have_prev_log_number)
		{
			prev_log_number = 0;
		}

		Version* recovered = NewVersion();
		builder.SaveTo(recovered);
		Finalize(recovered);
		AppendVersion(recovered);

		manifest_file_number_ = manifest_number;
		next_file_number_ = next_file + 1;
		last_sequence_ = last_sequence;
		log_number_ = log_number;
		prev_log_number_ = prev_log_number;

		MarkFileNumberUsed(prev_log_number_);
		MarkFileNumberUsed(log_number_);

		auto children = env_->GetChildren(dbname_);
		if (!children.has_value())
		{
			return children.error();
		}
		for (const std::string& name : children.value())
		{
			uint64_t number = 0;
			FileType type;
			if (!ParseFileName(name, &number, &type))
			{
				continue;
			}
			MarkFileNumberUsed(number);
		}

		*save_manifest = true;
		return Status::OK();
	}

	Status VersionSet::WriteSnapshot(log::Writer* log)
	{
		VersionEdit edit;
		edit.SetComparatorName(icmp_->user_comparator()->Name());

		for (int level = 0; level < kNumLevels; ++level)
		{
			if (!compact_pointer_[level].empty())
			{
				InternalKey key;
				key.DecodeFrom(compact_pointer_[level]);
				edit.SetCompactPointer(level, key);
			}
		}

		for (int level = 0; level < kNumLevels; ++level)
		{
			const std::vector<FileMetaData*>& files = current_->files(level);
			for (const FileMetaData* file : files)
			{
				edit.AddFile(level, file->number, file->file_size, file->smallest, file->largest);
			}
		}

		std::string record;
		edit.EncodeTo(&record);
		return log->AddRecord(record);
	}

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
		cmp.internal_comparator = vset_->icmp_;
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
					const int compare = vset_->icmp_->Compare(lhs->smallest.Encode(), rhs->smallest.Encode());
					if (compare != 0)
					{
						return compare < 0;
					}
					return lhs->number < rhs->number;
				});

				for (size_t i = 1; i < files.size(); ++i)
				{
					assert(vset_->icmp_->Compare(files[i - 1]->largest.Encode(), files[i]->smallest.Encode()) < 0);
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
