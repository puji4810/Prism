#include "version_set.h"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <mutex>
#include <shared_mutex>

#include "env.h"
#include "filename.h"
#include "log_reader.h"
#include "table/table.h"
#include "table_cache.h"

namespace prism
{
	static uint64_t MaxFileSizeForLevel(const Options* options, int level)
	{
		(void)level;
		return static_cast<uint64_t>(options->max_file_size);
	}

	static uint64_t MaxGrandParentOverlapBytes(const Options* options) { return 10ULL * MaxFileSizeForLevel(options, 0); }

	static uint64_t ExpandedCompactionByteSizeLimit(const Options* options) { return 25ULL * MaxFileSizeForLevel(options, 0); }

	static int64_t TotalFileSize(const std::vector<FileMetaData*>& files)
	{
		int64_t sum = 0;
		for (const FileMetaData* file : files)
		{
			sum += static_cast<int64_t>(file->file_size);
		}
		return sum;
	}

	static void GetRange(const std::vector<FileMetaData*>& inputs, InternalKey* smallest, InternalKey* largest)
	{
		assert(!inputs.empty());
		smallest->Clear();
		largest->Clear();
		for (size_t i = 0; i < inputs.size(); ++i)
		{
			FileMetaData* file = inputs[i];
			if (i == 0 || file->smallest.Encode().compare(smallest->Encode()) < 0)
			{
				*smallest = file->smallest;
			}
			if (i == 0 || file->largest.Encode().compare(largest->Encode()) > 0)
			{
				*largest = file->largest;
			}
		}
	}

	static void GetRange2(
	    const std::vector<FileMetaData*>& inputs1, const std::vector<FileMetaData*>& inputs2, InternalKey* smallest, InternalKey* largest)
	{
		std::vector<FileMetaData*> all = inputs1;
		all.insert(all.end(), inputs2.begin(), inputs2.end());
		GetRange(all, smallest, largest);
	}

	static bool FindLargestKey(const InternalKeyComparator& icmp, const std::vector<FileMetaData*>& files, InternalKey* largest_key)
	{
		if (files.empty())
		{
			return false;
		}

		*largest_key = files[0]->largest;
		for (size_t i = 1; i < files.size(); ++i)
		{
			if (icmp.Compare(files[i]->largest, *largest_key) > 0)
			{
				*largest_key = files[i]->largest;
			}
		}
		return true;
	}

	static Status RecoverLegacyTableBounds(
	    Env* env, const std::string& dbname, const Options* options, const InternalKeyComparator* icmp, uint64_t file_number,
	    uint64_t file_size, InternalKey* smallest, InternalKey* largest)
	{
		assert(env != nullptr);
		assert(options != nullptr);
		assert(icmp != nullptr);
		assert(smallest != nullptr);
		assert(largest != nullptr);

		auto file = env->NewRandomAccessFile(TableFileName(dbname, file_number));
		if (!file.has_value())
		{
			file = env->NewRandomAccessFile(SSTTableFileName(dbname, file_number));
			if (!file.has_value())
			{
				return file.error();
			}
		}

		Options table_options = *options;
		table_options.env = env;
		table_options.comparator = icmp;

		Table* raw_table = nullptr;
		Status s = Table::Open(table_options, file.value().get(), file_size, &raw_table);
		if (!s.ok())
		{
			return s;
		}

		std::unique_ptr<Table> table(raw_table);
		std::unique_ptr<Iterator> iter(table->NewIterator(ReadOptions()));
		iter->SeekToFirst();
		if (!iter->Valid())
		{
			if (!iter->status().ok())
			{
				return iter->status();
			}
			return Status::Corruption("legacy table is empty");
		}
		if (!smallest->DecodeFrom(iter->key()))
		{
			return Status::Corruption("legacy table has invalid smallest key");
		}

		iter->SeekToLast();
		if (!iter->Valid())
		{
			if (!iter->status().ok())
			{
				return iter->status();
			}
			return Status::Corruption("legacy table is empty");
		}
		if (!largest->DecodeFrom(iter->key()))
		{
			return Status::Corruption("legacy table has invalid largest key");
		}

		return Status::OK();
	}

	static FileMetaData* FindSmallestBoundaryFile(
	    const InternalKeyComparator& icmp, const std::vector<FileMetaData*>& level_files, const InternalKey& largest_key)
	{
		const Comparator* user_cmp = icmp.user_comparator();
		FileMetaData* smallest_boundary_file = nullptr;
		for (FileMetaData* file : level_files)
		{
			if (icmp.Compare(file->smallest, largest_key) > 0 && user_cmp->Compare(file->smallest.user_key(), largest_key.user_key()) == 0)
			{
				if (smallest_boundary_file == nullptr || icmp.Compare(file->smallest, smallest_boundary_file->smallest) < 0)
				{
					smallest_boundary_file = file;
				}
			}
		}
		return smallest_boundary_file;
	}

	Version::Version(VersionSet* vset)
	    : refs_(0)
	    , vset_(vset)
	    , prev_(this)
	    , next_(this)
	    , compaction_score_(-1)
	    , compaction_level_(-1)
	{
	}

	Version::~Version()
	{
		assert(refs_.load(std::memory_order_acquire) == 0);
		std::unique_lock<std::mutex> list_lock;
		if (vset_ != nullptr)
		{
			list_lock = std::unique_lock<std::mutex>(vset_->version_list_mutex_);
		}
		prev_->next_ = next_;
		next_->prev_ = prev_;
		prev_ = nullptr;
		next_ = nullptr;
		for (int level = 0; level < kNumLevels; ++level)
		{
			for (FileMetaData* file : files_[level])
			{
				assert(file->refs.load(std::memory_order_acquire) > 0);
				if (file->refs.fetch_sub(1, std::memory_order_acq_rel) == 1)
				{
					delete file;
				}
			}
		}
	}

	void Version::Ref() { refs_.fetch_add(1, std::memory_order_relaxed); }

	void Version::Unref()
	{
		assert(refs_.load(std::memory_order_acquire) > 0);
		if (refs_.fetch_sub(1, std::memory_order_acq_rel) == 1)
		{
			delete this;
		}
	}

	void Version::AddFile(int level, FileMetaData* file)
	{
		assert(level >= 0 && level < kNumLevels);
		assert(file != nullptr);
		file->refs.fetch_add(1, std::memory_order_relaxed);
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

	int Version::PickLevelForMemTableOutput(const Slice& smallest_user_key, const Slice& largest_user_key)
	{
		int level = 0;
		if (!OverlapInLevel(0, &smallest_user_key, &largest_user_key))
		{
			InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
			InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
			while (level < config::kMaxMemCompactLevel)
			{
				if (OverlapInLevel(level + 1, &smallest_user_key, &largest_user_key))
				{
					break;
				}
				if (level + 2 < kNumLevels)
				{
					std::vector<FileMetaData*> overlaps;
					GetOverlappingInputs(level + 2, &start, &limit, &overlaps);
					int64_t sum = 0;
					for (FileMetaData* f : overlaps)
					{
						sum += static_cast<int64_t>(f->file_size);
					}
					if (sum > 20 * 1048576)
					{
						break;
					}
				}
				++level;
			}
		}
		return level;
	}

	bool Version::OverlapInLevel(int level, const Slice* smallest_user_key, const Slice* largest_user_key)
	{
		assert(vset_ != nullptr);
		return SomeFileOverlapsRange(*vset_->Comparator(), level > 0, files_[level], smallest_user_key, largest_user_key);
	}

	void Version::GetOverlappingInputs(int level, const InternalKey* begin, const InternalKey* end, std::vector<FileMetaData*>* inputs)
	{
		inputs->clear();
		Slice user_begin;
		Slice user_end;
		if (begin != nullptr)
		{
			user_begin = begin->user_key();
		}
		if (end != nullptr)
		{
			user_end = end->user_key();
		}
		const Comparator* ucmp = vset_->Comparator()->user_comparator();
		for (FileMetaData* f : files_[level])
		{
			if (end != nullptr && ucmp->Compare(f->smallest.user_key(), user_end) > 0)
			{
				continue;
			}
			if (begin != nullptr && ucmp->Compare(f->largest.user_key(), user_begin) < 0)
			{
				continue;
			}
			inputs->push_back(f);
		}
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
		current_ = new Version(this);
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

	Version* VersionSet::NewVersion() const { return new Version(const_cast<VersionSet*>(this)); }

	void VersionSet::AppendVersion(Version* v)
	{
		assert(v != nullptr);
		Version* old = current_;
		{
			std::lock_guard<std::mutex> lock(version_list_mutex_);
			if (old != nullptr)
			{
				v->next_ = old->next_;
				v->prev_ = old;
				old->next_->prev_ = v;
				old->next_ = v;
			}
			current_ = v;
			current_->Ref();
		}
		if (old != nullptr)
		{
			old->Unref();
		}
	}

	Status VersionSet::LogAndApply(VersionEdit* edit, std::shared_mutex* mu)
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
		if (!edit->HasPrevLogNumber())
		{
			edit->SetPrevLogNumber(prev_log_number_);
		}

		uint64_t next_file_number = next_file_number_;
		uint64_t manifest_file_number = manifest_file_number_;
		std::string new_manifest_file;
		bool created_new_manifest = false;
		if (!descriptor_log_)
		{
			manifest_file_number = next_file_number++;
			created_new_manifest = true;
		}

		edit->SetNextFile(next_file_number);
		edit->SetLastSequence(last_sequence_);

		std::unique_ptr<Version> v(NewVersion());
		{
			Builder builder(this, current_);
			builder.Apply(edit);
			builder.SaveTo(v.get());
		}
		Finalize(v.get());

		Status s;
		if (created_new_manifest)
		{
			new_manifest_file = DescriptorFileName(dbname_, manifest_file_number);
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
			s = SetCurrentFile(env_, dbname_, manifest_file_number);
		}

		mu->lock();

		if (s.ok())
		{
			AppendVersion(v.release());
			manifest_file_number_ = manifest_file_number;
			log_number_ = edit->GetLogNumber();
			prev_log_number_ = edit->GetPrevLogNumber();
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

					InternalKey smallest;
					InternalKey largest;
					Status bounds = RecoverLegacyTableBounds(
					    env_, dbname_, options_, icmp_, number, static_cast<uint64_t>(file_size.value()), &smallest, &largest);
					if (!bounds.ok())
					{
						return bounds;
					}
					bootstrap_edit.AddFile(0, number, static_cast<uint64_t>(file_size.value()), smallest, largest);
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
			MarkFileNumberUsed(max_log_number);
			// Leave log_number at zero so DB recovery replays every legacy WAL.
			bootstrap_edit.SetLogNumber(0);

			std::shared_mutex mu;
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
		next_file_number_ = next_file;
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

	Compaction* VersionSet::PickCompaction()
	{
		if (current_ == nullptr)
		{
			return nullptr;
		}

		const bool size_compaction = (current_->compaction_score_ >= 1);
		if (!size_compaction)
		{
			return nullptr;
		}

		const int level = current_->compaction_level_;
		if (level < 0 || level + 1 >= kNumLevels)
		{
			return nullptr;
		}

		Compaction* c = new Compaction(options_, level);
		for (FileMetaData* file : current_->files_[level])
		{
			if (compact_pointer_[level].empty() || icmp_->Compare(file->largest.Encode(), compact_pointer_[level]) > 0)
			{
				c->inputs_[0].push_back(file);
				break;
			}
		}

		if (c->inputs_[0].empty())
		{
			c->inputs_[0].push_back(current_->files_[level][0]);
		}

		c->input_version_ = current_;
		c->input_version_->Ref();

		if (level == 0)
		{
			InternalKey smallest;
			InternalKey largest;
			GetRange(c->inputs_[0], &smallest, &largest);
			current_->GetOverlappingInputs(0, &smallest, &largest, &c->inputs_[0]);
			assert(!c->inputs_[0].empty());
		}

		SetupOtherInputs(c);
		return c;
	}

	void VersionSet::AddBoundaryInputs(
	    const InternalKeyComparator& icmp, const std::vector<FileMetaData*>& level_files, std::vector<FileMetaData*>* compaction_files)
	{
		InternalKey largest_key;
		if (!FindLargestKey(icmp, *compaction_files, &largest_key))
		{
			return;
		}

		while (true)
		{
			FileMetaData* smallest_boundary_file = FindSmallestBoundaryFile(icmp, level_files, largest_key);
			if (smallest_boundary_file == nullptr)
			{
				return;
			}

			compaction_files->push_back(smallest_boundary_file);
			largest_key = smallest_boundary_file->largest;
		}
	}

	void VersionSet::SetupOtherInputs(Compaction* c)
	{
		const int level = c->level();
		InternalKey smallest;
		InternalKey largest;

		AddBoundaryInputs(*icmp_, current_->files_[level], &c->inputs_[0]);
		GetRange(c->inputs_[0], &smallest, &largest);

		current_->GetOverlappingInputs(level + 1, &smallest, &largest, &c->inputs_[1]);
		AddBoundaryInputs(*icmp_, current_->files_[level + 1], &c->inputs_[1]);

		InternalKey all_start;
		InternalKey all_limit;
		GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);

		if (!c->inputs_[1].empty())
		{
			std::vector<FileMetaData*> expanded0;
			current_->GetOverlappingInputs(level, &all_start, &all_limit, &expanded0);
			AddBoundaryInputs(*icmp_, current_->files_[level], &expanded0);

			const uint64_t inputs1_size = static_cast<uint64_t>(TotalFileSize(c->inputs_[1]));
			const uint64_t expanded0_size = static_cast<uint64_t>(TotalFileSize(expanded0));
			if (expanded0.size() > c->inputs_[0].size() && inputs1_size + expanded0_size < ExpandedCompactionByteSizeLimit(options_))
			{
				InternalKey new_start;
				InternalKey new_limit;
				GetRange(expanded0, &new_start, &new_limit);
				std::vector<FileMetaData*> expanded1;
				current_->GetOverlappingInputs(level + 1, &new_start, &new_limit, &expanded1);
				AddBoundaryInputs(*icmp_, current_->files_[level + 1], &expanded1);
				if (expanded1.size() == c->inputs_[1].size())
				{
					smallest = new_start;
					largest = new_limit;
					c->inputs_[0] = expanded0;
					c->inputs_[1] = expanded1;
					GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);
				}
			}
		}

		if (level + 2 < kNumLevels)
		{
			current_->GetOverlappingInputs(level + 2, &all_start, &all_limit, &c->grandparents_);
		}

		compact_pointer_[level] = largest.Encode().ToString();
		c->edit_.SetCompactPointer(level, largest);
	}

	const std::string& VersionSet::compact_pointer(int level) const
	{
		assert(level >= 0 && level < kNumLevels);
		return compact_pointer_[level];
	}

	void VersionSet::AddLiveFiles(std::set<uint64_t>* live)
	{
		std::lock_guard<std::mutex> lock(version_list_mutex_);
		if (current_ == nullptr)
		{
			return;
		}

		const Version* v = current_;
		do
		{
			for (int level = 0; level < kNumLevels; ++level)
			{
				const auto& files = v->files(level);
				for (const FileMetaData* file : files)
				{
					live->insert(file->number);
				}
			}
		}
		while ((v = v->next_) != current_);
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
				if (file->refs.fetch_sub(1, std::memory_order_acq_rel) == 1)
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
			file->refs.store(1, std::memory_order_relaxed);
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

	Compaction::Compaction(const Options* options, int level)
	    : level_(level)
	    , level_out_(level + 1)
	    , max_output_file_size_(MaxFileSizeForLevel(options, level))
	    , max_grandparent_overlap_bytes_(MaxGrandParentOverlapBytes(options))
	    , input_version_(nullptr)
	    , grandparent_index_(0)
	    , seen_key_(false)
	    , overlapped_bytes_(0)
	{
		for (int level_index = 0; level_index < kNumLevels; ++level_index)
		{
			level_ptrs_[level_index] = 0;
		}
	}

	Compaction::~Compaction()
	{
		if (input_version_ != nullptr)
		{
			input_version_->Unref();
		}
	}

	bool Compaction::IsTrivialMove() const
	{
		return (num_input_files(0) == 1 && num_input_files(1) == 0
		    && static_cast<uint64_t>(TotalFileSize(grandparents_)) <= max_grandparent_overlap_bytes_);
	}

	void Compaction::AddInputDeletions(VersionEdit* edit)
	{
		for (int which = 0; which < 2; ++which)
		{
			for (FileMetaData* file : inputs_[which])
			{
				edit->RemoveFile(level_ + which, file->number);
			}
		}
	}

	bool Compaction::IsBaseLevelForKey(const Slice& user_key)
	{
		const Comparator* user_cmp = input_version_->vset_->Comparator()->user_comparator();
		for (int level = level_ + 2; level < kNumLevels; ++level)
		{
			const auto& files = input_version_->files_[level];
			while (level_ptrs_[level] < files.size())
			{
				FileMetaData* file = files[level_ptrs_[level]];
				if (user_cmp->Compare(user_key, file->largest.user_key()) <= 0)
				{
					if (user_cmp->Compare(user_key, file->smallest.user_key()) >= 0)
					{
						return false;
					}
					break;
				}
				++level_ptrs_[level];
			}
		}
		return true;
	}

	bool Compaction::ShouldStopBefore(const Slice& internal_key)
	{
		const VersionSet* vset = input_version_->vset_;
		while (grandparent_index_ < grandparents_.size()
		    && vset->Comparator()->Compare(internal_key, grandparents_[grandparent_index_]->largest.Encode()) > 0)
		{
			if (seen_key_)
			{
				overlapped_bytes_ += grandparents_[grandparent_index_]->file_size;
			}
			++grandparent_index_;
		}

		seen_key_ = true;
		if (overlapped_bytes_ > max_grandparent_overlap_bytes_)
		{
			overlapped_bytes_ = 0;
			return true;
		}
		return false;
	}

	void Compaction::ReleaseInputs()
	{
		if (input_version_ != nullptr)
		{
			input_version_->Unref();
			input_version_ = nullptr;
		}
	}

}
