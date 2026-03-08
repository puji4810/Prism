#include "db_impl.h"

#include "comparator.h"
#include "db.h"
#include "dbformat.h"
#include "env.h"
#include "filename.h"
#include "iterator.h"
#include "log_reader.h"
#include "options.h"
#include "slice.h"
#include "status.h"
#include "table_cache.h"
#include "table/merger.h"
#include "table/table_builder.h"
#include "version_edit.h"
#include "write_batch.h"
#include "write_batch_internal.h"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <memory>
#include <mutex>

namespace prism
{
	namespace
	{
		constexpr int kNumNonTableCacheFiles = 10;

		// ─────────────────────────────────────────────────────────────────────────
		// ReadView – a pinned snapshot of the DB read state.
		//
		// Captured while holding mutex_; refs on mem, imm, and current version are
		// bumped atomically so that all three survive independently of background
		// compaction and obsolete-file cleanup for the lifetime of an iterator or
		// point-read.
		// ─────────────────────────────────────────────────────────────────────────
		struct ReadView
		{
			MemTable* mem = nullptr; // Ref()-ed on capture, Unref()-ed on release.
			MemTable* imm = nullptr; // May be nullptr; Ref()-ed if non-null.
			Version* current = nullptr; // Ref()-ed on capture.
			SequenceNumber snapshot = 0; // Visible sequence number for this read.
		};

		// Release all resources captured in a ReadView and delete it.
		void ReleaseReadView(void* /*arg1*/, void* arg2)
		{
			auto* rv = reinterpret_cast<ReadView*>(arg2);
			if (rv->current != nullptr)
			{
				rv->current->Unref();
			}
			if (rv->imm != nullptr)
			{
				rv->imm->Unref();
			}
			if (rv->mem != nullptr)
			{
				rv->mem->Unref();
			}
			delete rv;
		}

		int TableCacheEntries(const Options& options)
		{
			const int entries = options.max_open_files - kNumNonTableCacheFiles;
			return (entries > 0) ? entries : 1;
		}

		Status BuildTable(const std::string& dbname, Env* env, const Options& options, TableCache* table_cache, uint64_t file_number,
		    Iterator* iter, uint64_t* file_size, InternalKey* smallest, InternalKey* largest)
		{
			*file_size = 0;
			smallest->Clear();
			largest->Clear();

			iter->SeekToFirst();
			if (!iter->Valid())
			{
				return iter->status();
			}

			const std::string fname = TableFileName(dbname, file_number);
			Status s;
			auto result = env->NewWritableFile(fname);
			if (!result.has_value())
			{
				return result.error();
			}
			auto file = std::move(result.value());

			TableBuilder builder(options, file.get());
			smallest->DecodeFrom(iter->key());

			for (; iter->Valid(); iter->Next())
			{
				largest->DecodeFrom(iter->key());
				builder.Add(iter->key(), iter->value());
			}

			if (iter->status().ok())
			{
				s = builder.Finish();
			}
			else
			{
				builder.Abandon();
				s = iter->status();
			}

			if (s.ok())
			{
				s = builder.status();
			}

			if (s.ok())
			{
				*file_size = builder.FileSize();
				s = file->Sync();
			}

			if (s.ok())
			{
				s = file->Close();
			}

			if (!s.ok())
			{
				env->RemoveFile(fname);
				return s;
			}

			std::unique_ptr<Iterator> check_iter(table_cache->NewIterator(ReadOptions(), file_number, *file_size));
			check_iter->SeekToFirst();
			s = check_iter->status();
			if (!s.ok())
			{
				env->RemoveFile(fname);
			}
			return s;
		}

		struct TableGetState
		{
			const Comparator* user_comparator;
			Slice user_key;
			std::string* value;
			bool value_found = false;
		};

		Status SaveValue(void* arg, const Slice& found_key, const Slice& found_value)
		{
			auto* state = reinterpret_cast<TableGetState*>(arg);
			ParsedInternalKey parsed;
			if (!ParseInternalKey(found_key, &parsed))
			{
				return Status::Corruption("bad internal key");
			}
			if (state->user_comparator->Compare(parsed.user_key, state->user_key) != 0)
			{
				return Status::OK();
			}

			switch (parsed.type)
			{
			case kTypeValue:
				state->value->assign(found_value.data(), found_value.size());
				state->value_found = true;
				return Status::OK();
			case kTypeDeletion:
				return Status::NotFound(Slice());
			default:
				return Status::Corruption("unknown value type");
			}
		}

		class DBIter: public Iterator
		{
		public:
			enum class Direction
			{
				kForward,
				kReverse
			};

			DBIter(const Comparator* user_comparator, Iterator* internal_iter, SequenceNumber sequence)
			    : user_comparator_(user_comparator)
			    , iter_(internal_iter)
			    , sequence_(sequence)
			    , direction_(Direction::kForward)
			    , valid_(false)
			{
			}

			~DBIter() override { delete iter_; }

			bool Valid() const override { return valid_; }

			Slice key() const override
			{
				assert(valid_);
				if (direction_ == Direction::kForward)
				{
					return ExtractUserKey(iter_->key());
				}
				return Slice(saved_key_);
			}

			Slice value() const override
			{
				assert(valid_);
				if (direction_ == Direction::kForward)
				{
					return iter_->value();
				}
				return Slice(saved_value_);
			}

			Status status() const override
			{
				if (status_.ok())
				{
					return iter_->status();
				}
				return status_;
			}

			void SeekToFirst() override
			{
				direction_ = Direction::kForward;
				ClearSavedValue();
				iter_->SeekToFirst();
				if (iter_->Valid())
				{
					FindNextUserEntry(false, &saved_key_ /*temporary*/);
				}
				else
				{
					valid_ = false;
				}
			}

			void SeekToLast() override
			{
				direction_ = Direction::kReverse;
				ClearSavedValue();
				iter_->SeekToLast();
				FindPrevUserEntry();
			}

			void Seek(const Slice& target) override
			{
				direction_ = Direction::kForward;
				ClearSavedValue();
				saved_key_.clear();
				AppendInternalKey(saved_key_, ParsedInternalKey(target, sequence_, kValueTypeForSeek));
				iter_->Seek(saved_key_);
				if (iter_->Valid())
				{
					FindNextUserEntry(false, &saved_key_ /*temporary*/);
				}
				else
				{
					valid_ = false;
				}
			}

			void Next() override
			{
				assert(valid_);

				if (direction_ == Direction::kReverse)
				{
					direction_ = Direction::kForward;
					if (!iter_->Valid())
					{
						iter_->SeekToFirst();
					}
					else
					{
						iter_->Next();
					}
					if (!iter_->Valid())
					{
						valid_ = false;
						saved_key_.clear();
						return;
					}
					// saved_key_ already contains the key to skip past.
				}
				else
				{
					SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
					iter_->Next();
					if (!iter_->Valid())
					{
						valid_ = false;
						saved_key_.clear();
						return;
					}
				}

				FindNextUserEntry(true, &saved_key_);
			}

			void Prev() override
			{
				assert(valid_);

				if (direction_ == Direction::kForward)
				{
					assert(iter_->Valid());
					SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
					while (true)
					{
						iter_->Prev();
						if (!iter_->Valid())
						{
							valid_ = false;
							saved_key_.clear();
							ClearSavedValue();
							return;
						}
						if (user_comparator_->Compare(ExtractUserKey(iter_->key()), saved_key_) < 0)
						{
							break;
						}
					}
					direction_ = Direction::kReverse;
				}

				FindPrevUserEntry();
			}

		private:
			bool ParseKey(ParsedInternalKey* ikey)
			{
				if (!ParseInternalKey(iter_->key(), ikey))
				{
					status_ = Status::Corruption("corrupted internal key in DBIter");
					return false;
				}
				return true;
			}

			void SaveKey(const Slice& k, std::string* dst) { dst->assign(k.data(), k.size()); }

			void ClearSavedValue()
			{
				if (saved_value_.capacity() > 1048576)
				{
					std::string empty;
					std::swap(empty, saved_value_);
				}
				else
				{
					saved_value_.clear();
				}
			}

			void FindNextUserEntry(bool skipping, std::string* skip)
			{
				assert(iter_->Valid());
				assert(direction_ == Direction::kForward);

				do
				{
					ParsedInternalKey ikey;
					if (ParseKey(&ikey) && ikey.sequence <= sequence_)
					{
						switch (ikey.type)
						{
						case kTypeDeletion:
							SaveKey(ikey.user_key, skip);
							skipping = true;
							break;
						case kTypeValue:
							if (skipping && user_comparator_->Compare(ikey.user_key, *skip) <= 0)
							{
								// Hidden.
							}
							else
							{
								valid_ = true;
								saved_key_.clear();
								return;
							}
							break;
						}
					}
					iter_->Next();
				} while (iter_->Valid());

				saved_key_.clear();
				valid_ = false;
			}

			void FindPrevUserEntry()
			{
				assert(direction_ == Direction::kReverse);

				ValueType value_type = kTypeDeletion;
				if (iter_->Valid())
				{
					do
					{
						ParsedInternalKey ikey;
						if (ParseKey(&ikey) && ikey.sequence <= sequence_)
						{
							if ((value_type != kTypeDeletion) && user_comparator_->Compare(ikey.user_key, saved_key_) < 0)
							{
								break;
							}

							value_type = ikey.type;
							if (value_type == kTypeDeletion)
							{
								saved_key_.clear();
								ClearSavedValue();
							}
							else
							{
								const Slice raw_value = iter_->value();
								if (saved_value_.capacity() > raw_value.size() + 1048576)
								{
									std::string empty;
									std::swap(empty, saved_value_);
								}
								SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
								saved_value_.assign(raw_value.data(), raw_value.size());
							}
						}
						iter_->Prev();
					} while (iter_->Valid());
				}

				if (value_type == kTypeDeletion)
				{
					valid_ = false;
					saved_key_.clear();
					ClearSavedValue();
					direction_ = Direction::kForward;
				}
				else
				{
					valid_ = true;
				}
			}

			const Comparator* user_comparator_;
			Iterator* iter_;
			SequenceNumber sequence_;
			Status status_;
			std::string saved_key_;
			std::string saved_value_;
			Direction direction_;
			bool valid_;
		};

		Iterator* NewDBIterator(const Comparator* user_comparator, Iterator* internal_iter, SequenceNumber sequence)
		{
			return new DBIter(user_comparator, internal_iter, sequence);
		}
	}

	// https://isocpp.github.io/CppCoreGuidelines/CppCoreGuidelines#c12-dont-make-data-members-const-or-references-in-a-copyable-or-movable-type
	// Data members that are not const or references should not be made copyable or movable.
	class DBImpl::RecoveryHandler: public WriteBatch::Handler
	{
	public:
		RecoveryHandler(MemTable* mem, SequenceNumber start)
		    : mem_(mem)
		    , sequence_(start)
		{
		}
		RecoveryHandler(const RecoveryHandler&) = delete;
		RecoveryHandler& operator=(const RecoveryHandler&) = delete;
		RecoveryHandler(RecoveryHandler&&) = delete;
		RecoveryHandler& operator=(RecoveryHandler&&) = delete;

		void Put(const Slice& key, const Slice& value) override { mem_->Add(sequence_++, kTypeValue, key, value); }

		void Delete(const Slice& key) override { mem_->Add(sequence_++, kTypeDeletion, key, Slice()); }

	private:
		MemTable* mem_;
		SequenceNumber sequence_;
	};

	DB::~DB() = default;

	Result<std::unique_ptr<DB>> DB::Open(const Options& options, const std::string& dbname)
	{
		Options opts = options;
		if (opts.env == nullptr)
		{
			opts.env = Env::Default();
		}
		if (opts.comparator == nullptr)
		{
			opts.comparator = BytewiseComparator();
		}

		auto impl = std::make_unique<DBImpl>(opts, dbname);
		Status s = impl->Recover();
		if (!s.ok())
		{
			return std::unexpected(s);
		}
		return std::unique_ptr<DB>(std::move(impl));
	}

	Result<std::unique_ptr<DB>> DB::Open(const std::string& dbname)
	{
		Options options;
		options.create_if_missing = true;
		return Open(options, dbname);
	}

	DBImpl::DBImpl(const Options& options, const std::string& dbname)
	    : env_(options.env)
	    , options_(options)
	    , dbname_(dbname)
	    , table_cache_(nullptr)
	    , mem_(nullptr)
	    , internal_comparator_(options.comparator)
	    , versions_(nullptr)
	{
		options_.comparator = &internal_comparator_;

		table_cache_ = new TableCache(dbname_, options_, TableCacheEntries(options_));
		versions_ = std::make_unique<VersionSet>(dbname_, &options_, table_cache_, &internal_comparator_);

		mem_ = new MemTable(internal_comparator_);
		mem_->Ref();
	}

	DBImpl::~DBImpl()
	{
		{
			std::unique_lock<std::mutex> lock(mutex_);
			shutting_down_.store(true, std::memory_order_release);
			while (bg_compaction_scheduled_)
			{
				background_work_finished_signal_.wait(lock);
			}
			background_work_finished_signal_.notify_all();
		}
		if (imm_)
		{
			imm_->Unref();
		}
		if (mem_)
		{
			mem_->Unref();
		}
		CloseLogFile();
		db_lock_.reset();
		delete table_cache_;
	}

	Status DBImpl::ApplyBatch(WriteBatch& batch)
	{
		RecoveryHandler handler(mem_, WriteBatchInternal::Sequence(&batch));
		return batch.Iterate(&handler);
	}

	Status DBImpl::CloseLogFile()
	{
		log_.reset();
		if (logfile_ == nullptr)
		{
			logfile_number_ = 0;
			return Status::OK();
		}
		Status s = logfile_->Close();
		delete logfile_;
		logfile_ = nullptr;
		logfile_number_ = 0;
		return s;
	}

	Status DBImpl::NewLogFile()
	{
		const uint64_t new_log_number = versions_->NewFileNumber();
		auto result = env_->NewWritableFile(LogFileName(dbname_, new_log_number));
		if (!result.has_value())
		{
			return result.error();
		}
		auto file = std::move(result.value());
		logfile_ = file.release();
		logfile_number_ = new_log_number;
		log_ = std::make_unique<log::Writer>(logfile_);
		return Status::OK();
	}

	Status DBImpl::Recover()
	{
		if (env_ == nullptr)
		{
			return Status::InvalidArgument("env is null");
		}

		Status s = env_->CreateDir(dbname_);
		if (!s.ok())
		{
			return s;
		}

		if (db_lock_ != nullptr)
		{
			return Status::InvalidArgument("db lock already held");
		}
		auto lock = env_->LockFile(LockFileName(dbname_));
		if (!lock.has_value())
		{
			return lock.error();
		}
		db_lock_ = std::move(lock.value());

		auto children_res = env_->GetChildren(dbname_);
		if (!children_res.has_value())
		{
			return children_res.error();
		}

		std::vector<uint64_t> log_numbers;
		bool db_exists = false;
		for (const auto& name : children_res.value())
		{
			uint64_t number = 0;
			FileType type;
			if (!ParseFileName(name, &number, &type))
			{
				continue;
			}
			if (type == FileType::kDBLockFile)
			{
				continue;
			}
			db_exists = true;
			if (type == FileType::kLogFile)
			{
				log_numbers.push_back(number);
			}
		}

		if (!db_exists)
		{
			if (!options_.create_if_missing)
			{
				return Status::InvalidArgument(dbname_, "does not exist (create_if_missing is false)");
			}
		}
		else if (options_.error_if_exists)
		{
			return Status::InvalidArgument(dbname_, "exists (error_if_exists is true)");
		}

		bool save_manifest = false;
		s = versions_->Recover(&save_manifest);
		if (s.IsNotFound())
		{
			if (!options_.create_if_missing)
			{
				return s;
			}
			VersionEdit new_db;
			new_db.SetComparatorName(internal_comparator_.user_comparator()->Name());
			new_db.SetLogNumber(0);
			new_db.SetPrevLogNumber(0);
			std::unique_lock<std::mutex> lock(mutex_);
			s = versions_->LogAndApply(&new_db, &mutex_);
			if (!s.ok())
			{
				return s;
			}
		}
		else if (!s.ok())
		{
			return s;
		}

		sequence_ = versions_->LastSequence() + 1;

		std::sort(log_numbers.begin(), log_numbers.end());
		std::vector<uint64_t> logs_to_recover;
		for (uint64_t number : log_numbers)
		{
			if (number >= versions_->LogNumber() || number == versions_->PrevLogNumber())
			{
				logs_to_recover.push_back(number);
			}
		}

		s = RecoverLogFiles(logs_to_recover);
		if (!s.ok())
		{
			return s;
		}

		versions_->SetLastSequence(sequence_ - 1);

		if (log_ == nullptr)
		{
			s = NewLogFile();
			if (s.ok())
			{
				VersionEdit edit;
				edit.SetLogNumber(logfile_number_);
				std::unique_lock<std::mutex> lock(mutex_);
				s = versions_->LogAndApply(&edit, &mutex_);
			}
		}
		return s;
	}

	void DBImpl::RemoveObsoleteFiles()
	{
		std::set<uint64_t> live;
		versions_->AddLiveFiles(&live);
		for (uint64_t number : pending_outputs_)
		{
			live.insert(number);
		}

		auto children = env_->GetChildren(dbname_);
		if (!children.has_value())
		{
			return;
		}

		for (const auto& filename : children.value())
		{
			uint64_t number = 0;
			FileType type;
			if (!ParseFileName(filename, &number, &type))
			{
				continue;
			}

			bool keep = true;
			switch (type)
			{
			case FileType::kLogFile:
				keep = (number >= versions_->LogNumber());
				break;
			case FileType::kDescriptorFile:
				keep = (number >= versions_->ManifestFileNumber());
				break;
			case FileType::kTableFile:
				keep = (live.count(number) > 0);
				break;
			case FileType::kTempFile:
				keep = (live.count(number) > 0);
				break;
			case FileType::kCurrentFile:
			case FileType::kDBLockFile:
			case FileType::kInfoLogFile:
				keep = true;
				break;
			}

			if (!keep)
			{
				if (type == FileType::kTableFile)
				{
					table_cache_->Evict(number);
				}
				env_->RemoveFile(dbname_ + "/" + filename);
			}
		}
	}

	Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version*)
	{
		assert(mem != nullptr);
		assert(edit != nullptr);

		const uint64_t table_number = versions_->NewFileNumber();
		pending_outputs_.insert(table_number);
		std::unique_ptr<Iterator> iter(mem->NewIterator());

		uint64_t file_size = 0;
		InternalKey smallest;
		InternalKey largest;
		mutex_.unlock();
		Status s = BuildTable(dbname_, env_, options_, table_cache_, table_number, iter.get(), &file_size, &smallest, &largest);
		mutex_.lock();

		pending_outputs_.erase(table_number);
		if (s.ok() && file_size > 0)
		{
			edit->AddFile(0, table_number, file_size, smallest, largest);
		}
		return s;
	}

	void DBImpl::CompactMemTable()
	{
		assert(imm_ != nullptr);
		VersionEdit edit;
		Version* base = versions_->current();
		base->Ref();
		Status s = WriteLevel0Table(imm_, &edit, base);
		base->Unref();

		if (s.ok() && shutting_down_.load(std::memory_order_acquire))
		{
			s = Status::IOError("Deleting DB during memtable compaction");
		}

		if (s.ok())
		{
			edit.SetPrevLogNumber(0);
			edit.SetLogNumber(logfile_number_);
			versions_->SetLastSequence(sequence_ - 1);
			s = versions_->LogAndApply(&edit, &mutex_);
		}

		if (s.ok())
		{
			imm_->Unref();
			imm_ = nullptr;
			RemoveObsoleteFiles();
		}
		else if (bg_error_.ok())
		{
			bg_error_ = s;
		}
	}

	void DBImpl::BackgroundCompaction()
	{
		if (imm_ != nullptr)
		{
			CompactMemTable();
		}
	}

	void DBImpl::MaybeScheduleCompaction()
	{
		if (bg_compaction_scheduled_)
		{
			return;
		}
		if (shutting_down_.load(std::memory_order_acquire))
		{
			return;
		}
		if (!bg_error_.ok())
		{
			return;
		}
		if (imm_ == nullptr)
		{
			return;
		}

		bg_compaction_scheduled_ = true;
		env_->Schedule(&DBImpl::BGWork, this);
	}

	void DBImpl::BGWork(void* db) { reinterpret_cast<DBImpl*>(db)->BackgroundCall(); }

	void DBImpl::BackgroundCall()
	{
		std::unique_lock<std::mutex> lock(mutex_);
		if (shutting_down_.load(std::memory_order_acquire))
		{
		}
		else if (!bg_error_.ok())
		{
		}
		else
		{
			BackgroundCompaction();
		}

		bg_compaction_scheduled_ = false;
		background_work_finished_signal_.notify_all();
		MaybeScheduleCompaction();
	}

	Status DBImpl::MakeRoomForWrite(bool force, std::unique_lock<std::mutex>& lock)
	{
		Status s;
		bool allow_delay = !force;
		while (true)
		{
			if (!bg_error_.ok())
			{
				s = bg_error_;
				break;
			}
			if (log_ == nullptr || logfile_ == nullptr)
			{
				s = Status::InvalidArgument("log file not open");
				break;
			}
			if (allow_delay && static_cast<int>(versions_->current()->files(0).size()) >= config::kL0_SlowdownWritesTrigger)
			{
				lock.unlock();
				env_->SleepForMicroseconds(1000);
				allow_delay = false;
				lock.lock();
			}
			else if (imm_ != nullptr)
			{
				background_work_finished_signal_.wait(lock);
			}
			else if (static_cast<int>(versions_->current()->files(0).size()) >= config::kL0_StopWritesTrigger)
			{
				background_work_finished_signal_.wait(lock);
			}
			else if (!force && mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)
			{
				break;
			}
			else
			{
				assert(versions_->PrevLogNumber() == 0);
				const uint64_t new_log_number = versions_->NewFileNumber();
				auto new_log_file = env_->NewWritableFile(LogFileName(dbname_, new_log_number));
				if (!new_log_file.has_value())
				{
					versions_->ReuseFileNumber(new_log_number);
					s = new_log_file.error();
					break;
				}

				const Status close_status = logfile_->Close();
				delete logfile_;
				logfile_ = new_log_file.value().release();
				logfile_number_ = new_log_number;
				log_ = std::make_unique<log::Writer>(logfile_);

				if (!close_status.ok())
				{
					if (bg_error_.ok())
					{
						bg_error_ = close_status;
					}
					s = close_status;
					break;
				}

				imm_ = mem_;
				mem_ = new MemTable(internal_comparator_);
				mem_->Ref();
				force = false;
				MaybeScheduleCompaction();
				break;
			}
		}

		return s;
	}

	Status DBImpl::RecoverLogFiles(const std::vector<uint64_t>& log_numbers)
	{
		struct LogReporter: public log::Reader::Reporter
		{
			Logger* info_log = nullptr;
			const char* fname = "";
			Status* status = nullptr;

			void Corruption(size_t bytes, const Status& s) override
			{
				Log(info_log, "%s: dropping %d bytes; %s", fname, static_cast<int>(bytes), s.ToString().c_str());
				if (status != nullptr && status->ok())
				{
					*status = s;
				}
			}
		};

		for (size_t i = 0; i < log_numbers.size(); ++i)
		{
			const uint64_t log_number = log_numbers[i];

			const std::string fname = LogFileName(dbname_, log_number);
			// SequentialFile* file = nullptr;
			Status s;
			auto file = env_->NewSequentialFile(fname);
			if (!file)
			{
				return file.error();
			}

			Status read_status;
			LogReporter reporter;
			reporter.info_log = options_.info_log;
			reporter.fname = fname.c_str();
			reporter.status = options_.paranoid_checks ? &read_status : nullptr;

			log::Reader reader(file.value().get(), &reporter, true /*verify_checksums*/, 0 /*initial_offset*/);

			std::string scratch;
			Slice record;
			while (reader.ReadRecord(&record, &scratch) && read_status.ok())
			{
				if (record.size() < kHeader)
				{
					reporter.Corruption(record.size(), Status::Corruption("log record too small"));
					continue;
				}

				WriteBatch batch;
				WriteBatchInternal::SetContents(&batch, record);

				const auto base = WriteBatchInternal::Sequence(&batch);
				const auto count = WriteBatchInternal::Count(&batch);

				RecoveryHandler handler(mem_, base);
				s = batch.Iterate(&handler);
				if (!s.ok())
				{
					break;
				}

				const SequenceNumber next = base + count;
				if (next > sequence_)
				{
					sequence_ = next;
				}

				if (mem_->ApproximateMemoryUsage() > options_.write_buffer_size)
				{
					const uint64_t table_number = versions_->NewFileNumber();
					pending_outputs_.insert(table_number);
					std::unique_ptr<Iterator> iter(mem_->NewIterator());

					uint64_t file_size = 0;
					InternalKey smallest;
					InternalKey largest;
					s = BuildTable(dbname_, env_, options_, table_cache_, table_number, iter.get(), &file_size, &smallest, &largest);
					pending_outputs_.erase(table_number);
					if (s.ok() && file_size > 0)
					{
						VersionEdit edit;
						edit.AddFile(0, table_number, file_size, smallest, largest);
						versions_->SetLastSequence(sequence_ - 1);
						std::unique_lock<std::mutex> lock(mutex_);
						s = versions_->LogAndApply(&edit, &mutex_);
					}
					mem_->Unref();
					mem_ = new MemTable(internal_comparator_);
					mem_->Ref();
					if (!s.ok())
					{
						break;
					}
				}
			}

			if (!s.ok())
			{
				return s;
			}
			if (!read_status.ok())
			{
				return read_status;
			}
			if (options_.paranoid_checks && !reader.status().ok())
			{
				return reader.status();
			}

			// Flush any remaining recovered entries into an sstable and delete this log file.
			if (mem_->ApproximateMemoryUsage() > 0)
			{
				const uint64_t table_number = versions_->NewFileNumber();
				pending_outputs_.insert(table_number);
				std::unique_ptr<Iterator> iter(mem_->NewIterator());

				uint64_t file_size = 0;
				InternalKey smallest;
				InternalKey largest;
				s = BuildTable(dbname_, env_, options_, table_cache_, table_number, iter.get(), &file_size, &smallest, &largest);
				pending_outputs_.erase(table_number);
				if (s.ok() && file_size > 0)
				{
					VersionEdit edit;
					edit.AddFile(0, table_number, file_size, smallest, largest);
					versions_->SetLastSequence(sequence_ - 1);
					std::unique_lock<std::mutex> lock(mutex_);
					s = versions_->LogAndApply(&edit, &mutex_);
				}
				mem_->Unref();
				mem_ = new MemTable(internal_comparator_);
				mem_->Ref();
				if (!s.ok())
				{
					return s;
				}
			}

			env_->RemoveFile(fname);
		}

		return Status::OK();
	}

	Status DBImpl::Put(const WriteOptions& write_options, const Slice& key, const Slice& value)
	{
		WriteBatch batch;
		batch.Put(key, value);
		return Write(write_options, std::move(batch));
	}

	Status DBImpl::Delete(const WriteOptions& write_options, const Slice& key)
	{
		WriteBatch batch;
		batch.Delete(key);
		return Write(write_options, std::move(batch));
	}

	Result<std::string> DBImpl::Get(const ReadOptions& read_options, const Slice& key)
	{
		// Phase 1: Capture a pinned read view under the lock.
		MemTable* mem = nullptr;
		MemTable* imm = nullptr;
		Version* current = nullptr;
		SequenceNumber snapshot = 0;
		{
			std::lock_guard<std::mutex> lock(mutex_);
			snapshot = (sequence_ == 0 ? 0 : sequence_ - 1);
			mem = mem_;
			if (mem != nullptr)
				mem->Ref();
			imm = imm_;
			if (imm != nullptr)
				imm->Ref();
			current = versions_->current();
			current->Ref();
		}

		// Phase 2: Look up key without holding the lock.
		std::string value;
		LookupKey lkey(key, snapshot);
		Status s;
		bool done = false;

		if (mem->Get(lkey, &value, &s))
		{
			done = true;
		}
		else if (imm != nullptr && imm->Get(lkey, &value, &s))
		{
			done = true;
		}
		else
		{
			InternalKey ikey(key, snapshot, kValueTypeForSeek);
			Slice internal_key = ikey.Encode();
			TableGetState state{ internal_comparator_.user_comparator(), key, &value };
			for (int level = 0; level < kNumLevels; ++level)
			{
				const auto& files = current->files(level);
				for (const FileMetaData* file : files)
				{
					s = table_cache_->Get(read_options, file->number, file->file_size, internal_key, &state, &SaveValue);
					if (!s.ok())
					{
						done = true;
						break;
					}
					if (state.value_found)
					{
						done = true;
						break;
					}
				}
				if (done)
					break;
			}
		}

		// Phase 3: Release all pinned resources.
		current->Unref();
		if (imm != nullptr)
			imm->Unref();
		mem->Unref();

		if (!s.ok())
		{
			return std::unexpected(s);
		}
		if (!value.empty())
		{
			return value;
		}
		return std::unexpected(Status::NotFound(Slice()));
	}

	Status DBImpl::Write(const WriteOptions& write_options, WriteBatch batch)
	{
		std::unique_lock<std::mutex> lock(mutex_);

		std::size_t count = WriteBatchInternal::Count(&batch);
		if (!count)
		{
			return Status::OK();
		}

		Status s = MakeRoomForWrite(false, lock);
		if (!s.ok())
		{
			return s;
		}
		if (log_ == nullptr || logfile_ == nullptr)
		{
			return Status::InvalidArgument("log file not open");
		}

		WriteBatchInternal::SetSequence(&batch, sequence_);
		sequence_ += count;
		versions_->SetLastSequence(sequence_ - 1);

		Slice record = WriteBatchInternal::Contents(&batch);
		s = log_->AddRecord(record);
		if (s.ok() && write_options.sync)
		{
			s = logfile_->Sync();
		}
		if (!s.ok())
		{
			return s;
		}

		s = ApplyBatch(batch);
		if (!s.ok())
		{
			return s;
		}
		return Status::OK();
	}

	std::unique_ptr<Iterator> DBImpl::NewIterator(const ReadOptions& read_options)
	{
		// Phase A (SuperVersion-lite): build a stable read view, then release the DB lock.
		//
		// TODO(phase-b): Replace this with a real Version/VersionSet + SuperVersion.
		// - Needed for compaction and safe table file deletion (iterators must pin versions/files).
		// TODO(snapshot): Implement Snapshot objects that pin the view (not just sequence).
		// TODO(async-scan): Provide AsyncIterator / NextAsync() to avoid blocking scans.
		struct SuperVersionLite
		{
			struct TableFileRef
			{
				uint64_t number;
				uint64_t file_size;
			};

			MemTable* mem = nullptr;
			MemTable* imm = nullptr;
			Version* current = nullptr;
			std::vector<TableFileRef> files;
			SequenceNumber snapshot = 0;
		};

		auto release_sv = [](void*, void* arg) {
			auto* sv = reinterpret_cast<SuperVersionLite*>(arg);
			if (sv->current != nullptr)
			{
				sv->current->Unref();
			}
			if (sv->imm != nullptr)
			{
				sv->imm->Unref();
			}
			if (sv->mem != nullptr)
			{
				sv->mem->Unref();
			}
			delete sv;
		};

		if (read_options.snapshot != nullptr)
		{
			return std::make_unique<EmptyIterator>(Status::NotSupported("snapshot"));
		}

		SuperVersionLite* sv = nullptr;
		{
			std::lock_guard<std::mutex> lock(mutex_);
			sv = new SuperVersionLite;
			sv->snapshot = sequence_ - 1;

			sv->mem = mem_;
			if (sv->mem != nullptr)
			{
				sv->mem->Ref();
			}

			sv->imm = imm_;
			if (sv->imm != nullptr)
			{
				sv->imm->Ref();
			}

			sv->current = versions_->current();
			sv->current->Ref();

			for (int level = 0; level < kNumLevels; ++level)
			{
				const auto& level_files = sv->current->files(level);
				for (const FileMetaData* file : level_files)
				{
					sv->files.push_back(SuperVersionLite::TableFileRef{ file->number, file->file_size });
				}
			}
		}

		std::vector<Iterator*> children;
		children.reserve(sv->files.size() + 2);

		children.push_back(sv->mem->NewIterator());
		if (sv->imm != nullptr)
		{
			children.push_back(sv->imm->NewIterator());
		}
		for (const auto& file : sv->files)
		{
			children.push_back(table_cache_->NewIterator(read_options, file.number, file.file_size));
		}

		Iterator* internal_iter = NewMergingIterator(&internal_comparator_, children.data(), static_cast<int>(children.size()));
		auto iter = std::make_unique<DBIter>(internal_comparator_.user_comparator(), internal_iter, sv->snapshot);
		iter->RegisterCleanup(release_sv, nullptr, sv);
		return iter;
	}

	const Snapshot* DBImpl::GetSnapshot()
	{
		std::lock_guard<std::mutex> lock(mutex_);
		return nullptr;
	}

	void DBImpl::ReleaseSnapshot(const Snapshot* /*snapshot*/) { std::lock_guard<std::mutex> lock(mutex_); }

	Version* DBImpl::TEST_CurrentVersion() const
	{
		std::lock_guard<std::mutex> lock(mutex_);
		return versions_->current();
	}

	int DBImpl::TEST_CurrentVersionRefs() const
	{
		std::lock_guard<std::mutex> lock(mutex_);
		return versions_->current()->TEST_Refs();
	}

	bool DBImpl::TEST_HasImmutableMemTable() const
	{
		std::lock_guard<std::mutex> lock(mutex_);
		return imm_ != nullptr;
	}

	int DBImpl::TEST_NumLevelFiles(int level) const
	{
		std::lock_guard<std::mutex> lock(mutex_);
		if (level < 0 || level >= kNumLevels)
		{
			return 0;
		}
		return static_cast<int>(versions_->current()->files(level).size());
	}

	void DBImpl::TEST_SetBackgroundError(const Status& status)
	{
		std::lock_guard<std::mutex> lock(mutex_);
		bg_error_ = status;
	}

	void DBImpl::TEST_SignalBackgroundWorkFinished()
	{
		std::lock_guard<std::mutex> lock(mutex_);
		background_work_finished_signal_.notify_all();
	}

	Status DestroyDB(const std::string& dbname, const Options& options)
	{
		Env* env = options.env ? options.env : Env::Default();
		auto filenames = env->GetChildren(dbname);
		if (!filenames.has_value())
		{
			// Ignore error in case directory does not exist
			return Status::OK();
		}
		Status result{};

		const std::string lockname = LockFileName(dbname);
		auto lock = env->LockFile(lockname);
		if (!lock.has_value())
		{
			return lock.error();
		}
		{
			auto lock_handle = std::move(lock.value());
			uint64_t number;
			FileType type;
			for (size_t i = 0; i < filenames.value().size(); i++)
			{
				if (ParseFileName(filenames.value()[i], &number, &type) && type != FileType::kDBLockFile)
				{ // Lock file will be deleted at end
					Status del = env->RemoveFile(dbname + "/" + filenames.value().at(i));
					if (result.ok() && !del.ok())
					{
						result = del;
					}
				}
			}
			lock_handle.reset();
			env->RemoveFile(lockname);
			env->RemoveDir(dbname); // Ignore error in case dir contains other files
		}
		return result;
	}
}
