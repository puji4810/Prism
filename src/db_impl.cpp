#include "db_impl.h"

#include "comparator.h"
#include "db.h"
#include "dbformat.h"
#include "env.h"
#include "filename.h"
#include "log_reader.h"
#include "options.h"
#include "slice.h"
#include "status.h"
#include "table_cache.h"
#include "table/merger.h"
#include "table/table_builder.h"
#include "write_batch.h"
#include "write_batch_internal.h"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <memory>

namespace prism
{
	namespace
	{
		constexpr int kNumNonTableCacheFiles = 10;

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
	    , mem_(nullptr)
	    , internal_comparator_(options.comparator)
	{
		options_.comparator = &internal_comparator_;

		table_cache_ = new TableCache(dbname_, options_, TableCacheEntries(options_));

		mem_ = new MemTable(internal_comparator_);
		mem_->Ref();
	}

	DBImpl::~DBImpl()
	{
		if (imm_)
		{
			imm_->Unref();
		}
		if (mem_)
		{
			mem_->Unref();
		}
		CloseLogFile();
		if (db_lock_ != nullptr)
		{
			env_->UnlockFile(db_lock_);
			db_lock_ = nullptr;
		}
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
		const uint64_t new_log_number = next_file_number_++;
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
		s = env_->LockFile(LockFileName(dbname_), &db_lock_);
		if (!s.ok())
		{
			return s;
		}

		std::vector<uint64_t> log_numbers;
		s = RecoverTableFiles(&log_numbers);
		if (!s.ok())
		{
			return s;
		}

		const bool db_exists = !files_.empty() || !log_numbers.empty();
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

		bool found_sequence = false;
		SequenceNumber max_sequence = 0;
		for (const auto& file : files_)
		{
			std::unique_ptr<Iterator> iter(table_cache_->NewIterator(ReadOptions(), file.number, file.file_size));
			for (iter->SeekToFirst(); iter->Valid(); iter->Next())
			{
				ParsedInternalKey parsed;
				if (!ParseInternalKey(iter->key(), &parsed))
				{
					return Status::Corruption("bad internal key");
				}
				found_sequence = true;
				if (parsed.sequence > max_sequence)
				{
					max_sequence = parsed.sequence;
				}
			}
			if (!iter->status().ok())
			{
				return iter->status();
			}
		}
		sequence_ = found_sequence ? (max_sequence + 1) : 0;

		s = RecoverLogFiles(log_numbers);
		if (!s.ok())
		{
			return s;
		}

		if (log_ == nullptr)
		{
			s = NewLogFile();
		}
		return s;
	}

	Status DBImpl::RecoverTableFiles(std::vector<uint64_t>* log_numbers)
	{
		files_.clear();
		log_numbers->clear();

		std::vector<std::string> filenames;
		Status s = env_->GetChildren(dbname_, &filenames);
		if (!s.ok())
		{
			return s;
		}

		uint64_t max_number = 0;
		for (const auto& name : filenames)
		{
			uint64_t number = 0;
			FileType type;
			if (!ParseFileName(name, &number, &type))
			{
				continue;
			}

			if (number > max_number)
			{
				max_number = number;
			}

			if (type == FileType::kLogFile)
			{
				log_numbers->push_back(number);
				continue;
			}

			if (type != FileType::kTableFile)
			{
				continue;
			}

			uint64_t file_size = 0;
			std::string fname = TableFileName(dbname_, number);
			s = env_->GetFileSize(fname, &file_size);
			if (!s.ok())
			{
				fname = SSTTableFileName(dbname_, number);
				s = env_->GetFileSize(fname, &file_size);
			}
			if (!s.ok())
			{
				continue;
			}

			std::unique_ptr<Iterator> iter(table_cache_->NewIterator(ReadOptions(), number, file_size));
			iter->SeekToFirst();
			if (!iter->status().ok())
			{
				return iter->status();
			}
			if (!iter->Valid())
			{
				continue;
			}

			FileMeta meta;
			meta.number = number;
			meta.file_size = file_size;
			meta.smallest.DecodeFrom(iter->key());
			iter->SeekToLast();
			if (iter->Valid())
			{
				meta.largest.DecodeFrom(iter->key());
			}
			files_.push_back(std::move(meta));
		}

		std::sort(files_.begin(), files_.end(), [](const FileMeta& a, const FileMeta& b) { return a.number < b.number; });
		std::sort(log_numbers->begin(), log_numbers->end());
		next_file_number_ = max_number + 1;
		if (next_file_number_ == 0)
		{
			next_file_number_ = 1;
		}
		return Status::OK();
	}

	Status DBImpl::FlushMemTable()
	{
		if (imm_ != nullptr)
		{
			return Status::InvalidArgument("immutable memtable already set");
		}
		if (log_ == nullptr || logfile_ == nullptr)
		{
			return Status::InvalidArgument("log file not open");
		}

		const uint64_t new_log_number = next_file_number_++;
		Status s;
		auto result = env_->NewWritableFile(LogFileName(dbname_, new_log_number));
		if (!result.has_value())
		{
			return result.error();
		}
		auto new_log_file = std::move(result.value()).release();

		const uint64_t old_log_number = logfile_number_;
		WritableFile* old_log_file = logfile_;
		auto old_log = std::move(log_);

		logfile_ = new_log_file;
		logfile_number_ = new_log_number;
		log_ = std::make_unique<log::Writer>(new_log_file);

		imm_ = mem_;
		mem_ = new MemTable(internal_comparator_);
		mem_->Ref();

		const uint64_t table_number = next_file_number_++;
		std::unique_ptr<Iterator> iter(imm_->NewIterator());

		uint64_t file_size = 0;
		InternalKey smallest;
		InternalKey largest;
		s = BuildTable(dbname_, env_, options_, table_cache_, table_number, iter.get(), &file_size, &smallest, &largest);
		if (!s.ok())
		{
			log_.reset();
			Status close_status = logfile_->Close();
			delete logfile_;
			logfile_ = nullptr;
			logfile_number_ = 0;
			env_->RemoveFile(LogFileName(dbname_, new_log_number));

			mem_->Unref();
			mem_ = imm_;
			imm_ = nullptr;

			logfile_ = old_log_file;
			logfile_number_ = old_log_number;
			log_ = std::move(old_log);
			if (!close_status.ok())
			{
				return close_status;
			}
			return s;
		}

		if (file_size > 0)
		{
			FileMeta meta;
			meta.number = table_number;
			meta.file_size = file_size;
			meta.smallest = smallest;
			meta.largest = largest;
			files_.push_back(std::move(meta));
		}

		imm_->Unref();
		imm_ = nullptr;

		old_log.reset();
		Status old_close = old_log_file->Close();
		delete old_log_file;
		if (old_close.ok())
		{
			env_->RemoveFile(LogFileName(dbname_, old_log_number));
			return Status::OK();
		}
		return old_close;
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
			const bool last_log = (i + 1 == log_numbers.size());

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
			int compactions = 0;
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
					++compactions;

					const uint64_t table_number = next_file_number_++;
					std::unique_ptr<Iterator> iter(mem_->NewIterator());

					uint64_t file_size = 0;
					InternalKey smallest;
					InternalKey largest;
					s = BuildTable(dbname_, env_, options_, table_cache_, table_number, iter.get(), &file_size, &smallest, &largest);
					if (s.ok() && file_size > 0)
					{
						FileMeta meta;
						meta.number = table_number;
						meta.file_size = file_size;
						meta.smallest = smallest;
						meta.largest = largest;
						files_.push_back(std::move(meta));
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

			// Try to reuse the last log file to avoid creating a new one on open.
			if (options_.reuse_logs && last_log && compactions == 0)
			{
				uint64_t file_size = 0;
				auto logfile_result = env_->NewAppendableFile(fname);
				if (!logfile_result)
				{
					// ignore and fall through
				}
				else if (env_->GetFileSize(fname, &file_size).ok())
				{
					logfile_ = logfile_result.value().release();
					logfile_number_ = log_number;
					log_ = std::make_unique<log::Writer>(logfile_, file_size);
					return Status::OK();
				}
			}

			// Flush any remaining recovered entries into an sstable and delete this log file.
			if (mem_->ApproximateMemoryUsage() > 0)
			{
				const uint64_t table_number = next_file_number_++;
				std::unique_ptr<Iterator> iter(mem_->NewIterator());

				uint64_t file_size = 0;
				InternalKey smallest;
				InternalKey largest;
				s = BuildTable(dbname_, env_, options_, table_cache_, table_number, iter.get(), &file_size, &smallest, &largest);
				if (s.ok() && file_size > 0)
				{
					files_.emplace_back(FileMeta{ table_number, file_size, smallest, largest });
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
		std::string value;
		const SequenceNumber snapshot = (sequence_ == 0 ? 0 : sequence_ - 1);
		LookupKey lkey(key, snapshot);
		Status s;
		if (mem_->Get(lkey, &value, &s))
		{
			if (s.ok())
			{
				return value;
			}
			return std::unexpected(s); // hit: OK or NotFound
		}
		if (imm_ && imm_->Get(lkey, &value, &s))
		{
			if (s.ok())
			{
				return value;
			}
			return std::unexpected(s);
		}

		InternalKey ikey(key, snapshot, kValueTypeForSeek);
		Slice internal_key = ikey.Encode();

		TableGetState state{ internal_comparator_.user_comparator(), key, &value };
		for (auto it = files_.rbegin(); it != files_.rend(); ++it)
		{
			s = table_cache_->Get(read_options, it->number, it->file_size, internal_key, &state, &SaveValue);
			if (!s.ok())
			{
				return std::unexpected(s);
			}
			if (state.value_found)
			{
				return value;
			}
		}
		return std::unexpected(Status::NotFound(Slice()));
	}

	Status DBImpl::Write(const WriteOptions& write_options, WriteBatch batch)
	{
		// TODO : Group commit
		std::size_t count = WriteBatchInternal::Count(&batch);
		if (!count)
		{
			return Status::OK();
		}
		if (log_ == nullptr || logfile_ == nullptr)
		{
			return Status::InvalidArgument("log file not open");
		}

		WriteBatchInternal::SetSequence(&batch, sequence_);
		sequence_ += count;

		Slice record = WriteBatchInternal::Contents(&batch);
		Status s = log_->AddRecord(record);
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

		if (mem_->ApproximateMemoryUsage() > options_.write_buffer_size)
		{
			return FlushMemTable();
		}
		return Status::OK();
	}

	Iterator* DBImpl::NewIterator(const ReadOptions& read_options)
	{
		if (read_options.snapshot != nullptr)
		{
			return NewErrorIterator(Status::NotSupported("snapshot"));
		}

		const SequenceNumber snapshot = (sequence_ == 0 ? 0 : sequence_ - 1);

		std::vector<Iterator*> children;
		children.reserve(files_.size() + 2);

		children.push_back(mem_->NewIterator());
		if (imm_ != nullptr)
		{
			children.push_back(imm_->NewIterator());
		}
		for (const auto& file : files_)
		{
			children.push_back(table_cache_->NewIterator(read_options, file.number, file.file_size));
		}

		Iterator* internal_iter = NewMergingIterator(&internal_comparator_, children.data(), static_cast<int>(children.size()));
		return NewDBIterator(internal_comparator_.user_comparator(), internal_iter, snapshot);
	}

	const Snapshot* DBImpl::GetSnapshot() { return nullptr; }

	void DBImpl::ReleaseSnapshot(const Snapshot* /*snapshot*/) {}

	Status DestroyDB(const std::string& dbname, const Options& options)
	{
		Env* env = options.env ? options.env : Env::Default();
		std::vector<std::string> filenames;
		Status result = env->GetChildren(dbname, &filenames);
		if (!result.ok())
		{
			// Ignore error in case directory does not exist
			return Status::OK();
		}

		FileLock* lock;
		const std::string lockname = LockFileName(dbname);
		result = env->LockFile(lockname, &lock);
		if (result.ok())
		{
			uint64_t number;
			FileType type;
			for (size_t i = 0; i < filenames.size(); i++)
			{
				if (ParseFileName(filenames[i], &number, &type) && type != FileType::kDBLockFile)
				{ // Lock file will be deleted at end
					Status del = env->RemoveFile(dbname + "/" + filenames[i]);
					if (result.ok() && !del.ok())
					{
						result = del;
					}
				}
			}
			env->UnlockFile(lock); // Ignore error since state is already gone
			env->RemoveFile(lockname);
			env->RemoveDir(dbname); // Ignore error in case dir contains other files
		}
		return result;
	}
}
