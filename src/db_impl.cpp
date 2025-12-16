#include "db_impl.h"

#include "comparator.h"
#include "dbformat.h"
#include "env.h"
#include "filename.h"
#include "slice.h"
#include "table_cache.h"
#include "table/table_builder.h"
#include "write_batch.h"
#include "write_batch_internal.h"

#include <algorithm>
#include <memory>

namespace prism
{
	namespace
	{
		constexpr uint64_t kLogFileNumber = 1;
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
			WritableFile* file = nullptr;
			Status s = env->NewWritableFile(fname, &file);
			if (!s.ok())
			{
				return s;
			}

			TableBuilder builder(options, file);
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

			delete file;

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

	std::unique_ptr<DB> DB::Open(const std::string& dbname)
	{
		Options options;
		if (options.env != nullptr)
		{
			Status s = options.env->CreateDir(dbname);
			if (!s.ok())
			{
				std::vector<std::string> children;
				if (!options.env->GetChildren(dbname, &children).ok())
				{
					return nullptr;
				}
			}
		}
		return std::make_unique<DBImpl>(options, dbname);
	}

	DBImpl::DBImpl(const Options& options, const std::string& dbname)
	    : env_(options.env)
	    , options_(options)
	    , dbname_(dbname)
	    , mem_(nullptr)
	    , writer_(LogFileName(dbname, kLogFileNumber))
	    , reader_(LogFileName(dbname, kLogFileNumber))
	    , internal_comparator_(options.comparator)
	{
		options_.comparator = &internal_comparator_;

		table_cache_ = new TableCache(dbname_, options_, TableCacheEntries(options_));

		mem_ = new MemTable(internal_comparator_);
		mem_->Ref();

		RecoverLogFile();
		RecoverTableFiles();
	}

	DBImpl::~DBImpl()
	{
		if (imm_)
			imm_->Unref();
		if (mem_)
			mem_->Unref();
		delete table_cache_;
	}

	Status DBImpl::ApplyBatch(WriteBatch& batch)
	{
		RecoveryHandler handler(mem_, WriteBatchInternal::Sequence(&batch));
		return batch.Iterate(&handler);
	}

	Status DBImpl::RecoverLogFile()
	{
		Slice record;
		while (reader_.ReadRecord(&record))
		{
			if (record.empty())
				continue;

			WriteBatch batch;
			WriteBatchInternal::SetContents(&batch, record);

			const auto base = WriteBatchInternal::Sequence(&batch);
			const auto count = WriteBatchInternal::Count(&batch);

			RecoveryHandler handler(mem_, base);
			Status s = batch.Iterate(&handler);
			if (!s.ok())
			{
				return s;
			}

			const SequenceNumber next = base + count;
			if (next > sequence_)
			{
				sequence_ = next;
			}
		}
		return Status::OK();
	}

	Status DBImpl::RecoverTableFiles()
	{
		files_.clear();

		std::vector<std::string> filenames;
		Status s = env_->GetChildren(dbname_, &filenames);
		if (!s.ok())
		{
			return s;
		}

		uint64_t max_number = kLogFileNumber;
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

		imm_ = mem_;
		mem_ = new MemTable(internal_comparator_);
		mem_->Ref();

		const uint64_t file_number = next_file_number_++;
		std::unique_ptr<Iterator> iter(imm_->NewIterator());

		uint64_t file_size = 0;
		InternalKey smallest;
		InternalKey largest;
		Status s = BuildTable(dbname_, env_, options_, table_cache_, file_number, iter.get(), &file_size, &smallest, &largest);
		if (!s.ok())
		{
			mem_->Unref();
			mem_ = imm_;
			imm_ = nullptr;
			return s;
		}

		if (file_size > 0)
		{
			FileMeta meta;
			meta.number = file_number;
			meta.file_size = file_size;
			meta.smallest = smallest;
			meta.largest = largest;
			files_.push_back(std::move(meta));
		}

		imm_->Unref();
		imm_ = nullptr;
		return Status::OK();
	}

	// Default implementation of Put
	Status DB::Put(const Slice& key, const Slice& value)
	{
		WriteBatch batch;
		batch.Put(key, value);
		return Write(batch);
	}

	// Default implementation of Delete
	Status DB::Delete(const Slice& key)
	{
		WriteBatch batch;
		batch.Delete(key);
		return Write(batch);
	}

	Status DBImpl::Put(const Slice& key, const Slice& value) { return DB::Put(key, value); }

	Status DBImpl::Get(const Slice& key, std::string* value)
	{
		const SequenceNumber snapshot = (sequence_ == 0 ? 0 : sequence_ - 1);
		LookupKey lkey(key, snapshot);
		Status s;
		if (mem_->Get(lkey, value, &s))
		{
			return s; // hit: OK or NotFound
		}
		if (imm_ && imm_->Get(lkey, value, &s))
		{
			return s;
		}

		InternalKey ikey(key, snapshot, kValueTypeForSeek);
		Slice internal_key = ikey.Encode();

		TableGetState state{ internal_comparator_.user_comparator(), key, value };
		ReadOptions read_options;
		for (auto it = files_.rbegin(); it != files_.rend(); ++it)
		{
			s = table_cache_->Get(read_options, it->number, it->file_size, internal_key, &state, &SaveValue);
			if (!s.ok())
			{
				return s;
			}
			if (state.value_found)
			{
				return Status::OK();
			}
		}
		return Status::NotFound(Slice());
	}

	Status DBImpl::Delete(const Slice& key) { return DB::Delete(key); }

	Status DBImpl::Write(WriteBatch& batch)
	{
		WriteBatchInternal::SetSequence(&batch, sequence_);
		sequence_ += WriteBatchInternal::Count(&batch);

		Slice record = WriteBatchInternal::Contents(&batch);
		writer_.AddRecord(record);

		Status s = ApplyBatch(batch);
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
}
