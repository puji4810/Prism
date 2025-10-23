#include "db_impl.h"
#include "dbformat.h"
#include "slice.h"
#include "write_batch.h"
#include "write_batch_internal.h"
#include <sstream>
#include <memory>

namespace prism
{
	class DBImpl;

	// https://isocpp.github.io/CppCoreGuidelines/CppCoreGuidelines#c12-dont-make-data-members-const-or-references-in-a-copyable-or-movable-type
	// Data members that are not const or references should not be made copyable or movable.
	// Here we make sure that the store_ is not copyable or movable.
	class DBImpl::RecoveryHandler: public WriteBatch::Handler
	{
	public:
		RecoveryHandler(std::unordered_map<std::string, std::string>& store)
		    : store_(store)
		{
		}
		RecoveryHandler(const RecoveryHandler&) = delete;
		RecoveryHandler& operator=(const RecoveryHandler&) = delete;
		RecoveryHandler(RecoveryHandler&&) = delete;
		RecoveryHandler& operator=(RecoveryHandler&&) = delete;

		void Put(const Slice& key, const Slice& value) override { store_[key.ToString()] = value.ToString(); }

		void Delete(const Slice& key) override { store_.erase(key.ToString()); }

	private:
		std::unordered_map<std::string, std::string>& store_;
	};

	DB::~DB() = default;

	std::unique_ptr<DB> DB::Open(const std::string& dbname) { return std::make_unique<DBImpl>(dbname); }

	DBImpl::DBImpl(const std::string& dbname)
	    : writer_{ dbname }
	    , reader_{ dbname }
	{
		RecoveryHandler handler(store_);
		Slice record;

		while (reader_.ReadRecord(&record))
		{
			if (record.empty())
				continue;

			WriteBatch batch;
			WriteBatchInternal::SetContents(&batch, record);

			auto batch_seq = WriteBatchInternal::Sequence(&batch);
			auto batch_count = WriteBatchInternal::Count(&batch);
			if (batch_seq + batch_count > sequence_)
			{
				sequence_ = batch_seq + batch_count;
			}

			auto status = batch.Iterate(&handler);
			if (!status.ok())
			{
			}
		}
	}

	DBImpl::~DBImpl() = default;

	Status DBImpl::ApplyBatch(WriteBatch& batch)
	{
		RecoveryHandler handler(store_);
		return batch.Iterate(&handler);
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
		auto it = store_.find(key.ToString());
		if (it != store_.end())
		{
			*value = it->second;
			return Status::OK();
		}
		return Status::NotFound("Key not found: " + key.ToString());
	}

	Status DBImpl::Delete(const Slice& key) { return DB::Delete(key); }

	Status DBImpl::Write(WriteBatch& batch)
	{
		WriteBatchInternal::SetSequence(&batch, sequence_);
		sequence_ += WriteBatchInternal::Count(&batch);

		Slice record = WriteBatchInternal::Contents(&batch);
		writer_.AddRecord(record);

		return ApplyBatch(batch);
	}
}