#include "db_impl.h"
#include "comparator.h"
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

	std::unique_ptr<DB> DB::Open(const std::string& dbname) { return std::make_unique<DBImpl>(dbname); }

	DBImpl::DBImpl(const std::string& dbname)
	    : writer_{ dbname }
	    , reader_{ dbname }
	    , internal_comparator_(BytewiseComparator())
	{
		mem_ = new MemTable(internal_comparator_);
		mem_->Ref();

		Slice record;
		while (reader_.ReadRecord(&record))
		{
			if (record.empty())
				continue;

			WriteBatch batch;
			WriteBatchInternal::SetContents(&batch, record);

			const auto base = WriteBatchInternal::Sequence(&batch);
			const auto cnt = WriteBatchInternal::Count(&batch);

			RecoveryHandler handler(mem_, base);
			Status s = batch.Iterate(&handler); // Iterate use the method in handler
			if (!s.ok())
			{
				// TODO: handle error
			}

			if (base + cnt > sequence_)
				sequence_ = base + cnt;
		}
	}

	DBImpl::~DBImpl()
	{
		if (mem_)
			mem_->Unref();
	}

	Status DBImpl::ApplyBatch(WriteBatch& batch)
	{
		RecoveryHandler handler(mem_, WriteBatchInternal::Sequence(&batch));
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
		const SequenceNumber snapshot = (sequence_ == 0 ? 0 : sequence_ - 1);
		LookupKey lkey(key, snapshot);
		Status s;
		if (mem_->Get(lkey, value, &s))
		{
			return s; // hit: OK or NotFound
		}
		return Status::NotFound(Slice()); // miss: only MemTable, return NotFound
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