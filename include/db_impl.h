#ifndef PRISM_DB_IMPL_H
#define PRISM_DB_IMPL_H

#include "db.h"
#include "log_writer.h"
#include "log_reader.h"
#include "memtable.h"

namespace prism
{
	class DBImpl: public DB
	{
	public:
		DBImpl(const std::string& dbname);
		~DBImpl() override;
		Status Put(const Slice& key, const Slice& value) override;
		Status Get(const Slice& key, std::string* value) override;
		Status Delete(const Slice& key) override;
		Status Write(WriteBatch& batch) override;

	private:
		class RecoveryHandler;

		Status ApplyBatch(WriteBatch& batch);

		MemTable* mem_;
		log::Writer writer_;
		log::Reader reader_;
		SequenceNumber sequence_ = 0;
		InternalKeyComparator internal_comparator_;
	};
} // namespace prism

#endif // PRISM_DB_IMPL_H