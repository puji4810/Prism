#ifndef PRISM_DB_IMPL_H
#define PRISM_DB_IMPL_H

#include "db.h"
#include "options.h"
#include "log_writer.h"
#include "log_reader.h"
#include "memtable.h"

#include <vector>

namespace prism
{
	class TableCache;

	class DBImpl: public DB
	{
	public:
		DBImpl(const Options& options, const std::string& dbname);
		~DBImpl() override;
		Status Put(const Slice& key, const Slice& value) override;
		Status Get(const Slice& key, std::string* value) override;
		Status Delete(const Slice& key) override;
		Status Write(WriteBatch& batch) override;

	private:
		class RecoveryHandler;
		struct FileMeta
		{
			uint64_t number;
			uint64_t file_size;
			InternalKey smallest;
			InternalKey largest;
		};

		Status ApplyBatch(WriteBatch& batch);
		Status FlushMemTable();
		Status RecoverLogFile();
		Status RecoverTableFiles();

		Env* env_;
		Options options_;
		std::string dbname_;
		TableCache* table_cache_ = nullptr;

		MemTable* mem_;
		MemTable* imm_ = nullptr;
		log::Writer writer_;
		log::Reader reader_;
		SequenceNumber sequence_ = 0;
		InternalKeyComparator internal_comparator_;

		uint64_t next_file_number_ = 1;
		std::vector<FileMeta> files_;
	};
} // namespace prism

#endif // PRISM_DB_IMPL_H
