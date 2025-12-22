#ifndef PRISM_DB_IMPL_H
#define PRISM_DB_IMPL_H

#include "db.h"
#include "options.h"
#include "log_writer.h"
#include "memtable.h"
#include "result.h"

#include <string>
#include <vector>

namespace prism
{
	class FileLock;
	class TableCache;

	class DBImpl: public DB
	{
	public:
		DBImpl(const Options& options, const std::string& dbname);
		~DBImpl() override;
		Status Put(const WriteOptions& options, const Slice& key, const Slice& value) override;
		Result<std::string> Get(const ReadOptions& options, const Slice& key) override;
		Status Delete(const WriteOptions& options, const Slice& key) override;
		Status Write(const WriteOptions& options, WriteBatch* batch) override;
		Iterator* NewIterator(const ReadOptions& options) override;
		const Snapshot* GetSnapshot() override;
		void ReleaseSnapshot(const Snapshot* snapshot) override;

	private:
		friend class DB;

		class RecoveryHandler;
		struct FileMeta
		{
			uint64_t number;
			uint64_t file_size;
			InternalKey smallest;
			InternalKey largest;
		};

		Status Recover();
		Status ApplyBatch(WriteBatch& batch);
		Status FlushMemTable();
		Status RecoverLogFiles(const std::vector<uint64_t>& log_numbers);
		Status RecoverTableFiles(std::vector<uint64_t>* log_numbers);
		Status NewLogFile();
		Status CloseLogFile();

		Env* env_;
		Options options_;
		std::string dbname_;
		TableCache* table_cache_ = nullptr;

		FileLock* db_lock_ = nullptr;

		WritableFile* logfile_ = nullptr;
		std::unique_ptr<log::Writer> log_;
		uint64_t logfile_number_ = 0;

		MemTable* mem_;
		MemTable* imm_ = nullptr;
		SequenceNumber sequence_ = 0;
		InternalKeyComparator internal_comparator_;

		uint64_t next_file_number_ = 1;
		std::vector<FileMeta> files_;
	};
} // namespace prism

#endif // PRISM_DB_IMPL_H
