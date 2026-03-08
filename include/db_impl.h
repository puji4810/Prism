#ifndef PRISM_DB_IMPL_H
#define PRISM_DB_IMPL_H

#include "db.h"
#include "options.h"
#include "log_writer.h"
#include "memtable.h"
#include "result.h"
#include "version_set.h"

#include <condition_variable>
#include <set>
#include <string>
#include <shared_mutex>

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
		Status Write(const WriteOptions& options, WriteBatch batch) override;
		std::unique_ptr<Iterator> NewIterator(const ReadOptions& options) override;
		const Snapshot* GetSnapshot() override;
		void ReleaseSnapshot(const Snapshot* snapshot) override;

		// ── Test-only accessors ──────────────────────────────────────────────
		// Returns the current Version pointer (no additional Ref).
		Version* TEST_CurrentVersion() const;
		// Returns the current ref count of the current Version.
		int TEST_CurrentVersionRefs() const;
	private:
		friend class DB;

		class RecoveryHandler;

		Status Recover();
		Status ApplyBatch(WriteBatch& batch);
		Status FlushMemTable();
		Status RecoverLogFiles(const std::vector<uint64_t>& log_numbers);
		Status NewLogFile();
		Status CloseLogFile();

		mutable std::shared_mutex mutex_;
		std::condition_variable_any background_work_finished_signal_;

		Env* env_;
		Options options_;
		std::string dbname_;
		TableCache* table_cache_ = nullptr;

		std::unique_ptr<FileLock> db_lock_;

		WritableFile* logfile_ = nullptr;
		std::unique_ptr<log::Writer> log_;
		uint64_t logfile_number_ = 0;

		MemTable* mem_;
		MemTable* imm_ = nullptr;
		// sequence_ is the next sequence number to assign to a new write.
		// VersionSet persists last_sequence (last assigned), so:
		//   sequence_ == versions_->LastSequence() + 1
		SequenceNumber sequence_ = 1;
		InternalKeyComparator internal_comparator_;

		std::unique_ptr<VersionSet> versions_;
		std::set<uint64_t> pending_outputs_;
		Status bg_error_;
		bool bg_compaction_scheduled_ = false;
	};
} // namespace prism

#endif // PRISM_DB_IMPL_H
