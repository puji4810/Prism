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
#include <shared_mutex>
#include <string>
#include <atomic>

namespace prism
{
	class FileLock;
	class TableCache;
	class CompactionExecutionTest;

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
		void TEST_RemoveObsoleteFiles() { RemoveObsoleteFiles(); }
		void TEST_AddPendingOutput(uint64_t number) { pending_outputs_.insert(number); }
		bool TEST_HasImmutableMemTable() const;
		int TEST_NumLevelFiles(int level) const;
		void TEST_SetBackgroundError(const Status& status);
		void TEST_SignalBackgroundWorkFinished();
		uint64_t TEST_NewFileNumber();
		Status TEST_AddFileToVersion(
		    int level, uint64_t number, uint64_t file_size, const InternalKey& smallest, const InternalKey& largest);
		Status TEST_RunPickedCompaction();
		Status TEST_RunBackgroundCompactionOnce();
		std::vector<FileMetaData> TEST_LevelFilesCopy(int level) const;
		TableCache* TEST_TableCache() const { return table_cache_; }
		Env* TEST_Env() const { return env_; }
		const Options& TEST_Options() const { return options_; }
		const std::string& TEST_DBName() const { return dbname_; }
		bool TEST_PendingOutputsEmpty() const;

	private:
		friend class DB;
		friend class CompactionExecutionTest;

		class RecoveryHandler;

		Status Recover();
		Status ApplyBatch(WriteBatch& batch);
		Status MakeRoomForWrite(bool force, std::unique_lock<std::shared_mutex>& lock);
		void MaybeScheduleCompaction();
		void RecordBackgroundError(const Status& status);
		void BackgroundCall();
		static void BGWork(void* db);
		void BackgroundCompaction();
		Status InstallCompactionResults(Compaction* compaction, const std::vector<FileMetaData>& outputs, uint64_t total_bytes);
		void CompactMemTable();
		Iterator* MakeInputIterator(Compaction* compaction);
		Status DoCompactionWork(Compaction* compaction);
		Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base);
		Status RecoverLogFiles(const std::vector<uint64_t>& log_numbers);
		void RemoveObsoleteFiles();
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
		std::atomic<bool> shutting_down_{ false };
	};
} // namespace prism

#endif // PRISM_DB_IMPL_H
