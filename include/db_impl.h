#ifndef PRISM_DB_IMPL_H
#define PRISM_DB_IMPL_H

#include "db.h"
#include "dbformat.h"
#include "options.h"
#include "memtable.h"
#include "result.h"
#include "task_scope.h"
#include "version_set.h"

#include <atomic>
#include <condition_variable>
#include <optional>
#include <set>
#include <shared_mutex>
#include <string>

namespace prism
{
	class CompactionController;
	class FileLock;
	struct RuntimeBundle;
	class SnapshotRegistry;
	class TableCache;
	class CompactionExecutionTest;

	class DBImpl
	{
	public:
		static Result<std::unique_ptr<DBImpl>> OpenInternal(const Options& options, const std::string& dbname);
		static Result<std::unique_ptr<DBImpl>> OpenInternal(const std::string& dbname);

		DBImpl(const Options& options, const std::string& dbname);
		~DBImpl();
		Status Put(const WriteOptions& options, const Slice& key, const Slice& value);
		Status Put(const Slice& key, const Slice& value) { return Put(WriteOptions(), key, value); }
		Result<std::string> Get(const ReadOptions& options, const Slice& key);
		Result<std::string> Get(const Slice& key) { return Get(ReadOptions(), key); }
		Status Delete(const WriteOptions& options, const Slice& key);
		Status Delete(const Slice& key) { return Delete(WriteOptions(), key); }
		// TODO(wal-rotation): Write() will gain a leader/follower group-commit queue.
		//   Batching policy: contiguous, same-sync, same-epoch, bounded by count/bytes.
		Status Write(const WriteOptions& options, WriteBatch batch);
		Status Write(WriteBatch batch) { return Write(WriteOptions(), std::move(batch)); }
		std::unique_ptr<Iterator> NewIterator(const ReadOptions& options);
		Snapshot CaptureSnapshot();
		std::optional<SequenceNumber> GetOldestLiveSnapshotSequence() const;

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
		void TEST_HoldBackgroundCompaction(bool hold);
		int TEST_BackgroundCompactionStartCount() const;
		void TEST_SignalBackgroundWorkFinished();
		void TEST_ScheduleCompaction();
		void TEST_RequestCompactionStop();
		bool TEST_HasInFlightCompaction() const;
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
		size_t TEST_ActiveSnapshotCount() const;

	private:
		friend class CompactionExecutionTest;
		friend class CompactionController;

		struct CompactionWorkResult
		{
			std::vector<FileMetaData> outputs;
			uint64_t total_bytes = 0;
			bool completed = false;
		};

		class RecoveryHandler;
		class LogFileGuard;

		Status Recover();
		Status ApplyBatch(WriteBatch& batch);
		Status MakeRoomForWrite(bool force, std::unique_lock<std::shared_mutex>& lock);
		void MaybeScheduleCompaction();
		void RecordBackgroundError(const Status& status);
		void BackgroundCompaction(StopToken stop_token);
		Status InstallCompactionResults(Compaction* compaction, const std::vector<FileMetaData>& outputs, uint64_t total_bytes);
		void CompactMemTable();
		Iterator* MakeInputIterator(Compaction* compaction);
		Status DoCompactionWork(Compaction* compaction, StopToken stop_token, CompactionWorkResult* result);
		Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base);
		Status RecoverLogFiles(const std::vector<uint64_t>& log_numbers);
		void RemoveObsoleteFiles();
		Status NewLogFile();
		Status CloseLogFile();
		Result<SequenceNumber> ResolveSnapshotSequence(const std::optional<Snapshot>& snapshot_handle) const;

		mutable std::shared_mutex mutex_;
		std::condition_variable_any background_work_finished_signal_;
		bool hold_background_compaction_ = false;
		int background_compaction_start_count_ = 0;

		Env* env_;
		Options options_;
		std::string dbname_;
		TableCache* table_cache_ = nullptr;

		std::unique_ptr<FileLock> db_lock_;

		// TODO(wal-rotation): Will add retired_wal_guard_ (unique_ptr<LogFileGuard>) and
		//   retired_wal_number_ (uint64_t) to track the single recovery-live retired WAL slot.
		//   Invariant: at most one retired recovery-live WAL exists alongside the active log.
		std::unique_ptr<LogFileGuard> log_file_guard_;
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
		std::shared_ptr<RuntimeBundle> runtime_bundle_;
		std::unique_ptr<CompactionController> compaction_controller_;
		Status bg_error_;
		std::atomic<bool> shutting_down_{ false };
		std::shared_ptr<SnapshotRegistry> snapshot_registry_;
	};
} // namespace prism

#endif // PRISM_DB_IMPL_H
