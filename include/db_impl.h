#ifndef PRISM_DB_IMPL_H
#define PRISM_DB_IMPL_H

#include "db.h"
#include "dbformat.h"
#include "options.h"
#include "memtable.h"
#include "result.h"
#include "async_write_op.h"
#include "task_scope.h"
#include "version_set.h"

#include <atomic>
#include <array>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <set>
#include <shared_mutex>
#include <span>
#include <string>

namespace prism
{
	class CompactionController;
	class FileLock;
	class AsyncRuntime;
	class AsyncWalWriter;
	class WritableFile;
	class SnapshotRegistry;
	class TableCache;
	class CompactionExecutionTest;
	class WriteCoordinator;
	namespace log
	{
		class Writer;
	}

	// Lightweight snapshot of DB background activity for benchmark/observability.
	// All fields are read without blocking the write path.
	struct CompactionStateSnapshot
	{
		bool compaction_in_flight = false;
		bool flush_in_flight = false;
		bool write_stalled = false;
		uint64_t compaction_start_count = 0;
		uint64_t compaction_finish_count = 0;
	};

	// Helper for benchmark code to access DBImpl from a Database handle.
	// Requires friend declaration in db.h.
	struct CompactionStateAccess
	{
		static DBImpl* GetDBImpl(Database& db);
		static Database& GetDatabase(AsyncDB& db);
	};

	struct SuperVersion
	{
		MemTable* mem = nullptr;
		MemTable* imm = nullptr;
		Version* current = nullptr;
		SequenceNumber sequence = 0;
		std::size_t point_read_epoch = 0;

		std::atomic<int> refs_{ 0 };

		void Ref() { refs_.fetch_add(1, std::memory_order_relaxed); }

		void Unref()
		{
			assert(refs_.load(std::memory_order_acquire) > 0);
			if (refs_.fetch_sub(1, std::memory_order_acq_rel) == 1)
			{
				if (current != nullptr)
					current->Unref();
				if (imm != nullptr)
					imm->Unref();
				if (mem != nullptr)
					mem->Unref();
				delete this;
			}
		}
	};

	class DBImpl
	{
	public:
		static Result<std::unique_ptr<DBImpl>> OpenInternal(const Options& options, const std::string& dbname);
		static Result<std::unique_ptr<DBImpl>> OpenInternal(const Options& options, const std::string& dbname, AsyncRuntime* runtime);
		static Result<std::unique_ptr<DBImpl>> OpenInternal(const std::string& dbname);

		DBImpl(const Options& options, const std::string& dbname, AsyncRuntime* runtime = nullptr);
		~DBImpl();
		Status Put(const WriteOptions& options, const Slice& key, const Slice& value);
		Status Put(const Slice& key, const Slice& value) { return Put(WriteOptions(), key, value); }
		Result<std::string> Get(const ReadOptions& options, const Slice& key);
		Result<std::string> Get(const Slice& key) { return Get(ReadOptions(), key); }
		void GetAsyncCallback(AsyncRuntime& runtime,
		    ReadOptions options,
		    std::string key,
		    std::move_only_function<void(Result<std::string>)> completion);
		Status Delete(const WriteOptions& options, const Slice& key);
		Status Delete(const Slice& key) { return Delete(WriteOptions(), key); }
		Status Write(const WriteOptions& options, WriteBatch batch);
		Status Write(WriteBatch batch) { return Write(WriteOptions(), std::move(batch)); }
		std::unique_ptr<Iterator> NewIterator(const ReadOptions& options);
		Snapshot CaptureSnapshot();
		// ADVISORY ONLY — reads snapshot_registry_ without holding mutex_. Snapshot
		// count/liveness may change between the query and any action taken based on it.
		// Use FreezeCompactionWatermark() inside the compaction path or any context
		// that derives reclaim/drop decisions.
		std::optional<SequenceNumber> GetOldestLiveSnapshotSequence() const;

		// ── Test-only accessors ──────────────────────────────────────────────
		// Returns the current Version pointer (no additional Ref).
		Version* TEST_CurrentVersion() const;
		// Returns the current ref count of the current Version.
		int TEST_CurrentVersionRefs() const;
		void TEST_RemoveObsoleteFiles()
		{
			std::unique_lock<std::shared_mutex> lock(mutex_);
			RemoveObsoleteFiles();
		}
		void TEST_AddPendingOutput(uint64_t number)
		{
			std::unique_lock<std::shared_mutex> lock(mutex_);
			pending_outputs_.insert(number);
		}
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
		SuperVersion* TEST_CurrentSuperVersion() const;
		std::vector<FileMetaData> TEST_LevelFilesCopy(int level) const;
		TableCache* TEST_TableCache() const { return table_cache_; }
		Env* TEST_Env() const { return env_; }
		const Options& TEST_Options() const { return options_; }
		const std::string& TEST_DBName() const { return dbname_; }
		bool TEST_PendingOutputsEmpty() const;
		size_t TEST_ActiveSnapshotCount() const;

		// Returns a lightweight snapshot of compaction/flush/stall state
		// without blocking the write path for more than a shared_lock.
		CompactionStateSnapshot GetCompactionState() const;

	private:
		friend class AsyncDB;
		friend class CompactionExecutionTest;
		friend class CompactionController;
		friend class WriteCoordinator;

		struct CompactionWorkResult
		{
			std::vector<FileMetaData> outputs;
			uint64_t total_bytes = 0;
			bool completed = false;
		};

		struct WritePlan
		{
			WritePlan() = default;
			~WritePlan();
			WritePlan(const WritePlan&) = delete;
			WritePlan& operator=(const WritePlan&) = delete;
			WritePlan(WritePlan&& other) noexcept;
			WritePlan& operator=(WritePlan&& other) noexcept;

			void CaptureCommitTarget(MemTable* target);
			void Reset() noexcept;

			log::Writer* writer = nullptr;
			WritableFile* file = nullptr;
			MemTable* commit_target = nullptr;
			bool sync = false;
			SequenceNumber visible_sequence = 0;
		};

		static constexpr std::size_t kPointReadEpochCount = 2;
		static constexpr std::size_t kPointReadShardCount = 64;
		struct alignas(64) PointReadCounter
		{
			std::atomic<uint32_t> value{ 0 };
		};
		struct PointReadGuard
		{
			PointReadGuard() = default;
			PointReadGuard(DBImpl* db_arg, SuperVersion* view_arg, std::size_t epoch_arg, std::size_t shard_arg);
			~PointReadGuard();
			PointReadGuard(PointReadGuard&& other) noexcept;
			PointReadGuard& operator=(PointReadGuard&& other) noexcept;
			PointReadGuard(const PointReadGuard&) = delete;
			PointReadGuard& operator=(const PointReadGuard&) = delete;

			DBImpl* db = nullptr;
			SuperVersion* view = nullptr;
			std::size_t epoch = 0;
			std::size_t shard = 0;
		};

		class RecoveryHandler;
		class LogFileGuard;

		AsyncWriteOp WriteAsync(const WriteOptions& options, WriteBatch batch);
		AsyncWriteOp WriteAsync(
		    const WriteOptions& options, WriteBatch batch, void* keep_alive, void (*release_keep_alive)(void*));
		Status PlanWriteGroupForCoordinator(std::span<WriteRequestState* const> requests, WriteBatch* merged, WritePlan* plan);
		void StartWritePlanForCoordinator(const WritePlan& plan, WriteBatch merged, std::function<void(Status)> completion);
		Status CommitWriteGroupForCoordinator(std::span<WriteRequestState* const> requests, const WritePlan& plan);
		void RecordWriteFailureForCoordinator(const Status& status);
		Status Recover();
		Status ApplyBatch(const WriteBatch& batch, MemTable* target);
		PointReadGuard AcquirePointRead();
		void ReleasePointRead(std::size_t epoch, std::size_t shard) noexcept;
		bool PointReadEpochIdle(std::size_t epoch) const noexcept;
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
		// Freeze the oldest-live-snapshot watermark while mutex_ is held.
		// The returned value must NOT be used outside the compaction path
		// and must never drive reclaim/drop decisions from an unlocked context.
		SequenceNumber FreezeCompactionWatermark() const;
		void InstallSuperVersion();
		Result<SequenceNumber> ResolveSnapshotSequence(const std::optional<Snapshot>& snapshot_handle) const;

		mutable std::shared_mutex mutex_;
		std::condition_variable_any background_work_finished_signal_;
		bool hold_background_compaction_ = false;
		int background_compaction_start_count_ = 0;

		Env* env_;
		Options options_;
		std::unique_ptr<Cache> owned_block_cache_;
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
		std::atomic<SuperVersion*> super_version_{ nullptr };
		std::vector<SuperVersion*> retired_super_versions_[2];
		int retired_index_ = 0;
		std::atomic<std::size_t> point_read_epoch_{ 0 };
		std::array<std::array<PointReadCounter, kPointReadShardCount>, kPointReadEpochCount> point_read_counters_;
		// sequence_ is the next sequence number to assign to a new write.
		// VersionSet persists last_sequence (last assigned), so:
		//   sequence_ == versions_->LastSequence() + 1
		SequenceNumber sequence_ = 1;
		// visible_sequence_ tracks the last sequence whose write has been fully
		// committed and published. It advances only after ApplyBatch completes
		// (under mutex_), never when a sequence is merely reserved. Readers
		// use this value for unsnapshotted visibility.
		std::atomic<SequenceNumber> visible_sequence_{ 0 };
		InternalKeyComparator internal_comparator_;

		std::unique_ptr<VersionSet> versions_;
		std::set<uint64_t> pending_outputs_;
		AsyncRuntime* runtime_;
		std::unique_ptr<AsyncWalWriter> async_wal_writer_;
		std::unique_ptr<CompactionController> compaction_controller_;
		std::atomic<uint64_t> background_compaction_finish_count_{ 0 };
		Status bg_error_;
		std::atomic<bool> shutting_down_{ false };
		std::shared_ptr<SnapshotRegistry> snapshot_registry_;
		std::unique_ptr<WriteCoordinator> write_coordinator_;
	};
} // namespace prism

#endif // PRISM_DB_IMPL_H
