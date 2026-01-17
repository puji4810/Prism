#ifndef PRISM_ASYNC_DB_H
#define PRISM_ASYNC_DB_H

#include "async_op.h"
#include "db.h"

#include <memory>
#include <string>

namespace prism
{
	// AsyncDB: Asynchronous wrapper around the synchronous DB interface.
	//
	// Current Design (Phase A - Thread Pool Offload):
	// - Wraps a synchronous DB instance
	// - All operations are submitted to ThreadPoolScheduler
	// - Each operation blocks a thread pool thread for its duration
	// - This is a "thick async wrapper" that provides immediate async API without modifying DBImpl
	//
	// Future Evolution (Phase B - Granular Async):
	// - Fast-path: Check MemTable/ImmMemTable synchronously without scheduling
	// - Slow-path: Only submit table/file I/O to thread pool
	// - Eventually: Use AsyncEnv with io_uring for true async I/O
	//
	// Trade-offs:
	// + Pro: Clean async API for users, easy coroutine integration
	// + Pro: No changes to existing DB/DBImpl (non-invasive)
	// - Con: Thread pool threads block during I/O (not true async yet)
	// - Con: Extra thread context switches (user thread -> pool thread -> I/O)
	//
	// Thread Safety:
	// - DB instance is protected by its internal shared_mutex
	// - Multiple concurrent async operations are safe
	// - Scheduler manages thread pool lifetime
	class AsyncDB
	{
	public:
		AsyncDB(ThreadPoolScheduler& scheduler, std::unique_ptr<DB> db);
		~AsyncDB();

		AsyncDB(const AsyncDB&) = delete;
		AsyncDB& operator=(const AsyncDB&) = delete;
		AsyncDB(AsyncDB&&) = default;
		AsyncDB& operator=(AsyncDB&&) = default;

		// Opens a database asynchronously.
		// Internally calls synchronous DB::Open() on a thread pool thread.
		static AsyncOp<Result<std::unique_ptr<AsyncDB>>> OpenAsync(
		    ThreadPoolScheduler& scheduler, const Options& options, std::string dbname);

		// TODO(phase-b): GetAsync should fast-path mem/imm without scheduling.
		// TODO(phase-b): Offload only table/file IO via AsyncEnv/Table.
		// TODO(async-scan): AsyncIterator for range scans.
		AsyncOp<Status> PutAsync(WriteOptions options, std::string key, std::string value);

		AsyncOp<Result<std::string>> GetAsync(ReadOptions options, std::string key);
		AsyncOp<Status> DeleteAsync(WriteOptions options, std::string key);
		AsyncOp<Status> WriteAsync(WriteOptions options, WriteBatch batch);

		// Access to underlying synchronous DB (for mixed sync/async usage).
		DB* SyncDB() const noexcept { return db_.get(); }

	private:
		ThreadPoolScheduler* scheduler_;
		std::unique_ptr<DB> db_;
	};
}

#endif
