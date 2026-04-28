#ifndef PRISM_ASYNC_DB_H
#define PRISM_ASYNC_DB_H

#include "async_op.h"
#include "options.h"
#include "result.h"
#include "status.h"
#include "write_batch.h"

#include <memory>
#include <string>

namespace prism
{
	class Database;

	// AsyncDB: Asynchronous wrapper around the database handle.
	//
	// Design:
	// - Wraps the synchronous Database handle behind private shared async state.
	// - Operations are submitted to ThreadPoolScheduler for execution.
	// - Provides a clean coroutine-based API (AsyncOp).
	// - Snapshot uses the same cheap-copy Snapshot handle as the sync API.
	//
	// Thread Safety:
	// - Multiple concurrent async operations are safe.
	// - Scheduler manages thread pool lifetime. Must outlive all awaiting AsyncOps.
	class AsyncDB
	{
	public:
		~AsyncDB();

		AsyncDB(const AsyncDB&) = delete;
		AsyncDB& operator=(const AsyncDB&) = delete;
		AsyncDB(AsyncDB&&) = default;
		AsyncDB& operator=(AsyncDB&&) = default;

		// Opens a database asynchronously and returns a by-value handle.
		// The AsyncDB handle is move-only and internally uses shared_ptr for async safety.
		static AsyncOp<Result<AsyncDB>> OpenAsync(ThreadPoolScheduler& scheduler, const Options& options, std::string dbname);

		AsyncOp<Status> PutAsync(const WriteOptions& options, std::string key, std::string value);

		AsyncOp<Result<std::string>> GetAsync(const ReadOptions& options, std::string key);
		AsyncOp<Status> DeleteAsync(const WriteOptions& options, std::string key);
		AsyncOp<Status> WriteAsync(const WriteOptions& options, WriteBatch batch);
		// Captures a cheap-copy Snapshot handle that can be stored by value in
		// the snapshot_handle field on ReadOptions for later async reads.
		Snapshot CaptureSnapshot();

	private:
		friend struct CompactionStateAccess;

		struct SharedState;

		AsyncDB(std::shared_ptr<SharedState> state);

		std::shared_ptr<SharedState> state_;
	};
}

#endif
