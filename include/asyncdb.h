#ifndef PRISM_ASYNC_DB_H
#define PRISM_ASYNC_DB_H

#include "async_write_op.h"
#include "async_op.h"
#include "options.h"
#include "result.h"
#include "status.h"
#include "write_batch.h"

#include <coroutine>
#include <memory>
#include <string>

namespace prism
{
	class Database;
	class AsyncRuntime;

	class AsyncGetOp
	{
	public:
		struct State;
		struct Awaiter
		{
			std::shared_ptr<State> state;

			~Awaiter();
			bool await_ready() const noexcept;
			bool await_suspend(std::coroutine_handle<> handle) const;
			Result<std::string> await_resume() const;
		};

		explicit AsyncGetOp(std::shared_ptr<State> state);
		~AsyncGetOp();
		AsyncGetOp(AsyncGetOp&&) noexcept;
		AsyncGetOp& operator=(AsyncGetOp&&) noexcept;
		AsyncGetOp(const AsyncGetOp&) = delete;
		AsyncGetOp& operator=(const AsyncGetOp&) = delete;

		Awaiter operator co_await() && noexcept;

	private:
		std::shared_ptr<State> state_;
	};

	// AsyncDB: Asynchronous wrapper around the database handle.
	//
	// Design:
	// - Wraps the synchronous Database handle behind private shared async state.
	// - Reads enter AsyncRuntime::DbReadExecutor() from external callers and run
	//   re-entrantly inline when a coroutine is already on a DBRead worker; writes
	//   enqueue on DBImpl's WriteCoordinator through AsyncWriteOp.
	// - Provides a clean coroutine-based API (AsyncOp).
	// - Snapshot uses the same cheap-copy Snapshot handle as the sync API.
	//
	// Thread Safety:
	// - Multiple concurrent async operations are safe.
	// - The supplied AsyncRuntime must outlive AsyncDB and all awaiting AsyncOps.
	class AsyncDB
	{
	public:
		~AsyncDB();

		AsyncDB(const AsyncDB&) = delete;
		AsyncDB& operator=(const AsyncDB&) = delete;
		AsyncDB(AsyncDB&& other) noexcept;
		AsyncDB& operator=(AsyncDB&& other) noexcept;

		// Opens a database asynchronously and returns a by-value handle.
		// The AsyncDB handle is move-only; outstanding operations pin internal state.
		static AsyncOp<Result<AsyncDB>> OpenAsync(AsyncRuntime& runtime, const Options& options, std::string dbname);

		AsyncWriteOp PutAsync(const WriteOptions& options, std::string key, std::string value);

		AsyncGetOp GetAsync(ReadOptions options, std::string key);
		AsyncWriteOp DeleteAsync(const WriteOptions& options, std::string key);
		AsyncWriteOp WriteAsync(const WriteOptions& options, WriteBatch batch);
		// Captures a cheap-copy Snapshot handle that can be stored by value in
		// the snapshot_handle field on ReadOptions for later async reads.
		Snapshot CaptureSnapshot();

	private:
		friend class AsyncGetOp;
		friend struct CompactionStateAccess;

		struct SharedState;

		AsyncDB(SharedState* state);

		SharedState* state_ = nullptr;
	};
}

#endif
