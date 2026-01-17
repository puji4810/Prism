#ifndef PRISM_ASYNC_DB_H
#define PRISM_ASYNC_DB_H

#include "async_op.h"
#include "db.h"

#include <memory>
#include <string>

namespace prism
{
	class AsyncDB
	{
	public:
		AsyncDB(ThreadPoolScheduler& scheduler, std::unique_ptr<DB> db);
		~AsyncDB();

		AsyncDB(const AsyncDB&) = delete;
		AsyncDB& operator=(const AsyncDB&) = delete;
		AsyncDB(AsyncDB&&) = default;
		AsyncDB& operator=(AsyncDB&&) = default;

		static AsyncOp<Result<std::unique_ptr<AsyncDB>>> OpenAsync(
		    ThreadPoolScheduler& scheduler, const Options& options, std::string dbname);

		// TODO(phase-b): Leaf-level async via AsyncEnv/Table/Log.
		// TODO(async-scan): AsyncIterator for range scans.

		AsyncOp<Status> PutAsync(WriteOptions options, std::string key, std::string value);

		AsyncOp<Result<std::string>> GetAsync(ReadOptions options, std::string key);
		AsyncOp<Status> DeleteAsync(WriteOptions options, std::string key);
		AsyncOp<Status> WriteAsync(WriteOptions options, WriteBatch batch);

		DB* SyncDB() const noexcept { return db_.get(); }

	private:
		ThreadPoolScheduler* scheduler_;
		std::unique_ptr<DB> db_;
	};
}

#endif
