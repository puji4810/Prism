#include "asyncdb.h"

#include "result.h"
#include "status.h"

#include <utility>

namespace prism
{
	AsyncDB::AsyncDB(ThreadPoolScheduler& scheduler, std::shared_ptr<DB> db)
	    : scheduler_(&scheduler)
	    , db_(std::move(db))
	{
	}

	AsyncDB::~AsyncDB() = default;

	AsyncOp<Result<AsyncDB>> AsyncDB::OpenAsync(ThreadPoolScheduler& scheduler, const Options& options, std::string dbname)
	{
		return AsyncOp<Result<AsyncDB>>(scheduler, [&scheduler, options, dbname = std::move(dbname)]() -> Result<AsyncDB> {
			auto db = DB::Open(options, dbname);
			if (!db.has_value())
			{
				return std::unexpected(db.error());
			}
			return AsyncDB(scheduler, std::shared_ptr<DB>(std::move(db.value())));
		});
	}

	AsyncOp<Status> AsyncDB::PutAsync(const WriteOptions& options, std::string key, std::string value)
	{
		auto db = db_;
		return AsyncOp<Status>(*scheduler_,
		    [db, opts = options, key = std::move(key), value = std::move(value)]() { return db->Put(opts, Slice(key), Slice(value)); });
	}

	AsyncOp<Result<std::string>> AsyncDB::GetAsync(const ReadOptions& options, std::string key)
	{
		auto db = db_;
		return AsyncOp<Result<std::string>>(
		    *scheduler_, [db, opts = options, key = std::move(key)]() { return db->Get(opts, Slice(key)); });
	}

	AsyncOp<Status> AsyncDB::DeleteAsync(const WriteOptions& options, std::string key)
	{
		auto db = db_;
		return AsyncOp<Status>(*scheduler_, [db, opts = options, key = std::move(key)]() { return db->Delete(opts, Slice(key)); });
	}

	AsyncOp<Status> AsyncDB::WriteAsync(const WriteOptions& options, WriteBatch batch)
	{
		auto db = db_;
		return AsyncOp<Status>(
		    *scheduler_, [db, opts = options, batch = std::move(batch)]() mutable { return db->Write(opts, std::move(batch)); });
	}
}
