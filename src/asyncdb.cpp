#include "asyncdb.h"

#include "result.h"
#include "status.h"

#include <utility>

namespace prism
{
	AsyncDB::AsyncDB(ThreadPoolScheduler& scheduler, std::unique_ptr<DB> db)
	    : scheduler_(&scheduler)
	    , db_(std::move(db))
	{
	}

	AsyncDB::~AsyncDB() = default;

	AsyncOp<Result<std::unique_ptr<AsyncDB>>> AsyncDB::OpenAsync(ThreadPoolScheduler& scheduler, const Options& options, std::string dbname)
	{
		return AsyncOp<Result<std::unique_ptr<AsyncDB>>>(
		    scheduler, [&scheduler, options, dbname = std::move(dbname)]() mutable -> Result<std::unique_ptr<AsyncDB>> {
			    auto db = DB::Open(options, dbname);
			    if (!db.has_value())
			    {
				    return std::unexpected(db.error());
			    }
			    return std::make_unique<AsyncDB>(scheduler, std::move(db.value()));
		    });
	}

	AsyncOp<Status> AsyncDB::PutAsync(WriteOptions options, std::string key, std::string value)
	{
		return AsyncOp<Status>(*scheduler_, [this, options, key = std::move(key), value = std::move(value)]() mutable {
			return db_->Put(options, Slice(key), Slice(value));
		});
	}

	AsyncOp<Result<std::string>> AsyncDB::GetAsync(ReadOptions options, std::string key)
	{
		return AsyncOp<Result<std::string>>(
		    *scheduler_, [this, options, key = std::move(key)]() mutable { return db_->Get(options, Slice(key)); });
	}

	AsyncOp<Status> AsyncDB::DeleteAsync(WriteOptions options, std::string key)
	{
		return AsyncOp<Status>(*scheduler_, [this, options, key = std::move(key)]() mutable { return db_->Delete(options, Slice(key)); });
	}

	AsyncOp<Status> AsyncDB::WriteAsync(WriteOptions options, WriteBatch batch)
	{
		return AsyncOp<Status>(
		    *scheduler_, [this, options, batch = std::move(batch)]() mutable { return db_->Write(options, std::move(batch)); });
	}
}
