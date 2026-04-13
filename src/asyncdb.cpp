#include "asyncdb.h"

#include "db.h"
#include "result.h"
#include "status.h"

#include <utility>

namespace prism
{
	struct AsyncDB::SharedState
	{
		SharedState(ThreadPoolScheduler& scheduler, Database db)
		    : scheduler_(&scheduler)
		    , db_(std::move(db))
		{
		}

		ThreadPoolScheduler* scheduler_;
		Database db_;
	};

	AsyncDB::AsyncDB(std::shared_ptr<SharedState> state)
	    : state_(std::move(state))
	{
	}

	AsyncDB::~AsyncDB() = default;

	AsyncOp<Result<AsyncDB>> AsyncDB ::OpenAsync(ThreadPoolScheduler& scheduler, const Options& options, std::string dbname)
	{
		return AsyncOp<Result<AsyncDB>>(scheduler, [&scheduler, options, dbname = std::move(dbname)]() -> Result<AsyncDB> {
			auto db = Database::Open(options, dbname);
			if (!db.has_value())
			{
				return std::unexpected(db.error());
			}
			return AsyncDB(std::make_shared<SharedState>(scheduler, std::move(db.value())));
		});
	}

	AsyncOp<Status> AsyncDB::PutAsync(const WriteOptions& options, std::string key, std::string value)
	{
		auto state = state_;
		return AsyncOp<Status>(*state->scheduler_, [state, opts = options, key = std::move(key), value = std::move(value)]() {
			return state->db_.Put(opts, Slice(key), Slice(value));
		});
	}

	AsyncOp<Result<std::string>> AsyncDB::GetAsync(const ReadOptions& options, std::string key)
	{
		auto state = state_;
		return AsyncOp<Result<std::string>>(
		    *state->scheduler_, [state, opts = options, key = std::move(key)]() { return state->db_.Get(opts, Slice(key)); });
	}

	AsyncOp<Status> AsyncDB::DeleteAsync(const WriteOptions& options, std::string key)
	{
		auto state = state_;
		return AsyncOp<Status>(
		    *state->scheduler_, [state, opts = options, key = std::move(key)]() { return state->db_.Delete(opts, Slice(key)); });
	}

	AsyncOp<Status> AsyncDB::WriteAsync(const WriteOptions& options, WriteBatch batch)
	{
		auto state = state_;
		return AsyncOp<Status>(*state->scheduler_,
		    [state, opts = options, batch = std::move(batch)]() mutable { return state->db_.Write(opts, std::move(batch)); });
	}

	Snapshot AsyncDB::CaptureSnapshot() { return state_->db_.CaptureSnapshot(); }
}
