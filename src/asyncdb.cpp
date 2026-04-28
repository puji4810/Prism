#include "asyncdb.h"

#include "db.h"
#include "db_impl.h"
#include "result.h"
#include "runtime_executor.h"
#include "runtime_metrics.h"
#include "status.h"

#include <utility>

namespace prism
{
	namespace
	{
		RuntimeBundle& RuntimeFrom(const std::shared_ptr<RuntimeBundle>& runtime) { return *runtime; }
	}

	struct AsyncDB::SharedState
	{
		SharedState(std::shared_ptr<RuntimeBundle> runtime, Database db)
		    : runtime_(std::move(runtime))
		    , db_(std::move(db))
		{
		}

		std::shared_ptr<RuntimeBundle> runtime_;
		Database db_;
	};

	AsyncDB::AsyncDB(std::shared_ptr<SharedState> state)
	    : state_(std::move(state))
	{
	}

	AsyncDB::~AsyncDB() = default;

	AsyncOp<Result<AsyncDB>> AsyncDB ::OpenAsync(ThreadPoolScheduler& scheduler, const Options& options, std::string dbname)
	{
		auto runtime = AcquireRuntimeBundle(scheduler);
		return AsyncOp<Result<AsyncDB>>(RuntimeFrom(runtime).runtime_scheduler,
		    [runtime, options, dbname = std::move(dbname)]() -> Result<AsyncDB> {
			auto db = Database::Open(options, dbname);
			if (!db.has_value())
			{
				return std::unexpected(db.error());
			}
			return AsyncDB(std::make_shared<SharedState>(runtime, std::move(db.value())));
		});
	}

	AsyncOp<Status> AsyncDB::PutAsync(const WriteOptions& options, std::string key, std::string value)
	{
		return AsyncOp<Status>(RuntimeFrom(state_->runtime_).runtime_scheduler,
		    [state = state_, opts = options, key = std::move(key), value = std::move(value)]() {
			return state->db_.Put(opts, Slice(key), Slice(value));
		});
	}

	AsyncOp<Result<std::string>> AsyncDB::GetAsync(const ReadOptions& options, std::string key)
	{
		return AsyncOp<Result<std::string>>(RuntimeFrom(state_->runtime_).runtime_scheduler,
		    [state = state_, opts = options, key = std::move(key)]() {
#ifdef PRISM_RUNTIME_METRICS
			auto db_start = std::chrono::steady_clock::now();
			auto result = state->db_.Get(opts, Slice(key));
			auto db_elapsed_us = static_cast<uint64_t>(
			    std::chrono::duration_cast<std::chrono::microseconds>(
			        std::chrono::steady_clock::now() - db_start)
			        .count());
			auto& rm = RuntimeMetrics::Instance();
			rm.get_async_db_op_total_us.fetch_add(db_elapsed_us, std::memory_order_relaxed);
			rm.get_async_db_op_count.fetch_add(1, std::memory_order_relaxed);
			return result;
#else
			return state->db_.Get(opts, Slice(key));
#endif
		    });
	}

	AsyncOp<Status> AsyncDB::DeleteAsync(const WriteOptions& options, std::string key)
	{
		return AsyncOp<Status>(RuntimeFrom(state_->runtime_).runtime_scheduler,
		    [state = state_, opts = options, key = std::move(key)]() { return state->db_.Delete(opts, Slice(key)); });
	}

	AsyncOp<Status> AsyncDB::WriteAsync(const WriteOptions& options, WriteBatch batch)
	{
		return AsyncOp<Status>(RuntimeFrom(state_->runtime_).runtime_scheduler,
		    [state = state_, opts = options, batch = std::move(batch)]() mutable {
			return state->db_.Write(opts, std::move(batch));
		});
	}

	Snapshot AsyncDB::CaptureSnapshot() { return state_->db_.CaptureSnapshot(); }

	Database& CompactionStateAccess::GetDatabase(AsyncDB& db) { return db.state_->db_; }
}
