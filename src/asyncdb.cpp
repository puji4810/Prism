#include "asyncdb.h"

#include "db.h"
#include "db_impl.h"
#include "result.h"
#include "async_runtime.h"
#include "runtime_metrics.h"
#include "status.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <utility>

namespace prism
{
	namespace
	{
		bool IsDefaultReadOptions(const ReadOptions& options)
		{
			return !options.verify_checksums && options.fill_cache && !options.snapshot_handle.has_value();
		}

		Result<std::string> RunGet(Database& db, const ReadOptions& options, const std::string& key)
		{
#ifdef PRISM_RUNTIME_METRICS
			auto db_start = std::chrono::steady_clock::now();
			auto result = db.Get(options, Slice(key));
			auto db_elapsed_us = static_cast<uint64_t>(
			    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - db_start).count());
			auto& rm = RuntimeMetrics::Instance();
			rm.get_async_db_op_total_us.fetch_add(db_elapsed_us, std::memory_order_relaxed);
			rm.get_async_db_op_count.fetch_add(1, std::memory_order_relaxed);
			return result;
#else
			return db.Get(options, Slice(key));
#endif
		}
	}

	struct AsyncDB::SharedState
	{
		SharedState(std::shared_ptr<RuntimeBundle> runtime, Database db)
		    : runtime_(std::move(runtime))
		    , db_(std::move(db))
		{
		}

		void Ref() { refs_.fetch_add(1, std::memory_order_relaxed); }

		void Unref()
		{
			if (refs_.fetch_sub(1, std::memory_order_acq_rel) == 1)
			{
				delete this;
			}
		}

		std::atomic<uint32_t> refs_{ 1 };
		std::shared_ptr<RuntimeBundle> runtime_;
		Database db_;
	};

	AsyncDB::AsyncDB(SharedState* state)
	    : state_(state)
	{
	}

	AsyncDB::~AsyncDB()
	{
		if (state_ != nullptr)
		{
			state_->Unref();
		}
	}

	AsyncDB::AsyncDB(AsyncDB&& other) noexcept
	    : state_(std::exchange(other.state_, nullptr))
	{
	}

	AsyncDB& AsyncDB::operator=(AsyncDB&& other) noexcept
	{
		if (this != &other)
		{
			if (state_ != nullptr)
			{
				state_->Unref();
			}
			state_ = std::exchange(other.state_, nullptr);
		}
		return *this;
	}

	AsyncOp<Result<AsyncDB>> AsyncDB ::OpenAsync(ThreadPoolScheduler& scheduler, const Options& options, std::string dbname)
	{
		auto runtime = AcquireRuntimeBundle(scheduler);
		return AsyncOp<Result<AsyncDB>>(scheduler, [runtime, options, dbname = std::move(dbname)]() -> Result<AsyncDB> {
			auto db = Database::Open(options, dbname);
			if (!db.has_value())
			{
				return std::unexpected(db.error());
			}
			return AsyncDB(new SharedState(runtime, std::move(db.value())));
		});
	}

	AsyncOp<Status> AsyncDB::PutAsync(const WriteOptions& options, std::string key, std::string value)
	{
		SharedState* raw_state = state_;
		raw_state->Ref();
		try
		{
			return AsyncOp<Status>(
			    *raw_state->runtime_->timer_source, raw_state, [](void* state) { static_cast<SharedState*>(state)->Unref(); },
			    [raw_state, opts = options, key = std::move(key), value = std::move(value)]() {
				    return raw_state->db_.Put(opts, Slice(key), Slice(value));
			    });
		}
		catch (...)
		{
			raw_state->Unref();
			throw;
		}
	}

	AsyncOp<Result<std::string>> AsyncDB::GetAsync(ReadOptions options, std::string key)
	{
		SharedState* raw_state = state_;
		raw_state->Ref();
		try
		{
			if (IsDefaultReadOptions(options))
			{
				return AsyncOp<Result<std::string>>(
				    *raw_state->runtime_->timer_source, raw_state, [](void* state) { static_cast<SharedState*>(state)->Unref(); },
				    [raw_state, key = std::move(key)]() {
					    ReadOptions default_options;
					    return RunGet(raw_state->db_, default_options, key);
				    });
			}

			return AsyncOp<Result<std::string>>(
			    *raw_state->runtime_->timer_source, raw_state, [](void* state) { static_cast<SharedState*>(state)->Unref(); },
			    [raw_state, opts = std::move(options), key = std::move(key)]() { return RunGet(raw_state->db_, opts, key); });
		}
		catch (...)
		{
			raw_state->Unref();
			throw;
		}
	}

	AsyncOp<Status> AsyncDB::DeleteAsync(const WriteOptions& options, std::string key)
	{
		SharedState* raw_state = state_;
		raw_state->Ref();
		try
		{
			return AsyncOp<Status>(
			    *raw_state->runtime_->timer_source, raw_state, [](void* state) { static_cast<SharedState*>(state)->Unref(); },
			    [raw_state, opts = options, key = std::move(key)]() { return raw_state->db_.Delete(opts, Slice(key)); });
		}
		catch (...)
		{
			raw_state->Unref();
			throw;
		}
	}

	AsyncOp<Status> AsyncDB::WriteAsync(const WriteOptions& options, WriteBatch batch)
	{
		SharedState* raw_state = state_;
		raw_state->Ref();
		try
		{
			return AsyncOp<Status>(
			    *raw_state->runtime_->timer_source, raw_state, [](void* state) { static_cast<SharedState*>(state)->Unref(); },
			    [raw_state, opts = options, batch = std::move(batch)]() mutable { return raw_state->db_.Write(opts, std::move(batch)); });
		}
		catch (...)
		{
			raw_state->Unref();
			throw;
		}
	}

	Snapshot AsyncDB::CaptureSnapshot() { return state_->db_.CaptureSnapshot(); }

	Database& CompactionStateAccess::GetDatabase(AsyncDB& db) { return db.state_->db_; }
}
