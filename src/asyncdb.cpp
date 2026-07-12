#include "asyncdb.h"

#include "db.h"
#include "db_impl.h"
#include "result.h"
#include "async_runtime.h"
#include "runtime_metrics.h"
#include "status.h"

#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <optional>
#include <utility>

namespace prism
{
	struct AsyncDB::SharedState
	{
		static constexpr std::size_t kReadRefShardCount = 64;
		static constexpr uint32_t kReadRefReleased = uint32_t{ 1 } << 31;
		static constexpr uint32_t kReadRefCountMask = kReadRefReleased - 1;

		struct alignas(64) ReadRefShard
		{
			std::atomic<uint32_t> state{ 0 };
		};

		SharedState(AsyncRuntime& runtime, Database db)
		    : runtime_(&runtime)
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

		std::size_t RefOperation()
		{
			static std::atomic<std::size_t> next_shard{ 0 };
			static thread_local const std::size_t shard_index
			    = next_shard.fetch_add(1, std::memory_order_relaxed) % kReadRefShardCount;
			const uint32_t previous = read_refs_[shard_index].state.fetch_add(1, std::memory_order_relaxed);
			assert((previous & kReadRefReleased) == 0);
			assert((previous & kReadRefCountMask) != kReadRefCountMask);
			return shard_index;
		}

		void UnrefOperation(std::size_t shard_index)
		{
			auto& shard = read_refs_[shard_index];
			const uint32_t previous = shard.state.fetch_sub(1, std::memory_order_release);
			assert((previous & kReadRefReleased) == 0);
			assert((previous & kReadRefCountMask) > 0);
			if ((previous & kReadRefCountMask) == 1 && owner_released_.load(std::memory_order_acquire))
			{
				ReleaseReadShard(shard);
			}
		}

		void ReleaseOwner()
		{
			owner_released_.store(true, std::memory_order_release);
			for (auto& shard : read_refs_)
			{
				ReleaseReadShard(shard);
			}
			Unref();
		}

	private:
		void ReleaseReadShard(ReadRefShard& shard)
		{
			uint32_t expected = 0;
			if (shard.state.compare_exchange_strong(
			        expected, kReadRefReleased, std::memory_order_acq_rel, std::memory_order_acquire))
			{
				Unref();
			}
		}

	public:
		std::atomic<uint32_t> refs_{ static_cast<uint32_t>(1 + kReadRefShardCount) };
		std::atomic<bool> owner_released_{ false };
		std::array<ReadRefShard, kReadRefShardCount> read_refs_;
		AsyncRuntime* runtime_;
		Database db_;
	};

	struct AsyncGetOp::State
	{
		static constexpr int kSuspending = 0;
		static constexpr int kCompleted = 1;
		static constexpr int kSuspended = 2;

		AsyncDB::SharedState* owner = nullptr;
		ReadOptions options;
		std::string key;
		std::optional<Result<std::string>> result;
		std::exception_ptr exception;
		std::coroutine_handle<> continuation;
		std::atomic<int> status{ kSuspending };
		std::size_t owner_ref_shard = 0;

		State(AsyncDB::SharedState* owner_arg, ReadOptions options_arg, std::string key_arg)
		    : owner(owner_arg)
		    , options(std::move(options_arg))
		    , key(std::move(key_arg))
		{
			owner_ref_shard = owner->RefOperation();
		}

		~State()
		{
			if (owner != nullptr)
			{
				owner->UnrefOperation(owner_ref_shard);
			}
		}

		void Finish()
		{
			auto expected = kSuspending;
			if (status.compare_exchange_strong(expected, kCompleted, std::memory_order_acq_rel, std::memory_order_acquire))
			{
				return;
			}
			continuation.resume();
		}

		bool TrySuspend()
		{
			auto expected = kSuspending;
			return status.compare_exchange_strong(expected, kSuspended, std::memory_order_acq_rel, std::memory_order_acquire);
		}

		void Run(const std::shared_ptr<State>& self)
		{
			try
			{
				auto runtime = owner->runtime_;
				auto* impl = owner->db_.impl_.get();
				impl->GetAsyncCallback(
				    *runtime, std::move(options), std::move(key), [self](Result<std::string> get_result) mutable {
#ifdef PRISM_RUNTIME_METRICS
					    auto db_elapsed_us = static_cast<uint64_t>(
					        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - self->db_start)
					            .count());
					    auto& rm = RuntimeMetrics::Instance();
					    rm.get_async_db_op_total_us.fetch_add(db_elapsed_us, std::memory_order_relaxed);
					    rm.get_async_db_op_count.fetch_add(1, std::memory_order_relaxed);
#endif
					    self->result = std::move(get_result);
					    self->Finish();
				    });
			}
			catch (...)
			{
				exception = std::current_exception();
				Finish();
			}
		}

		void Start(const std::shared_ptr<State>& self)
		{
			auto& executor = owner->runtime_->DbReadExecutor();
			if (executor.TryRunInline([this, &self] { Run(self); }))
			{
				return;
			}
			executor.Submit([self] { self->Run(self); });
		}
#ifdef PRISM_RUNTIME_METRICS
		std::chrono::steady_clock::time_point db_start;
#endif
	};

	AsyncGetOp::AsyncGetOp(std::shared_ptr<State> state)
	    : state_(std::move(state))
	{
	}

	AsyncGetOp::~AsyncGetOp() = default;
	AsyncGetOp::AsyncGetOp(AsyncGetOp&&) noexcept = default;
	AsyncGetOp& AsyncGetOp::operator=(AsyncGetOp&&) noexcept = default;
	AsyncGetOp::Awaiter::~Awaiter() = default;

	bool AsyncGetOp::Awaiter::await_ready() const noexcept
	{
		return state->status.load(std::memory_order_acquire) == State::kCompleted;
	}

	bool AsyncGetOp::Awaiter::await_suspend(std::coroutine_handle<> handle) const
	{
		state->continuation = handle;
#ifdef PRISM_RUNTIME_METRICS
		state->db_start = std::chrono::steady_clock::now();
#endif
		state->Start(state);
		return state->TrySuspend();
	}

	Result<std::string> AsyncGetOp::Awaiter::await_resume() const
	{
		if (state->exception)
		{
			std::rethrow_exception(state->exception);
		}
		return std::move(*state->result);
	}

	AsyncGetOp::Awaiter AsyncGetOp::operator co_await() && noexcept { return Awaiter{ std::move(state_) }; }

	AsyncDB::AsyncDB(SharedState* state)
	    : state_(state)
	{
	}

	AsyncDB::~AsyncDB()
	{
		if (state_ != nullptr)
		{
			state_->ReleaseOwner();
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
				state_->ReleaseOwner();
			}
			state_ = std::exchange(other.state_, nullptr);
		}
		return *this;
	}

	AsyncOp<Result<AsyncDB>> AsyncDB::OpenAsync(AsyncRuntime& runtime, const Options& options, std::string dbname)
	{
		return AsyncOp<Result<AsyncDB>>(runtime.BlockingIoExecutor(), [&runtime, options, dbname = std::move(dbname)]() -> Result<AsyncDB> {
			auto impl = DBImpl::OpenInternal(options, dbname, &runtime);
			if (!impl.has_value())
			{
				return std::unexpected(impl.error());
			}
			return AsyncDB(new SharedState(runtime, Database(std::move(impl.value()))));
		});
	}

	AsyncWriteOp AsyncDB::PutAsync(const WriteOptions& options, std::string key, std::string value)
	{
		SharedState* raw_state = state_;
		raw_state->Ref();
		try
		{
			WriteBatch batch;
			batch.Put(Slice(key), Slice(value));
			return raw_state->db_.impl_->WriteAsync(
			    options, std::move(batch), raw_state, [](void* state) { static_cast<SharedState*>(state)->Unref(); });
		}
		catch (...)
		{
			raw_state->Unref();
			throw;
		}
	}

	AsyncGetOp AsyncDB::GetAsync(ReadOptions options, std::string key)
	{
		return AsyncGetOp(std::make_shared<AsyncGetOp::State>(state_, std::move(options), std::move(key)));
	}

	AsyncWriteOp AsyncDB::DeleteAsync(const WriteOptions& options, std::string key)
	{
		SharedState* raw_state = state_;
		raw_state->Ref();
		try
		{
			WriteBatch batch;
			batch.Delete(Slice(key));
			return raw_state->db_.impl_->WriteAsync(
			    options, std::move(batch), raw_state, [](void* state) { static_cast<SharedState*>(state)->Unref(); });
		}
		catch (...)
		{
			raw_state->Unref();
			throw;
		}
	}

	AsyncWriteOp AsyncDB::WriteAsync(const WriteOptions& options, WriteBatch batch)
	{
		SharedState* raw_state = state_;
		raw_state->Ref();
		try
		{
			return raw_state->db_.impl_->WriteAsync(
			    options, std::move(batch), raw_state, [](void* state) { static_cast<SharedState*>(state)->Unref(); });
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
