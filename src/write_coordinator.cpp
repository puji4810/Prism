#include "write_coordinator.h"

#include "async_runtime.h"
#include "db_impl.h"
#include "runtime_metrics.h"
#include "write_batch_internal.h"

#include <cassert>
#include <cstdlib>
#include <memory>
#include <utility>

namespace prism
{
	WriteRequestState::WriteRequestState(WriteOptions write_options, WriteBatch write_batch, BlockingWait* blocking_wait_arg)
	    : options(write_options)
	    , batch(std::move(write_batch))
	    , blocking_wait(blocking_wait_arg)
	{
	}

	void WriteRequestState::WaitBlocking()
	{
		assert(blocking_wait != nullptr);
		std::unique_lock lock(blocking_wait->mutex);
		blocking_wait->cv.wait(lock, [this] { return blocking_wait->done; });
	}

	bool WriteRequestState::TrySuspend() noexcept
	{
		int expected = kSubmitting;
		return state.compare_exchange_strong(expected, kSuspended, std::memory_order_acq_rel, std::memory_order_acquire);
	}

	void WriteRequestState::Complete(Status completion_status)
	{
		status = std::move(completion_status);
		if (blocking_wait != nullptr)
		{
			{
				std::lock_guard lock(blocking_wait->mutex);
				blocking_wait->done = true;
			}
			blocking_wait->cv.notify_one();
		}

		int expected = kSubmitting;
		if (state.compare_exchange_strong(expected, kCompleted, std::memory_order_acq_rel, std::memory_order_acquire))
		{
			return;
		}
		if (expected == kSuspended && continuation)
		{
			continuation.resume();
		}
	}

	AsyncWriteOp::AsyncWriteOp(WriteCoordinator& coordinator, WriteOptions options, WriteBatch batch)
	    : coordinator_(&coordinator)
	    , state_(std::make_unique<WriteRequestState>(options, std::move(batch)))
	{
	}

	AsyncWriteOp::AsyncWriteOp(
	    WriteCoordinator& coordinator, void* keep_alive, void (*release_keep_alive)(void*), WriteOptions options, WriteBatch batch)
	    : coordinator_(&coordinator)
	    , state_(std::make_unique<WriteRequestState>(options, std::move(batch)))
	    , keep_alive_(keep_alive)
	    , release_keep_alive_(release_keep_alive)
	{
	}

	AsyncWriteOp::~AsyncWriteOp()
	{
		if (keep_alive_ != nullptr)
		{
			release_keep_alive_(keep_alive_);
		}
	}

	AsyncWriteOp::AsyncWriteOp(AsyncWriteOp&& other) noexcept
	    : coordinator_(std::exchange(other.coordinator_, nullptr))
	    , state_(std::move(other.state_))
	    , keep_alive_(std::exchange(other.keep_alive_, nullptr))
	    , release_keep_alive_(std::exchange(other.release_keep_alive_, nullptr))
	{
	}

	AsyncWriteOp& AsyncWriteOp::operator=(AsyncWriteOp&& other) noexcept
	{
		if (this != &other)
		{
			if (keep_alive_ != nullptr)
			{
				release_keep_alive_(keep_alive_);
			}
			coordinator_ = std::exchange(other.coordinator_, nullptr);
			state_ = std::move(other.state_);
			keep_alive_ = std::exchange(other.keep_alive_, nullptr);
			release_keep_alive_ = std::exchange(other.release_keep_alive_, nullptr);
		}
		return *this;
	}

	AsyncWriteOp::Awaiter AsyncWriteOp::operator co_await() && noexcept
	{
		return Awaiter{
			coordinator_,
			std::move(state_),
			std::exchange(keep_alive_, nullptr),
			std::exchange(release_keep_alive_, nullptr),
		};
	}

	bool AsyncWriteOp::Awaiter::await_ready() const noexcept { return false; }

	bool AsyncWriteOp::Awaiter::await_suspend(std::coroutine_handle<> handle)
	{
		state->continuation = handle;
		coordinator->Enqueue(state.get());
		return state->TrySuspend();
	}

	Status AsyncWriteOp::Awaiter::await_resume() { return std::move(state->status); }

	AsyncWriteOp::Awaiter::~Awaiter()
	{
		if (keep_alive != nullptr)
		{
			release_keep_alive(keep_alive);
		}
	}

	WriteCoordinator::WriteCoordinator(DBImpl& db, SerialExecutor& executor)
	    : db_(db)
	    , executor_(executor)
	{
	}

	Status WriteCoordinator::SubmitSync(WriteOptions options, WriteBatch batch)
	{
		if (executor_.IsCurrentWorker())
		{
			std::terminate();
		}

		WriteRequestState::BlockingWait blocking_wait;
		WriteRequestState state(options, std::move(batch), &blocking_wait);
		Enqueue(&state);
		state.WaitBlocking();
		return std::move(state.status);
	}

	AsyncWriteOp WriteCoordinator::SubmitAsync(WriteOptions options, WriteBatch batch)
	{
		return AsyncWriteOp(*this, options, std::move(batch));
	}

	void WriteCoordinator::Enqueue(WriteRequestState* request)
	{
		bool should_schedule = false;
		bool reject = false;
		{
			std::lock_guard lock(mutex_);
			if (shutting_down_)
			{
				reject = true;
			}
			else
			{
				queue_.push_back(request);
				if (!drain_scheduled_ && !group_in_flight_)
				{
					drain_scheduled_ = true;
					should_schedule = true;
				}
			}
		}

		if (reject)
		{
			request->Complete(Status::IOError("database shutting down"));
			return;
		}
		if (should_schedule)
		{
			executor_.Submit([this] { Drain(); });
		}
	}

	std::vector<WriteRequestState*> WriteCoordinator::SelectGroupLocked()
	{
		std::vector<WriteRequestState*> group;
		if (queue_.empty())
		{
			return group;
		}

		const bool sync = queue_.front()->options.sync;
		std::size_t total_bytes = 0;
		// TODO(write-group-policy): Consider allowing sync=false requests immediately
		// behind a sync=true group to piggyback on the same fsync. This requires clearly
		// documenting durability semantics and proving that it does not reorder visible
		// sequence publication.
		while (!queue_.empty() && group.size() < kMaxGroupRequests)
		{
			WriteRequestState* request = queue_.front();
			if (request->options.sync != sync)
			{
				break;
			}

			const std::size_t request_bytes = request->batch.ApproximateSize();
			if (!group.empty() && total_bytes + request_bytes > kMaxGroupBytes)
			{
				break;
			}

			total_bytes += request_bytes;
			group.push_back(request);
			queue_.pop_front();
		}
		return group;
	}

	void WriteCoordinator::Drain()
	{
		std::vector<WriteRequestState*> group;
		{
			std::lock_guard lock(mutex_);
			if (group_in_flight_)
			{
				return;
			}
			if (queue_.empty())
			{
				drain_scheduled_ = false;
				idle_cv_.notify_all();
				return;
			}
			group = SelectGroupLocked();
			group_in_flight_ = true;
		}

		WriteBatch merged;
		DBImpl::WritePlan plan;
		Status s = db_.PlanWriteGroupForCoordinator(group, &merged, &plan);
#ifdef PRISM_RUNTIME_METRICS
		RuntimeMetrics::Instance().write_groups_completed.fetch_add(1, std::memory_order_relaxed);
		RuntimeMetrics::Instance().write_group_requests_total.fetch_add(group.size(), std::memory_order_relaxed);
		RuntimeMetrics::Instance().write_group_bytes_total.fetch_add(merged.ApproximateSize(), std::memory_order_relaxed);
#endif
		if (!s.ok() || plan.visible_sequence == 0)
		{
			const bool should_continue = FinishGroup();
			CompleteGroup(std::move(group), std::move(s));
			if (should_continue)
			{
				Drain();
			}
			return;
		}

		auto plan_state = std::make_shared<DBImpl::WritePlan>(std::move(plan));
		db_.StartWritePlanForCoordinator(*plan_state, std::move(merged), [this, group = std::move(group), plan_state](Status wal_status) mutable {
			executor_.Submit([this, group = std::move(group), plan_state, wal_status = std::move(wal_status)]() mutable {
				Status group_status = std::move(wal_status);
				if (group_status.ok())
				{
					group_status = db_.CommitWriteGroupForCoordinator(group, *plan_state);
				}
				else
				{
					db_.RecordWriteFailureForCoordinator(group_status);
				}
				const bool should_continue = FinishGroup();
				CompleteGroup(std::move(group), std::move(group_status));
				if (should_continue)
				{
					Drain();
				}
			});
		});
	}

	void WriteCoordinator::CompleteGroup(std::vector<WriteRequestState*> group, Status status)
	{
		for (WriteRequestState* request : group)
		{
			request->Complete(request->status.ok() ? status : request->status);
		}
	}

	bool WriteCoordinator::FinishGroup()
	{
		bool should_continue = false;
		{
			std::lock_guard lock(mutex_);
			group_in_flight_ = false;
			if (!queue_.empty() && !shutting_down_)
			{
				should_continue = true;
			}
			else
			{
				drain_scheduled_ = false;
			}
			idle_cv_.notify_all();
		}

		return should_continue;
	}

	void WriteCoordinator::Shutdown()
	{
		std::vector<WriteRequestState*> queued;
		{
			std::unique_lock lock(mutex_);
			shutting_down_ = true;
			while (!queue_.empty())
			{
				queued.push_back(queue_.front());
				queue_.pop_front();
			}
			drain_scheduled_ = group_in_flight_;
		}

		CompleteGroup(std::move(queued), Status::IOError("database shutting down"));

		std::unique_lock lock(mutex_);
		idle_cv_.wait(lock, [this] { return !group_in_flight_; });
	}
} // namespace prism
