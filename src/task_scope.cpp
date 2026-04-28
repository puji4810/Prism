#include "task_scope.h"

namespace prism
{
	struct StopSource::StopState
	{
		std::atomic<bool> stop_requested{ false };
	};

	struct StopToken::ChainNode
	{
		std::shared_ptr<StopSource::StopState> state;
		std::shared_ptr<const ChainNode> parent;
	};

	StopSource::StopSource()
	    : state_(std::make_shared<StopState>())
	{
	}

	void StopSource::RequestStop() noexcept
	{
		state_->stop_requested.store(true, std::memory_order_release);
	}

	bool StopSource::StopRequested() const noexcept
	{
		return state_->stop_requested.load(std::memory_order_acquire);
	}

	StopToken StopSource::Token() const { return Token(StopToken{}); }

	StopToken StopSource::Token(StopToken parent) const
	{
		auto chain = std::make_shared<StopToken::ChainNode>();
		chain->state = state_;
		chain->parent = std::move(parent.chain_);
		return StopToken(std::move(chain));
	}

	bool StopToken::StopRequested() const noexcept
	{
		auto current = chain_;
		while (current)
		{
			if (current->state && current->state->stop_requested.load(std::memory_order_acquire))
			{
				return true;
			}
			current = current->parent;
		}
		return false;
	}

	bool StopToken::CheckStop() const noexcept
	{
		const bool stop_requested = StopRequested();
		if (stop_requested)
		{
			RuntimeMetrics::Instance().cooperative_checkpoint_cancel.fetch_add(1, std::memory_order_relaxed);
		}
		return stop_requested;
	}

	void Quarantine::StoreCancelled()
	{
		std::lock_guard lock(mutex_);
		entries_.push_back(Entry{ EntryKind::kCancelled, std::any{} });
	}

	void Quarantine::StoreUnit()
	{
		std::lock_guard lock(mutex_);
		entries_.push_back(Entry{ EntryKind::kUnit, std::any{} });
	}

	void Quarantine::StoreException(std::exception_ptr exception)
	{
		std::lock_guard lock(mutex_);
		entries_.push_back(Entry{ EntryKind::kException, std::any(std::move(exception)) });
	}

	std::size_t Quarantine::Size() const
	{
		std::lock_guard lock(mutex_);
		return entries_.size();
	}

	std::size_t Quarantine::Count(EntryKind kind) const
	{
		std::lock_guard lock(mutex_);
		std::size_t count = 0;
		for (const auto& entry: entries_)
		{
			if (entry.kind == kind)
			{
				++count;
			}
		}
		return count;
	}

	std::vector<Quarantine::Entry> Quarantine::Snapshot() const
	{
		std::lock_guard lock(mutex_);
		return entries_;
	}

	TaskScope::TaskScope(IContinuationExecutor& executor)
	    : executor_(&executor)
	    , quarantine_(std::make_shared<Quarantine>())
	{
	}

	TaskScope::TaskScope(IContinuationExecutor& executor, StopToken parent_token)
	    : executor_(&executor)
	    , parent_token_(std::move(parent_token))
	    , quarantine_(std::make_shared<Quarantine>())
	{
	}

	TaskScope::TaskScope(IContinuationExecutor& executor, StopSource& parent_source)
	    : TaskScope(executor, parent_source.Token())
	{
	}

	TaskScope::~TaskScope()
	{
		RequestStop();
		Join();
	}

	void TaskScope::RequestStop() noexcept { stop_source_.RequestStop(); }

	bool TaskScope::StopRequested() const noexcept { return GetStopToken().StopRequested(); }

	StopToken TaskScope::GetStopToken() const { return stop_source_.Token(parent_token_); }

	void TaskScope::Join()
	{
		std::unique_lock lock(join_mutex_);
		join_cv_.wait(lock, [this] { return in_flight_.load(std::memory_order_acquire) == 0; });
	}

	std::size_t TaskScope::InFlight() const noexcept { return in_flight_.load(std::memory_order_acquire); }

	Quarantine& TaskScope::QuarantineSink() noexcept { return *quarantine_; }

	const Quarantine& TaskScope::QuarantineSink() const noexcept { return *quarantine_; }

	void TaskScope::BeginChild() { in_flight_.fetch_add(1, std::memory_order_acq_rel); }

	void TaskScope::FinishChild() noexcept
	{
		if (in_flight_.fetch_sub(1, std::memory_order_acq_rel) == 1)
		{
			std::lock_guard lock(join_mutex_);
			join_cv_.notify_all();
		}
	}

} // namespace prism
