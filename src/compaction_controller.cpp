#include "compaction_controller.h"

#include "db_impl.h"
#include "runtime_metrics.h"

namespace prism
{
	CompactionController::CompactionController(BlockingExecutor& compaction_executor, DBImpl& db)
	    : compaction_executor_(&compaction_executor)
	    , db_(&db)
	{
	}

	bool CompactionController::NeedsBackgroundWork() const
	{
		return db_->imm_ != nullptr || db_->versions_->current()->compaction_score() >= 1;
	}

	void CompactionController::TrySubmitLocked()
	{
		if (!work_requested_ || lane_active_)
		{
			return;
		}

		lane_active_ = true;
		work_requested_ = false;
		SubmitBackgroundCompaction();
	}

	void CompactionController::ScheduleIfNeeded()
	{
		if (db_->shutting_down_.load(std::memory_order_acquire))
		{
			return;
		}
		if (stop_source_.StopRequested())
		{
			return;
		}
		if (!db_->bg_error_.ok())
		{
			return;
		}
		if (!NeedsBackgroundWork())
		{
			return;
		}

		work_requested_ = true;
		TrySubmitLocked();
	}

	void CompactionController::SubmitBackgroundCompaction()
	{
		RuntimeMetrics::Instance().compaction_jobs_submitted.fetch_add(1, std::memory_order_relaxed);
		compaction_executor_->Submit([this] { RunBackgroundCompaction(); });
	}

	void CompactionController::RunBackgroundCompaction()
	{
		std::unique_lock<std::shared_mutex> lock(db_->mutex_);
		assert(lane_active_);
		RuntimeMetrics::Instance().active_compaction_lane.store(1, std::memory_order_relaxed);
		++db_->background_compaction_start_count_;
		auto stop_token = GetStopToken();
		while (db_->hold_background_compaction_ && !db_->shutting_down_.load(std::memory_order_acquire) &&
		       !stop_token.StopRequested())
		{
			db_->background_work_finished_signal_.wait(lock);
		}
		if (!stop_token.StopRequested() && !db_->shutting_down_.load(std::memory_order_acquire) && db_->bg_error_.ok())
		{
			db_->BackgroundCompaction(stop_token);
		}

		OnWorkFinished();
	}

	void CompactionController::OnWorkFinished()
	{
		lane_active_ = false;
		RuntimeMetrics::Instance().active_compaction_lane.store(0, std::memory_order_relaxed);
		RuntimeMetrics::Instance().compaction_jobs_completed.fetch_add(1, std::memory_order_relaxed);
		if (!stop_source_.StopRequested() && !db_->shutting_down_.load(std::memory_order_acquire) && db_->bg_error_.ok() &&
		    NeedsBackgroundWork())
		{
			work_requested_ = true;
		}
		else
		{
			work_requested_ = false;
		}
		db_->background_work_finished_signal_.notify_all();
		TrySubmitLocked();
	}

	bool CompactionController::HasInFlightWork() const
	{
		return lane_active_;
	}
} // namespace prism
