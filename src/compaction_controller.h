#ifndef PRISM_COMPACTION_CONTROLLER_H
#define PRISM_COMPACTION_CONTROLLER_H

#include "runtime_executor.h"
#include "task_scope.h"

namespace prism
{
	class DBImpl;

	class CompactionController
	{
	public:
		CompactionController(BlockingExecutor& compaction_executor, DBImpl& db);
		~CompactionController() = default;

		CompactionController(const CompactionController&) = delete;
		CompactionController& operator=(const CompactionController&) = delete;
		CompactionController(CompactionController&&) = delete;
		CompactionController& operator=(CompactionController&&) = delete;

		void ScheduleIfNeeded();
		void OnWorkFinished();
		bool HasInFlightWork() const;
		void RequestStop() noexcept { stop_source_.RequestStop(); }
		bool StopRequested() const noexcept { return stop_source_.StopRequested(); }
		StopToken GetStopToken() const { return stop_source_.Token(); }

	private:
		bool NeedsBackgroundWork() const;
		void TrySubmitLocked();
		void SubmitBackgroundCompaction();
		void RunBackgroundCompaction();

		BlockingExecutor* compaction_executor_;
		DBImpl* db_;
		StopSource stop_source_;
		bool work_requested_{ false };
		// True while a single background compaction lane is queued or running.
		bool lane_active_{ false };
	};
} // namespace prism

#endif // PRISM_COMPACTION_CONTROLLER_H
