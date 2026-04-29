#ifndef PRISM_RUNTIME_METRICS_H
#define PRISM_RUNTIME_METRICS_H

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace prism
{
	struct RuntimeMetrics
	{
		// Existing metrics
		std::atomic<std::size_t> cancelled_before_start{ 0 };
		std::atomic<std::size_t> cooperative_checkpoint_cancel{ 0 };
		std::atomic<std::size_t> late_completion_quarantined{ 0 };
		std::atomic<std::size_t> fallback_to_blocking_count{ 0 };
		std::atomic<int> active_compaction_lane{ 0 };
		std::atomic<std::size_t> shutdown_wait_count{ 0 };
		std::atomic<uint64_t> shutdown_wait_duration_us{ 0 };

		// Foreground read-lane queue metrics — instrumented via PRISM_RUNTIME_METRICS
		// #ifdef. When PRISM_RUNTIME_METRICS is NOT defined, these counters remain at
		// 0 and the BlockingExecutor hot-path is identical to uninstrumented code.
		std::atomic<uint64_t> blocking_jobs_submitted{ 0 };
		std::atomic<uint64_t> blocking_jobs_completed{ 0 };
		std::atomic<uint64_t> blocking_peak_queue_depth{ 0 };
		std::atomic<uint64_t> blocking_enqueue_wait_total_us{ 0 };
		std::atomic<uint64_t> blocking_exec_time_total_us{ 0 };

		// Dedicated compaction-lane queue metrics.
		std::atomic<uint64_t> compaction_queue_jobs_submitted{ 0 };
		std::atomic<uint64_t> compaction_queue_jobs_completed{ 0 };
		std::atomic<uint64_t> compaction_peak_queue_depth{ 0 };
		std::atomic<uint64_t> compaction_enqueue_wait_total_us{ 0 };
		std::atomic<uint64_t> compaction_exec_time_total_us{ 0 };

		// Continuation scheduling delay — instrumented in AsyncOp::await_suspend.
		std::atomic<uint64_t> continuation_delay_total_us{ 0 };
		std::atomic<uint64_t> continuation_count{ 0 };

		// GetAsync DB-operation timing (synchronous DB::Get portion inside work lambda).
		std::atomic<uint64_t> get_async_db_op_total_us{ 0 };
		std::atomic<uint64_t> get_async_db_op_count{ 0 };

		// Compaction lifecycle counters — incremented when compaction work is
		// scheduled/completed by CompactionController.
		std::atomic<uint64_t> compaction_jobs_submitted{ 0 };
		std::atomic<uint64_t> compaction_jobs_completed{ 0 };

		// Scheduler-aware counters — always available (no PRISM_RUNTIME_METRICS gate).
		// These track the redesigned split-role scheduler's fast path, steal, and
		// locality behavior. Atomic increments are negligible overhead on the hot path.
		std::atomic<uint64_t> foreground_fastpath_submits{ 0 };   // Submit(job,0) that bypassed dispatcher
		std::atomic<uint64_t> foreground_fallback_submits{ 0 };   // Submit() that fell back to priority path
		std::atomic<uint64_t> steal_attempts{ 0 };                // TrySteal() invocations
		std::atomic<uint64_t> steal_successes{ 0 };               // TrySteal() that moved jobs
		std::atomic<uint64_t> worker_local_jobs_completed{ 0 };   // jobs completed from own queue
		std::atomic<uint64_t> stolen_jobs_completed{ 0 };         // jobs completed after being stolen

		void Reset() noexcept;

		void PrintMetrics() const noexcept;

		static RuntimeMetrics& Instance() noexcept;
	};
} // namespace prism

#endif // PRISM_RUNTIME_METRICS_H
