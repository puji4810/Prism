#include "runtime_metrics.h"

#include <cstdio>

namespace prism
{
	void RuntimeMetrics::Reset() noexcept
	{
		cancelled_before_start.store(0, std::memory_order_relaxed);
		cooperative_checkpoint_cancel.store(0, std::memory_order_relaxed);
		late_completion_quarantined.store(0, std::memory_order_relaxed);
		fallback_to_blocking_count.store(0, std::memory_order_relaxed);
		active_compaction_lane.store(0, std::memory_order_relaxed);
		shutdown_wait_count.store(0, std::memory_order_relaxed);
		shutdown_wait_duration_us.store(0, std::memory_order_relaxed);

		blocking_jobs_submitted.store(0, std::memory_order_relaxed);
		blocking_jobs_completed.store(0, std::memory_order_relaxed);
		blocking_peak_queue_depth.store(0, std::memory_order_relaxed);
		blocking_enqueue_wait_total_us.store(0, std::memory_order_relaxed);
		blocking_exec_time_total_us.store(0, std::memory_order_relaxed);

		compaction_queue_jobs_submitted.store(0, std::memory_order_relaxed);
		compaction_queue_jobs_completed.store(0, std::memory_order_relaxed);
		compaction_peak_queue_depth.store(0, std::memory_order_relaxed);
		compaction_enqueue_wait_total_us.store(0, std::memory_order_relaxed);
		compaction_exec_time_total_us.store(0, std::memory_order_relaxed);

		continuation_delay_total_us.store(0, std::memory_order_relaxed);
		continuation_count.store(0, std::memory_order_relaxed);

		get_async_db_op_total_us.store(0, std::memory_order_relaxed);
		get_async_db_op_count.store(0, std::memory_order_relaxed);

		compaction_jobs_submitted.store(0, std::memory_order_relaxed);
		compaction_jobs_completed.store(0, std::memory_order_relaxed);

		foreground_fastpath_submits.store(0, std::memory_order_relaxed);
		foreground_fallback_submits.store(0, std::memory_order_relaxed);
		steal_attempts.store(0, std::memory_order_relaxed);
		steal_successes.store(0, std::memory_order_relaxed);
		worker_local_jobs_completed.store(0, std::memory_order_relaxed);
		stolen_jobs_completed.store(0, std::memory_order_relaxed);
	}

	void RuntimeMetrics::PrintMetrics() const noexcept
	{
		auto read_in_flight = blocking_jobs_submitted.load(std::memory_order_relaxed) -
		                      blocking_jobs_completed.load(std::memory_order_relaxed);
		auto compaction_in_flight = compaction_queue_jobs_submitted.load(std::memory_order_relaxed) -
		                            compaction_queue_jobs_completed.load(std::memory_order_relaxed);

		std::fprintf(stderr,
		    "--- RuntimeMetrics ---\n"
		    "read_jobs_submitted:        %lu\n"
		    "read_jobs_completed:        %lu\n"
		    "read_in_flight:             %lu\n"
		    "read_peak_queue_depth:      %lu\n"
		    "read_enqueue_wait_total_us: %lu\n"
		    "read_exec_time_total_us:    %lu\n"
		    "read_avg_wait_us:           %.1f\n"
		    "read_avg_exec_us:           %.1f\n"
		    "compaction_queue_jobs_submitted:    %lu\n"
		    "compaction_queue_jobs_completed:    %lu\n"
		    "compaction_queue_in_flight:         %lu\n"
		    "compaction_peak_queue_depth:        %lu\n"
		    "compaction_enqueue_wait_total_us:   %lu\n"
		    "compaction_exec_time_total_us:      %lu\n"
		    "compaction_avg_wait_us:             %.1f\n"
		    "compaction_avg_exec_us:             %.1f\n"
		    "continuation_count:         %lu\n"
		    "continuation_delay_total_us:%lu\n"
		    "continuation_avg_delay_us:  %.1f\n"
		    "get_async_db_op_count:      %lu\n"
		    "get_async_db_op_total_us:   %lu\n"
		    "get_async_avg_db_op_us:     %.1f\n"
		    "compaction_jobs_submitted:  %lu\n"
		    "compaction_jobs_completed:  %lu\n"
		    "active_compaction_lane:     %d\n"
		    "fallback_to_blocking_count: %lu\n"
		    "shutdown_wait_count:        %lu\n"
		    "shutdown_wait_duration_us:  %lu\n"
		    "foreground_fastpath_submits:    %lu\n"
		    "foreground_fallback_submits:    %lu\n"
		    "steal_attempts:             %lu\n"
		    "steal_successes:            %lu\n"
		    "worker_local_jobs_completed:%lu\n"
		    "stolen_jobs_completed:      %lu\n",
		    static_cast<unsigned long>(blocking_jobs_submitted.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(blocking_jobs_completed.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(read_in_flight),
		    static_cast<unsigned long>(blocking_peak_queue_depth.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(blocking_enqueue_wait_total_us.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(blocking_exec_time_total_us.load(std::memory_order_relaxed)),
		    blocking_jobs_completed > 0
		        ? static_cast<double>(blocking_enqueue_wait_total_us.load(std::memory_order_relaxed)) /
		              static_cast<double>(blocking_jobs_completed.load(std::memory_order_relaxed))
		        : 0.0,
		    blocking_jobs_completed > 0
		        ? static_cast<double>(blocking_exec_time_total_us.load(std::memory_order_relaxed)) /
		              static_cast<double>(blocking_jobs_completed.load(std::memory_order_relaxed))
		        : 0.0,
		    static_cast<unsigned long>(compaction_queue_jobs_submitted.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(compaction_queue_jobs_completed.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(compaction_in_flight),
		    static_cast<unsigned long>(compaction_peak_queue_depth.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(compaction_enqueue_wait_total_us.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(compaction_exec_time_total_us.load(std::memory_order_relaxed)),
		    compaction_queue_jobs_completed > 0
		        ? static_cast<double>(compaction_enqueue_wait_total_us.load(std::memory_order_relaxed)) /
		              static_cast<double>(compaction_queue_jobs_completed.load(std::memory_order_relaxed))
		        : 0.0,
		    compaction_queue_jobs_completed > 0
		        ? static_cast<double>(compaction_exec_time_total_us.load(std::memory_order_relaxed)) /
		              static_cast<double>(compaction_queue_jobs_completed.load(std::memory_order_relaxed))
		        : 0.0,
		    static_cast<unsigned long>(continuation_count.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(continuation_delay_total_us.load(std::memory_order_relaxed)),
		    continuation_count > 0
		        ? static_cast<double>(continuation_delay_total_us.load(std::memory_order_relaxed)) /
		              static_cast<double>(continuation_count.load(std::memory_order_relaxed))
		        : 0.0,
		    static_cast<unsigned long>(get_async_db_op_count.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(get_async_db_op_total_us.load(std::memory_order_relaxed)),
		    get_async_db_op_count > 0
		        ? static_cast<double>(get_async_db_op_total_us.load(std::memory_order_relaxed)) /
		              static_cast<double>(get_async_db_op_count.load(std::memory_order_relaxed))
		        : 0.0,
		    static_cast<unsigned long>(compaction_jobs_submitted.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(compaction_jobs_completed.load(std::memory_order_relaxed)),
		    active_compaction_lane.load(std::memory_order_relaxed),
		    static_cast<unsigned long>(fallback_to_blocking_count.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(shutdown_wait_count.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(shutdown_wait_duration_us.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(foreground_fastpath_submits.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(foreground_fallback_submits.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(steal_attempts.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(steal_successes.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(worker_local_jobs_completed.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(stolen_jobs_completed.load(std::memory_order_relaxed)));
	}

	RuntimeMetrics& RuntimeMetrics::Instance() noexcept
	{
		static RuntimeMetrics metrics;
		return metrics;
	}
} // namespace prism
