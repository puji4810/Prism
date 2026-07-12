#include "runtime_metrics.h"

#ifdef PRISM_RUNTIME_METRICS
#include <cstdio>
#endif

namespace prism
{
	void RuntimeMetrics::Reset() noexcept
	{
#ifdef PRISM_RUNTIME_METRICS
		cancelled_before_start.store(0, std::memory_order_relaxed);
		cooperative_checkpoint_cancel.store(0, std::memory_order_relaxed);
		late_completion_quarantined.store(0, std::memory_order_relaxed);
		fallback_to_blocking_count.store(0, std::memory_order_relaxed);
		active_compaction_lane.store(0, std::memory_order_relaxed);
		shutdown_wait_count.store(0, std::memory_order_relaxed);
		shutdown_wait_duration_us.store(0, std::memory_order_relaxed);

		db_read_jobs_submitted.store(0, std::memory_order_relaxed);
		db_read_jobs_completed.store(0, std::memory_order_relaxed);
		db_read_peak_queue_depth.store(0, std::memory_order_relaxed);
		db_read_enqueue_wait_total_us.store(0, std::memory_order_relaxed);
		db_read_exec_time_total_us.store(0, std::memory_order_relaxed);
		db_read_steal_attempts.store(0, std::memory_order_relaxed);
		db_read_steal_successes.store(0, std::memory_order_relaxed);

		blocking_io_jobs_submitted.store(0, std::memory_order_relaxed);
		blocking_io_jobs_completed.store(0, std::memory_order_relaxed);
		blocking_io_peak_queue_depth.store(0, std::memory_order_relaxed);
		blocking_io_enqueue_wait_total_us.store(0, std::memory_order_relaxed);
		blocking_io_exec_time_total_us.store(0, std::memory_order_relaxed);
		blocking_io_steal_attempts.store(0, std::memory_order_relaxed);
		blocking_io_steal_successes.store(0, std::memory_order_relaxed);

		compaction_queue_jobs_submitted.store(0, std::memory_order_relaxed);
		compaction_queue_jobs_completed.store(0, std::memory_order_relaxed);
		compaction_peak_queue_depth.store(0, std::memory_order_relaxed);
		compaction_enqueue_wait_total_us.store(0, std::memory_order_relaxed);
		compaction_exec_time_total_us.store(0, std::memory_order_relaxed);

		continuation_delay_total_us.store(0, std::memory_order_relaxed);
		continuation_count.store(0, std::memory_order_relaxed);

		get_async_db_op_total_us.store(0, std::memory_order_relaxed);
		get_async_db_op_count.store(0, std::memory_order_relaxed);

		wal_append_reactor_count.store(0, std::memory_order_relaxed);
		wal_append_fallback_count.store(0, std::memory_order_relaxed);
		wal_fsync_reactor_count.store(0, std::memory_order_relaxed);
		wal_fsync_fallback_count.store(0, std::memory_order_relaxed);
		wal_append_latency_total_us.store(0, std::memory_order_relaxed);
		wal_fsync_latency_total_us.store(0, std::memory_order_relaxed);
		async_wal_inflight_total_us.store(0, std::memory_order_relaxed);
		async_wal_inflight_count.store(0, std::memory_order_relaxed);
		write_groups_completed.store(0, std::memory_order_relaxed);
		write_group_requests_total.store(0, std::memory_order_relaxed);
		write_group_bytes_total.store(0, std::memory_order_relaxed);
		write_plan_mutex_total_us.store(0, std::memory_order_relaxed);
		write_plan_mutex_count.store(0, std::memory_order_relaxed);
		write_apply_total_us.store(0, std::memory_order_relaxed);
		write_apply_count.store(0, std::memory_order_relaxed);
		write_publish_mutex_total_us.store(0, std::memory_order_relaxed);
		write_publish_mutex_count.store(0, std::memory_order_relaxed);
		write_commit_total_us.store(0, std::memory_order_relaxed);
		write_commit_count.store(0, std::memory_order_relaxed);

		compaction_jobs_submitted.store(0, std::memory_order_relaxed);
		compaction_jobs_completed.store(0, std::memory_order_relaxed);

		foreground_fastpath_submits.store(0, std::memory_order_relaxed);
		foreground_fallback_submits.store(0, std::memory_order_relaxed);
		steal_attempts.store(0, std::memory_order_relaxed);
		steal_successes.store(0, std::memory_order_relaxed);
		worker_local_jobs_completed.store(0, std::memory_order_relaxed);
		stolen_jobs_completed.store(0, std::memory_order_relaxed);
#endif
	}

	void RuntimeMetrics::PrintMetrics() const noexcept
	{
#ifdef PRISM_RUNTIME_METRICS
		const auto db_read_completed = db_read_jobs_completed.load(std::memory_order_relaxed);
		const auto blocking_io_completed = blocking_io_jobs_completed.load(std::memory_order_relaxed);
		const auto wal_inflight_count = async_wal_inflight_count.load(std::memory_order_relaxed);
		const auto write_group_count = write_groups_completed.load(std::memory_order_relaxed);
		const auto write_plan_count = write_plan_mutex_count.load(std::memory_order_relaxed);
		const auto write_apply_stage_count = write_apply_count.load(std::memory_order_relaxed);
		const auto write_publish_count = write_publish_mutex_count.load(std::memory_order_relaxed);
		const auto write_commit_stage_count = write_commit_count.load(std::memory_order_relaxed);
		auto read_in_flight = db_read_jobs_submitted.load(std::memory_order_relaxed) - db_read_completed;
		auto blocking_io_in_flight = blocking_io_jobs_submitted.load(std::memory_order_relaxed) - blocking_io_completed;
		auto compaction_in_flight = compaction_queue_jobs_submitted.load(std::memory_order_relaxed)
		    - compaction_queue_jobs_completed.load(std::memory_order_relaxed);

		std::fprintf(stderr,
		    "--- RuntimeMetrics ---\n"
		    "db_read_jobs_submitted:     %lu\n"
		    "db_read_jobs_completed:     %lu\n"
		    "db_read_in_flight:          %lu\n"
		    "db_read_peak_queue_depth:   %lu\n"
		    "db_read_enqueue_wait_total_us: %lu\n"
		    "db_read_exec_time_total_us: %lu\n"
		    "db_read_avg_wait_us:        %.1f\n"
		    "db_read_avg_exec_us:        %.1f\n"
		    "db_read_steal_attempts:     %lu\n"
		    "db_read_steal_successes:    %lu\n"
		    "blocking_io_jobs_submitted: %lu\n"
		    "blocking_io_jobs_completed: %lu\n"
		    "blocking_io_in_flight:      %lu\n"
		    "blocking_io_peak_queue_depth: %lu\n"
		    "blocking_io_enqueue_wait_total_us: %lu\n"
		    "blocking_io_exec_time_total_us: %lu\n"
		    "blocking_io_avg_wait_us:    %.1f\n"
		    "blocking_io_avg_exec_us:    %.1f\n"
		    "blocking_io_steal_attempts: %lu\n"
		    "blocking_io_steal_successes:%lu\n"
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
		    "wal_append_reactor_count:   %lu\n"
		    "wal_append_fallback_count:  %lu\n"
		    "wal_fsync_reactor_count:    %lu\n"
		    "wal_fsync_fallback_count:   %lu\n"
		    "wal_append_latency_total_us:%lu\n"
		    "wal_fsync_latency_total_us: %lu\n"
		    "async_wal_inflight_count:   %lu\n"
		    "async_wal_inflight_total_us:%lu\n"
		    "async_wal_avg_inflight_us:  %.1f\n"
		    "write_groups_completed:     %lu\n"
		    "write_group_requests_total: %lu\n"
		    "write_group_bytes_total:    %lu\n"
		    "write_group_avg_requests:   %.1f\n"
		    "write_group_avg_bytes:      %.1f\n"
		    "write_plan_mutex_count:     %lu\n"
		    "write_plan_mutex_total_us:  %lu\n"
		    "write_plan_mutex_avg_us:    %.1f\n"
		    "write_apply_count:          %lu\n"
		    "write_apply_total_us:       %lu\n"
		    "write_apply_avg_us:         %.1f\n"
		    "write_publish_mutex_count:  %lu\n"
		    "write_publish_mutex_total_us:%lu\n"
		    "write_publish_mutex_avg_us: %.1f\n"
		    "write_commit_count:         %lu\n"
		    "write_commit_total_us:      %lu\n"
		    "write_commit_avg_us:        %.1f\n"
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
		    static_cast<unsigned long>(db_read_jobs_submitted.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(db_read_completed), static_cast<unsigned long>(read_in_flight),
		    static_cast<unsigned long>(db_read_peak_queue_depth.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(db_read_enqueue_wait_total_us.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(db_read_exec_time_total_us.load(std::memory_order_relaxed)),
		    db_read_completed > 0 ? static_cast<double>(db_read_enqueue_wait_total_us.load(std::memory_order_relaxed))
		            / static_cast<double>(db_read_completed)
		                          : 0.0,
		    db_read_completed > 0 ? static_cast<double>(db_read_exec_time_total_us.load(std::memory_order_relaxed))
		            / static_cast<double>(db_read_completed)
		                          : 0.0,
		    static_cast<unsigned long>(db_read_steal_attempts.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(db_read_steal_successes.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(blocking_io_jobs_submitted.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(blocking_io_completed), static_cast<unsigned long>(blocking_io_in_flight),
		    static_cast<unsigned long>(blocking_io_peak_queue_depth.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(blocking_io_enqueue_wait_total_us.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(blocking_io_exec_time_total_us.load(std::memory_order_relaxed)),
		    blocking_io_completed > 0 ? static_cast<double>(blocking_io_enqueue_wait_total_us.load(std::memory_order_relaxed))
		            / static_cast<double>(blocking_io_completed)
		                              : 0.0,
		    blocking_io_completed > 0 ? static_cast<double>(blocking_io_exec_time_total_us.load(std::memory_order_relaxed))
		            / static_cast<double>(blocking_io_completed)
		                              : 0.0,
		    static_cast<unsigned long>(blocking_io_steal_attempts.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(blocking_io_steal_successes.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(compaction_queue_jobs_submitted.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(compaction_queue_jobs_completed.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(compaction_in_flight),
		    static_cast<unsigned long>(compaction_peak_queue_depth.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(compaction_enqueue_wait_total_us.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(compaction_exec_time_total_us.load(std::memory_order_relaxed)),
		    compaction_queue_jobs_completed > 0 ? static_cast<double>(compaction_enqueue_wait_total_us.load(std::memory_order_relaxed))
		            / static_cast<double>(compaction_queue_jobs_completed.load(std::memory_order_relaxed))
		                                        : 0.0,
		    compaction_queue_jobs_completed > 0 ? static_cast<double>(compaction_exec_time_total_us.load(std::memory_order_relaxed))
		            / static_cast<double>(compaction_queue_jobs_completed.load(std::memory_order_relaxed))
		                                        : 0.0,
		    static_cast<unsigned long>(continuation_count.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(continuation_delay_total_us.load(std::memory_order_relaxed)),
		    continuation_count > 0 ? static_cast<double>(continuation_delay_total_us.load(std::memory_order_relaxed))
		            / static_cast<double>(continuation_count.load(std::memory_order_relaxed))
		                           : 0.0,
		    static_cast<unsigned long>(get_async_db_op_count.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(get_async_db_op_total_us.load(std::memory_order_relaxed)),
		    get_async_db_op_count > 0 ? static_cast<double>(get_async_db_op_total_us.load(std::memory_order_relaxed))
		            / static_cast<double>(get_async_db_op_count.load(std::memory_order_relaxed))
		                              : 0.0,
		    static_cast<unsigned long>(wal_append_reactor_count.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(wal_append_fallback_count.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(wal_fsync_reactor_count.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(wal_fsync_fallback_count.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(wal_append_latency_total_us.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(wal_fsync_latency_total_us.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(wal_inflight_count),
		    static_cast<unsigned long>(async_wal_inflight_total_us.load(std::memory_order_relaxed)),
		    wal_inflight_count > 0 ? static_cast<double>(async_wal_inflight_total_us.load(std::memory_order_relaxed))
		            / static_cast<double>(wal_inflight_count)
		                           : 0.0,
		    static_cast<unsigned long>(write_group_count),
		    static_cast<unsigned long>(write_group_requests_total.load(std::memory_order_relaxed)),
		    static_cast<unsigned long>(write_group_bytes_total.load(std::memory_order_relaxed)),
		    write_group_count > 0 ? static_cast<double>(write_group_requests_total.load(std::memory_order_relaxed))
		            / static_cast<double>(write_group_count)
		                          : 0.0,
		    write_group_count > 0 ? static_cast<double>(write_group_bytes_total.load(std::memory_order_relaxed))
		            / static_cast<double>(write_group_count)
		                          : 0.0,
		    static_cast<unsigned long>(write_plan_count),
		    static_cast<unsigned long>(write_plan_mutex_total_us.load(std::memory_order_relaxed)),
		    write_plan_count > 0 ? static_cast<double>(write_plan_mutex_total_us.load(std::memory_order_relaxed))
		            / static_cast<double>(write_plan_count)
		                         : 0.0,
		    static_cast<unsigned long>(write_apply_stage_count),
		    static_cast<unsigned long>(write_apply_total_us.load(std::memory_order_relaxed)),
		    write_apply_stage_count > 0 ? static_cast<double>(write_apply_total_us.load(std::memory_order_relaxed))
		            / static_cast<double>(write_apply_stage_count)
		                                : 0.0,
		    static_cast<unsigned long>(write_publish_count),
		    static_cast<unsigned long>(write_publish_mutex_total_us.load(std::memory_order_relaxed)),
		    write_publish_count > 0 ? static_cast<double>(write_publish_mutex_total_us.load(std::memory_order_relaxed))
		            / static_cast<double>(write_publish_count)
		                            : 0.0,
		    static_cast<unsigned long>(write_commit_stage_count),
		    static_cast<unsigned long>(write_commit_total_us.load(std::memory_order_relaxed)),
		    write_commit_stage_count > 0 ? static_cast<double>(write_commit_total_us.load(std::memory_order_relaxed))
		            / static_cast<double>(write_commit_stage_count)
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
#endif
	}

	RuntimeMetrics& RuntimeMetrics::Instance() noexcept
	{
		static RuntimeMetrics metrics;
		return metrics;
	}
} // namespace prism
