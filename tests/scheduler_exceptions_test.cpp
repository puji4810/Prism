#include "scheduler.h"

#include <atomic>
#include <gtest/gtest.h>
#include <semaphore>
#include <stdexcept>

using namespace prism;
using namespace std::chrono_literals;

// ---------------------------------------------------------------------------
// WorkerLoopThrowTerminates
// A job submitted via Submit() executes on a worker thread. Any exception
// escaping the job must trigger std::terminate() (fail-fast contract).
// ---------------------------------------------------------------------------
TEST(SchedulerExceptionsTest, WorkerLoopThrowTerminates)
{
	ASSERT_DEATH(
	    {
		    ThreadPoolScheduler scheduler(2);
		    std::binary_semaphore job_started{ 0 };

		    scheduler.Submit([&job_started]() {
			    job_started.release();
			    throw std::runtime_error("contract violation");
		    });

		    // Wait until the throw is actually in-flight before the process dies.
		    job_started.acquire();
		    // Brief yield to let the exception propagate into the catch handler.
		    std::this_thread::yield();
	    },
	    "");
}

// ---------------------------------------------------------------------------
// AffinityJobThrowTerminates
// A job submitted via SubmitIn() (worker-affinity path) also executes on a
// worker thread through the same Consume() loop. Exceptions must terminate.
// ---------------------------------------------------------------------------
TEST(SchedulerExceptionsTest, AffinityJobThrowTerminates)
{
	ASSERT_DEATH(
	    {
		    ThreadPoolScheduler scheduler(2);
		    std::binary_semaphore affinity_submitted{ 0 };

		    // Submit a job that captures its own context and re-submits a
		    // throwing job back to itself via affinity.
		    scheduler.Submit([&scheduler, &affinity_submitted]() {
			    auto ctx = scheduler.CaptureContext();
			    scheduler.SubmitIn(ctx, []() { throw std::logic_error("affinity contract violation"); });
			    affinity_submitted.release();
		    });

		    affinity_submitted.acquire();
		    // The throwing affinity job is now queued; give it time to run.
		    std::this_thread::yield();
	    },
	    "");
}

// ---------------------------------------------------------------------------
// DestructorDrainLazyThrowTerminates
// A delayed task with a far-future deadline is promoted to immediate
// execution during destructor drain (drain path 2a). If it throws,
// std::terminate() must be called.
// ---------------------------------------------------------------------------
TEST(SchedulerExceptionsTest, DestructorDrainLazyThrowTerminates)
{
	ASSERT_DEATH(
	    {
		    ThreadPoolScheduler scheduler(2);

		    // Deadline far in the future — will not fire normally; must be
		    // promoted by the destructor drain.
		    auto future = std::chrono::steady_clock::now() + std::chrono::hours(24);
		    scheduler.SubmitAfter(future, []() { throw std::logic_error("lazy drain contract violation"); });

		    // Destructor triggers here, draining lazy_queue_ inline.
	    },
	    "");
}

// ---------------------------------------------------------------------------
// DestructorDrainPriorityThrowTerminates
// A job still in the priority queue when the destructor runs is executed
// inline (drain path 2b). If it throws, std::terminate() must be called.
// ---------------------------------------------------------------------------
TEST(SchedulerExceptionsTest, DestructorDrainPriorityThrowTerminates)
{
	ASSERT_DEATH(
	    {
		    // Use 2 workers; block both to saturate the thread pool so the
		    // throwing job stays in the priority queue until destructor drain.
		    constexpr int kWorkers = 2;
		    ThreadPoolScheduler scheduler(kWorkers);

		    std::atomic<int> blockers_entered{ 0 };
		    std::atomic<bool> exit_requested{ false };

		    for (int i = 0; i < kWorkers; ++i)
		    {
			    scheduler.Submit([&blockers_entered, &exit_requested]() {
				    blockers_entered.fetch_add(1, std::memory_order_relaxed);
				    // Spin until exit is signalled so we don't hold a semaphore.
				    while (!exit_requested.load(std::memory_order_acquire))
					    std::this_thread::yield();
			    });
		    }

		    // Wait until all workers are executing the blocking jobs.
		    while (blockers_entered.load(std::memory_order_acquire) < kWorkers)
			    std::this_thread::yield();

		    // Submit the throwing job — both workers occupied, goes to
		    // priority_queue_ and stays there.
		    scheduler.Submit([]() { throw std::runtime_error("priority drain contract violation"); });

		    // Signal blocking jobs to finish so workers complete and can be joined.
		    // After joining, destructor drain 2b runs the throwing job inline.
		    exit_requested.store(true, std::memory_order_release);

		    // Destructor runs here — joins threads, then drains priority_queue_.
	    },
	    "");
}

// ---------------------------------------------------------------------------
// DrainRemainingThrowTerminates
// A job placed in a worker's local queue (via SubmitIn) during the destructor's
// sequential drain phase is executed by WorkThread::DrainRemaining() after that
// worker's thread has been joined. Any exception escaping that job must trigger
// std::terminate() via the DrainRemaining() try/catch(...) callsite specifically.
//
// How determinism is guaranteed:
//   1. A trampoline job is submitted via SubmitAfter (24h deadline) so it stays
//      in lazy_queue_ and is never dispatched to a worker during normal operation.
//   2. The scheduler destructor sets t_current_scheduler = this on the destructor
//      thread before the sequential drain loop, making CaptureContext() return a
//      valid Context(this, 0) (worker_index defaults to 0 on the destructor thread).
//   3. Destructor drain 2a executes the trampoline; the trampoline calls
//      scheduler.CaptureContext() to obtain Context(this, 0) then calls
//      scheduler.SubmitIn(ctx, throwing_job), which calls WorkThread::Push() for
//      worker 0 — adding the throwing job directly to that worker's local deque.
//   4. With work_remains=true the drain loop continues; 2c calls DrainRemaining()
//      on worker 0 and executes the throwing job, triggering std::terminate().
//
// No sleeps, no timing dependencies, no external synchronisation required.
// ---------------------------------------------------------------------------
TEST(SchedulerExceptionsTest, DrainRemainingThrowTerminates)
{
	ASSERT_DEATH(
	    {
		    ThreadPoolScheduler scheduler(2);
		    // Schedule a trampoline with a far-future deadline so it never fires
		    // normally and is guaranteed to stay in lazy_queue_ until destructor.
		    auto far_future = std::chrono::steady_clock::now() + std::chrono::hours(24);
		    scheduler.SubmitAfter(far_future, [&scheduler]() {
			    // During destructor drain 2a, t_current_scheduler == &scheduler and
			    // t_current_worker_index == 0 (destructor thread, not a worker).
			    // CaptureContext() therefore returns Context(&scheduler, 0).
			    auto ctx = scheduler.CaptureContext();
			    // SubmitIn with a valid context calls WorkThread::Push() directly,
			    // placing the throwing job in worker 0's local deque.
			    scheduler.SubmitIn(ctx, []() { throw std::runtime_error("drain remaining contract violation"); });
		    });
		    // Destructor runs here:
		    //   Exit() → join threads → drain loop:
		    //     2a: trampoline runs, pushing throwing job to worker 0 queue
		    //     2c: DrainRemaining() on worker 0 executes throwing job → terminate()
		    // The throwing job's path through DrainRemaining is the ONLY path it can
		    // take: workers are already joined and no Consume() loop is running.
	    },
	    "");
}

// ---------------------------------------------------------------------------
// GoogleTest main
// ---------------------------------------------------------------------------
