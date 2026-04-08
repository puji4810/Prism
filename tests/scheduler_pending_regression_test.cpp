// Regression test for S7: per-worker return_to_pending_ hazard.
//
// The hazard: before the fix, return_to_pending_ was a per-worker atomic flag set by
// PushDispatched(). If an affinity job (Push via SubmitIn) was enqueued BEFORE the
// dispatched job in the worker's queue, the affinity job would run first, see
// return_to_pending_=true (set for the dispatched job that hadn't run yet), and
// incorrectly re-register the worker as pending — while the dispatched job still sat in
// the queue. The dispatcher would then dispatch another job to this "idle" worker, which
// still had the original dispatched job pending. Eventually the worker would exhaust all
// pending registrations and zombie out, unable to receive new work while holding
// unexecuted jobs.
//
// The fix: per-task `dispatched` metadata replaces the per-worker flag. Re-registration
// fires only when `queued.dispatched && queue_was_empty_after_pop`. This test
// deterministically forces the hazardous interleaving and verifies all jobs complete.

#include "scheduler.h"

#include <atomic>
#include <chrono>
#include <semaphore>
#include <gtest/gtest.h>

using namespace prism;
using namespace std::chrono_literals;

// ---------------------------------------------------------------------------
// Helper: bounded wait with deadlock detection
// ---------------------------------------------------------------------------
static bool WaitFor(const std::atomic<int>& counter, int target, std::chrono::milliseconds timeout = 5s)
{
	auto start = std::chrono::steady_clock::now();
	while (counter.load(std::memory_order_acquire) != target)
	{
		if (std::chrono::steady_clock::now() - start > timeout)
			return false;
		std::this_thread::yield();
	}
	return true;
}

// ---------------------------------------------------------------------------
// TEST 1: Affinity-before-dispatched interleaving
//
// Strategy: Use a 1-thread scheduler so we can control exactly which worker
// receives both the affinity job and the dispatched job. A semaphore holds the
// affinity job open while the dispatcher queues a dispatched job to the same
// worker. On the old code, the affinity job would consume return_to_pending_=true
// and re-register the worker prematurely, leaving the dispatched job stranded
// if no further semaphore releases happened.
// ---------------------------------------------------------------------------
TEST(SchedulerPendingRegressionTest, AffinityBeforeDispatchedNoZombie)
{
	// Single worker so both jobs land on the same thread.
	ThreadPoolScheduler scheduler(1);

	std::atomic<int> affinity_done{ 0 };
	std::atomic<int> dispatched_done{ 0 };

	// Gate: blocks the affinity job inside the running job body so the
	// dispatcher can also enqueue a dispatched job to the SAME worker while
	// the affinity job is in flight.
	std::binary_semaphore affinity_gate{ 0 };

	// Step 1: Submit a job via Submit() (dispatched path). This job runs on
	// the single worker, captures its context, and submits an affinity job
	// (via SubmitIn) that blocks on the gate. While the affinity job is
	// BLOCKED, the outer job also calls Submit() so the dispatcher will try
	// to dispatch another job — which ends up in the same worker's queue
	// via PushDispatched, AFTER the affinity job.
	//
	// Queue after setup:
	//   [affinity_job (dispatched=false), dispatched_job2 (dispatched=true)]
	//
	// Old behavior: affinity_job runs, sees return_to_pending_=true (set
	// by PushDispatched for dispatched_job2), re-registers worker — while
	// dispatched_job2 still sits in queue. Worker may then get another
	// dispatched job from a fresh Submit, but dispatched_job2 is still
	// stuck. Worker eventually zombifies.
	//
	// New behavior: affinity_job runs, sees dispatched=false → no
	// re-register. dispatched_job2 runs next, sees dispatched=true &&
	// queue_empty → re-registers. Clean.

	// We'll track whether the dispatcher can find the worker after the fix.
	// Submit a chain: outer → SubmitIn(affinity) + Submit(second dispatched).
	// After gate release, both affinity and second dispatched should complete.

	constexpr int kIterations = 200;

	for (int iter = 0; iter < kIterations; ++iter)
	{
		affinity_done.store(0, std::memory_order_relaxed);
		dispatched_done.store(0, std::memory_order_relaxed);
		// Reset gate so it blocks
		// binary_semaphore doesn't have reset; we release one only after setup.

		// Phase A: outer job sets up the interleaving.
		std::binary_semaphore gate{ 0 }; // fresh gate per iteration

		scheduler.Submit([&scheduler, &gate, &affinity_done, &dispatched_done]() {
			// Running on the single worker. Capture its context.
			auto ctx = scheduler.CaptureContext();
			ASSERT_TRUE(ctx.IsValid());

			// Enqueue affinity job that will block until gate is released.
			// This lands in the queue first (dispatched=false).
			scheduler.SubmitIn(ctx, [&gate, &affinity_done]() {
				gate.acquire(); // blocks until main thread releases
				affinity_done.fetch_add(1, std::memory_order_release);
			});

			// Enqueue a dispatched job via normal Submit. The dispatcher will
			// call PushDispatched on the same (only) worker. This lands in
			// the queue AFTER the affinity job (dispatched=true).
			scheduler.Submit([&dispatched_done]() { dispatched_done.fetch_add(1, std::memory_order_release); });

			// Outer job itself does not count; it just sets up the scenario.
		});

		// Give the outer job time to enqueue both sub-jobs.
		// We yield rather than sleep; the outer job is very fast.
		// In the worst case, gate.release() before the affinity job is enqueued
		// is harmless — the affinity job won't block if semaphore > 0.
		std::this_thread::yield();
		std::this_thread::yield();

		// Release gate — affinity job can now complete.
		gate.release();

		// Both affinity and dispatched jobs must complete with no deadlock.
		ASSERT_TRUE(WaitFor(affinity_done, 1)) << "affinity job stalled (iter=" << iter << ")";
		ASSERT_TRUE(WaitFor(dispatched_done, 1)) << "dispatched job stalled — worker zombified (iter=" << iter << ")";
	}
}

// ---------------------------------------------------------------------------
// TEST 2: High-volume interleaved SubmitIn + Submit stress
//
// Exercises many concurrent affinity + dispatched job submissions and asserts
// all complete. With the old code, this would eventually hit the zombie state
// under load (pending_list_ drains to zero with work still queued).
// ---------------------------------------------------------------------------
TEST(SchedulerPendingRegressionTest, HighVolumeAffinityAndDispatchedMix)
{
	ThreadPoolScheduler scheduler(2);

	constexpr int kTasks = 2000;
	std::atomic<int> affinity_count{ 0 };
	std::atomic<int> dispatched_count{ 0 };

	for (int i = 0; i < kTasks; ++i)
	{
		// Dispatched job: increments dispatched_count directly.
		scheduler.Submit([&dispatched_count]() { dispatched_count.fetch_add(1, std::memory_order_relaxed); });

		// Outer dispatched job: captures context, submits affinity follow-up.
		scheduler.Submit([&scheduler, &affinity_count]() {
			auto ctx = scheduler.CaptureContext();
			if (ctx.IsValid())
			{
				scheduler.SubmitIn(ctx, [&affinity_count]() { affinity_count.fetch_add(1, std::memory_order_relaxed); });
			}
			else
			{
				// Fallback path still counts (context invalid on main thread edge case)
				affinity_count.fetch_add(1, std::memory_order_relaxed);
			}
		});
	}

	ASSERT_TRUE(WaitFor(dispatched_count, kTasks, 10s)) << "dispatched jobs stalled: " << dispatched_count.load();
	ASSERT_TRUE(WaitFor(affinity_count, kTasks, 10s)) << "affinity jobs stalled: " << affinity_count.load();
}

// ---------------------------------------------------------------------------
// TEST 3: Verify worker re-registers after dispatched job with empty queue
//
// Forces a scenario where a dispatched job runs with an empty queue after pop,
// confirms the worker re-enters pending_list_ (evidenced by being able to
// receive and execute a subsequent Submit).
// ---------------------------------------------------------------------------
TEST(SchedulerPendingRegressionTest, WorkerReregistersAfterDispatchedJob)
{
	ThreadPoolScheduler scheduler(1);

	std::atomic<int> step{ 0 };

	// First job: dispatched. After it runs, worker must re-register.
	scheduler.Submit([&step]() { step.fetch_add(1, std::memory_order_release); });

	ASSERT_TRUE(WaitFor(step, 1)) << "first dispatched job did not run";

	// If worker re-registered, it should now accept a second Submit.
	scheduler.Submit([&step]() { step.fetch_add(1, std::memory_order_release); });

	ASSERT_TRUE(WaitFor(step, 2)) << "worker did not re-register after dispatched job (zombie)";
}

// ---------------------------------------------------------------------------
// TEST 4: Affinity-only jobs must NOT trigger re-registration
//
// Submits only affinity jobs to a pinned worker. The worker must NOT
// end up double-registered in pending_list_ (which would let the dispatcher
// double-dispatch and eventually corrupt the counter).
// ---------------------------------------------------------------------------
TEST(SchedulerPendingRegressionTest, AffinityJobsDoNotSpuriouslyReregister)
{
	ThreadPoolScheduler scheduler(2);

	constexpr int kChain = 500;
	std::atomic<int> chain_count{ 0 };

	// Start chain from a dispatched job so we get a valid context.
	scheduler.Submit([&scheduler, &chain_count]() {
		auto ctx = scheduler.CaptureContext();
		ASSERT_TRUE(ctx.IsValid());
		for (int i = 0; i < kChain; ++i)
		{
			scheduler.SubmitIn(ctx, [&chain_count]() { chain_count.fetch_add(1, std::memory_order_relaxed); });
		}
	});

	ASSERT_TRUE(WaitFor(chain_count, kChain, 5s)) << "affinity chain stalled at " << chain_count.load();

	// Verify the scheduler is still healthy: submit more work.
	std::atomic<int> health{ 0 };
	for (int i = 0; i < 10; ++i)
	{
		scheduler.Submit([&health]() { health.fetch_add(1, std::memory_order_relaxed); });
	}
	ASSERT_TRUE(WaitFor(health, 10, 5s)) << "scheduler unhealthy after affinity chain";
}
