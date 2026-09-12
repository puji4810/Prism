// scheduler_invariant_test.cpp
//
// Targeted regression tests that freeze correctness invariants most at risk
// during scheduler surgery: local routing, best-effort affinity, wakeup
// correctness, and zero-wakeup edge cases.

#include "scheduler.h"

#include <atomic>
#include <chrono>
#include <gtest/gtest.h>
#include <memory>
#include <mutex>
#include <semaphore>
#include <thread>
#include <vector>

using namespace prism;
using namespace std::chrono_literals;

// ---------------------------------------------------------------------------
// Helper: poll until predicate or timeout
// ---------------------------------------------------------------------------
template <typename Pred>
static bool PollUntil(Pred pred, std::chrono::milliseconds timeout = 5000ms)
{
	auto start = std::chrono::steady_clock::now();
	while (std::chrono::steady_clock::now() - start < timeout)
	{
		if (pred())
			return true;
		std::this_thread::yield();
	}
	return pred();
}

// ---------------------------------------------------------------------------
// INVARIANT 1: SelfSubmitRunsOnOwnWorker
//
// With peers still in their initial sleep and only one child, Submit() retains
// local routing without dispatcher mediation. Larger backlogs can migrate.
// ---------------------------------------------------------------------------
TEST(SchedulerInvariantTest, SelfSubmitRunsOnOwnWorker)
{
	CpuThreadPool scheduler(4);

	std::atomic<bool> done{ false };
	std::thread::id outer_tid;
	std::thread::id inner_tid;

	scheduler.Submit([&scheduler, &done, &outer_tid, &inner_tid]() {
		outer_tid = std::this_thread::get_id();
		// Self-submit: worker submits a job from within its own execution.
		scheduler.Submit([&done, &inner_tid]() {
			inner_tid = std::this_thread::get_id();
			done.store(true, std::memory_order_release);
		});
	});

	ASSERT_TRUE(PollUntil([&] { return done.load(std::memory_order_acquire); })) << "Timed out waiting for self-submitted job";

	// The inner job must run on the same worker as the outer job.
	EXPECT_EQ(outer_tid, inner_tid) << "Self-submit from worker must run on same worker thread (no dispatcher mediation)";
}

// ---------------------------------------------------------------------------
// INVARIANT 2: SubmitInPinnedTaskStaysOnWorker
//
// A small burst on a fresh pool retains local routing while peers sleep.
// SubmitIn affinity is best effort; large backlogs may be shared or stolen.
// ---------------------------------------------------------------------------
TEST(SchedulerInvariantTest, SubmitInPinnedTaskStaysOnWorker)
{
	CpuThreadPool scheduler(4);

	std::atomic<int> done_count{ 0 };
	std::thread::id outer_tid;
	std::vector<std::thread::id> inner_tids;
	std::mutex tids_mutex;
	constexpr int kAffinityJobs = 10;

	scheduler.Submit([&scheduler, &done_count, &outer_tid, &inner_tids, &tids_mutex]() {
		outer_tid = std::this_thread::get_id();
		auto ctx = scheduler.CaptureContext();
		ASSERT_TRUE(ctx.IsValid());

		// Submit a small burst through the captured context.
		for (int i = 0; i < kAffinityJobs; ++i)
		{
			scheduler.SubmitIn(ctx, [&done_count, &inner_tids, &tids_mutex]() {
				{
					std::lock_guard lock(tids_mutex);
					inner_tids.push_back(std::this_thread::get_id());
				}
				done_count.fetch_add(1, std::memory_order_release);
			});
		}
	});

	ASSERT_TRUE(PollUntil([&] { return done_count.load(std::memory_order_acquire) == kAffinityJobs; }))
	    << "Timed out waiting for affinity jobs";

	// Every single affinity job must have run on the same worker.
	std::lock_guard lock(tids_mutex);
	ASSERT_EQ(inner_tids.size(), static_cast<std::size_t>(kAffinityJobs));
	for (std::size_t i = 0; i < inner_tids.size(); ++i)
	{
		EXPECT_EQ(inner_tids[i], outer_tid) << "Affinity job " << i << " ran on wrong worker — pinning invariant violated";
	}
}

// ---------------------------------------------------------------------------
// INVARIANT 3: NoLostWakeupsAcrossFallbackPaths
//
// Under high-volume mixed submission (Submit + SubmitIn), every job must
// complete exactly once. No wakeups may be lost across the fallback/dispatch
// paths. This stress-tests semaphore/futex correctness.
// ---------------------------------------------------------------------------
TEST(SchedulerInvariantTest, NoLostWakeupsAcrossFallbackPaths)
{
	CpuThreadPool scheduler(4);

	auto submit_counter = std::make_shared<std::atomic<int>>(0);
	auto affinity_counter = std::make_shared<std::atomic<int>>(0);
	constexpr int kNumTasks = 500;

	// Submit a mix of dispatched and affinity jobs.
	for (int i = 0; i < kNumTasks; ++i)
	{
		scheduler.Submit([submit_counter]() { submit_counter->fetch_add(1, std::memory_order_relaxed); });

		scheduler.Submit([&scheduler, affinity_counter]() {
			auto ctx = scheduler.CaptureContext();
			if (ctx.IsValid())
			{
				scheduler.SubmitIn(ctx, [affinity_counter]() { affinity_counter->fetch_add(1, std::memory_order_relaxed); });
			}
			else
			{
				// Fallback: context invalid (shouldn't happen from worker)
				affinity_counter->fetch_add(1, std::memory_order_relaxed);
			}
		});
	}

	ASSERT_TRUE(PollUntil(
	    [&] {
		    return submit_counter->load(std::memory_order_acquire) == kNumTasks
		        && affinity_counter->load(std::memory_order_acquire) == kNumTasks;
	    },
	    10s))
	    << "Lost wakeups: submit=" << submit_counter->load() << " affinity=" << affinity_counter->load() << " expected=" << kNumTasks;

	EXPECT_EQ(submit_counter->load(), kNumTasks);
	EXPECT_EQ(affinity_counter->load(), kNumTasks);
}

// ---------------------------------------------------------------------------
// INVARIANT 4: ZeroWakeupEmptySchedulerMainSubmit
//
// Submitting a single job from the main thread to an otherwise idle scheduler
// must complete without any prior wakeup activity. This is the zero-wakeup
// cold-start edge case.
// ---------------------------------------------------------------------------
TEST(SchedulerInvariantTest, ZeroWakeupEmptySchedulerMainSubmit)
{
	CpuThreadPool scheduler(4);

	std::atomic<bool> done{ false };

	// Single job, no other work in the system.
	scheduler.Submit([&done]() { done.store(true, std::memory_order_release); });

	ASSERT_TRUE(PollUntil([&] { return done.load(std::memory_order_acquire); }))
	    << "Single main-thread submit to idle scheduler never completed";

	EXPECT_TRUE(done.load());
}

// ---------------------------------------------------------------------------
// INVARIANT 5: PriorityTasksAllCompleteNoStarvation
//
// Submit tasks with mixed priorities to the priority-queue path. Every task
// must execute exactly once, and no priority level must starve. This
// characterizes the priority heap dispatch behavior before refactoring.
// ---------------------------------------------------------------------------
TEST(SchedulerInvariantTest, PriorityTasksAllCompleteNoStarvation)
{
	CpuThreadPool scheduler(2);

	auto counter = std::make_shared<std::atomic<int>>(0);
	constexpr int kNumTasks = 500;

	for (int i = 0; i < kNumTasks; ++i)
	{
		scheduler.SubmitWithPriority([counter]() { counter->fetch_add(1, std::memory_order_relaxed); }, static_cast<std::size_t>(i % 10));
	}

	ASSERT_TRUE(PollUntil([&] { return counter->load(std::memory_order_acquire) == kNumTasks; }, 10s))
	    << "Priority tasks not all completed: " << counter->load() << "/" << kNumTasks;
	EXPECT_EQ(counter->load(), kNumTasks);
}

// ---------------------------------------------------------------------------
// INVARIANT 6: SamePriorityTasksAllComplete
//
// Submit many tasks with identical priority. All must execute without loss
// or starvation, verifying FIFO-approximate behavior within the same priority
// bucket.
// ---------------------------------------------------------------------------
TEST(SchedulerInvariantTest, SamePriorityTasksAllComplete)
{
	CpuThreadPool scheduler(4);

	auto counter = std::make_shared<std::atomic<int>>(0);
	constexpr int kNumTasks = 1000;
	constexpr std::size_t kPriority = 42;

	for (int i = 0; i < kNumTasks; ++i)
	{
		scheduler.SubmitWithPriority([counter]() { counter->fetch_add(1, std::memory_order_relaxed); }, kPriority);
	}

	ASSERT_TRUE(PollUntil([&] { return counter->load(std::memory_order_acquire) == kNumTasks; }, 10s))
	    << "Same-priority tasks not all completed: " << counter->load() << "/" << kNumTasks;
	EXPECT_EQ(counter->load(), kNumTasks);
}

// ---------------------------------------------------------------------------
// INVARIANT 7: ConcurrentPrioritySubmissionPreservesProgress
//
// Multiple threads concurrently submitting tasks with mixed priorities must
// not drop any task. This stress-tests priority_mutex_ and priority_queue_
// under concurrent access.
// ---------------------------------------------------------------------------
TEST(SchedulerInvariantTest, ConcurrentPrioritySubmissionPreservesProgress)
{
	CpuThreadPool scheduler(4);

	auto counter = std::make_shared<std::atomic<int>>(0);
	constexpr int kThreads = 4;
	constexpr int kPerThread = 200;

	std::vector<std::thread> threads;
	for (int t = 0; t < kThreads; ++t)
	{
		threads.emplace_back([&scheduler, counter, t]() {
			for (int i = 0; i < kPerThread; ++i)
			{
				scheduler.SubmitWithPriority(
				    [counter]() { counter->fetch_add(1, std::memory_order_relaxed); }, static_cast<std::size_t>((i + t) % 10));
			}
		});
	}

	for (auto& th : threads)
		th.join();

	const int expected = kThreads * kPerThread;
	ASSERT_TRUE(PollUntil([&] { return counter->load(std::memory_order_acquire) == expected; }, 10s))
	    << "Concurrent priority submits lost tasks: " << counter->load() << "/" << expected;
	EXPECT_EQ(counter->load(), expected);
}

// ---------------------------------------------------------------------------
// INVARIANT 8: HighPriorityTasksProgressUnderConcurrentLoad
//
// Under sustained concurrent submission of mixed-priority tasks, high-priority
// tasks must eventually complete. This guards against unbounded starvation
// where low-priority tasks could consume all worker capacity.
// ---------------------------------------------------------------------------
TEST(SchedulerInvariantTest, HighPriorityTasksProgressUnderConcurrentLoad)
{
	CpuThreadPool scheduler(2);

	auto counter = std::make_shared<std::atomic<int>>(0);
	constexpr int kHighPriority = 100;
	constexpr int kLowPriority = 2000;
	constexpr std::size_t kHighPri = 1000;
	constexpr std::size_t kLowPri = 1;

	// Submit high-priority tasks first, then flood with low-priority.
	for (int i = 0; i < kHighPriority; ++i)
	{
		scheduler.SubmitWithPriority([counter]() { counter->fetch_add(1, std::memory_order_relaxed); }, kHighPri);
	}
	for (int i = 0; i < kLowPriority; ++i)
	{
		scheduler.SubmitWithPriority([counter]() { counter->fetch_add(1, std::memory_order_relaxed); }, kLowPri);
	}

	const int expected = kHighPriority + kLowPriority;
	ASSERT_TRUE(PollUntil([&] { return counter->load(std::memory_order_acquire) == expected; }, 15s))
	    << "Mixed high/low priority tasks not all completed: " << counter->load() << "/" << expected;
	EXPECT_EQ(counter->load(), expected);
}

TEST(SchedulerInvariantTest, BurstThenIdleThenSingleSubmitKeepsWaking)
{
	std::atomic<int> completed{ 0 };
	CpuThreadPool scheduler(4);
	int expected = 0;
	for (int round = 0; round < 40; ++round)
	{
		for (int i = 0; i < 256; ++i)
			scheduler.Submit([&] { completed.fetch_add(1, std::memory_order_release); });
		expected += 256;
		ASSERT_TRUE(PollUntil([&] { return completed.load(std::memory_order_acquire) == expected; }));
		// Exercise both active-to-idle races and already sleeping workers.
		if (round % 2 == 0)
			std::this_thread::sleep_for(1ms);
		scheduler.Submit([&] { completed.fetch_add(1, std::memory_order_release); });
		++expected;
		ASSERT_TRUE(PollUntil([&] { return completed.load(std::memory_order_acquire) == expected; }));
	}
}

TEST(SchedulerInvariantTest, ConcurrentSubmissionsAndChildrenExecuteEachJobOnce)
{
	constexpr int kProducers = 4;
	constexpr int kPerProducer = 1000;
	std::vector<std::atomic<int>> visits(kProducers * kPerProducer * 2);
	std::atomic<int> completed{ 0 };
	{
		CpuThreadPool scheduler(4);
		std::vector<std::jthread> producers;
		for (int p = 0; p < kProducers; ++p)
		{
			producers.emplace_back([&, p] {
				for (int i = 0; i < kPerProducer; ++i)
				{
					const int id = (p * kPerProducer + i) * 2;
					scheduler.Submit([&, id] {
						visits[id].fetch_add(1, std::memory_order_relaxed);
						scheduler.SubmitIn(scheduler.CaptureContext(), [&, id] {
							visits[id + 1].fetch_add(1, std::memory_order_relaxed);
							completed.fetch_add(1, std::memory_order_release);
						});
						completed.fetch_add(1, std::memory_order_release);
					});
				}
			});
		}
		for (auto& producer : producers)
			producer.join();
		EXPECT_TRUE(PollUntil([&] { return completed.load(std::memory_order_acquire) == static_cast<int>(visits.size()); }));
	}
	for (std::size_t id = 0; id < visits.size(); ++id)
		EXPECT_EQ(visits[id].load(), 1) << "job " << id;
}

TEST(SchedulerInvariantTest, PeerDrainsBusyOwnersBacklogIncludingSubmitIn)
{
	constexpr int kJobs = 4096;
	std::vector<std::atomic<int>> visits(kJobs);
	std::atomic<int> completed{ 0 };
	std::binary_semaphore queued(0), release_owner(0);
	{
		CpuThreadPool scheduler(2);
		scheduler.Submit([&] {
			const auto ctx = scheduler.CaptureContext();
			for (int i = 0; i < kJobs; ++i)
			{
				auto job = [&, id = std::make_unique<int>(i)] {
					visits[*id].fetch_add(1, std::memory_order_relaxed);
					completed.fetch_add(1, std::memory_order_release);
				};
				if (i % 2 == 0)
					scheduler.Submit(std::move(job));
				else
					scheduler.SubmitIn(ctx, std::move(job));
			}
			queued.release();
			release_owner.acquire();
		});
		const bool owner_queued = queued.try_acquire_for(5s);
		if (owner_queued)
		{
			// The next external submission goes to the other worker. The owner
			// cannot execute any child before the assertion below, so all must be stolen.
			scheduler.Submit([] { });
			EXPECT_TRUE(PollUntil([&] { return completed.load(std::memory_order_acquire) == kJobs; }));
		}
		// Release before asserting setup success so failures cannot strand the owner.
		release_owner.release();
		EXPECT_TRUE(owner_queued);
	}
	for (int id = 0; id < kJobs; ++id)
		EXPECT_EQ(visits[id].load(), 1) << "job " << id;
}

TEST(SchedulerInvariantTest, LocalFanoutWakesSleepingPeersWithoutExternalSubmit)
{
	constexpr int kJobs = 1024;
	std::vector<std::atomic<int>> visits(kJobs);
	std::atomic<int> completed{ 0 };
	std::binary_semaphore queued(0), release_owner(0);
	{
		CpuThreadPool scheduler(4);
		scheduler.Submit([&] {
			const auto ctx = scheduler.CaptureContext();
			for (int i = 0; i < kJobs; ++i)
			{
				auto child = [&, i] {
					visits[i].fetch_add(1, std::memory_order_relaxed);
					completed.fetch_add(1, std::memory_order_release);
				};
				if (i % 2 == 0)
					scheduler.Submit(std::move(child));
				else
					scheduler.SubmitIn(ctx, std::move(child));
			}
			queued.release();
			release_owner.acquire();
		});
		const bool owner_queued = queued.try_acquire_for(2s);
		const bool peers_drained = owner_queued && PollUntil([&] { return completed.load(std::memory_order_acquire) == kJobs; }, 2s);
		// Unblock the owner even on failure, so a broken wake protocol fails rather than hangs.
		release_owner.release();
		EXPECT_TRUE(owner_queued);
		EXPECT_TRUE(peers_drained) << "Local children remained queued while peers slept";
	}
	for (int i = 0; i < kJobs; ++i)
		EXPECT_EQ(visits[i].load(), 1) << "job " << i;
}

TEST(SchedulerInvariantTest, LocalFanoutActivatesEveryIdlePeer)
{
	constexpr int kJobs = 256;
	std::binary_semaphore release_owner(0);
	std::counting_semaphore<kJobs> started(0), release_children(0);
	CpuThreadPool scheduler(4);
	scheduler.Submit([&] {
		for (int i = 0; i < kJobs; ++i)
			scheduler.Submit([&] {
				started.release();
				release_children.acquire();
			});
		release_owner.acquire();
	});
	// Each child blocks its worker, so three starts require all three peers.
	int active_peers = 0;
	while (active_peers < 3 && started.try_acquire_for(2s))
		++active_peers;
	release_children.release(kJobs);
	release_owner.release();
	EXPECT_EQ(active_peers, 3);
}

TEST(SchedulerInvariantTest, LocalBurstsRaceWithPeerSleepWithoutLosingWork)
{
	constexpr int kJobs = 256;
	std::atomic<int> completed{ 0 };
	std::binary_semaphore release_owner(0), owner_done(0);
	CpuThreadPool scheduler(4);
	for (int round = 0; round < 40; ++round)
	{
		if (round % 2 == 0)
			std::this_thread::sleep_for(1ms);
		scheduler.Submit([&] {
			for (int i = 0; i < kJobs; ++i)
				scheduler.Submit([&] { completed.fetch_add(1, std::memory_order_release); });
			release_owner.acquire();
			owner_done.release();
		});
		const bool drained = PollUntil([&] { return completed.load(std::memory_order_acquire) == (round + 1) * kJobs; }, 2s);
		release_owner.release();
		const bool owner_returned = owner_done.try_acquire_for(2s);
		ASSERT_TRUE(drained) << "burst " << round;
		ASSERT_TRUE(owner_returned);
	}
}
