# Scheduler Optimization Review

Date: 2026-06-01

This note answers the current scheduler/routing concern directly: the old design had too many logical scheduler roles on the hot path. The recommended direction is to keep physical isolation only where it protects correctness or prevents blocking, and to collapse foreground DB work onto the shared CPU scheduler fast path.

## Executive Summary

The scheduler design is partially over-built. The useful parts are:

- `ThreadPoolScheduler`: keep as the shared CPU executor, timer source, affinity source, and work-stealing pool.
- `read_scheduler`: keep for file I/O calls that may block and for metadata operations.
- `serial_scheduler`: keep for ordered writable-file lifecycle operations.
- `compaction_scheduler`: keep only as a physical single-lane executor for background compaction/flush isolation.

The redundant or high-cost parts addressed by the current cleanup are:

- Foreground `AsyncDB::{Get,Put,Write,Delete}` should not route through a blocking/read lane. These operations are CPU-resident DB operations from the async API perspective and should use the shared CPU pool.
- `BlockingScheduler()` / `ContinuationScheduler()` were no longer justified. `AsyncOp` now submits to the scheduler it receives and resumes inline on the completing worker, so the split-routing hooks were removed from `IScheduler`.
- The priority dispatcher should not sit on the normal priority-0 foreground path. It is useful for non-zero priority/background fallback and delayed tasks, but it is too expensive as the default submit path.
- `AsyncEnvBackendMode::kThreadPool` and `kBlockingLane` resolved to the same `read_scheduler`; the runtime enum was removed and `AsyncEnv` now has one explicit file-I/O policy.

## Current Target Topology

Use this as the intended design:

```text
AsyncDB foreground operations
  -> RuntimeBundle::foreground_db_scheduler
  -> ThreadPoolExecutor
  -> ThreadPoolScheduler::Submit(priority = 0)
  -> worker-local queue + stealing
  -> coroutine resumes inline on completing worker

AsyncEnv file and metadata operations that may block
  -> RuntimeBundle::read_scheduler
  -> BlockingExecutor(4)
  -> coroutine resumes inline on completing read worker

Writable file ordering
  -> RuntimeBundle::serial_scheduler
  -> SerialLane(1)

Background compaction/flush isolation
  -> RuntimeBundle::compaction_scheduler
  -> BlockingExecutor(1)
```

This topology gives the project four physical execution sources:

- Shared CPU pool: `ThreadPoolScheduler`
- Blocking read/file lane: `BlockingExecutor(4)`
- Compaction lane: `BlockingExecutor(1)`
- Ordered write-control lane: `SerialLane(1)`

That is a reasonable amount of machinery. The complexity becomes excessive only when every logical role is treated as a separate scheduler hop on foreground operations.

## Optimizations Already Applied

The current worktree contains these aligned changes:

- `AsyncDB::{OpenAsync,GetAsync,PutAsync,DeleteAsync,WriteAsync}` route through `foreground_db_scheduler`, so foreground DB work uses the shared CPU pool instead of the read lane.
- `ThreadPoolExecutor::Submit()` submits with priority `0`, enabling the direct worker-local fast path.
- `ThreadPoolScheduler::Submit(priority=0)` routes directly to worker-local queues:
  - worker self-submit goes back to the same worker;
  - external submit uses a round-robin worker choice;
  - stealing remains available to balance load.
- `AsyncOp<T>` and `AsyncOp<void>` resume the awaiting coroutine inline on the worker that completed the operation, avoiding a second queue hop through `ContinuationScheduler()`.
- `AsyncOp` state no longer uses a virtual `StateBase::Execute()/ConsumeValue()` dispatch layer.
- `IScheduler` is now submit-only. This makes the scheduling semantic explicit: a scheduler is an execution target, not a routing policy object.
- `ExecutorSchedulerAdapter` now only wraps an executor. It no longer stores optional blocking/continuation scheduler pointers.
- `AsyncEnvBackendMode` was removed from `RuntimeBundle`; `AsyncEnv` file and metadata operations always route to `read_scheduler`.
- `kv_bench --no_latency` now skips per-operation `NowNs()` calls, which makes perf sampling less dominated by measurement overhead.

## Evidence Collected

Build and correctness checks:

- `xmake f -m releasedbg`: passed
- `xmake build async_op_test`: passed
- `xmake build runtime_lane_test`: passed
- `xmake build async_env_test`: passed
- `xmake build structured_task_test`: passed
- `xmake build executor_microbench`: passed
- `xmake build async_bench`: passed
- `xmake build kv_bench`: passed
- `xmake run async_op_test`: 11 tests passed
- `xmake run runtime_lane_test`: 7 tests passed
- `xmake run async_env_test`: 11 tests passed
- `xmake run structured_task_test`: 8 tests passed

Lightweight performance checks on this machine:

```text
executor_microbench --workers=4 --tasks=200000
  delay_avg=1.9 us
  delay_max=241 us
  throughput=0.993 tasks/us

kv_bench --run=async --bench=mixed --clients=8 --workers=8 --ops=20000
         --value_size=8 --read_ratio=100 --rounds=2 --warmup_rounds=1
         --inflight_per_client=16 --no_latency --prefill=1
  round 1: 4.28M ops/s
  round 2: 4.19M ops/s

releasedbg perf-record run:
  kv_bench same workload with --ops=100000 --no_latency
  4.96M ops/s
  perf captured 1934 samples, no lost samples

steady-state read run with a prefilled DB, excluding prefill:
  before AsyncOp/iterator micro-optimizations:
    4.75M ops/s, 2827 samples
  after AsyncOp unique-state and block-key reserve:
    5.04M ops/s, 2699 samples
```

`releasedbg` emits a stripped runnable binary plus a `.sym` file with debug information. With that symbol file available, `perf report` resolves the main project hotspots:

- `SkipList<...>::Insert`: about 7.35 percent self
- `InternalKeyComparator::Compare`: about 7.35 percent self
- `Block::Iter::Seek`: about 6.19 percent self
- `pthread_mutex_lock`: about 5.08 percent self, visible under worker queue/cache locking
- `RunAsyncMixedClient` coroutine actor: about 4.06 percent self, mostly benchmark inflight atomics
- `GetVarint32Ptr`: about 4.04 percent self
- `DBImpl::Get`: about 3.28 percent self
- `AsyncDB::GetAsync`: about 2.33 percent self, mostly `shared_ptr` reference-counting
- `std::move_only_function` lambda manager for `AsyncDB::GetAsync`: about 1.80 percent self, mostly `shared_ptr` release/destruction

The sampled evidence supports the current direction: after removing the read-lane and dispatcher hop from foreground operations, the next wins are DB-format/comparator work, benchmark measurement overhead, small-object allocation/type-erasure, `shared_ptr` reference counting, and queue locking rather than another scheduler lane.

## Additional Hot-Path Optimizations Applied

### AsyncOp state ownership

`AsyncOp` state is now owned by the awaiter with `std::unique_ptr`. The scheduler job captures a raw `State*` instead of another `std::shared_ptr<State>`.

Why this is safe:

- The awaiter lives in the awaiting coroutine frame until `await_resume()`.
- The worker never touches `State` after calling `handle.resume()`.
- The existing precondition remains: the awaiting coroutine must remain alive until resumption.

Why it helps:

- Removes an atomic `shared_ptr` retain/release pair from every submitted async operation.
- Removes shared-state destruction work from the scheduler lambda.
- In steady-state read perf, `AsyncDB::GetAsync` dropped from about 4.09 percent self in the initial prefilled read sample to about 2.62 percent self after the change. The `std::move_only_function` manager path also dropped from about 3.28 percent to about 2.37 percent.

### Internal key comparator

`InternalKeyComparator::Compare` now computes the user-key slices once and reuses them for bytewise and custom-comparator paths.

This is intentionally small: the comparator is hot, but its semantics are delicate because internal-key order is user-key ascending and sequence/type descending.

### Block iterator key buffer

`Block::Iter` now reserves a small key buffer up front and reconstructs decoded keys with one `resize()` plus `memcpy()` instead of `resize()` plus `append()`. This reduces first-append allocation and trims string mutation overhead in `ParseNextKey()` for the common short-key path.

In the initial steady-state read sample, `malloc` showed up through `ParseNextKey()` / `std::string::_M_append`. After reserving the key buffer, malloc fell out of the top visible hotspots for this workload.

### AsyncDB operation lifetime

`AsyncDB::SharedState` now uses an internal intrusive refcount instead of capturing a `std::shared_ptr` in every operation lambda. `AsyncOp::State` has a small keep-alive slot (`void*` plus release callback), so the submitted lambda can capture only the raw DB state pointer while the operation state owns the lifetime reference.

This keeps the public safety property unchanged: destroying an `AsyncDB` handle while an operation is outstanding does not invalidate the operation. `AsyncDBTest.DestroyBeforeAwait` covers this path.

### DBImpl point-read view acquisition

`DBImpl::Get()` no longer increments and decrements `SuperVersion::refs_` for every point read. The published and retired `SuperVersion` objects are already kept alive until DB teardown, so point reads can use the acquired pointer directly. Long-lived iterators still take an explicit ref because they can outlive a single call.

### Cache shard contention

The LRU block cache now uses 64 shards instead of 16, and each shard is cache-line aligned. This reduces cache mutex contention and false sharing in multi-worker read-heavy runs without changing cache API semantics.

### Benchmark inflight accounting

For `--no_latency` runs, `kv_bench` now reports the configured outstanding window directly instead of updating global/client inflight atomics on every operation. Latency runs still collect observed max inflight. This makes hotspot sampling reflect DB/runtime work rather than benchmark instrumentation.

### Latest steady-state read sample

Using the prefilled DB at `/tmp/prism_perf_read_db_20260602`:

```text
kv_bench --run=async --bench=mixed --clients=8 --workers=8 --ops=200000 \
  --value_size=8 --read_ratio=100 --rounds=1 --warmup_rounds=0 \
  --inflight_per_client=16 --no_latency --prefill=0 \
  --db_dir=/tmp/prism_perf_read_db_20260602 --keep_db=1
```

Latest non-perf run: about 7.22M ops/s. Under `perf record`, the same run measured about 6.98M ops/s. The earlier comparable steady-state run after the first AsyncOp/comparator/block changes was about 5.04M ops/s.

Top remaining self-costs in `/tmp/prism-kv-bench-read-steady-final2.perf.data`:

- `pthread_mutex_lock`: about 7.95 percent, now split across cache lookup/release and scheduler queue operations.
- `AsyncDB::GetAsync`: about 7.88 percent, mostly `ReadOptions`/key capture and callable setup.
- `Block::Iter::Seek`: about 7.43 percent.
- `InternalKeyComparator::Compare`: about 5.63 percent.
- `AsyncDB` keep-alive release callback: about 4.45 percent.
- `Block::Iter::ParseNextKey`: about 4.44 percent.
- `DBImpl::Get`: about 2.23 percent.

### Follow-up mutex/callable cleanup sample

Follow-up changes applied on 2026-06-02:

- Removed the now-unused scheduler idle registry (`pending_list_` / `pending_mutex_`). Priority and lazy work already route directly to worker queues, so completed dispatched jobs no longer need to re-register with a separate idle-worker list.
- Changed cache `NewId()` from a dedicated mutex to a relaxed atomic counter. Cache lookups/releases remain protected by shard-local LRU mutexes.
- Specialized `Block::Iter` bytewise seek scanning so linear seek compares the decoded prefix-compressed key against the target while parsing, before the reconstructed key is used for the next entry.
- Changed `InternalKeyComparator::Compare` bytewise user-key comparison to a direct `memcmp` over the user-key portions.
- Changed `AsyncDB::GetAsync` to take `ReadOptions` by value and added a smaller default-read-options callable path; snapshot and non-default options still capture the full options object.

Validation after these changes:

```text
xmake build
xmake run asyncdb_test
xmake run scheduler_stress_test
xmake run scheduler_shutdown_test
xmake run scheduler_context_test
xmake run scheduler_pending_regression_test
xmake run table_test
xmake run block_test
xmake run dbformat_test
```

All passed.

Performance sample using `/tmp/prism_perf_read_db_after_20260602`:

```text
kv_bench --run=async --bench=mixed --clients=8 --workers=8 --ops=200000 \
  --value_size=8 --read_ratio=100 --rounds=3 --warmup_rounds=0 \
  --inflight_per_client=16 --no_latency --prefill=0 \
  --db_dir=/tmp/prism_perf_read_db_after_20260602 --keep_db=1
```

Observed steady-state read results:

```text
first 200k-op run:  6.61M, 6.89M, 7.03M ops/s; average 6.84M ops/s
repeat 200k-op run: 6.97M, 7.11M, 7.01M ops/s; average 7.03M ops/s
```

Longer sustained run:

```text
kv_bench --run=async --bench=mixed --clients=8 --workers=8 --ops=1000000 \
  --value_size=8 --read_ratio=100 --rounds=3 --warmup_rounds=0 \
  --inflight_per_client=16 --no_latency --prefill=0 \
  --db_dir=/tmp/prism_perf_read_db_after_20260602 --keep_db=1

1M-op rounds: 5.89M, 5.93M, 5.87M ops/s; average 5.89M ops/s
```

Interpretation: short steady-state runs are still around the previously observed 7M ops/s band, while longer 1M-op rounds on this machine settled closer to 5.9M ops/s. Treat these as local throughput samples, not a strict before/after result, because the earlier 7.22M note used a shorter non-perf run and machine load/frequency state visibly affects this workload.

### Follow-up inline job storage

Implemented after the mutex/callable cleanup:

- Added `InlineJob`, a move-only `void()` job wrapper with 128-byte inline storage and heap fallback for larger or over-aligned callables.
- Changed `IScheduler::Job`, `IContinuationExecutor` queues, `BlockingExecutor`, `SerialLane`, and `ThreadPoolExecutor` to use `InlineJob` instead of `std::move_only_function<void()>`.
- Added C++20 `requires`-constrained `ThreadPoolScheduler::Submit(F&&)`, `SubmitAfter(F&&)`, and `SubmitIn(F&&)` overloads. Concrete scheduler call sites now construct the callable directly inside the target queue entry where possible.
- Added a direct `ThreadPoolScheduler` path in `AsyncOp` and routed `AsyncDB` foreground operations through `RuntimeBundle::timer_source`, bypassing the foreground adapter/`IContinuationExecutor` hop for the hot DB path.

Validation:

```text
xmake test
```

Result: 40/40 tests passed.

Quick throughput check, same command and DB as above:

```text
200k-op run after InlineJob: 6.72M, 7.07M, 7.11M ops/s; average 6.96M ops/s
```

Repeat verification on the current tree:

```text
kv_bench --run=async --bench=mixed --clients=8 --workers=8 --ops=200000 \
  --value_size=8 --read_ratio=100 --rounds=3 --warmup_rounds=0 \
  --inflight_per_client=16 --no_latency --prefill=0 \
  --db_dir=/tmp/prism_perf_read_db_inline_verify_20260602 --keep_db=1

200k-op rounds: 9.92M, 10.05M, 9.97M ops/s; average 9.98M ops/s
```

## Recommended Design Decisions

### 1. Treat `ThreadPoolScheduler` as the only foreground scheduler

Do not add more foreground DB schedulers. `foreground_db_scheduler` can stay as a named adapter for clarity, but it should always map to `ThreadPoolScheduler::Submit(priority=0)`.

Rationale:

- Foreground DB work is latency-sensitive and high-frequency.
- A logical lane that only forwards to another executor adds no isolation.
- The worker-local fast path plus stealing is enough for load balance.

### 2. Keep the read lane only for operations that may block in the OS

Keep `read_scheduler` for:

- `Env` metadata calls
- fallback file reads when the reactor cannot handle the file
- random-access file construction/open calls

Do not use it for `AsyncDB::GetAsync()` itself. `GetAsync()` may internally touch tables/cache, but the async API's unit of scheduling is DB work, not raw file I/O.

### 3. Keep `serial_scheduler` for ordering, not performance

The serial lane should be considered a correctness lane. It protects file lifecycle ordering such as append/flush/sync/close. It should not be generalized into a foreground routing path.

### 4. Keep `compaction_scheduler` as a physical lane, but avoid logical scheduler expansion

The single compaction executor is justified because compaction is background, blocking, and should remain single-flight. Avoid adding per-level or per-purpose compaction schedulers until there is evidence that compaction parallelism is a real bottleneck.

### 5. Keep `IScheduler` submit-only

Current `AsyncOp` resumes inline after the work function completes. That makes explicit continuation-routing hooks unnecessary for production AsyncOp routing.

Implemented migration:

- Removed `BlockingScheduler()` and `ContinuationScheduler()` from `IScheduler`.
- Removed split-route state from `ExecutorSchedulerAdapter`.
- Updated tests to assert physical lane behavior instead of self-routing hook identity.

### 6. Collapse `AsyncEnvBackendMode`

`kThreadPool`, `kBlockingLane`, and `kDefault` selected the same read lane. That was confusing API surface.

Implemented migration:

- Removed the runtime enum and `RuntimeBundle::async_env_backend`.
- Kept `async_bench --backend=thread_pool|blocking_lane` as a compatibility label only; both names now exercise the same fixed AsyncEnv read-lane policy.

## Code-Level Optimization Opportunities

### P0: Continue foreground operation-state allocation work

Scheduler-side void job type-erasure has been addressed by `InlineJob` plus templated `ThreadPoolScheduler::Submit(F&&)`. `AsyncOp` still allocates shared state per operation and stores the result-producing work in `std::move_only_function<T()>`, so there is still per-operation setup cost above the scheduler queue layer.

Remaining possible next step:

- Keep `AsyncOp<T>` as the public type.
- Internally add a small-object optimized state path or a custom operation state pool for hot DB operations.
- Consider a concrete hot `AsyncDB::GetAsync` operation state so the DB read callable itself does not need to be stored in `std::move_only_function<T()>`.
- Measure with `kv_bench --no_latency` before and after.

Risk: medium. Lifetime and resume-before-suspend safety must remain exactly as tested in `async_op_test`.

Implemented scheduler-side direction:

- `IScheduler::Job` is now `InlineJob`, with inline storage and static invoke/move/destroy thunks.
- Concrete `ThreadPoolScheduler` producers can call `Submit(F&&)` / `SubmitAfter(F&&)` / `SubmitIn(F&&)` and construct jobs directly into queue storage.
- `AsyncDB` foreground operations now use the concrete scheduler path instead of the foreground scheduler adapter.

### P0: Specialize hot `AsyncDB::GetAsync` capture/setup

The `shared_ptr` cost is gone, default `ReadOptions` now use a smaller callable capture, and foreground `AsyncDB` operations bypass the scheduler adapter. `AsyncDB::GetAsync` can still spend visible time setting up the result-producing work lambda and copying/moving key state.

Possible next step:

- Add a dedicated point-read operation state that stores `ReadOptions` and key inline instead of inside `std::move_only_function`.
- Keep the generic `AsyncOp<T>` path for less frequent operations.
- Store the common no-snapshot read flags directly in a concrete op state if the next perf sample still shows `GetAsync` setup cost.
- Measure with the same prefilled `--no_latency` command.

Risk: medium. This overlaps with the foreground submit allocation work and must preserve the destroy-before-await lifetime guarantee.

Design direction to revisit:

- Consider a sender/receiver-style internal operation model for hot DB calls: operation state is concrete, start/resume is explicit, and completion stores directly into the awaiting state.
- Do not switch the public API to sender/receiver yet. Use it internally only if it removes callable setup and makes lifetime clearer than the generic `AsyncOp` path.
- Keep coroutine-facing `co_await db.GetAsync(...)` behavior unchanged.

ABI/register-argument ring-buffer idea:

- Do not make this a generic `ThreadPoolScheduler` API first. The current generic job call already passes the inline job storage pointer as a register argument (`invoke_(ptr_)`), while the captured callable state still lives in memory. A generic ABI-shaped job path would add complexity without clearly attacking the largest remaining costs.
- Revisit it only as part of a concrete hot `AsyncDB::GetAsync` operation state. A fixed point-read job can store stable ownership in the operation state, then call a normal function such as `RunGetJob(op, db_state, key_data, key_size, flags)`, allowing the small scalar fields to use ABI register passing at the worker call site.
- If a ring buffer is introduced, its primary justification should be reducing priority-0 queue mutex/object movement on a measured hot path. Register argument passing is a secondary benefit, not the main design reason.
- Key lifetime is the main correctness constraint: any queued `key_data/key_size` pair must point into the concrete operation state or another stable owner, never into a temporary caller string.

### P1: Keep hotspot runs on `--no_latency`

`kv_bench` accepts `--no_latency` as a flag, not `--no_latency=1`. The parser now rejects unknown arguments so mistyped profiling flags fail loudly instead of silently recording latency vectors.

Possible next step:

- Add a test that `--no_latency` produces no p50/p95 fields and does not populate latency vectors.

Risk: low.

### P1: Reduce queue mutex pressure in `ThreadPoolScheduler`

`perf` shows `pthread_mutex_lock` in the top visible costs. Each worker queue uses a mutex-protected deque.

Possible next steps:

- Keep the current mutex queue until a repeatable benchmark proves it is limiting throughput.
- If needed, replace only the priority-0 worker-local queue with an MPSC queue for external submits plus a local deque for worker-owned tasks.
- Keep the priority/lazy queues unchanged; they are not the foreground hot path anymore.

Risk: high. Queue rewrites are easy to get wrong around shutdown, stealing, and affinity.

Current hotspot to revisit:

- Separate cache mutex from scheduler mutex in perf reports before changing queue code.
- For scheduler mutex: measure worker-local submit, external submit, stealing, and wakeup paths independently.
- For cache mutex: measure whether 64 shards are enough under higher client counts before considering LRU hit-path policy changes.

### P1: Continue DB key/iterator/comparator hot-path work

Remaining visible costs:

- `Block::Iter::Seek`: binary restart search plus linear scan still repeatedly decodes entries and reconstructs prefix-compressed keys.
- `Block::Iter::ParseNextKey`: decode and key reconstruction remain visible after the `resize()+memcpy()` change.
- `InternalKeyComparator::Compare`: user-key compare and sequence/type ordering are still hot in table seek and file search.

Possible next steps:

- Prototype a block-entry cursor that can compare a target against the current prefix-compressed key pieces before fully materializing skipped keys.
- Keep full key reconstruction only for the entry the iterator lands on, if prefix dependencies can be preserved safely.
- Add block seek edge-case tests around restart boundaries, shared-prefix keys, and seek-past-last before landing any more invasive change.
- For `InternalKeyComparator`, keep bytewise fast paths explicit and avoid repeated `ExtractUserKey`/trailer decoding in nested search paths.

Risk: medium. Prefix-compressed block semantics are easy to subtly break.

### P1: Remove `pending_list_` from foreground reasoning

`pending_list_` is now only useful for dispatcher/background semantics. The docs and comments should stop describing it as part of normal foreground dispatch.

Possible next step:

- Rename comments to "dispatcher idle registry".
- Audit whether `TryReserveIdleWorker()` and `ReturnReservedIdleWorker()` are still needed. If they are unused after the fast-path change, remove them.

### P2: Make benchmark output less visible to perf

`snprintf` appears in perf samples. Some of that comes from benchmark key creation/output/setup, not steady-state work.

Possible next steps:

- Use prefilled DB directories and `--prefill=0` for long profiling runs.
- Keep `--no_latency` for CPU hotspot sampling.
- For micro-hotspot runs, add a phase that excludes output/key-generation from the sampled window.

### P2: Clean up compatibility names

Names like `timer_source` still imply implementation details that are no longer central to the hot path.

Possible next steps:

- Rename `timer_source` to `scheduler`.
- Keep `foreground_db_scheduler` only if it improves call-site readability.

## Suggested Follow-Up Plan

1. Land the current fast-path and inline-resume changes with the scheduler/runtime tests above.
2. Add a CI benchmark smoke command using `kv_bench --no_latency` to catch large regressions.
3. Rename `timer_source` if a broader runtime naming cleanup is needed.
4. Only after stable perf data, consider a more invasive queue or AsyncOp allocation optimization.

The important simplification is conceptual: there should be one foreground scheduler path and a small number of physical isolation lanes. Avoid creating new logical schedulers unless they either protect ordering/correctness or isolate real blocking work.
