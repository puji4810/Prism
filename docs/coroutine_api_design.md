# Coroutine and Async API Design

This document describes Prism's structured concurrency runtime, coroutine integration, and execution model. Prism uses a lane-isolated executor architecture where async DB work is routed to dedicated blocking lanes, with foreground operations using the read lane and background maintenance using a separate compaction lane.

## 1. Architectural Layers

```
Layer 1: TaskScope — structured concurrency, stop propagation, join, quarantine
Layer 2: RuntimeBundle — executor dispatch
  |-- CPU Executor (ThreadPoolScheduler — general CPU dispatch)
  |-- Read Executor (BlockingExecutor — foreground async DB/file work)
  |-- Compaction Executor (BlockingExecutor — background compaction/flush)
  |-- Serial Lane (SerialLane — FIFO ordered writable files)
Layer 3: AsyncOp — awaitable bridge, suspend/resume handshake
Layer 4: AsyncEnv / AsyncDB — public async API surface
```

### 1.1 Overall Four-Layer Architecture

```mermaid
graph TD
    subgraph L4["Layer 4 — Public API"]
        ADB[AsyncDB]
        AEnv[AsyncEnv]
        AF[AsyncFile]
    end
    subgraph L3["Layer 3 — Awaitable Bridge"]
        AOp["AsyncOp<T>"]
    end
    subgraph L2["Layer 2 — Runtime Bundle"]
        CPU["cpu_executor"]
        READ["read_executor"]
        COMP["compaction_executor"]
        SER["serial_lane"]
    end
    subgraph L1["Layer 1 — Structured Concurrency"]
        SCOPE["TaskScope"]
        STOP["StopSource / StopToken"]
        Q["Quarantine"]
    end

    ADB --> AOp
    AEnv --> AOp
    AF --> AOp
    AOp --> READ
    AOp --> COMP
    AOp --> SER
    SCOPE --> STOP
    STOP --> AOp
    STOP --> Q
```

### 1.2 AsyncOp: Lane-Based Suspend/Resume Handshake

```mermaid
sequenceDiagram
    participant UC as User Coroutine
    participant AOp as AsyncOp
    participant READ as read_executor

    UC->>AOp: co_await
    AOp->>READ: BlockingScheduler()->Submit(work)
    activate READ
    READ->>READ: st->work()  (foreground async work)
    READ-->>AOp: result ready
    READ->>READ: CAS suspend/resume handshake
    READ->>UC: handle.resume() inline
    deactivate READ
    UC->>UC: continue after co_await
```

**Current rule**: `include/async_op.h` submits only the blocking work through `BlockingScheduler()`. When that work completes, the worker runs the same three-state suspend/resume handshake inline and calls `handle.resume()` directly if the coroutine is already suspended. This keeps async DB work on the read lane without paying a second queue/mutex hop for every continuation. `AcquireRuntimeBundle()` protects shutdown by deferring `RuntimeBundle` deletion to a small cleanup thread if the last reference is released from a bundle-owned worker.

### 1.3 Executor Routing: Who Runs What

```mermaid
graph LR
    subgraph Work["Incoming Work"]
        COMP["Compaction"]
        IO["Blocking I/O"]
        DB["DB Put/Get"]
        WRITE["Ordered File Write"]
    end

    subgraph Execs["Executors"]
        READ["read_executor (4 thr)"]
        COMP["compaction_executor (1 thr)"]
        CPU["cpu_executor (N thr)"]
        SER["serial_lane (1 thr)"]
    end

    COMP --> COMP
    IO --> READ
    DB --> READ
    WRITE --> SER

    READ -->|"inline AsyncOp resume"| READ
    SER -->|"Continuation"| CPU
    CPU -->|"general dispatch"| CPU
```

- **read_executor**: foreground async DB operations and blocking file reads
- **compaction_executor**: background compaction/flush work with single-flight control
- **serial_lane**: FIFO-ordered writable file appends/flushes/closes
- **cpu_executor**: shared `ThreadPoolScheduler` for general CPU dispatch and `AsyncOp` resume continuations

### 1.4 Compaction: Structured Lifetime & Cancellation

```mermaid
stateDiagram-v2
    [*] --> Idle: DB open
    Idle --> Submitted: ScheduleIfNeeded()
    Submitted --> Running: TrySubmitLocked()
    Running --> Checkpoint: stop_token.CheckStop()?
    Checkpoint --> Aborted: before side-effects
    Checkpoint --> Cleanup: after output written
    Running --> Done: work completes normally
    Aborted --> Idle: OnWorkFinished()
    Cleanup --> Idle: install results, no reschedule
    Done --> Idle: OnWorkFinished() may reschedule

    state Aborted {
        [*] --> ReleaseInputs
        ReleaseInputs --> WakeWriters
        WakeWriters --> [*]
    }

    state Cleanup {
        [*] --> InstallResults
        InstallResults --> WakeWriters
        WakeWriters --> [*]
    }
```

**Checkpoint semantics** (phased best-effort):

1. **Pre-start**: stop before picking compaction → no side effects, bail out immediately
2. **Pre-commit**: stop before `LogAndApply()` (manifest install) → release inputs, wake writers
3. **Post-commit**: outputs already on disk → install manifest edits (cleanup), but suppress follow-up scheduling

## 2. Runtime Bundle and Execution Model

The `RuntimeBundle` is the central runtime container, shared via `std::shared_ptr` across runtime components. It groups executors by their scheduling semantics:


| Executor            | Type                       | Threads         | Schedules                                                                    |
| ------------------- | -------------------------- | --------------- | ---------------------------------------------------------------------------- |
| `read_executor`     | `BlockingExecutor`         | 4               | Foreground async DB work and blocking reads                                  |
| `compaction_executor` | `BlockingExecutor`       | 1               | Background compaction/flush single-flight work                               |
| `cpu_executor`      | `ThreadPoolExecutor`       | N (shared pool) | General CPU dispatch                                                         |
| `serial_lane`       | `SerialLane`               | 1               | FIFO-ordered file writes                                                     |
| `runtime_scheduler` | `ExecutorSchedulerAdapter` | routing only    | Delegates blocking work → read lane; `AsyncOp` resumes inline after completion |

### Why Separate Lanes?

Without separation, a long-running compaction would occupy the same execution lane as foreground reads. The split ensures:

- **Foreground work never waits behind compaction** — reads use their own lane
- **Compaction never blocks foreground progress** — compaction stays isolated on its own lane
- **Single-flight compaction is enforced by `CompactionController`**, not by thread pool availability

### The `runtime_scheduler` Router

Every `AsyncOp` receives an `IScheduler*`. At `await_suspend()`:

1. `scheduler->BlockingScheduler()` → `read_executor` — where `st->work()` executes
2. The completing worker runs the CAS handshake inline and calls `st->handle.resume()` directly when the coroutine has already suspended

This keeps the read-heavy hot path on the lightweight read lane while avoiding a second `BlockingExecutor::Submit()` for every operation. If coroutine teardown releases the final `RuntimeBundle` reference on that lane, the custom `AcquireRuntimeBundle()` deleter transfers actual destruction to an external cleanup thread, so the read executor never joins itself.

## 3. Core Abstraction: `AsyncOp<T>`

The `AsyncOp<T>` class is the heart of Prism's async design. It is a C++20 awaitable that bridges synchronous work with asynchronous execution.

### Task-Agnostic Design

Unlike many async frameworks, Prism does not force a specific `Task` type on the user. Instead, Prism functions return `AsyncOp<T>`, which can be `co_await`ed in any coroutine that supports the standard C++20 coroutine protocol. This approach has two major benefits:

1. **Zero-Frame Overhead**: `AsyncOp` is not a coroutine function itself (it contains no `co_await` or `co_return`), so it does not create a coroutine frame when called. A frame is only created if the user decides to `co_await` it.
2. **Flexibility**: Users can integrate Prism into their existing cororuntimeutine systems (e.g., `asio::awaitable`, `boost::cobalt`, or custom task types).

### The Suspend/Resume Handshake

To prevent a common race condition where a background task completes before the calling coroutine has finished suspending (which would lead to Undefined Behavior), `AsyncOp` implements a three-state atomic handshake.

```mermaid
stateDiagram-v2
    [*] --> kSuspending: co_await starts
    kSuspending --> kSuspended: Coroutine wins (await_suspend returns true)
    kSuspending --> kCompleted: Worker wins (await_suspend returns false)
    kSuspended --> [*]: Worker resumes coroutine
    kCompleted --> [*]: Coroutine continues immediately
```

1. **kSuspending**: The initial state when `await_suspend` is entered.
2. **kSuspended**: The coroutine has successfully suspended. The worker thread is now responsible for calling `resume()`.
3. **kCompleted**: The worker thread finished the task before the coroutine could suspend. In this case, `await_suspend` returns `false`, and the coroutine continues execution on the current thread without actual suspension.

## 4. Async Environment and Files (`AsyncEnv`)

`AsyncEnv` provides async wrappers for filesystem operations. Since coroutines often involve complex lifetime requirements, the async file APIs differ slightly from their synchronous counterparts.

### Buffer Lifetime Rules

In the synchronous `Env` API, `Read` often uses a `Slice*` and a `scratch` buffer. This is dangerous in coroutines because the `scratch` buffer (often on the stack) might be destroyed while the coroutine is suspended.

Prism provides two solutions in `AsyncRandomAccessFile`:

1. **`ReadAtAsync(uint64_t off, std::span<std::byte> dst)`**: A zero-copy API where the caller provides the buffer. The caller **must** ensure the buffer remains valid until the `co_await` completes.
2. **`ReadAtStringAsync(uint64_t off, size_t n)`**: A convenience API that returns an owning `std::string`. This is safer but involves a heap allocation and a copy.

### FIFO Serialization in `AsyncWritableFile`

Database writes (appends to logs or SSTables) must be strictly ordered. `AsyncWritableFile` serializes operations through the `SerialLane` executor in `RuntimeBundle`, which provides FIFO-ordered execution on a single dedicated thread. This replaces the earlier ticket/CV scheme, eliminating per-operation worker thread blocking.

## 5. Async Database (`AsyncDB`)

`AsyncDB` is a high-level wrapper around the synchronous `Database` engine. It provides a move-only handle that internally manages shared state for safe async access.

### Lifecycle and Usage

- **Opening**: Use `AsyncDB::OpenAsync(scheduler, options, dbname)` which returns an `AsyncOp<Result<AsyncDB>>`.
- **Snapshots**: Call `db.CaptureSnapshot()` to obtain a synchronous RAII `Snapshot` handle. This handle is cheap to copy and can be passed into `ReadOptions` for async reads.
- **Operations**: `GetAsync`, `PutAsync`, `DeleteAsync`, and `WriteAsync` are the primary entry points.

### Implementation (Offload Model)

- **Runtime dispatch**: Every call is packaged into a lambda and submitted through the `runtime_scheduler`. Foreground async DB work and the follow-up resume path both run through the read lane today.
- **Simplicity**: This allows for a clean async API without requiring a massive refactor of the core `DBImpl` engine.
- **Snapshot Safety**: Since SSTables are immutable and MemTables use sequence-number-based MVCC, a `Snapshot` obtained from the sync engine is safe to use across execution boundaries.

## 6. Migration Guide

Prism now documents only the final by-value handle model: `Database` for sync work, `AsyncDB` for coroutine work, and cheap-copy RAII `Snapshot` values routed through the `snapshot_handle` field on `ReadOptions`.

### Before

```cpp
// Older style: manual-lifetime sync handle plus a separately managed snapshot token.
auto legacy_handle = OpenLegacySyncHandle(options, name);
auto legacy_snapshot = AcquireLegacySnapshot(legacy_handle);

LegacyReadOptions read_options;
read_options.snapshot_handle = legacy_snapshot;
auto value = LegacyRead(legacy_handle, read_options, "k");
ReleaseLegacySnapshot(legacy_handle, legacy_snapshot);
```

### After (sync)

```cpp
auto db_result = prism::Database::Open(options, name);
if (!db_result.has_value()) {
    co_return db_result.error();
}

auto db = std::move(db_result.value());
prism::Snapshot snapshot = db.CaptureSnapshot();

prism::ReadOptions read_options;
read_options.snapshot_handle = snapshot;
auto value = db.Get(read_options, "k");
```

### After (async)

```cpp
auto db_result = co_await prism::AsyncDB::OpenAsync(scheduler, options, name);
if (!db_result.has_value()) {
    co_return db_result.error();
}

auto db = std::move(db_result.value());
prism::Snapshot snapshot = db.CaptureSnapshot();

prism::ReadOptions read_options;
read_options.snapshot_handle = snapshot;
auto value = co_await db.GetAsync(read_options, "k");
```

## 7. Status and Future Evolution

### Completed

1. **Lane-isolated runtime model** — `read_executor` + `compaction_executor` + `serial_lane`, with async DB work routed through the read lane (Wave 1-2)
2. **CompactionController** — DB-owned single-flight compaction lane backed by `BlockingExecutor`
3. **TaskScope and structured concurrency** — `StopSource` / `StopToken` (chainable), `Quarantine`, `OperationState<T>` for structured task lifecycle
4. **`Env::Schedule()` / `StartThread()` migration** — DB core no longer uses them; retained as POSIX-background facility in PosixEnv
5. **SerialLane migration** — `AsyncWritableFile` now uses `SerialLane` executor for FIFO-ordered file writes
6. **Internal cancellation** — `StopSource` propagation through `TaskScope` quarantine and `CompactionController` stop tokens
7. **Backend selection and metrics** — `BackendSelect()` unifies routing; `RuntimeMetrics` provides executor-level counters
8. **io_uring backend** — future work outside the VTune remediation scope; current async reads use executor-lane offload

### Deferred

9. **Public cancellation API**: Expose user-facing `Cancel()` on `AsyncDB` operations.
10. **Full-engine async rewrite**: Core `DBImpl` remains sync; `AsyncDB` is an offload wrapper.
11. **Async Iterators**: Implement `AsyncIterator` to allow non-blocking range scans.
12. **Granular Async (Phase B)**: Modify `AsyncDB` to check MemTable and Immutable MemTable synchronously; only offload disk reads (SSTable) to the blocking executor.
