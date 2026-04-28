# Prism Documentation

Prism is a LevelDB-inspired LSM-tree key-value storage engine implemented in modern C++.

## Public API at a Glance

- `Database` is the synchronous move-only handle returned by `Database::Open(...)`.
- `AsyncDB` is the coroutine-friendly wrapper returned by `AsyncDB::OpenAsync(...)`.
- `Snapshot` is a cheap-copy RAII handle captured with `CaptureSnapshot()` and stored by value in the `snapshot_handle` field on `ReadOptions`.

## 📚 Documentation Index

### Architecture & Design

- **[Architecture Overview](architecture.md)** - System components, data flow, and design principles
  - Write/Read/Recovery paths
  - Component interactions
  - Memory management hierarchy
  - Future roadmap
### Async & Coroutines

- **[Coroutine API Design](coroutine_api_design.md)** - High-level async/coroutine architecture and layer design
  - Task-agnostic awaitables (`AsyncOp<T>`)
  - Suspend/resume handshake logic
  - Async environment and database interfaces
- **[Thread Pool Scheduler](thread_pool.md)** - Low-level execution engine and runtime for async operations
  - Multi-queue dispatch architecture
  - Priority and lazy (timer) dispatchers
  - Thread affinity and shutdown handling

### Data Formats

- **[Database Format](dbformat.md)** - InternalKey and LookupKey design

  - InternalKey structure (user_key + sequence + type)
  - Comparison rules for MVCC
  - LookupKey encoding
  - Sequence number management
- **[MemTable Format](memtable_format.md)** - In-memory storage format

  - Entry encoding (length-prefixed)
  - Add/Get operation details
  - KeyComparator design
  - Reference counting
  - Memory management with Arena
- **[WriteBatch Format](writebatch_format.md)** - Atomic batch operations

  - Binary layout (sequence + count + records)
  - Record types (kTypeValue, kTypeDeletion)
  - Varint encoding for keys/values
- **[Log Format](log_format.md)** - Write-Ahead Log (WAL)

  - Block structure (32KB)
  - Record types (FULL, FIRST, MIDDLE, LAST)
  - CRC32C checksums
- **[Table Format](table_format.md)** - SSTable on-disk storage

  - Block structure (data/index/filter blocks)
  - BlockHandle and Footer format
  - Prefix compression and restart points
  - Two-level iteration
  - CRC32C checksums

### Benchmarking & Profiling

- **[Benchmark Tools](benchmark.md)** - Complete guide to Prism's benchmark suite
  - `kv_bench` binary — all CLI flags and benchmark modes
  - `run_benchmark_matrix.sh` — canonical 5+4 variant throughput matrix
  - `acceptance_harness.sh` — before/after regression test suite
  - `compare_perf.py` — results comparison with regression gates
  - `profile_workflow.sh` — FlameGraph profiling workflow
  - Common workflows (pre-commit check, full characterization, CI)

## 🏗️ Implementation Status

### ✅ Completed

- [X]  **Slice** - Zero-copy string view
- [X]  **Status** - Error handling
- [X]  **Coding utilities** - Varint and fixed-width encoding/decoding
- [X]  **Arena** - Fast memory allocator
- [X]  **SkipList** - Probabilistic ordered index
- [X]  **WriteBatch** - Atomic batch operations
- [X]  **Log Writer/Reader** - WAL (Env::SequentialFile/WritableFile + numbered logs)
- [X]  **InternalKey & LookupKey** - MVCC support
- [X]  **MemTable** - In-memory write buffer
- [X]  **Iterator** - Base iterator + TwoLevelIterator + MemTable/Table iterators
- [X]  **MergingIterator + DBIter** - `Database::NewIterator()` over MemTable + SSTable
- [X]  **Env (PosixEnv)** - Filesystem abstraction
- [X]  **Cache (LRU)** - LRU block/metadata cache
- [X]  **SSTable (core)** - Table/TableBuilder/TableCache (read/write)
- [X]  **DBImpl (L0-only)** - WAL + MemTable + flush to SSTable + reads across MemTable/SSTable
- [X]  **DB API (partial LevelDB-aligned)** - `Database::Open(options, dbname)`, Read/WriteOptions, LOCK file
- [X]  **Snapshot API** - `CaptureSnapshot()` + the `snapshot_handle` field on `ReadOptions` for stable reads
- [X]  **AsyncDB API** - `AsyncDB::OpenAsync(...)` + async CRUD wrappers over the same engine

### 🚧 In Progress

- [X]  **Filter block / Bloom filter** - write-side FilterBlockBuilder integration
- [X]  **Version & VersionSet** - MANIFEST-based metadata + recovery
- [X]  **Compaction** - Background merging (multi-level)

### 📋 Planned

- [ ]  **Sharded cache** - Concurrent cache implementation
- [ ]  **Compression** - Snappy/Zstd (block compression)

## 🔑 Key Concepts

### LSM-Tree (Log-Structured Merge-Tree)

```
Writes:  App → MemTable → WAL → SSTable (L0 → L1 → ... → L6)
Reads:   App → MemTable → Imm → L0 → L1 → ... → L6
```

**Advantages:**

- Fast sequential writes
- Good write throughput
- Efficient compaction

**Trade-offs:**

- Read amplification (check multiple levels)
- Write amplification (compaction rewrites data)

### MVCC (Multi-Version Concurrency Control)

- Each write gets a unique **sequence number**
- Multiple versions of same key coexist
- Snapshots see consistent view at specific sequence
- Old versions removed during compaction

### Write-Ahead Logging

1. Write to WAL (durable)
2. Write to MemTable (volatile)
3. Acknowledge to client

**Guarantees:**

- Durability (committed writes survive crashes)
- Atomicity (batch commits together)
- Recoverability (replay WAL on restart)

### Arena Allocation

- Bump-pointer allocator (O(1) fast path)
- Block-based (4KB chunks)
- No individual free
- Bulk deallocation (free entire MemTable)

**Benefits:**

- Zero malloc overhead
- Cache locality
- Fast allocation/deallocation

## 📖 Reading Guide

### For Understanding Design

1. Start with [Architecture Overview](architecture.md)
2. Read [Database Format](dbformat.md) for key encoding
3. Study [MemTable Format](memtable_format.md) for in-memory structure
4. Review [WriteBatch Format](writebatch_format.md) and [Log Format](log_format.md)
5. Explore [Coroutine API Design](coroutine_api_design.md) and [Thread Pool Scheduler](thread_pool.md) for async operations
6. See [Benchmark Tools](benchmark.md) for performance measurement and regression testing

### For Implementation

1. Review existing code:

   - `include/slice.h` - Basic types
   - `util/coding.h` - Encoding utilities
   - `util/arena.h` - Memory allocator
   - `include/skiplist.h` - Ordered index
2. Understand the key abstractions:

   - `Database` (sync) and `AsyncDB` (coroutine) are the public entry points
   - `DBImpl` is the internal engine — see `include/db_impl.h`
   - `Snapshot` is a cheap-copy RAII handle for point-in-time reads
   - The `RuntimeBundle` manages lane-isolated executors for async I/O
   - Compaction is snapshot-aware and runs on a dedicated background lane
3. See `docs/architecture.md` for the full system design.

## 🧪 Testing

Each component has comprehensive tests:

- `tests/coding_test.cpp` - Encoding/decoding
- `tests/arena_test.cpp` - Memory allocation
- `tests/skiplist_test.cpp` - Ordered index
- `tests/db_test.cpp` - Database operations

Run all tests:

```bash
xmake test
```

## 🔄 Breaking Change Migration Note

Prism now documents only the final public handles: `Database`, `AsyncDB`, and RAII `Snapshot` values. Code written against older manual-lifetime patterns should move to by-value handles and `CaptureSnapshot()`.

### Before

```cpp
// Legacy style: open through an old sync wrapper and manage a borrowed snapshot manually.
auto legacy_db = OpenLegacyHandle(options, name);
auto legacy_snapshot = AcquireLegacySnapshot(legacy_db);

LegacyReadOptions ro;
ro.snapshot_handle = legacy_snapshot;
auto value = LegacyRead(legacy_db, ro, "k");
ReleaseLegacySnapshot(legacy_db, legacy_snapshot);
```

### After

```cpp
auto db_result = prism::Database::Open(options, name);
if (!db_result.has_value()) {
    return db_result.error();
}

auto db = std::move(db_result.value());
prism::Snapshot snapshot = db.CaptureSnapshot();

prism::ReadOptions ro;
ro.snapshot_handle = snapshot;
auto value = db.Get(ro, "k");
```

Async callers follow the same snapshot model and open through `prism::AsyncDB::OpenAsync(...)`.

## 🎯 Design Goals

1. **Correctness** - Comprehensive tests, clear invariants
2. **Performance** - Fast writes, acceptable read latency
3. **Modern C++** - RAII, zero-copy, clear ownership
4. **Understandability** - Clear code, good documentation
5. **LevelDB compatibility** - Follow proven design patterns

## 📚 References

### LevelDB Documentation

- [LevelDB Implementation Notes](https://github.com/google/leveldb/blob/main/doc/impl.md)
- [LevelDB Table Format](https://github.com/google/leveldb/blob/main/doc/table_format.md)
- [LevelDB Log Format](https://github.com/google/leveldb/blob/main/doc/log_format.md)

### Papers

- [The Log-Structured Merge-Tree (LSM-Tree)](https://www.cs.umb.edu/~poneil/lsmtree.pdf) - O'Neil et al., 1996
- [Bigtable: A Distributed Storage System](https://research.google/pubs/pub27898/) - Chang et al., 2006
- [In Search of an Understandable Consensus Algorithm (Raft)](https://raft.github.io/raft.pdf) - Ongaro & Ousterhout, 2014

### SkipList

- [Skip Lists: A Probabilistic Alternative to Balanced Trees](https://15721.courses.cs.cmu.edu/spring2018/papers/08-oltpindexes1/pugh-skiplists-cacm1990.pdf) - Pugh, 1990

## 🛠️ Development Setup

### Build System

- **xmake** - Modern C++ build system
- C++23 standard
- Google Test for unit tests

### Dependencies

- CRC32C - Checksum library
- Google Test - Testing framework

### Build Commands

```bash
# Build all targets
xmake build

# Build specific target
xmake build skiplist_test

# Run tests
xmake test

# Clean
xmake clean
```

## 📝 Contributing

When adding new components:

1. **Design first** - Document format/algorithm in `docs/`
2. **Implement** - Write clean, well-commented code
3. **Test** - Comprehensive unit tests
4. **Integrate** - Update DBImpl and existing components
5. **Document** - Update this README and architecture docs

## 🗺️ Roadmap

### Phase 1: Core Engine (Completed)

- [X]  Basic infrastructure (Slice, Status, Coding)
- [X]  Memory management (Arena, SkipList)
- [X]  Persistence (WriteBatch, WAL format)
- [X]  **In-memory storage (MemTable)**
- [X]  SSTable core (Table/TableBuilder/TableCache)
- [X]  DBImpl (L0-only) end-to-end read/write/recover

### Phase 2: Versioning & Compaction (Completed)

- [X]  VersionEdit/VersionSet + MANIFEST
- [X]  Minor compaction (MemTable → L0) background
- [X]  Major compaction (Ln → Ln+1)
- [X]  Bloom filter (write-side) integration

### Phase 3: Optimization

- [ ]  Sharded cache + tuning
- [ ]  Compression (Snappy/Zstd)

## 📄 License

See [LICENSE](../LICENSE) file in the project root.

---

**Last Updated:** 2026-04-28
**Version:** 0.2.0-dev
