# Compaction Architecture

Prism uses a LevelDB-inspired Log-Structured Merge-Tree (LSM-Tree) architecture. Data is first written to a mutable in-memory buffer (**MemTable**) and a Write-Ahead Log (**WAL**). When the MemTable reaches a size threshold, it is converted to an immutable MemTable and flushed to disk as a Sorted String Table (**SSTable**) in Level 0.

As files accumulate in Level 0 and subsequent levels, **Compaction** merges overlapping key ranges, moves data to deeper levels, and eventually reclaims space by removing obsolete versions and deleted keys.

## Metadata Truth and Recovery

Prism follows the "MANIFEST-as-authority" design.

### Metadata Authority
- **The MANIFEST file** is the sole source of truth for which SSTable files belong to the database.
- **The CURRENT file** is a pointer to the active MANIFEST.
- Directory scans are **never** used to determine database state during normal operation. They are only used during a one-time "legacy bootstrap" phase to migrate old Prism/LevelDB data into a MANIFEST.

### Recovery Ordering
When a database is opened, Prism follows this strict recovery sequence:
1. **Find CURRENT**: Read the `CURRENT` file to locate the latest `MANIFEST`.
2. **Replay MANIFEST**: Read the `MANIFEST` log to reconstruct the `VersionSet`. This recovers the list of live SSTable files, the last used sequence number, and the next available file number.
3. **Replay WAL**: Identify which WAL (log) files contain data not yet present in the recovered SSTables (based on `log_number` in the MANIFEST). Replay these logs into a fresh MemTable.
4. **Establish New State**: Once logs are replayed, a new `VersionEdit` is applied to the `VersionSet` (advancing the `log_number`), and the database is ready for new writes.

## Background Operations

Prism uses a background thread pool to handle flushes and compactions without blocking user writes.

### 1. Minor Compaction (Flush)
**Flow**: `MemTable` → `Immutable MemTable` → `Level 0 SSTable`

When the active MemTable is full:
1. It is moved to the `imm_` (immutable) slot.
2. A new MemTable and WAL are created for incoming writes.
3. A background task picks up `imm_` and writes its contents to a new L0 SSTable.
4. The `VersionSet` is updated via `LogAndApply` to include the new file and obsolete the old WAL.

### 2. Major Compaction (Leveled)
**Flow**: `Level N` → `Level N+1`

When a level exceeds its allowed size budget (e.g., 10MB for L1, 100MB for L2), a major compaction is triggered:
1. **PickCompaction**: The `VersionSet` selects a level and a key range to compact.
2. **SetupOtherInputs**: Prism expands the range to include all overlapping files in Level N and Level N+1. It also performs "boundary expansion" to ensure all keys with the same user-key (but different sequence numbers) stay together during the merge.
3. **Execution**: A `MergingIterator` combines all input files. The merge loop produces new SSTables for Level N+1.
4. **Install**: The old files are removed from the `Version` and new files are added in a single atomic `LogAndApply` operation.

## Design Policies

### Conservative Tombstone Policy (v1)
In the current implementation (v1), Prism uses a **conservative merge-only** policy:
- **No dropping**: Deletion markers (tombstones) and older versions of keys are **not** dropped during compaction. 
- **Reasoning**: Snapshot-aware reclamation must respect readers holding a `Snapshot` through the `snapshot_handle` field on `ReadOptions`; dropping data prematurely could otherwise resurrect old values or hide still-visible history.
- **Future**: Data dropping can become more aggressive once snapshot-aware obsolete-file reclamation is fully integrated.

### Differences from LevelDB
While Prism strives for LevelDB compatibility, there are intentional divergences:
- **Modern C++**: Uses `std::expected` for error handling and `std::unique_ptr` for RAII-based file management.
- **Async Environment**: Built-in support for coroutine-based I/O and thread-pool scheduling.
- **Strict Metadata**: Prism is more aggressive about failing fast if the MANIFEST or CURRENT files are corrupted, rather than attempting risky directory-based recovery.

## Deferred Backlog

The following features are identified as necessary follow-ups to the core compaction modernization:

1. **Snapshots**: Continue refining point-in-time read behavior and snapshot-aware reclamation rules.
2. **Manual Compaction**: Add a user-facing manual compaction entry point for forced space reclamation.
3. **Seek-Triggered Compaction**: Recording "allowed_seeks" and triggering compaction when a file is repeatedly searched but doesn't contain the requested keys (optimizing for "holes" in the data).
4. **Bloom Filter Wiring**: Completing the read-side integration of Bloom filters to skip SSTable reads during `Get()` and Compaction.
5. **Group Commit**: Optimizing WAL syncs by grouping multiple concurrent `Write` calls into a single disk I/O.
6. **Async Scan/Prefetch**: Utilizing the coroutine engine to prefetch data blocks during compaction to improve throughput.
