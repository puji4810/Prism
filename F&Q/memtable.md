# MemTable Q&A

## Core Concepts

- **What is the role of a MemTable in an LSM-Tree?**

  - An in-memory, sorted write buffer that holds the newest updates. Writes go to the MemTable and a WAL; when it exceeds the configured size, it is frozen as an immutable MemTable and later flushed to an on-disk SSTable through compaction.
- **Why choose a SkipList over other data structures?**

  - It provides ordered inserts and lookups in expected O(log n) with a simple, low-constant-factor implementation. It supports in-order iteration, has predictable memory locality via arena allocation, and is simpler than balanced trees while being fast enough under single-writer patterns typical for MemTables.
- **What is the difference between InternalKey and user_key?**

  - `user_key` is the application key. `InternalKey = user_key || tag`, where `tag = (sequence << 8) | type`. The comparator sorts first by `user_key` (ascending), then by `sequence` (descending) so the newest version comes first, resolving visibility/versioning efficiently.

## Data Structures

- **How does `Arena::Allocate` differ from `malloc`?**

  - Arena uses a bump-pointer strategy for fast O(1) allocations from 4KB blocks, batches frees by deleting whole blocks at destruction, and separates large objects (≥ 1KB) into dedicated blocks to reduce internal fragmentation. `malloc`/`free` incur per-allocation metadata and fragmentation overhead and require per-object frees.
- **What is the ordering rule of `InternalKeyComparator`, and why?**

  - Order by `user_key` ascending; if equal, by `sequence` descending (newest first); if needed, by `type` to break ties. This ensures a single seek lands on the latest visible version for a `user_key` and allows efficient shadowing by newer entries.
- **What is Varint32, and why use it?**

  - A variable-length encoding that stores 7 bits per byte with a continuation bit. Small integers (e.g., lengths) use 1 byte; larger ones expand to 2–5 bytes. It reduces storage and improves cache locality for common small values.

## Operation Semantics

- **What is the full flow of `Add`?**

  1) Compute `internal_key_size = user_key.size() + 8` and total encoded length: varint(internal_key_size) + user_key + 8B tag + varint(value_size) + value.
  2) Allocate one contiguous buffer from the Arena.
  3) Encode `internal_key_size` (varint), copy `user_key`, encode `tag = (sequence << 8) | type` (fixed64), encode `value_size` (varint), copy `value`.
  4) Insert the pointer to this buffer into the SkipList; no further copying.
- **How does `Get` find the correct version?**

  - Build a lookup internal key as `(user_key, sequence = kMaxSequenceNumber, type = kTypeValue)` and seek. The first matching entry (due to sequence-descending order) is the newest: return `value` if `type == kTypeValue`; treat as not found if `type == kTypeDeletion`.
- **How does `KeyComparator` extract the `InternalKey` from `const char*`?**

  - Entries in the MemTable are length-prefixed. The comparator uses `GetLengthPrefixedSlice` to read varint length and slice the internal key bytes; it then delegates to `InternalKeyComparator`.

## Memory Management

- **Why does `MemTable` need reference counting?**

  - Multiple owners may hold the MemTable simultaneously (DB core, iterators, background compaction). Ref-counting ensures it is destroyed only when the last holder releases it, preventing use-after-free while avoiding leaks.
- **When is Arena memory freed?**

  - When the `MemTable` is destroyed (i.e., its ref-count drops to zero). The `Arena` destructor frees all allocated blocks in bulk, turning potentially millions of frees into O(number_of_blocks).
- **What does `ApproximateMemoryUsage` measure?**

  - The total bytes allocated by the `Arena` (sum of block sizes plus small bookkeeping like the block pointer entries). It excludes trivial object headers outside the Arena; SkipList nodes are in the Arena and therefore included.

## Encoding Format

- **What is the complete byte layout of a MemTable entry?**

```text
[ varint32: internal_key_len ]
[ bytes:   user_key ]
[ uint64:  tag = (sequence << 8) | type ]
[ varint32: value_len ]
[ bytes:   value ]
```

- `internal_key_len = user_key.size() + 8`.
- **What is the purpose of `GetLengthPrefixedSlice`?**

  - To decode a length-prefixed field: read a varint length, then return a slice of that many bytes immediately following the length. It is used to parse the `InternalKey` from an entry’s `const char*` buffer.
- **Why use `tag = (sequence << 8) | type` instead of two separate fields?**

  - Space efficiency (at least 1 byte saved; better packing/alignment), fewer compares (a single 64-bit compare after `user_key`), and a compact, cache-friendly representation that matches the comparator’s ordering needs.

## Arena Allocation Strategy Highlights

- Bump-pointer fast path when the current block has enough space.
- Large requests (≥ 1KB) get dedicated blocks to avoid wasting the remainder of a 4KB block.
- Bulk free at destruction drastically reduces deallocation overhead.

## Practical Impact

- Fast writes and lookups in memory with predictable performance.
- Efficient version visibility due to internal ordering.
- Low allocation overhead and reduced fragmentation via Arena.
