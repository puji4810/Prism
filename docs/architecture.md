# Prism Architecture Overview

## System Components

```plaintext
┌────────────────────────────────────────────────────────────┐
│                      User API Layer                        │
│  DB::Open(), DB::Put(), DB::Get(), DB::Delete()            │
└────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌────────────────────────────────────────────────────────────┐
│                        DBImpl                              │
│  - Manages MemTable (mem_ / imm_)                          │
│  - Coordinates WAL (Write-Ahead Log)                       │
│  - Sequence number management                              │
│  - Background compaction (future)                          │
└────────────────────────────────────────────────────────────┘
          │                    │                    │
          ▼                    ▼                    ▼
    ┌──────────┐        ┌──────────┐        ┌──────────┐
    │ MemTable │        │   WAL    │        │ SSTable  │
    │          │        │ (Log)    │        │ (future) │
    └──────────┘        └──────────┘        └──────────┘
          │
          ▼
    ┌──────────┐
    │ SkipList │
    │  + Arena │
    └──────────┘
```

---

## Write Path

```plaintext
1. User calls DB::Put(key, value)
         │
         ▼
2. DBImpl creates WriteBatch
   - batch.Put(key, value)
   - batch.sequence = current_seq++
         │
         ▼
3. DBImpl::Write(batch)
   - Encode batch to bytes
   - Write to WAL (log_writer_)
   - Sync to disk (optional)
         │
         ▼
4. DBImpl::ApplyBatch(batch)
   - mem_->Add(seq, kTypeValue, key, value)
   - Updates in-memory SkipList
         │
         ▼
5. Return Status::OK()
```

**Key points:**

- **WAL first**: Ensures durability before in-memory update
- **Batch atomic**: Multiple operations commit together
- **Sequence number**: Monotonically increasing for MVCC

---

## Read Path

```plaintext
1. User calls DB::Get(key)
         │
         ▼
2. DBImpl::Get(key)
   - Create LookupKey(key, current_seq)
         │
         ▼
3. Search MemTable (mem_)
   - mem_->Get(lookup_key, &value, &s)
   - If found → return value
   - If deleted → return NotFound
         │
         ▼
4. Search Immutable MemTable (imm_) [future]
   - imm_->Get(lookup_key, &value, &s)
   - If found → return value
         │
         ▼
5. Search SSTables (Level 0 → Level N) [future]
   - Seek in bloom filters
   - Binary search in index blocks
   - Read data blocks
         │
         ▼
6. Return Result<std::string> or NotFound
```

**Key points:**

- **Newest first**: MemTable → Immutable MemTable → SSTables
- **Short-circuit**: Return immediately on first match
- **Snapshot isolation**: LookupKey includes sequence number

---

## Recovery Path

```plaintext
1. DB::Open(path)
         │
         ▼
2. DBImpl constructor
   - Open WAL file for reading
         │
         ▼
3. Replay WAL records
   for each log record:
     - Decode WriteBatch
     - Extract sequence number
     - mem_->Add() for each operation
     - Update last_sequence
         │
         ▼
4. Resume normal operation
   - Start from last_sequence + 1
   - Reopen WAL for appending
```

**Key points:**

- **Idempotent**: Replaying same record multiple times is safe
- **Sequence tracking**: Ensures monotonicity after restart
- **No data loss**: All committed writes are in WAL

---

## Data Structures

### MemTable

**Purpose:** In-memory write buffer

**Implementation:**

- SkipList (sorted map)
- Arena allocator (memory pool)
- InternalKey encoding (MVCC)

**Characteristics:**

- Fast writes: O(log N)
- Fast reads: O(log N)
- Sorted iteration
- Memory-only (volatile)

**Size limit:** Configured by `write_buffer_size` (default 4MB in LevelDB)

**State transitions:**

```plaintext
mem_ (active) → imm_ (immutable) → compacted to SSTable → deleted
```

### SkipList

**Purpose:** Ordered in-memory index

**Properties:**

- Probabilistic balanced tree
- Expected O(log N) search/insert
- No rebalancing needed
- Lock-free reads (with memory barriers)

**Implementation details:**

- Key type: `const char*` (pointer to encoded entry)
- Max height: 12 levels
- Branching factor: 4 (25% probability of next level)
- Memory: Allocated from Arena

### Arena

**Purpose:** Fast memory allocator

**Strategy:**

- Bump-pointer allocation (fast path)
- Block-based (4KB default)
- No individual free (bulk deallocation)

**Usage:**

- MemTable entries (key + value + metadata)
- SkipList nodes (pointers)

**Benefits:**

- Zero malloc overhead for small objects
- Cache-friendly (sequential allocation)
- Fast destruction (free blocks, not objects)

### WriteBatch

**Purpose:** Atomic batch of operations

**Format:**

```
WriteBatch := sequence (8 bytes) | count (4 bytes) | records
Record := kTypeValue key value | kTypeDeletion key
```

**Uses:**

- User API batching
- WAL format
- Replication unit (future Raft integration)

### Log (WAL)

**Purpose:** Write-Ahead Log for durability

**Format:**

```
Block := Record*
Record := checksum | length | type | data
```

**Types:**

- FULL: Complete WriteBatch
- FIRST, MIDDLE, LAST: Fragmented WriteBatch

**Properties:**

- 32KB block size
- CRC32C checksums
- Handles spanning records

---

## Memory Management Hierarchy

```plaintext
DBImpl
  │
  ├─> MemTable (mem_)
  │     ├─> Arena
  │     │     ├─> Block 1 (4KB)
  │     │     ├─> Block 2 (4KB)
  │     │     └─> Block N (4KB)
  │     └─> SkipList
  │           ├─> head_ (allocated from Arena)
  │           └─> nodes (allocated from Arena)
  │
  ├─> MemTable (imm_) [optional]
  │     └─> Arena (separate instance)
  │
  └─> LogWriter (log_)
        └─> File handle
```

**Lifetime management:**

- **MemTable**: Reference counted (Ref/Unref)
- **Arena**: Owned by MemTable, destroyed with it
- **SkipList**: Embedded in MemTable
- **LogWriter**: Owned by DBImpl

---

## Encoding Strategies

### Varint Encoding

**Purpose:** Compact integer representation

**Format:**

- 1 byte if value < 128
- 2 bytes if value < 16384
- Up to 5 bytes for uint32

**Used for:**

- Key lengths
- Value lengths
- Record counts

### Fixed Encoding

**Purpose:** Fast decoding, known size

**Used for:**

- Sequence numbers (8 bytes)
- Tags (8 bytes)
- Checksums (4 bytes)
- Record lengths (2 bytes)

### Length-Prefixed Strings

**Format:** `varint32(len) | bytes[len]`

**Used for:**

- Keys in WriteBatch
- Values in WriteBatch
- Internal keys in MemTable

---

## Comparator Hierarchy

```plaintext
User provides:
  Comparator (user_comparator)
    └─> Compare(user_key_a, user_key_b)

Database creates:
  InternalKeyComparator
    ├─> user_comparator
    └─> Compare(internal_key_a, internal_key_b)
          - Compare user_key (ASC)
          - Compare sequence (DESC)
          - Compare type (DESC)

MemTable uses:
  KeyComparator
    ├─> InternalKeyComparator
    └─> operator()(const char* a, const char* b)
          - Decode length-prefixed keys
          - Delegate to InternalKeyComparator
```

---

## Sequence Number Management

```plaintext
DBImpl:
  uint64_t last_sequence_ = 0;

Write operation:
  1. batch->SetSequence(++last_sequence_)
  2. Write batch to WAL
  3. Apply batch to MemTable
     - Each operation gets sequence++
   
Read operation:
  LookupKey lkey(user_key, last_sequence_);
  // Sees all committed writes
  
Snapshot (future):
  Snapshot* snap = db->GetSnapshot();
  ReadOptions opts;
  opts.snapshot = snap;
  db->Get(opts, key, &value);
  // Sees data at snapshot sequence
```

---

## Error Handling

### Status and Result<T>

**Status:** `std::expected<void, Error>`

- Used for operations that don't return values
- Examples: Put, Delete, Write

**Result<T>:** `std::expected<T, Error>`

- Used for operations that return values
- Examples: Get returns `Result<std::string>`

**Error types:**

```cpp
enum class ErrorCode {
    OK,
    NotFound,
    Corruption,
    NotSupported,
    InvalidArgument,
    IOError
};
```

**Usage:**

```cpp
// Writing
Status s = db->Put(key, value);
if (!s.has_value()) {
    // Handle error: s.error()
}

// Reading
Result<std::string> result = db->Get(key);
if (result.has_value()) {
    std::string value = *result;
} else {
    Error err = result.error();
}
```

---

## Concurrency Model (Current)

**Current implementation:**

- Single-threaded (no locking)
- External synchronization required

**Future multi-threaded design:**

```plaintext
Writers:
  - Mutex around DBImpl::Write()
  - WAL writes serialized
  - MemTable allows concurrent Add() with writer lock

Readers:
  - Lock-free reads from MemTable
  - Snapshot isolation via sequence numbers
  - SkipList supports concurrent reads

Compaction:
  - Background thread
  - Immutable MemTable (imm_) doesn't need locks
  - SSTable files are immutable
```

---

## Future Components

### SSTable (Sorted String Table)

**Purpose:** On-disk sorted key-value storage

**Structure:**

```
SSTable := Data Blocks | Meta Block | Index Block | Footer
```

**Properties:**

- Immutable
- Bloom filter for fast negative lookups
- Binary searchable index
- Compressed (optional)

### Version and VersionSet

**Purpose:** Manage multiple SSTable versions

**Responsibilities:**

- Track active SSTables per level
- Coordinate compaction
- Provide consistent snapshots

### Compaction

**Purpose:** Merge MemTable/SSTables, remove old versions

**Types:**

- Minor compaction: MemTable → Level 0 SSTable
- Major compaction: Level N → Level N+1

**Goals:**

- Reclaim space (remove deleted/overwritten keys)
- Maintain LSM-tree invariants
- Bound read amplification

---

## Design Principles

1. **Write-Ahead Logging**: Durability before acknowledgment
2. **Immutability**: SSTables never modified, only created/deleted
3. **LSM-Tree**: Log-Structured Merge tree for write optimization
4. **MVCC**: Multi-version concurrency control via sequence numbers
5. **Zero-copy**: Minimize data copying (Slice, Arena, pointers)
6. **Batch operations**: Amortize costs (WriteBatch, Block writes)
7. **Memory management**: Arena for fast allocation/deallocation

---

## Configuration (Future)

```cpp
struct Options {
    // MemTable
    size_t write_buffer_size = 4 * 1024 * 1024;  // 4MB
  
    // WAL
    bool sync = false;  // fsync after every write?
  
    // Compaction
    int max_open_files = 1000;
    size_t block_size = 4 * 1024;  // 4KB
    int block_restart_interval = 16;
  
    // Compression
    CompressionType compression = kSnappyCompression;
  
    // Cache
    size_t block_cache_size = 8 * 1024 * 1024;  // 8MB
  
    // Comparator
    const Comparator* comparator = BytewiseComparator();
};
```
