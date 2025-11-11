# SSTable Format

SSTable (Sorted String Table) is the on-disk storage format for Prism. It stores sorted key-value pairs in an immutable, block-based structure optimized for both sequential scans and random lookups.

## File Layout

```mermaid
graph TB
    subgraph SSTable File
        DB1[Data Block 1]
        DB2[Data Block 2]
        DBN[Data Block N]
        MB1[Meta Block 1]
        MBK[Meta Block K]
        MIB[MetaIndex Block]
        IB[Index Block]
        F[Footer - 48 bytes]
    end
  
    DB1 --> DB2
    DB2 --> DBN
    DBN --> MB1
    MB1 --> MBK
    MBK --> MIB
    MIB --> IB
    IB --> F
  
    style F fill:#FF6B6B,stroke:#333,stroke-width:2px,color:#000
    style IB fill:#4ECDC4,stroke:#333,stroke-width:2px,color:#000
    style MIB fill:#4ECDC4,stroke:#333,stroke-width:2px,color:#000
    style MB1 fill:#95E1D3,stroke:#333,stroke-width:2px,color:#000
    style MBK fill:#95E1D3,stroke:#333,stroke-width:2px,color:#000
```

**Structure:**

```
<beginning_of_file>
[data block 1]
[data block 2]
...
[data block N]
[meta block 1]          (optional, e.g., filter block)
...
[meta block K]          (optional, e.g., stats block)
[metaindex block]
[index block]
[footer]                (fixed 48 bytes)
<end_of_file>
```

---

## BlockHandle

A `BlockHandle` is a pointer to a block within the file:

```cpp
struct BlockHandle {
    uint64_t offset;    // varint64: byte offset in file
    uint64_t size;      // varint64: block content size (excludes trailer)
};
```

**Encoding:** Two consecutive varint64 values.

**Usage:**

- Index block entries point to data blocks
- MetaIndex entries point to meta blocks (e.g., filter)
- Footer contains handles for metaindex and index blocks

---

## Data Block Format

Data blocks store sorted key-value pairs with prefix compression.

### Block Structure

```mermaid
graph LR
    subgraph Data Block
        R1[Record 1<br/>shared=0]
        R2[Record 2<br/>shared=3]
        R3[Record 3<br/>shared=5]
        RN[Record N<br/>shared=4]
        RA[Restart Array<br/>4 bytes each]
        RC[Restart Count<br/>4 bytes]
    end
  
    R1 --> R2
    R2 --> R3
    R3 --> RN
    RN --> RA
    RA --> RC
  
    style R1 fill:#2ECC71,stroke:#333,color:#000
    style RA fill:#FF6B6B,stroke:#333,color:#000
    style RC fill:#FF6B6B,stroke:#333,color:#000
```

### Entry Encoding

Each key-value entry is encoded as:

```
Entry := shared       (varint32)    // bytes shared with previous key
       | non_shared   (varint32)    // bytes following shared prefix
       | value_length (varint32)    // length of value
       | key_delta    [non_shared]  // unshared part of key
       | value        [value_length]
```

**Example:**

```
Keys: "apple", "apply", "application"

Entry 1: shared=0, non_shared=5, value_len=10
         key_delta="apple", value="<10 bytes>"

Entry 2: shared=4, non_shared=1, value_len=8
         key_delta="y", value="<8 bytes>"
         (reconstructed key = "appl" + "y" = "apply")

Entry 3: shared=4, non_shared=7, value_len=12
         key_delta="ication", value="<12 bytes>"
         (reconstructed key = "appl" + "ication" = "application")
```

### Restart Points

Every `block_restart_interval` entries (default 16), a **restart point** is created:

- `shared = 0` (full key stored)
- Offset recorded in restart array

**Purpose:** Enable binary search within block without decoding all entries.

### Block Trailer

```
Block Trailer := compression_type (1 byte)
               | crc32c           (4 bytes)
```

- **Compression type:** `0x00` = none, `0x01` = Snappy
- **CRC32C:** Checksum of `block_contents + compression_type`

---

## Index Block Format

The index block maps keys to data block locations.

```mermaid
graph TB
    subgraph Index Block
        E1["Key: separator1<br/>Value: BlockHandle1"]
        E2["Key: separator2<br/>Value: BlockHandle2"]
        EN["Key: separatorN<br/>Value: BlockHandleN"]
    end
  
    E1 --> E2
    E2 --> EN
  
    subgraph Corresponding Data Blocks
        DB1[Data Block 1]
        DB2[Data Block 2]
        DBN[Data Block N]
    end
  
    E1 -.-> DB1
    E2 -.-> DB2
    EN -.-> DBN
```

**Index Entry:**

```
Key   := shortest separator between blocks
         (last_key_in_block[i] < separator <= first_key_in_block[i+1])
Value := BlockHandle (varint64 offset | varint64 size)
```

**Separator Generation:**

For block `i`, the index key is:

```cpp
if (i < num_blocks - 1) {
    // Use shortest separator between this block's last key and next block's first key
    comparator->FindShortestSeparator(&last_key_block_i, first_key_block_i+1);
} else {
    // Last block: use short successor of last key
    comparator->FindShortSuccessor(&last_key_block_i);
}
```

**Benefits:**

- Reduced index size (shorter keys)
- Maintains correctness (separator still routes lookups correctly)

---

## Meta Blocks

Meta blocks store auxiliary data that helps optimize table operations. Unlike data blocks which contain actual key-value pairs, meta blocks contain metadata.

**Common meta block types:**

1. **Filter block** - Bloom filters for fast negative lookups
2. **Stats block** - Statistics about the table (future)
3. **Compression dictionary** - Shared dictionary for compression (future)

**Properties:**

- Stored after all data blocks
- Each has a unique name (e.g., `"filter.bloom"`, `"stats"`)
- **Mapped by MetaIndex block** - readers use MetaIndex to locate specific meta blocks
- Formatted using `BlockBuilder` (can have restart points, block trailer)
- Optional (table valid without them)

**Distinction from Index Block:**

While **Index block** maps data block separators to data blocks, **Meta blocks + MetaIndex** work as a pair:

- **Meta blocks** = storage for metadata (e.g., filter data)
- **MetaIndex block** = index/directory of meta blocks

Think of it as: Data blocks → indexed by → Index block, while Meta blocks → indexed by → MetaIndex block.

---

## MetaIndex Block Format

The MetaIndex block maps meta block names to their locations:

```
Entry: Key   = meta_block_name (e.g., "filter.bloom", "stats")
       Value = BlockHandle (varint64 offset | varint64 size)
```

**Purpose:** Allow readers to locate specific meta blocks by name without scanning.

**Example entries:**

```
Key="filter.bloom" → Value=BlockHandle(offset=12345, size=4096)
Key="stats"        → Value=BlockHandle(offset=16441, size=256)
Key="dict"         → Value=BlockHandle(offset=16697, size=1024)
```

**Format Characteristics:**

MetaIndex blocks **technically use the same BlockBuilder** as all other blocks (with restart points and trailer), but behave **more like Index blocks** in practice:


| Characteristic         | Index Block                                      | MetaIndex Block                                        |
| ---------------------- | ------------------------------------------------ | ------------------------------------------------------ |
| **Entry count**        | Medium (10s-100s)                                | Small (typically < 10)                                 |
| **Prefix compression** | Effective (data block separators share prefixes) | Ineffective (meta block names have no common prefixes) |
| **Restart points**     | Multiple (one per 16 entries)                    | Usually only 1 (at offset 0)                           |
| **Search method**      | Binary search                                    | Linear scan (enough for small count)                   |

**Typical structure:**

```
Entry 1: shared=0, key="dict",          value=BlockHandle(...)
Entry 2: shared=0, key="filter.bloom",  value=BlockHandle(...)
Entry 3: shared=0, key="stats",         value=BlockHandle(...)

Restart Array: [0]                      (only one restart point)
Restart Count: 1
Block Trailer: compression_type | crc32c
```

**Why use BlockBuilder if it's mostly ineffective?**

- **Code reuse:** Single block format for all block types
- **Consistency:** Unified reading/verification logic for all blocks
- **Future-proof:** If meta block names grow or share prefixes, compression becomes useful

---

## Block Types Comparison

To clarify the three main block types and their relationships:

```mermaid
graph TB
    subgraph Data_Layer["Data Layer"]
        DB1["Data Block 1<br/>(KV pairs)"]
        DB2["Data Block 2<br/>(KV pairs)"]
        DBN["Data Block N<br/>(KV pairs)"]
    end
  
    subgraph Index_Layer["Index Layer"]
        IB["Index Block<br/>(separator → BlockHandle)"]
    end
  
    subgraph Meta_Layer["Meta Layer"]
        MB1["Filter Block<br/>(filter data)"]
        MB2["Stats Block<br/>(statistics)"]
        MIB["MetaIndex Block<br/>(name → BlockHandle)"]
    end
  
    DB1 --> IB
    DB2 --> IB
    DBN --> IB
  
    MB1 --> MIB
    MB2 --> MIB
  
    style DB1 fill:#2ECC71,stroke:#333,stroke-width:2px,color:#000
    style DB2 fill:#2ECC71,stroke:#333,stroke-width:2px,color:#000
    style DBN fill:#2ECC71,stroke:#333,stroke-width:2px,color:#000
    style IB fill:#4ECDC4,stroke:#333,stroke-width:2px,color:#000
    style MB1 fill:#F39C12,stroke:#333,stroke-width:2px,color:#000
    style MB2 fill:#F39C12,stroke:#333,stroke-width:2px,color:#000
    style MIB fill:#FF6B6B,stroke:#333,stroke-width:2px,color:#000
```

### Detailed Comparison


| Aspect                 | Data Block            | Index Block              | MetaIndex Block               |
| ---------------------- | --------------------- | ------------------------ | ----------------------------- |
| **Purpose**            | Store actual KV pairs | Index data blocks        | Index meta blocks             |
| **Indexed by**         | Index block           | Footer                   | Footer                        |
| **Entry content**      | Key → Value          | Separator → BlockHandle | Meta name → BlockHandle      |
| **Entry count**        | Large (100s-1000s)    | Medium (10s-100s)        | Small (typically < 10)        |
| **Prefix compression** | Very effective        | Effective                | Ineffective (names differ)    |
| **Restart interval**   | Every 16 entries      | Every 16 entries         | Usually only 1 (all shared=0) |
| **Must have**          | Yes                   | Yes                      | Only if meta blocks exist     |
| **Format type**        | `BlockBuilder` output | `BlockBuilder` output    | `BlockBuilder` output         |
| **Typical size**       | ~4 KB each            | A few KB                 | < 1 KB                        |

### Storage Hierarchy

```
SSTable File Structure:
├── Layer 1: Data
│   ├── Data Block 1 (4 KB)
│   ├── Data Block 2 (4 KB)
│   └── ...
├── Layer 2: Metadata
│   ├── Filter Block (2 KB)
│   ├── Stats Block (256 B)
│   └── ...
├── Layer 3: Indices
│   ├── MetaIndex Block (200 B) ← indexes Layer 2
│   └── Index Block (4 KB)      ← indexes Layer 1
└── Layer 4: Navigation
    └── Footer (48 B)           ← indexes Layer 3
```

### Query Flow Examples

**Example 1: Find data value**

```
Footer → Index Block location
       → Read Index Block
       → Binary search for key
       → Get Data Block location
       → Read Data Block
       → Seek/Linear scan for value
```

**Example 2: Use filter block**

```
Footer → MetaIndex Block location
       → Read MetaIndex Block
       → Lookup "filter.bloom"
       → Get Filter Block location
       → Read Filter Block
       → Check if key might exist
       → If no → return NotFound (fast path)
       → If yes → proceed with Example 1
```

---

## Filter Block Format

Stores Bloom filters for data blocks.

```
[filter 0]                            // For keys in data blocks [0*base, 1*base)
[filter 1]                            // For keys in data blocks [1*base, 2*base)
...
[filter N-1]

[offset of filter 0]                  // fixed32
[offset of filter 1]                  // fixed32
...
[offset of filter N-1]                // fixed32

[offset of offset array]              // fixed32
lg(base)                              // 1 byte (default: lg(2048) = 11)
```

**Base:** 2KB (2048 bytes). All keys in data blocks whose file offset falls in `[i*2KB, (i+1)*2KB)` are combined into filter `i`.

---

## Footer Format

The footer is **fixed 48 bytes** at the end of the file:

```
Footer := metaindex_handle (varint64 offset | varint64 size, padded to 20 bytes)
        | index_handle     (varint64 offset | varint64 size, padded to 20 bytes)
        | padding          (fill to 40 bytes total)
        | magic            (0xdb4775248b80fb57, fixed64, 8 bytes)
```

**Total:** 40 bytes (handles + padding) + 8 bytes (magic) = 48 bytes.

**Magic number:** `0xdb4775248b80fb57` (little-endian)

**Reading:**

1. Seek to `file_size - 48`
2. Read 48 bytes
3. Verify magic number
4. Decode `metaindex_handle` and `index_handle`

---

## Building an SSTable

```mermaid
sequenceDiagram
    participant TB as TableBuilder
    participant DBB as DataBlockBuilder
    participant IBB as IndexBlockBuilder
    participant File as SSTable File

    loop For each sorted key-value pair
        TB->>DBB: Add(key, value)
        alt Block size >= threshold
            DBB->>TB: Finish() → block_contents
            TB->>File: Write data block + trailer
            TB->>IBB: Add(separator, BlockHandle)
            TB->>DBB: Reset()
        end
    end
  
    TB->>DBB: Finish() (final block)
    TB->>File: Write final data block + trailer
    TB->>IBB: Add(separator, BlockHandle)
  
    opt If filter policy
        TB->>File: Write filter block + trailer
        TB->>TB: Record filter BlockHandle
    end
  
    TB->>File: Write metaindex block + trailer
    TB->>IBB: Finish()
    TB->>File: Write index block + trailer
    TB->>File: Write footer (48 bytes)
```

**Key steps:**

1. **Add entries:** Call `TableBuilder::Add(key, value)` in sorted order
2. **Flush data blocks:** When size threshold reached, flush to file
3. **Build index:** Record separator key → BlockHandle for each data block
4. **Write filter:** Optionally write Bloom filter
5. **Write metaindex:** Map "filter.bloom" → filter BlockHandle
6. **Write index:** Flush index block
7. **Write footer:** Record metaindex/index BlockHandles + magic

---

## Reading from SSTable

```mermaid
sequenceDiagram
    participant C as Client
    participant T as Table
    participant IB as Index Block
    participant DB as Data Block
    participant Filter as Filter Block

    C->>T: Open(filename)
    T->>T: Read footer (last 48 bytes)
    T->>T: Verify magic, extract handles
    T->>T: Read index block
    T->>C: Table ready

    C->>T: InternalGet(key)
    T->>IB: Seek(key)
    IB->>T: BlockHandle of candidate data block
  
    opt If filter exists
        T->>Filter: KeyMayMatch(key, block_id)
        alt Filter returns false
            T->>C: NotFound (fast path)
        end
    end
  
    T->>DB: ReadBlock(BlockHandle)
    DB->>T: Block iterator
    T->>DB: Seek(key) in block
    alt Key found
        DB->>T: Value
        T->>C: Value or NotFound (if kTypeDeletion)
    else Key not found
        T->>C: NotFound
    end
```

**Two-level lookup:**

1. **Index block:** Binary search to find data block containing key
2. **Data block:** Binary search on restart points, then linear scan

---

## Iterator Implementation

### Two-Level Iterator

```mermaid
graph TB
    subgraph TwoLevelIterator
        Index[Index Block Iterator<br/>outer]
        Data[Data Block Iterator<br/>inner]
    end
  
    Index -->|current entry's BlockHandle| Data
  
    subgraph Operations
        S[Seek/Next/Prev]
        S --> Index
        Index --> Data
    end
```

**Algorithm:**

```
TwoLevelIterator:
    outer: index_block_iterator
    inner: data_block_iterator (lazy loaded)

    Seek(target):
        outer.Seek(target)
        InitInner()  // Read data block at outer.value()
        inner.Seek(target)

    Next():
        inner.Next()
        if !inner.Valid():
            outer.Next()
            if outer.Valid():
                InitInner()
                inner.SeekToFirst()

    key() := inner.key()
    value() := inner.value()
```

---

## Compression

**Supported types:**

- `kNoCompression (0x00)`
- `kSnappyCompression (0x01)`

**Applied to:** Block contents (before appending trailer)

**Trade-off:**

- **Pros:** Reduced disk I/O, storage space
- **Cons:** CPU overhead on read/write

**Default:** Snappy (fast compression/decompression)

---

## CRC32C Checksum

**Purpose:** Detect data corruption

**Coverage:** `block_contents + compression_type`

**Algorithm:** CRC32C (Castagnoli polynomial)

**Location:** Last 4 bytes of block trailer

**Verification:**

```cpp
uint32_t expected_crc = DecodeFixed32(trailer + 1);
uint32_t actual_crc = crc32c::Value(block_contents);
actual_crc = crc32c::Extend(actual_crc, &compression_type, 1);
if (actual_crc != expected_crc) {
    return Status::Corruption("Block checksum mismatch");
}
```

**Note:** Unlike WAL, table block CRCs are **not masked**.

---

## InternalKey Encoding in SSTable

Keys stored in SSTable are **InternalKeys**:

```
InternalKey := user_key (variable length)
             | sequence_number (7 bytes, big-endian subset of tag)
             | value_type (1 byte: kTypeValue=1, kTypeDeletion=0)
```

**Tag:** `(sequence << 8) | type`, stored as fixed64 (little-endian)

**Comparison order:**

1. User key: ascending (lexicographic)
2. Sequence number: descending (newer first)
3. Value type: descending (value before deletion)

**Example:**

```
User puts: key="foo", value="v1" at seq=10
User puts: key="foo", value="v2" at seq=20
User deletes: key="foo" at seq=30

SSTable stores (sorted by InternalKey):
  InternalKey("foo", 30, kTypeDeletion) → "" (empty value)
  InternalKey("foo", 20, kTypeValue) → "v2"
  InternalKey("foo", 10, kTypeValue) → "v1"

Get("foo", snapshot_seq=25) → "v2" (ignores seq=30)
Get("foo", snapshot_seq=35) → NotFound (sees deletion at seq=30)
```

---

## Size Limits and Thresholds


| Parameter                | Default | Purpose                     |
| ------------------------ | ------- | --------------------------- |
| `block_size`             | 4 KB    | Target size for data blocks |
| `block_restart_interval` | 16      | Restart points per block    |
| `write_buffer_size`      | 4 MB    | MemTable flush threshold    |
| `filter_base`            | 2 KB    | Filter granularity          |

**Why 4KB blocks?**

- Matches typical filesystem/SSD page size
- Good balance: small enough for random reads, large enough for compression

---

## TableCache (Future)

```mermaid
graph LR
    subgraph TableCache
        LRU[LRU Cache]
        H1[Handle: file_number=1 → Table*]
        H2[Handle: file_number=2 → Table*]
        HN[Handle: file_number=N → Table*]
    end
  
    LRU --> H1
    LRU --> H2
    LRU --> HN
  
    H1 --> T1[Table 1<br/>index in memory]
    H2 --> T2[Table 2<br/>index in memory]
    HN --> TN[Table N<br/>index in memory]
```

**Purpose:**

- Keep recently-used tables open (file handles + index blocks)
- Avoid repeated file open/close overhead
- Amortize index block parsing

**Eviction:** LRU (Least Recently Used)

---

## Integration with DBImpl

### Minor Compaction (MemTable → SSTable)

```mermaid
sequenceDiagram
    participant DB as DBImpl
    participant Mem as MemTable
    participant Imm as Immutable MemTable
    participant TB as TableBuilder
    participant File as L0 SSTable

    DB->>Mem: Write reaches threshold
    DB->>DB: mem_ → imm_, create new mem_
  
    DB->>Imm: NewIterator()
    Imm->>DB: Iterator (sorted)
  
    loop For each entry
        DB->>TB: Add(internal_key, value)
    end
  
    TB->>File: Finish() → Write blocks, index, footer
    DB->>DB: Update version (add new L0 file)
    DB->>Imm: Unref() → delete
```

### Read Path

```mermaid
graph TB
    Start[DBImpl::Get]
    Mem{Check mem_}
    Imm{Check imm_}
    L0{Check L0<br/>SSTables}
    L1{Check L1+<br/>SSTables}
    NotFound[Return NotFound]
  
    Start --> Mem
    Mem -->|Miss| Imm
    Mem -->|Hit| Return
    Imm -->|Miss| L0
    Imm -->|Hit| Return
    L0 -->|Miss| L1
    L0 -->|Hit| Return
    L1 -->|Miss| NotFound
    L1 -->|Hit| Return
  
    style Return fill:#2ECC71,stroke:#333,stroke-width:2px,color:#000
    style NotFound fill:#FF6B6B,stroke:#333,stroke-width:2px,color:#000
```

**Search order:**

1. `mem_` (current MemTable)
2. `imm_` (immutable MemTable being compacted)
3. Level 0 SSTables (may overlap)
4. Level 1+ SSTables (non-overlapping within level)

## File Naming Convention

```
<dbname>/
  ├── CURRENT                  # Points to current MANIFEST
  ├── MANIFEST-000001          # Version metadata
  ├── 000003.log               # WAL (Write-Ahead Log)
  ├── 000005.sst               # SSTable files
  ├── 000007.sst
  └── LOG                      # Human-readable log
```

**SSTable naming:** `<file_number>.sst` or `<file_number>.ldb`

- File number: monotonically increasing uint64
- Managed by `VersionSet`

## References

- [LevelDB Table Format](https://github.com/google/leveldb/blob/main/doc/table_format.md)
- [LevelDB Implementation Notes](https://github.com/google/leveldb/blob/main/doc/impl.md)
- [LSM-Tree Paper](https://www.cs.umb.edu/~poneil/lsmtree.pdf) - O'Neil et al., 1996

---

**Last Updated:** 2025-11-1
**Status:** Design documentation (implementation pending)
