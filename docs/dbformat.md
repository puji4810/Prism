# Database Format Design

## Internal Key Format

LevelDB uses **InternalKey** instead of raw user keys to support MVCC (Multi-Version Concurrency Control) and snapshots.

```mermaid
---
title: "InternalKey Structure"
---
packet-beta
0-255: "user_key: bytes[user_key_len]"
256-319: "sequence: uint56 (bits 8-63)"
320-327: "type: uint8 (bits 0-7)"
```

```plaintext
InternalKey :=
   user_key: bytes[user_key_len]
   tag: uint64
   
tag := (sequence << 8) | type

where:
   sequence: uint56 (0 to 2^56 - 1)
   type: uint8 (kTypeDeletion = 0x0, kTypeValue = 0x1)
```

### InternalKey Comparison Rules

InternalKey comparison follows these rules (in order):

1. **Compare user_key** (ascending order)
   - If `user_key_a < user_key_b`, then `InternalKey_a < InternalKey_b`
   
2. **If user_key equal, compare sequence** (descending order)
   - Newer entries (larger sequence) come first
   - If `seq_a > seq_b`, then `InternalKey_a < InternalKey_b`
   
3. **If sequence equal, compare type** (descending order)
   - `kTypeValue` (0x1) < `kTypeDeletion` (0x0)
   - Values come before deletions

**Why descending sequence?**
- When searching for a key, we want to find the newest version first
- SkipList returns the first match, so newer versions must sort earlier

---

## LookupKey Format

`LookupKey` is used for querying in MemTable. It encodes the search target.

```mermaid
---
title: "LookupKey Memory Layout"
---
packet-beta
0-31: "internal_key_len: varint32"
32-255: "user_key: bytes[user_key_len]"
256-319: "tag: uint64 (seq << 8 | type)"
```

```plaintext
LookupKey Layout:
┌─────────────────┬──────────────────────┬─────────────────┐
│ internal_key_len│      user_key        │      tag        │
│   (varint32)    │   (user_key_len)     │    (uint64)     │
└─────────────────┴──────────────────────┴─────────────────┘
     ↑                 ↑                                    ↑
   start_           kstart_                               end_
```

**Three key pointers:**
- `start_`: Points to `internal_key_len` (for MemTable queries)
- `kstart_`: Points to `user_key` (for InternalKey extraction)
- `end_`: Points to end of tag

**Usage:**
```cpp
LookupKey lkey(user_key, sequence);
Slice memtable_key = lkey.memtable_key();  // [start_, end_)
Slice internal_key = lkey.internal_key();   // [kstart_, end_)
Slice user_key = lkey.user_key();           // [kstart_, end_ - 8)
```

---

## Value Types

```cpp
enum ValueType { 
    kTypeDeletion = 0x0,  // Tombstone marker
    kTypeValue = 0x1      // Actual value
};
```

**Why store deletions?**
- Deletions are **tombstone markers** in LSM-tree
- They mark keys as deleted without physically removing old versions
- Compaction will eventually remove tombstones and their older versions

---

## Sequence Numbers

```cpp
using SequenceNumber = uint64_t;
static const SequenceNumber kMaxSequenceNumber = ((0x1ull << 56) - 1);
```

- **Range**: 0 to 2^56 - 1 (leaves 8 bits for `ValueType`)
- **Purpose**: MVCC version control
- **Monotonically increasing**: Each write operation increments the sequence
- **Snapshot isolation**: Readers can specify a sequence to read consistent data

---

## Comparator Design

### InternalKeyComparator

```cpp
class InternalKeyComparator {
public:
    explicit InternalKeyComparator(const Comparator* user_cmp);
    
    // Compare two InternalKeys
    int Compare(const Slice& a, const Slice& b) const;
    
private:
    const Comparator* user_comparator_;
};
```

**Comparison logic:**
```cpp
int InternalKeyComparator::Compare(const Slice& a, const Slice& b) const {
    // 1. Extract user keys (strip last 8 bytes)
    Slice user_a = ExtractUserKey(a);
    Slice user_b = ExtractUserKey(b);
    
    // 2. Compare user keys (ascending)
    int r = user_comparator_->Compare(user_a, user_b);
    if (r != 0) return r;
    
    // 3. Extract tags (last 8 bytes)
    uint64_t tag_a = DecodeFixed64(a.data() + a.size() - 8);
    uint64_t tag_b = DecodeFixed64(b.data() + b.size() - 8);
    
    // 4. Compare tags (descending - larger tag comes first)
    if (tag_a > tag_b) return -1;
    if (tag_a < tag_b) return +1;
    return 0;
}
```

---

## Design Rationale

### Why InternalKey?

1. **MVCC Support**: Multiple versions of the same key can coexist
2. **Snapshot Reads**: Readers can see a consistent view at a specific sequence
3. **Efficient Updates**: No need to physically delete old versions immediately
4. **Conflict Resolution**: Newer writes naturally override older ones

### Why Length-Prefixed Encoding?

- MemTable stores `const char*` pointers, not Key objects
- Length-prefixed format allows decoding without knowing the key size beforehand
- Efficient for variable-length keys

### Memory Layout Considerations

```plaintext
Traditional approach (not used):
  Node { Key key; Value value; Node* next[]; }
  
LevelDB approach:
  Node { const char* entry; Node* next[]; }
  
  entry points to:
  ┌──────────────┬─────────┬─────┬───────────┬───────┐
  │ internal_key │ user_key│ tag │ value_len │ value │
  │     _len     │         │     │           │       │
  └──────────────┴─────────┴─────┴───────────┴───────┘
```

**Benefits:**
- Single memory allocation for key+value+metadata
- Better cache locality
- Arena-friendly (no small object allocations)
- Pointer-sized node overhead

