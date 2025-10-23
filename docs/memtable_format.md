# MemTable Design

## Overview

MemTable is an in-memory write buffer that:

- Stores recent writes in sorted order
- Uses SkipList for efficient lookups and iteration
- Manages memory with Arena allocator
- Supports MVCC through InternalKey encoding

```plaintext
┌──────────────────────────────────────────────┐
│              MemTable                        │
├──────────────────────────────────────────────┤
│  - Arena (memory pool)                       │
│  - SkipList<const char*, KeyComparator>      │
│  - Reference counting                        │
│  - InternalKeyComparator                     │
└──────────────────────────────────────────────┘
         │
         ├─> Entry 1: const char* ──> [encoded data in Arena]
         ├─> Entry 2: const char* ──> [encoded data in Arena]
         └─> Entry N: const char* ──> [encoded data in Arena]
```

---

## Entry Encoding Format

Each entry in MemTable is encoded as a single byte array allocated from Arena.

```mermaid
---
title: "MemTable Entry Format"
---
packet-beta
0-31: "internal_key_len: varint32"
32-255: "user_key: bytes[user_key_len]"
256-319: "tag: uint64 (seq << 8 | type)"
320-351: "value_len: varint32"
352-383: "value: bytes[value_len]"
```

```plaintext
Entry Layout (stored in Arena):
┌─────────────────┬──────────────┬─────┬─────────────┬──────────────┐
│ internal_key_len│  user_key    │ tag │ value_len   │   value      │
│   (varint32)    │(user_key_len)│(u64)│ (varint32)  │ (value_len)  │
└─────────────────┴──────────────┴─────┴─────────────┴──────────────┘
     ↑                                                       ↑
   buf (inserted into SkipList)                             end

where:
  internal_key_len = user_key_len + 8
  tag = (sequence << 8) | type
```

**Size calculation:**

```cpp
size_t encoded_len = 
    VarintLength(internal_key_size) +  // 1-5 bytes
    internal_key_size +                 // user_key_len + 8
    VarintLength(value_size) +          // 1-5 bytes
    value_size;                         // value_len
```

---

## MemTable::Add() - Insert Operation

```cpp
void MemTable::Add(SequenceNumber seq, ValueType type, 
                   const Slice& key, const Slice& value)
{
    // Step 1: Calculate sizes
    size_t key_size = key.size();
    size_t val_size = value.size();
    size_t internal_key_size = key_size + 8;
    size_t encoded_len = VarintLength(internal_key_size) +
                         internal_key_size + 
                         VarintLength(val_size) + val_size;
  
    // Step 2: Allocate from Arena (single allocation)
    char* buf = arena_.Allocate(encoded_len);
  
    // Step 3: Encode entry
    char* p = EncodeVarint32(buf, internal_key_size);
    std::memcpy(p, key.data(), key_size);
    p += key_size;
    EncodeFixed64(p, (seq << 8) | type);
    p += 8;
    p = EncodeVarint32(p, val_size);
    std::memcpy(p, value.data(), val_size);
  
    // Step 4: Insert pointer into SkipList
    table_.Insert(buf);
}
```

**Example:**

```plaintext
Input:
  sequence = 100
  type = kTypeValue (0x1)
  key = "foo" (3 bytes)
  value = "bar" (3 bytes)

Encoded entry (hex):
  0B        // internal_key_len = 11 (varint32)
  66 6F 6F  // "foo"
  01 64 00 00 00 00 00 00  // tag = (100 << 8) | 1
  03        // value_len = 3 (varint32)
  62 61 72  // "bar"
  
Total: 1 + 3 + 8 + 1 + 3 = 16 bytes
```

---

## MemTable::Get() - Lookup Operation

```cpp
bool MemTable::Get(const LookupKey& key, std::string* value, Status* s)
{
    // Step 1: Get memtable_key from LookupKey
    Slice memkey = key.memtable_key();
  
    // Step 2: Seek to first entry >= memkey
    Table::Iterator iter(&table_);
    iter.Seek(memkey.data());
  
    if (iter.Valid()) {
        const char* entry = iter.key();
      
        // Step 3: Parse entry and extract internal_key
        uint32_t key_length;
        const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
      
        // Step 4: Compare user_key
        if (comparator_.user_comparator()->Compare(
                Slice(key_ptr, key_length - 8),  // Extract user_key
                key.user_key()) == 0) {
          
            // Step 5: Check type in tag
            uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
            ValueType type = static_cast<ValueType>(tag & 0xff);
          
            switch (type) {
                case kTypeValue: {
                    // Step 6: Extract value
                    Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
                    value->assign(v.data(), v.size());
                    return true;
                }
                case kTypeDeletion:
                    *s = Status::NotFound();
                    return true;
            }
        }
    }
    return false;  // Key not found
}
```

**Search example:**

```plaintext
Query: user_key = "foo", sequence = 200

LookupKey encoding:
  0B 66 6F 6F [C8 00 00 00 00 00 00 01]
  │  └─ "foo" └─ tag = (200 << 8) | kTypeValue
  └─ internal_key_len = 11

SkipList has:
  Entry1: 0B 66 6F 6F [64 00 00 00 00 00 00 01] 03 62 61 72  (seq=100)
  Entry2: 0B 66 6F 6F [C8 00 00 00 00 00 00 01] 03 62 61 7A  (seq=200)
  Entry3: 0B 66 6F 6F [2C 01 00 00 00 00 00 01] 03 62 61 78  (seq=300)

Seek(LookupKey) → finds Entry2 (first entry >= search key)
Since InternalKeyComparator sorts by (user_key ASC, seq DESC),
Entry3 (seq=300) comes before Entry2 (seq=200)

Corrected SkipList order:
  Entry3: seq=300 (newest)
  Entry2: seq=200
  Entry1: seq=100 (oldest)

Seek with seq=200 → should find Entry2 or Entry3 depending on exact comparison
```

---

## KeyComparator Design

MemTable's SkipList compares `const char*` pointers, not InternalKey objects.

```cpp
struct KeyComparator {
    const InternalKeyComparator comparator;
  
    explicit KeyComparator(const InternalKeyComparator& c) 
        : comparator(c) {}
  
    // Compare two entry pointers
    int operator()(const char* aptr, const char* bptr) const {
        // Extract InternalKey from length-prefixed encoding
        Slice a = GetLengthPrefixedSlice(aptr);
        Slice b = GetLengthPrefixedSlice(bptr);
      
        // Use InternalKeyComparator
        return comparator.Compare(a, b);
    }
};
```

**How it works:**

```plaintext
aptr → [0B 66 6F 6F 64 00 00 00 00 00 00 01 03 62 61 72]
        └─ internal_key_len=11
                │
                v
       GetLengthPrefixedSlice
                │
                v
       Slice("foo\x64\x00\x00\x00\x00\x00\x00\x01", 11)
                │
                v
       InternalKeyComparator::Compare()
```

---

## Reference Counting

MemTable uses manual reference counting to manage lifetime.

```cpp
class MemTable {
public:
    explicit MemTable(const InternalKeyComparator& cmp)
        : comparator_(cmp), refs_(0), table_(comparator_, &arena_) {}
  
    void Ref() { ++refs_; }
  
    void Unref() {
        --refs_;
        assert(refs_ >= 0);
        if (refs_ <= 0) {
            delete this;  // Self-destruct
        }
    }
  
private:
    ~MemTable() { assert(refs_ == 0); }  // Private destructor
  
    int refs_;
    // ...
};
```

**Usage pattern:**

```cpp
// DBImpl holds MemTable
MemTable* mem_ = new MemTable(comparator);
mem_->Ref();  // refs_ = 1

// When switching to immutable MemTable
MemTable* imm_ = mem_;
imm_->Ref();  // refs_ = 2

mem_ = new MemTable(comparator);
mem_->Ref();  // refs_ = 1

// After compaction completes
imm_->Unref();  // refs_ = 1
imm_->Unref();  // refs_ = 0 → delete this
```

**Why reference counting?**

- Multiple components may hold MemTable (DBImpl, iterators, compaction)
- Allows safe concurrent reads during compaction
- Prevents premature deletion

---

## Memory Management

### Arena Integration

```cpp
class MemTable {
private:
    Arena arena_;
    Table table_;  // SkipList uses &arena_
};

// SkipList constructor
SkipList(Comparator cmp, Arena* arena)
    : arena_(arena), ... {}

// Node allocation
Node* SkipList::NewNode(const Key& key, int height) {
    char* mem = arena_->AllocateAligned(
        sizeof(Node) + sizeof(atomic<Node*>) * (height - 1));
    return new (mem) Node(key);
}
```

**Memory layout:**

```plaintext
Arena blocks:
┌──────────────────────────────────────────────┐
│ Block 1 (4KB)                                │
├──────────────────────────────────────────────┤
│ Entry1 [key+value data]                      │
│ SkipList Node1 [pointers]                    │
│ Entry2 [key+value data]                      │
│ SkipList Node2 [pointers]                    │
│ ...                                          │
└──────────────────────────────────────────────┘
┌──────────────────────────────────────────────┐
│ Block 2 (4KB)                                │
├──────────────────────────────────────────────┤
│ Entry N [key+value data]                     │
│ ...                                          │
└──────────────────────────────────────────────┘
```

**Benefits:**

- No per-object allocation overhead
- Better cache locality
- Fast allocation (bump-pointer in most cases)
- Bulk deallocation (free entire MemTable at once)

---

## Iterator Design

```cpp
class MemTableIterator : public Iterator {
public:
    explicit MemTableIterator(MemTable::Table* table) 
        : iter_(table) {}
  
    bool Valid() const override { return iter_.Valid(); }
    void Next() override { iter_.Next(); }
    void Prev() override { iter_.Prev(); }
    void SeekToFirst() override { iter_.SeekToFirst(); }
    void SeekToLast() override { iter_.SeekToLast(); }
  
    void Seek(const Slice& target) override {
        iter_.Seek(EncodeKey(&tmp_, target));
    }
  
    Slice key() const override {
        return GetLengthPrefixedSlice(iter_.key());
    }
  
    Slice value() const override {
        Slice key_slice = GetLengthPrefixedSlice(iter_.key());
        return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
    }
  
private:
    MemTable::Table::Iterator iter_;  // SkipList::Iterator
    std::string tmp_;  // Scratch space for encoding
};
```

---

## Approximate Memory Usage

```cpp
size_t MemTable::ApproximateMemoryUsage() {
    return arena_.MemoryUsage();
}
```

**What's counted:**

- All Arena blocks (entry data + SkipList nodes)
- Block overhead (`sizeof(char*)` per block)

**What's NOT counted:**

- MemTable object itself (`sizeof(MemTable)`)
- KeyComparator
- SkipList metadata (head node is in Arena)

**Usage:**

```cpp
// DBImpl decides when to create immutable MemTable
if (mem_->ApproximateMemoryUsage() > options_.write_buffer_size) {
    // Switch to new MemTable
    imm_ = mem_;
    mem_ = new MemTable(comparator_);
    // Trigger compaction...
}
```

---

## Design Decisions

### Why `const char*` instead of Key objects?

**Rejected design:**

```cpp
using Table = SkipList<InternalKey, Comparator>;
```

**Problems:**

- Each node stores a copy of InternalKey
- Expensive key copies during insertion
- Separate allocation for value

**Chosen design:**

```cpp
using Table = SkipList<const char*, KeyComparator>;
```

**Benefits:**

- Single allocation for key + value + metadata
- Zero-copy insertion (just pointer)
- Arena-friendly (variable-size data)

### Why length-prefixed encoding?

**Alternative: Fixed struct**

```cpp
struct Entry {
    uint32_t key_len;
    uint32_t value_len;
    char data[];
};
```

**Problems:**

- Wastes space for small keys/values
- Alignment padding
- Harder to extend format

**Length-prefixed (varint) benefits:**

- Compact (1 byte for lengths < 128)
- Self-describing
- Easy to parse forward

### Why Arena for MemTable?

**Benefits:**

- Fast allocation (no malloc overhead)
- No fragmentation
- Bulk deallocation
- Cache-friendly (sequential allocations)

**Trade-offs:**

- Cannot delete individual entries
- Memory held until entire MemTable is freed
- OK for MemTable because it's immutable after becoming `imm_`

---

## Performance Characteristics


| Operation         | Time Complexity | Notes                      |
| ----------------- | --------------- | -------------------------- |
| Add               | O(log N)        | SkipList insertion         |
| Get               | O(log N)        | SkipList search            |
| Iterator creation | O(1)            | Lightweight wrapper        |
| Iterator advance  | O(1) amortized  | SkipList traversal         |
| Memory usage      | O(N)            | Linear in data size        |
| Destruction       | O(B)            | B = number of Arena blocks |

**Space overhead:**

```plaintext
Per entry:
  - Varint lengths: 2-10 bytes (typically 2)
  - Tag: 8 bytes
  - SkipList node: 16-32 bytes (depends on height)
  Total overhead: ~26-50 bytes per entry

For 1M entries of 100 bytes each:
  Data: 100 MB
  Overhead: ~26-50 MB
  Total: ~126-150 MB
```
