# src/ - Core Engine Implementation

**Purpose**: Database engine core implementation

## Files

| File | Purpose |
|------|---------|
| `db_impl.cpp` | Main DB implementation (48K lines) - write path, compaction, recovery |
| `memtable.cpp` | In-memory write buffer operations |
| `version_set.cpp` | Metadata management, compaction picker |
| `version_edit.cpp` | Version change records |
| `log_reader.cpp` | WAL read/parse |
| `log_writer.cpp` | WAL append |
| `cache.cpp` | LRU block cache |
| `table_cache.cpp` | SSTable file cache |
| `write_batch.cpp` | Batch operation encoding |
| `asyncdb.cpp` | Async database API |
| `async_env.cpp` | Async environment |
| `dbformat.cpp` | Internal key encoding |
| `filename.cpp` | File naming conventions |
| `comparator.cpp` | Key comparison |
| `iterator.cpp` | Iterator base |

## Key Classes

- **DBImpl**: Main database instance, owns MemTable, WAL, VersionSet
- **Compaction**: Background merge logic
- **RecoveryHandler**: Startup recovery from WAL

## Conventions

- All code in `prism` namespace
- Heavy use of `std::unique_ptr` for ownership
- Mutex-protected shared state
- Background work via the compaction controller (`CompactionController`)

## Testing

Tests are in `tests/`, not here. See `tests/db_test.cpp` for integration tests.
