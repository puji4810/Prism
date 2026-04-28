# tests/ - Test Suites

**Purpose**: GoogleTest-based unit and integration tests

## Structure

31 test files covering all components:

| Test File | Component Tested |
|-----------|------------------|
| `db_test.cpp` | End-to-end DB operations |
| `memtable_test.cpp` | MemTable/SkipList |
| `version_set_test.cpp` | VersionSet, compaction |
| `compaction_*_test.cpp` | Compaction logic |
| `table_test.cpp` | SSTable read/write |
| `block_test.cpp` | Block format |
| `filter_block_test.cpp` | Bloom filters |
| `log_*_test.cpp` | WAL reader/writer |
| `async*_test.cpp` | Async/coroutine API |
| `scheduler_*_test.cpp` | Thread pool scheduler |
| `arena_test.cpp` | Arena allocator |
| `skiplist_test.cpp` | SkipList |
| `coding_test.cpp` | Encoding utilities |
| `status_test.cpp` | Result/Status types |
| `env_posix_test.cpp` | Filesystem abstraction |
| `fault_injection_test.cpp` | Error handling |

## Build System

Tests defined in `tests/xmake.lua`:
```lua
target("memtable_test")
    set_kind("binary")
    add_files("memtable_test.cpp", "test_main.cpp")
    add_tests("default")
```

## Entry Points

Two entry point patterns:
- `test_main.cpp` - Standard GoogleTest main
- `main.cpp` - Used by some older tests (legacy)

## Running Tests

```bash
# All tests
xmake test

# Specific test
xmake build memtable_test
xmake r memtable_test

# With sanitizers
xmake f -m debug
xmake r db_test
```

## Test Conventions

- Use `TEST()` macros from GoogleTest
- Create temp directories via `env_->GetTestDirectory()`
- Clean up files in test destructor
- Use `ASSERT_*` for fatal errors, `EXPECT_*` for non-fatal
