# Prism Benchmark Tools

This document describes Prism's benchmark suite — the `kv_bench` binary, the shell scripts that automate it, and the Python comparison tool.

## Quick Start

```bash
# Build
xmake f -m release && xmake build kv_bench

# Run a quick sync + async mixed workload
./build/linux/x86_64/release/kv_bench --run=both --bench=mixed --ops=10000

# Run the canonical 5-variant matrix with perf stat
./benchmark/run_benchmark_matrix.sh results_baseline

# Compare two results
python3 benchmark/compare_perf.py results_baseline/ results_optimized/ --threshold throughput=-10%
```

---

## 1. `kv_bench` — Core Benchmark Binary

Built from `benchmark/kv_bench.cpp` + `benchmark/kv_bench_lib.cpp`. Supports sync and async execution modes with configurable workload parameters.

### CLI Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--run=<mode>` | `sync\|async\|both` | `both` | Which execution model to benchmark |
| `--bench=<mode>` | `mixed\|disk_read\|sst_read_pipeline\|durability_write\|compaction_overlap` | `mixed` | Workload type |
| `--clients=<n>` | int | 4 | Number of concurrent clients |
| `--workers=<n>` | int | 4 | Worker threads for async scheduler |
| `--ops=<n>` | int | 10000 | Operations per client |
| `--value_size=<n>` | int | 100 | Value size in bytes |
| `--read_ratio=<n>` | int (0-100) | 0 | Percentage of reads in mixed mode |
| `--inflight_per_client=<n>` | int | 1 | Outstanding operations per client (async only) |
| `--rounds=<n>` | int | 3 | Number of measurement rounds |
| `--warmup_rounds=<n>` | int | 0 | Warmup rounds before measurement |
| `--write_buffer_size=<n>` | int | 4MB | MemTable flush threshold |
| `--prefill=<n>` | int (-1/0/1) | -1 | Prefill database before benchmark: -1=auto, 0=off, 1=force |
| `--db_dir=<path>` | string | temp | Use existing directory (enables reuse across runs) |
| `--keep_db=<0\|1>` | int | 0 | Keep DB directory after run |
| `--sync` | flag | — | Run only sync benchmark (shorthand) |
| `--async` | flag | — | Run only async benchmark (shorthand) |
| `--no_latency` | flag | off | Skip p50/p95 percentile collection |
| `--phase=<mode>` | `full\|prefill_only\|warmup_only\|steady_state\|compaction_overlap_only` | `full` | Profiling phase isolation |
| `--profile-pause-prefill` | flag | off | Pause VTune/ITT during prefill phase |
| `--help` | flag | — | Print help |

### Building `kv_bench` for Perf Profiling

`PRISM_RUNTIME_METRICS` enables per-lane queue-depth / enqueue-wait / execution-time counters.
This instrumentation adds **~15% overhead** and will skew `perf record` / `perf stat` results.

**By default, `PRISM_RUNTIME_METRICS` is disabled.** Enable it only when you need the
`RuntimeMetrics` counters (e.g. to verify lane queue depths during a new scheduler optimization).

**To build with runtime metrics enabled:**

```bash
xmake f -m release --runtime_metrics=y && xmake build kv_bench
```

**For throughput/latency/perf profiling, build without runtime metrics (default):**

```bash
xmake f -m release && xmake build kv_bench
```

### Benchmark Modes

| Mode | Description | Requires |
|------|-------------|----------|
| `mixed` | Interleaved reads and writes at `--read_ratio` | — |
| `disk_read` | Reopen DB after prefill, then read-only | Disk-backed DB |
| `sst_read_pipeline` | SSTable read path stress (async only) | Prefilled DB |
| `durability_write` | Write with `sync=true` for WAL-fsync stress | — |
| `compaction_overlap` | Read + write during active compaction (async only) | Prefilled DB |

### Output Format

Each round prints:
```
ops/s=382023  time=2.6168s  total_ops=1000000  p50_us=23.20  p95_us=31.10  rounds=2/3
```
Plus summary lines:
```
max_inflight=4 max_client_inflight=2 bg_sleeps=0
```

When `--no_latency` is set, `p50_us` and `p95_us` are omitted (lower overhead, suitable for perf sampling).

### Usage Examples

```bash
# Sync-only mixed, 99% reads, 24 clients, 50k ops each
kv_bench --run=sync --bench=mixed --clients=24 --ops=50000 --read_ratio=99 --no_latency

# Async mixed with 16 in-flight per client, 60% reads
kv_bench --run=async --bench=mixed --clients=24 --workers=24 --ops=50000 \
  --value_size=1000 --inflight_per_client=16 --read_ratio=60 --prefill=1 --no_latency

# Disk read workload (reopen + read-only after prefill)
kv_bench --run=async --bench=disk_read --clients=8 --ops=10000 --value_size=1000

# Durability write benchmark
kv_bench --run=sync --bench=durability_write --ops=10000 --value_size=100

# Compaction overlap benchmark
kv_bench --run=async --bench=compaction_overlap --clients=4 --ops=20000 --value_size=1000
```

---

## 2. `run_benchmark_matrix.sh` — Canonical 5-Variant Matrix

Runs a standardized set of benchmark variants with `perf stat` for hardware counter collection.

### Usage

```bash
./benchmark/run_benchmark_matrix.sh [OUTPUT_DIR]
```

| Argument | Default | Description |
|----------|---------|-------------|
| `OUTPUT_DIR` | `benchmark_results` | Directory for per-variant results |

### Environment Variables

| Variable | Effect |
|----------|--------|
| `KV_BENCH` | Path to `kv_bench` binary (default: `build/linux/x86_64/release/kv_bench`) |
| `SKIP_PERF=1` | Skip `perf stat` capture (still runs benchmarks) |

> **⚠️ Perf Profiling:** For accurate `perf stat` results, build `kv_bench` **without** `PRISM_RUNTIME_METRICS` (see [Building for Perf](#building-kv_bench-for-perf-profiling)). The instrumentation adds ~15% overhead.

### Variants

| Variant | Clients | Workers | Value Size | Inflight | Focus |
|---------|---------|---------|------------|----------|-------|
| `v1-c24w24-vs1000` | 24 | 24 | 1000B | 1 | Balanced throughput |
| `v2-c24w8-vs1000` | 24 | 8 | 1000B | 1 | Worker-limited |
| `v3-c48w24-vs1000` | 48 | 24 | 1000B | 1 | Client saturation |
| `v4-c24w24-vs100` | 24 | 24 | 100B | 1 | Small-value, cache-friendly |
| `v5-c24w24-vs10000` | 24 | 24 | 10000B | 1 | Large-value, disk-backed |
| `v6-c24w24-vs1000-inf16` | 24 | 24 | 1000B | 16 | Balanced + high inflight |
| `v7-c24w8-vs1000-inf16` | 24 | 8 | 1000B | 16 | Worker-limited + high inflight |
| `v8-c48w24-vs1000-inf16` | 48 | 24 | 1000B | 16 | Client saturation + high inflight |
| `v9-c24w24-vs10000-inf16` | 24 | 24 | 10000B | 16 | Large-value disk + high inflight |

All variants use: `--run=async --bench=mixed --ops=50000 --prefill=1 --no_latency --rounds=1 --read_ratio=100 --keep_db=0`

### Output Structure

```
benchmark_results/
  v1-c24w24-vs1000/
    stdout.txt          — raw kv_bench output
    perf-stat.txt       — perf stat hardware counters (or MISSING_PERF marker)
    exitcode.txt        — kv_bench exit code
  v2-c24w8-vs1000/
    ...
  ...
```

### Usage Examples

```bash
# Full matrix run
./benchmark/run_benchmark_matrix.sh baseline_results

# With custom binary path
KV_BENCH=./build/my_kv_bench ./benchmark/run_benchmark_matrix.sh custom_results

# Without perf (e.g., in CI without perf_event access)
SKIP_PERF=1 ./benchmark/run_benchmark_matrix.sh ci_results
```

---

## 3. `compare_perf.py` — Results Comparison Tool

Compares two benchmark runs and produces a markdown report with delta percentages and regression detection.

### Usage

```bash
python3 benchmark/compare_perf.py BASELINE_DIR CURRENT_DIR [OPTIONS]
```

### Options

| Option | Description |
|--------|-------------|
| `--threshold METRIC=±VALUE%` | Set regression threshold (repeatable). Negative = drop is bad. Positive = increase is bad. |
| `--output FILE` | Write markdown report to file instead of stdout |
| `--json` | Also emit JSON summary to stdout |

### Supported Metrics

| Metric Key | Description | Typical Threshold |
|------------|-------------|-------------------|
| `throughput` | ops/s | `--threshold throughput=-10%` |
| `task_clock` | Task-clock in msec | `--threshold task_clock=+20%` |
| `cycles` | CPU Cycles | — |
| `instructions` | Instructions | — |
| `ipc` | Instructions per cycle | — |
| `context_switches` | Context-switches | `--threshold context_switches=+0%` |
| `l1d_misses` | L1-dcache-load-misses | — |
| `llc_loads` | LLC-loads | — |
| `branch_misses` | Branch-misses | — |

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | All comparisons pass |
| 1 | One or more thresholds exceeded |
| 2 | Invalid arguments or missing directories |

### Usage Examples

```bash
# Basic comparison (no thresholds — informational only)
python3 benchmark/compare_perf.py baseline_results/ optimized_results/

# With regression gates
python3 benchmark/compare_perf.py baseline_results/ optimized_results/ \
    --threshold throughput=-10% \
    --threshold context_switches=+0%

# Output to file + JSON
python3 benchmark/compare_perf.py baseline/ current/ \
    --threshold throughput=-10% \
    --output report.md --json
```

### Sample Output

```markdown
# Perf Comparison Report

**Baseline**: `baseline_results/`
**Current**:  `optimized_results/`

## Summary

| Variant | Metric | Baseline | Current | Delta | Threshold | Status |
|---------|--------|----------|---------|-------|-----------|--------|
| V1: c24 w24 vs1000 | Throughput (ops/s) | 388,990 | 1,138,247 | +192.59% | — | — |
| V1: c24 w24 vs1000 | Context-switches | 1,137,827 | 147,891 | -87.00% | +0.0% | PASS |
...
```

---

## 4. `acceptance_harness.sh` — Before/After Acceptance Harness

Runs a lightweight set of sync+async variants and produces output compatible with `compare_perf.py`. Designed for quick before/after regression testing during development.

### Usage

```bash
./benchmark/acceptance_harness.sh [OPTIONS]
```

### Options

| Option | Description |
|--------|-------------|
| `--output-dir DIR` | Root directory for results (default: `acceptance_results`) |
| `--variants NAME1,NAME2,...` | Comma-separated list of specific variants to run |
| `--kv-bench PATH` | Path to `kv_bench` binary |
| `--build-first` | Build `kv_bench` before running |
| `--skip-perf` | Skip `perf stat` capture |
| `--help` | Show help |

### Built-in Variants

| Variant | Run | Clients | Workers | Ops | VS | RR | Inflight | Notes |
|---------|-----|---------|---------|-----|----|----|----------|-------|
| `sync-small` | sync | 2 | 2 | 1000 | 100 | 100 | 1 | Fast smoke test |
| `async-small` | async | 2 | 2 | 1000 | 100 | 100 | 1 | Fast smoke test |
| `sync-medium` | sync | 4 | 4 | 5000 | 1000 | 100 | 1 | Medium load |
| `async-medium` | async | 4 | 4 | 5000 | 1000 | 100 | 1 | Medium load |
| `async-inflight` | async | 4 | 4 | 5000 | 1000 | 100 | 4 | Inflight contention |
| `async-latency` | async | 2 | 2 | 500 | 100 | 100 | 1 | With p50/p95 collection |

Each run creates a timestamped subdirectory under `--output-dir`, making it safe for repeated runs.

### Output Structure

```
acceptance_results/
  20260428_143022/           # timestamped run directory
    sync-small/
      stdout.txt
      perf-stat.txt
      exitcode.txt
      metadata.txt
    async-small/
      stdout.txt
      ...
    summary.txt              # aggregated results table
```

### Usage Examples

```bash
# Full acceptance run
./benchmark/acceptance_harness.sh

# Only specific variants
./benchmark/acceptance_harness.sh --variants async-medium,async-inflight

# Compare two runs (different timestamps)
python3 benchmark/compare_perf.py \
    acceptance_results/20260428_120000/ \
    acceptance_results/20260428_143022/
```

---

## 5. `profile_workflow.sh` — FlameGraph Profiling Workflow

Profiles a single benchmark variant with `perf record` and generates an interactive FlameGraph SVG for hotspot analysis.

### Usage

```bash
./benchmark/profile_workflow.sh --variant NAME --bench-args "..." [OPTIONS]
```

### Options

| Option | Description |
|--------|-------------|
| `--variant NAME` | **(required)** Variant identifier (used as sub-directory) |
| `--bench-args "..."` | **(required)** Arguments passed to `kv_bench` |
| `--output-dir DIR` | Output root (default: `evidence/`) |
| `--kv-bench PATH` | Path to `kv_bench` binary |
| `--help` | Show help |

### Environment

| Variable | Effect |
|----------|--------|
| `PROFILE_WORKFLOW_SKIP_PERF=1` | Skip all perf operations |

### Requirements

- `perf` (linux-tools)
- [FlameGraph](https://github.com/brendangregg/FlameGraph) scripts at `third-party/FlameGraph/`
- `/proc/sys/kernel/perf_event_paranoid` <= 2 for non-root perf
- **IMPORTANT**: Build `kv_bench` **without** `PRISM_RUNTIME_METRICS` before profiling — the instrumentation adds ~15% overhead and distorts the flamegraph. See [Building kv_bench for Perf Profiling](#building-kv_bench-for-perf-profiling).

### Output Structure

Per variant:
```
evidence/{variant}/
  benchmark-stdout.txt    — raw kv_bench output
  benchmark-cmd.txt       — full command line used
  perf-stat.txt           — perf stat counters
  perf-record.data        — perf record sampling data
  stacks.folded           — collapsed stacks
  flamegraph.svg          — generated FlameGraph SVG (open in browser)
  metadata.txt            — timestamp, hostname, perf version
```

### Usage Examples

```bash
# Profile a specific workload
./benchmark/profile_workflow.sh \
    --variant async_mixed_c24w24 \
    --bench-args "--run=async --bench=mixed --clients=24 --workers=24 --ops=50000 --value_size=1000 --read_ratio=99 --prefill=1 --no_latency --rounds=1"

# Custom output directory
./benchmark/profile_workflow.sh \
    --variant compaction_overlap \
    --output-dir profiling_results \
    --bench-args "--run=async --bench=compaction_overlap --clients=4 --ops=20000 --value_size=1000"

# Open the generated flamegraph
firefox evidence/async_mixed_c24w24/flamegraph.svg
```

---

## 6. `async_optimization.md` — Optimization Guide

See `docs/async_optimization.md` for the comprehensive performance optimization guide that includes:

- Performance baseline data (small vs large working sets)
- Phase 1: Scheduler optimizations (lock-free queues, batch submission, inline execution)
- Phase 2: Cache miss optimizations (prefetching, Bloom filters, NUMA)
- Phase 3: Pipeline optimizations (PGO, code layout, devirtualization, SIMD)
- Measurement and regression testing workflows

## Common Workflows

### Pre-commit Regression Check

```bash
# 1. Capture baseline
xmake f -m release && xmake build kv_bench
./benchmark/acceptance_harness.sh --output-dir regress_check --skip-perf
BASELINE=$(ls -d regress_check/*/ | tail -n1)

# 2. Make changes, rebuild
xmake f -m release && xmake build kv_bench

# 3. Run again and compare
./benchmark/acceptance_harness.sh --output-dir regress_check --skip-perf
CURRENT=$(ls -d regress_check/*/ | tail -n1)

python3 benchmark/compare_perf.py "$BASELINE" "$CURRENT" \
    --threshold throughput=-5%
```

### Full Performance Characterization

```bash
# 0. Build WITHOUT PRISM_RUNTIME_METRICS for accurate perf results
#    Comment out line 14 (`add_defines("PRISM_RUNTIME_METRICS")`) in benchmark/xmake.lua
xmake f -m release && xmake build kv_bench

# 1. Run the full 9-variant matrix
./benchmark/run_benchmark_matrix.sh baseline_results

# 2. Pick a hotspot for flamegraph analysis
./benchmark/profile_workflow.sh \
    --variant v5_disk_backed \
    --bench-args "--run=async --bench=mixed --clients=24 --workers=24 --ops=50000 --value_size=10000 --read_ratio=99 --prefill=1 --no_latency --rounds=1"
```

### CI-Compatible (No perf Required)

```bash
SKIP_PERF=1 ./benchmark/run_benchmark_matrix.sh ci_results

# Or with acceptance harness
./benchmark/acceptance_harness.sh --skip-perf --variants sync-small,async-small
```

---

## File Reference

| File | Language | Purpose |
|------|----------|---------|
| `kv_bench.cpp` | C++23 | Benchmark main entry point |
| `kv_bench_lib.cpp` | C++23 | Benchmark engine (sync/async runners, stats, parsing) |
| `kv_bench_lib.h` | C++23 | Public benchmark API |
| `bench_env_wrapper.h` | C++ | Environment wrapper for benchmarks |
| `async_bench.cpp` | C++23 | Async microbenchmark |
| `executor_microbench.cpp` | C++23 | Executor queue isolation microbenchmark |
| `function_overhead_bench.cpp` | C++23 | `std::function` vs custom callable overhead |
| `run_benchmark_matrix.sh` | Bash | 5-variant throughput matrix with perf stat |
| `acceptance_harness.sh` | Bash | Before/after acceptance suite |
| `profile_workflow.sh` | Bash | FlameGraph profiling workflow |
| `compare_perf.py` | Python 3 | Perf comparison with regression gates |
| `xmake.lua` | Lua | Build targets for benchmark binaries |

## Adding New Benchmarks

To add a new benchmark workload:

1. **Define the mode** in `BenchMode` enum (`kv_bench_lib.h`)
2. **Implement a runner** in `kv_bench_lib.cpp` (sync and/or async client functions)
3. **Wire it** into the `main()` dispatch in `kv_bench.cpp`
4. **Add a variant** to `run_benchmark_matrix.sh` for automated coverage
5. **Update** `compare_perf.py` `VARIANTS` list to recognize new variant names
