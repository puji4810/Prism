#!/usr/bin/env python3
"""
compare_perf.py — Compare benchmark results between two directories.

Usage:
    compare_perf.py BASELINE_DIR CURRENT_DIR [--threshold METRIC=VALUE%] ... [--output FILE]

Arguments:
    BASELINE_DIR    Directory with benchmark results (from run_benchmark_matrix.sh)
    CURRENT_DIR     Directory with benchmark results to compare against baseline

Options:
    --threshold METRIC=VALUE%
        Set a regression threshold. METRIC is one of:
        throughput, task_clock, cycles, instructions, ipc,
        context_switches, l1d_misses, llc_loads, branch_misses.
        VALUE is a signed percentage. Negative means drop is bad
        (e.g. throughput=-10 means >10% ops/s drop = FAIL).
        Positive means increase is bad
        (e.g. task_clock=+20 means >20% increase = FAIL).
    --output FILE   Write markdown report to FILE instead of stdout.
    --json          Also emit a JSON summary to stdout (after markdown).

Exit codes:
    0  — All comparisons pass, no threshold exceeded.
    1  — One or more thresholds exceeded.
    2  — Invalid arguments or missing directories.

Supported environments:
    - Python 3.8+ (standard library only)
    - Linux with results from run_benchmark_matrix.sh

Failure modes:
    - Missing variant in one directory: skipped with N/A.
    - Missing perf data: metrics marked N/A, not a failure.
    - Missing stdout or parse error: variant skipped.
"""

import argparse
import json
import os
import re
import sys
from typing import Dict, List, Optional, Tuple


VARIANTS = [
    "v1-c24w24-vs1000",
    "v2-c24w8-vs1000",
    "v3-c48w24-vs1000",
    "v4-c24w24-vs100",
    "v5-c24w24-vs10000",
    "v6-c24w24-vs1000-inf16",
    "v7-c24w8-vs1000-inf16",
    "v8-c48w24-vs1000-inf16",
    "v9-c24w24-vs10000-inf16",
]

VARIANT_LABELS = {
    "v1-c24w24-vs1000": "V1: c24 w24 vs1000",
    "v2-c24w8-vs1000": "V2: c24 w8 vs1000",
    "v3-c48w24-vs1000": "V3: c48 w24 vs1000",
    "v4-c24w24-vs100": "V4: c24 w24 vs100",
    "v5-c24w24-vs10000": "V5: c24 w24 vs10000",
    "v6-c24w24-vs1000-inf16": "V6: c24 w24 vs1000 inf16",
    "v7-c24w8-vs1000-inf16": "V7: c24 w8 vs1000 inf16",
    "v8-c48w24-vs1000-inf16": "V8: c48 w24 vs1000 inf16",
    "v9-c24w24-vs10000-inf16": "V9: c24 w24 vs10000 inf16",
}

METRIC_NAMES = {
    "throughput": "Throughput (ops/s)",
    "task_clock": "Task-clock (msec)",
    "cycles": "CPU Cycles",
    "instructions": "Instructions",
    "ipc": "IPC",
    "context_switches": "Context-switches",
    "l1d_misses": "L1-dcache-load-misses",
    "llc_loads": "LLC-loads",
    "branch_misses": "Branch-misses",
}


def parse_number(s: str) -> float:
    s = s.replace(",", "").strip()
    if s.endswith("K"):
        return float(s[:-1]) * 1_000
    if s.endswith("M"):
        return float(s[:-1]) * 1_000_000
    if s.endswith("G"):
        return float(s[:-1]) * 1_000_000_000
    return float(s)


def parse_stdout(path: str) -> Optional[Dict[str, float]]:
    if not os.path.exists(path):
        return None
    with open(path, "r") as f:
        text = f.read()
    m = re.search(r"ops/s=(\d+)", text)
    if not m:
        return None
    return {"throughput": float(m.group(1))}


def parse_perf_stat(path: str) -> Optional[Dict[str, float]]:
    if not os.path.exists(path):
        return None
    with open(path, "r") as f:
        text = f.read()

    if "MISSING_PERF" in text:
        return None

    result: Dict[str, float] = {}

    # task-clock: "14,687.50 msec task-clock"
    m = re.search(r"([\d,.]+)\s+msec\s+task-clock", text)
    if m:
        result["task_clock"] = parse_number(m.group(1))

    # context-switches: "1,137,827      context-switches"
    m = re.search(r"([\d,.]+)\s+context-switches", text)
    if m:
        result["context_switches"] = parse_number(m.group(1))

    # cpu-cycles (with or without cpu_core/ prefix)
    m = re.search(r"([\d,.]+)\s+(?:cpu_core/)?cpu-cycles", text)
    if m:
        result["cycles"] = parse_number(m.group(1))

    # instructions
    m = re.search(r"([\d,.]+)\s+(?:cpu_core/)?instructions", text)
    if m:
        result["instructions"] = parse_number(m.group(1))

    # branch-misses
    m = re.search(r"([\d,.]+)\s+(?:cpu_core/)?branch-misses", text)
    if m:
        result["branch_misses"] = parse_number(m.group(1))

    # L1-dcache-load-misses
    m = re.search(r"([\d,.]+)\s+(?:cpu_core/)?L1-dcache-load-misses", text)
    if m:
        result["l1d_misses"] = parse_number(m.group(1))

    # LLC-loads
    m = re.search(r"([\d,.]+)\s+(?:cpu_core/)?LLC-loads", text)
    if m:
        result["llc_loads"] = parse_number(m.group(1))

    if "cycles" in result and "instructions" in result and result["cycles"] > 0:
        result["ipc"] = result["instructions"] / result["cycles"]

    return result if result else None


def parse_threshold(s: str) -> Tuple[str, float]:
    m = re.match(r"([\w_]+)=([+-]?\d+(?:\.\d+)?)%", s)
    if not m:
        raise ValueError(f"Invalid threshold format: {s}")
    return m.group(1), float(m.group(2))


def compute_delta(baseline: float, current: float) -> float:
    if baseline == 0:
        return 0.0
    return ((current - baseline) / baseline) * 100.0


def check_threshold(metric: str, baseline: Optional[float], current: Optional[float],
                    threshold: float) -> Tuple[str, Optional[float]]:
    if baseline is None or current is None:
        return "N/A", None
    delta = compute_delta(baseline, current)
    if threshold < 0:
        # Negative threshold: drop beyond this magnitude is a failure
        if delta < threshold:
            return "FAIL", delta
    else:
        # Positive threshold: increase beyond this magnitude is a failure
        if delta > threshold:
            return "FAIL", delta
    return "PASS", delta


def format_number(v: Optional[float], decimals: int = 0) -> str:
    if v is None:
        return "N/A"
    if decimals == 0:
        return f"{int(v):,}"
    return f"{v:,.{decimals}f}"


def format_delta(v: Optional[float]) -> str:
    if v is None:
        return "N/A"
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:.2f}%"


def discover_variants(baseline_dir: str, current_dir: str) -> List[str]:
    """Auto-discover variant directories when hardcoded VARIANTS don't match."""
    # Check if any hardcoded variant exists in either dir
    for v in VARIANTS:
        if (os.path.isfile(os.path.join(baseline_dir, v, "stdout.txt")) or
                os.path.isfile(os.path.join(current_dir, v, "stdout.txt"))):
            return VARIANTS

    # No hardcoded variants found; discover from both dirs
    discovered = set()
    for d in [baseline_dir, current_dir]:
        if not os.path.isdir(d):
            continue
        for entry in sorted(os.listdir(d)):
            subdir = os.path.join(d, entry)
            if os.path.isdir(subdir) and os.path.isfile(os.path.join(subdir, "stdout.txt")):
                discovered.add(entry)
    return sorted(discovered)


def build_report(baseline_dir: str, current_dir: str,
                 thresholds: Dict[str, float]) -> Tuple[str, bool, dict]:
    rows: List[dict] = []
    any_fail = False
    summary_metrics = ["throughput", "task_clock", "cycles", "instructions", "ipc",
                       "context_switches", "l1d_misses", "llc_loads", "branch_misses"]

    variants = discover_variants(baseline_dir, current_dir)

    for variant in variants:
        label = VARIANT_LABELS.get(variant, variant)
        base_stdout = os.path.join(baseline_dir, variant, "stdout.txt")
        base_perf = os.path.join(baseline_dir, variant, "perf-stat.txt")
        cur_stdout = os.path.join(current_dir, variant, "stdout.txt")
        cur_perf = os.path.join(current_dir, variant, "perf-stat.txt")

        base_data = parse_stdout(base_stdout) or {}
        base_data.update(parse_perf_stat(base_perf) or {})
        cur_data = parse_stdout(cur_stdout) or {}
        cur_data.update(parse_perf_stat(cur_perf) or {})

        row = {
            "variant": variant,
            "label": label,
            "baseline": {},
            "current": {},
            "deltas": {},
            "statuses": {},
            "has_data": bool(base_data or cur_data),
        }

        for metric in summary_metrics:
            b = base_data.get(metric)
            c = cur_data.get(metric)
            row["baseline"][metric] = b
            row["current"][metric] = c
            thresh = thresholds.get(metric)
            if thresh is not None and b is not None and c is not None:
                status, delta = check_threshold(metric, b, c, thresh)
                row["deltas"][metric] = delta
                row["statuses"][metric] = status
                if status == "FAIL":
                    any_fail = True
            elif b is not None and c is not None:
                row["deltas"][metric] = compute_delta(b, c)
                row["statuses"][metric] = "—"
            else:
                row["deltas"][metric] = None
                row["statuses"][metric] = "N/A"

        rows.append(row)

    lines: List[str] = []
    lines.append("# Perf Comparison Report")
    lines.append("")
    lines.append(f"**Baseline**: `{baseline_dir}`")
    lines.append(f"**Current**:  `{current_dir}`")
    lines.append("")

    lines.append("## Summary")
    lines.append("")
    lines.append("| Variant | Metric | Baseline | Current | Delta | Threshold | Status |")
    lines.append("|---------|--------|----------|---------|-------|-----------|--------|")

    for row in rows:
        first = True
        for metric in summary_metrics:
            name = METRIC_NAMES[metric]
            b = format_number(row["baseline"].get(metric), 2 if metric == "ipc" else 0)
            c = format_number(row["current"].get(metric), 2 if metric == "ipc" else 0)
            d = format_delta(row["deltas"].get(metric))
            thresh = thresholds.get(metric)
            thresh_str = f"{thresh:+.1f}%" if thresh is not None else "—"
            status = row["statuses"][metric]
            status_badge = f"**{status}**" if status == "FAIL" else status
            variant_cell = row["label"] if first else ""
            lines.append(f"| {variant_cell} | {name} | {b} | {c} | {d} | {thresh_str} | {status_badge} |")
            first = False
        if not first:
            lines.append("| | | | | | | |")
    lines.append("")

    lines.append("## Per-Variant Detail")
    lines.append("")

    for row in rows:
        if not row["has_data"]:
            lines.append(f"### {row['label']}")
            lines.append("")
            lines.append("*No data available for this variant.*")
            lines.append("")
            continue

        lines.append(f"### {row['label']}")
        lines.append("")
        lines.append("| Metric | Baseline | Current | Delta | Status |")
        lines.append("|--------|----------|---------|-------|--------|")
        for metric in summary_metrics:
            name = METRIC_NAMES[metric]
            b = format_number(row["baseline"].get(metric), 2 if metric == "ipc" else 0)
            c = format_number(row["current"].get(metric), 2 if metric == "ipc" else 0)
            d = format_delta(row["deltas"].get(metric))
            status = row["statuses"][metric]
            status_badge = f"**{status}**" if status == "FAIL" else status
            lines.append(f"| {name} | {b} | {c} | {d} | {status_badge} |")
        lines.append("")

    lines.append("## Overall Summary")
    lines.append("")
    if any_fail:
        lines.append("**Result: FAIL** — One or more thresholds exceeded.")
    else:
        lines.append("**Result: PASS** — All configured thresholds satisfied.")
    lines.append("")

    failed = []
    for row in rows:
        for metric, status in row["statuses"].items():
            if status == "FAIL":
                failed.append(f"- {row['label']} / {METRIC_NAMES[metric]}: "
                              f"delta={format_delta(row['deltas'][metric])}, "
                              f"threshold={thresholds[metric]:+.1f}%")
    if failed:
        lines.append("### Failures")
        lines.append("")
        lines.extend(failed)
        lines.append("")

    json_summary = {
        "baseline_dir": baseline_dir,
        "current_dir": current_dir,
        "thresholds": thresholds,
        "result": "FAIL" if any_fail else "PASS",
        "variants": rows,
    }

    return "\n".join(lines), any_fail, json_summary


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare benchmark results between two directories.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("baseline_dir", help="Baseline results directory")
    parser.add_argument("current_dir", help="Current results directory")
    parser.add_argument("--threshold", action="append", default=[],
                        help="Regression threshold (e.g. throughput=-10%%)")
    parser.add_argument("--output", help="Write report to file")
    parser.add_argument("--json", action="store_true", help="Emit JSON summary")

    args = parser.parse_args()

    if not os.path.isdir(args.baseline_dir):
        print(f"[ERROR] Baseline directory not found: {args.baseline_dir}", file=sys.stderr)
        return 2
    if not os.path.isdir(args.current_dir):
        print(f"[ERROR] Current directory not found: {args.current_dir}", file=sys.stderr)
        return 2

    thresholds: Dict[str, float] = {}
    for t in args.threshold:
        try:
            metric, value = parse_threshold(t)
            thresholds[metric] = value
        except ValueError as e:
            print(f"[ERROR] {e}", file=sys.stderr)
            return 2

    report, any_fail, json_summary = build_report(
        args.baseline_dir, args.current_dir, thresholds
    )

    if args.output:
        with open(args.output, "w") as f:
            f.write(report)
    else:
        print(report)

    if args.json:
        print(json.dumps(json_summary, indent=2))

    return 1 if any_fail else 0


if __name__ == "__main__":
    sys.exit(main())
