#!/usr/bin/env bash
#
# profile_workflow.sh — Profile kv_bench with perf stat/record and FlameGraph SVG output.
#
# Usage:
#   ./profile_workflow.sh --variant NAME --bench-args "..." [options]
#
# Required:
#   --variant NAME       Variant identifier (used as sub-directory name)
#   --bench-args "..."   Arguments passed to kv_bench (e.g. "--run=async --bench=mixed ...")
#
# Options:
#   --output-dir DIR     Output root (default: root/evidence)
#   --kv-bench PATH      Path to kv_bench binary (default: build/linux/x86_64/release/kv_bench)
#   --help               Print this help message
#
# Environment:
#   PROFILE_WORKFLOW_SKIP_PERF   Set to 1 to skip all perf operations (still runs benchmarks)
#
# Output structure (deterministic per variant):
#   {output-dir}/{variant}/
#     benchmark-stdout.txt    — raw benchmark stdout+stderr
#     benchmark-cmd.txt       — full command line used
#     perf-stat.txt           — perf stat counters (or MISSING_PERF marker)
#     perf-record.data        — perf record sampling data (if perf available)
#     stacks.folded           — collapsed stacks from stackcollapse-perf.pl
#     flamegraph.svg          — generated FlameGraph SVG
#     metadata.txt            — timestamp, variant params, hostname, perf version
#
# Exit codes:
#   0  — Benchmark completed successfully (perf is optional)
#   1  — kv_bench binary missing and build failed
#   2  — kv_bench run failed (non-zero exit)
#   3  — Invalid arguments (missing --variant or --bench-args)
#
# Supported environments:
#   - Linux with bash 4.2+, xmake, and optional perf (linux-tools)
#   - Non-root perf works if /proc/sys/kernel/perf_event_paranoid <= 2
#   - Missing perf is handled gracefully: MISSING_PERF marker written
#   - PROFILE_WORKFLOW_SKIP_PERF=1 skips all perf operations entirely

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ------------------------------------------------------------------
# Defaults
# ------------------------------------------------------------------
DEFAULT_OUTPUT_DIR="${PROJECT_ROOT}/evidence"
DEFAULT_KV_BENCH="${PROJECT_ROOT}/build/linux/x86_64/release/kv_bench"
FLAMEGRAPH_DIR="${PROJECT_ROOT}/third-party/FlameGraph"

OUTPUT_DIR=""
VARIANT=""
BENCH_ARGS=""
KV_BENCH="${KV_BENCH:-${DEFAULT_KV_BENCH}}"
SKIP_PERF="${PROFILE_WORKFLOW_SKIP_PERF:-0}"

# ------------------------------------------------------------------
# Help
# ------------------------------------------------------------------
usage() {
	cat >&2 <<'HELP'
Usage: profile_workflow.sh --variant NAME --bench-args "..." [options]

Required:
  --variant NAME       Variant identifier (used as sub-directory name)
  --bench-args "..."   Arguments passed to kv_bench (e.g. "--run=async --bench=mixed ...")

Options:
  --output-dir DIR     Output root (default: root/evidence)
  --kv-bench PATH      Path to kv_bench binary (default: build/linux/x86_64/release/kv_bench)
  --help               Print this help message

Environment:
  PROFILE_WORKFLOW_SKIP_PERF   Set to 1 to skip all perf operations

Output structure (deterministic per variant):
  {output-dir}/{variant}/
    benchmark-stdout.txt    — raw benchmark stdout+stderr
    benchmark-cmd.txt       — full command line used
    perf-stat.txt           — perf stat counters (or MISSING_PERF marker)
    perf-record.data        — perf record sampling data (if perf available)
    stacks.folded           — collapsed stacks from stackcollapse-perf.pl
    flamegraph.svg          — generated FlameGraph SVG
    metadata.txt            — timestamp, variant params, hostname, perf version
HELP
	exit 0
}

# ------------------------------------------------------------------
# Parse arguments
# ------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
	case "$1" in
		--help)
			usage
			;;
		--variant)
			if [[ -z "${2:-}" ]]; then
				echo "[ERROR] --variant requires a value" >&2
				exit 3
			fi
			VARIANT="$2"
			shift 2
			;;
		--output-dir)
			if [[ -z "${2:-}" ]]; then
				echo "[ERROR] --output-dir requires a value" >&2
				exit 3
			fi
			OUTPUT_DIR="$2"
			shift 2
			;;
		--kv-bench)
			if [[ -z "${2:-}" ]]; then
				echo "[ERROR] --kv-bench requires a value" >&2
				exit 3
			fi
			KV_BENCH="$2"
			shift 2
			;;
		--bench-args)
			if [[ -z "${2:-}" ]]; then
				echo "[ERROR] --bench-args requires a value" >&2
				exit 3
			fi
			BENCH_ARGS="$2"
			shift 2
			;;
		*)
			echo "[ERROR] Unknown argument: $1" >&2
			echo "Use --help for usage." >&2
			exit 3
			;;
	esac
done

# ------------------------------------------------------------------
# Validate required arguments
# ------------------------------------------------------------------
if [[ -z "${VARIANT}" ]]; then
	echo "[ERROR] --variant is required" >&2
	exit 3
fi

if [[ -z "${BENCH_ARGS}" ]]; then
	echo "[ERROR] --bench-args is required" >&2
	exit 3
fi

# ------------------------------------------------------------------
# Resolve output directory
# ------------------------------------------------------------------
if [[ -z "${OUTPUT_DIR}" ]]; then
	OUTPUT_DIR="${DEFAULT_OUTPUT_DIR}"
fi
mkdir -p "${OUTPUT_DIR}"
OUTPUT_DIR="$(cd "${OUTPUT_DIR}" && pwd)"

VARIANT_DIR="${OUTPUT_DIR}/${VARIANT}"
mkdir -p "${VARIANT_DIR}"

# ------------------------------------------------------------------
# Ensure kv_bench binary exists
# ------------------------------------------------------------------
if [[ ! -x "${KV_BENCH}" ]]; then
	echo "[INFO] kv_bench not found at ${KV_BENCH}, building..." >&2
	cd "${PROJECT_ROOT}"
	if ! xmake f -m release >/dev/null 2>&1 || ! xmake build kv_bench >/dev/null 2>&1; then
		echo "[ERROR] Binary not found and build failed" >&2
		exit 1
	fi
fi

if [[ ! -x "${KV_BENCH}" ]]; then
	echo "[ERROR] kv_bench still missing after build" >&2
	exit 1
fi

# ------------------------------------------------------------------
# Detect perf availability
# ------------------------------------------------------------------
PERF_AVAILABLE=0
if [[ "${SKIP_PERF}" != "1" ]] && command -v perf >/dev/null 2>&1; then
	if perf stat -o /dev/null sleep 0.1 >/dev/null 2>&1; then
		PERF_AVAILABLE=1
	else
		echo "[WARN] perf found but perf_event_open denied (check /proc/sys/kernel/perf_event_paranoid)" >&2
	fi
else
	if [[ "${SKIP_PERF}" == "1" ]]; then
		echo "[INFO] PROFILE_WORKFLOW_SKIP_PERF=1, skipping perf capture" >&2
	else
		echo "[WARN] perf not found, skipping perf capture" >&2
	fi
fi

# ------------------------------------------------------------------
# File paths
# ------------------------------------------------------------------
STDOUT_FILE="${VARIANT_DIR}/benchmark-stdout.txt"
CMD_FILE="${VARIANT_DIR}/benchmark-cmd.txt"
PERF_STAT_FILE="${VARIANT_DIR}/perf-stat.txt"
PERF_RECORD_FILE="${VARIANT_DIR}/perf-record.data"
STACKS_FILE="${VARIANT_DIR}/stacks.folded"
FLAMEGRAPH_FILE="${VARIANT_DIR}/flamegraph.svg"
METADATA_FILE="${VARIANT_DIR}/metadata.txt"
MISSING_MARKER="${VARIANT_DIR}/MISSING_PERF"

# ------------------------------------------------------------------
# Record the command line
# ------------------------------------------------------------------
echo "# ${KV_BENCH} ${BENCH_ARGS}" >"${CMD_FILE}"

# ------------------------------------------------------------------
# Write metadata
# ------------------------------------------------------------------
{
	echo "variant=${VARIANT}"
	echo "timestamp=$(date --iso-8601=seconds 2>/dev/null || date -u '+%Y-%m-%dT%H:%M:%S%z')"
	echo "hostname=$(hostname 2>/dev/null || echo 'unknown')"
	echo "kv_bench=${KV_BENCH}"
	echo "bench_args=${BENCH_ARGS}"
	if command -v perf >/dev/null 2>&1; then
		echo "perf_version=$(perf --version 2>/dev/null || echo 'unknown')"
	else
		echo "perf_version=not_installed"
	fi
	echo "output_dir=${OUTPUT_DIR}"
	echo "variant_dir=${VARIANT_DIR}"
} >"${METADATA_FILE}"

# ------------------------------------------------------------------
# Run benchmark with perf stat
# ------------------------------------------------------------------
echo "[RUN] ${VARIANT}" >&2

rm -f "${MISSING_MARKER}"

# Split bench args for execution. We need to be careful with quoting.
# BENCH_ARGS is a single string; we read it into an array to pass correctly.
eval "BENCH_ARGS_SPLIT=(${BENCH_ARGS})"

EXITCODE=0
if [[ "${PERF_AVAILABLE}" == "1" ]]; then
	# perf stat: capture hardware counters
	perf stat -d -o "${PERF_STAT_FILE}" "${KV_BENCH}" "${BENCH_ARGS_SPLIT[@]}" >"${STDOUT_FILE}" 2>&1 || EXITCODE=$?
else
	# perf not available: write MISSING_PERF marker
	echo "# MISSING_PERF: perf not available or denied" >"${PERF_STAT_FILE}"
	touch "${MISSING_MARKER}"
	"${KV_BENCH}" "${BENCH_ARGS_SPLIT[@]}" >"${STDOUT_FILE}" 2>&1 || EXITCODE=$?
fi

if [[ "${EXITCODE}" -ne 0 ]]; then
	echo "[FAIL] ${VARIANT} exited with code ${EXITCODE}" >&2
	exit 2
fi

OPS=$(grep -oP 'ops/s=\K[0-9]+' "${STDOUT_FILE}" | tail -n1 || true)
if [[ -n "${OPS}" ]]; then
	echo "[OK]   ${VARIANT} ops/s=${OPS}" >&2
else
	echo "[OK]   ${VARIANT} (no ops/s parsed)" >&2
fi

# ------------------------------------------------------------------
# perf record sampling + FlameGraph generation
# ------------------------------------------------------------------
if [[ "${PERF_AVAILABLE}" == "1" ]]; then
	echo "[PERF] Recording perf sample data..." >&2
	perf record -g -F 99 -o "${PERF_RECORD_FILE}" "${KV_BENCH}" "${BENCH_ARGS_SPLIT[@]}" >"${STDOUT_FILE}" 2>&1 || {
		echo "[WARN] perf record failed, skipping flamegraph generation" >&2
		rm -f "${PERF_RECORD_FILE}"
	}

	# Generate flamegraph if perf-record.data exists
	if [[ -f "${PERF_RECORD_FILE}" ]]; then
		echo "[PERF] Generating folded stacks..." >&2
		perf script -i "${PERF_RECORD_FILE}" 2>/dev/null | \
			"${FLAMEGRAPH_DIR}/stackcollapse-perf.pl" >"${STACKS_FILE}" 2>/dev/null || {
			echo "[WARN] stackcollapse-perf.pl failed, skipping flamegraph" >&2
			rm -f "${STACKS_FILE}"
		}

		if [[ -f "${STACKS_FILE}" ]] && [[ -s "${STACKS_FILE}" ]]; then
			echo "[PERF] Generating flamegraph SVG..." >&2
			"${FLAMEGRAPH_DIR}/flamegraph.pl" "${STACKS_FILE}" >"${FLAMEGRAPH_FILE}" 2>/dev/null || {
				echo "[WARN] flamegraph.pl failed" >&2
				rm -f "${FLAMEGRAPH_FILE}"
			}

			if [[ -f "${FLAMEGRAPH_FILE}" ]]; then
				echo "[PERF] FlameGraph: ${FLAMEGRAPH_FILE}" >&2
			fi
		else
			rm -f "${STACKS_FILE}" 2>/dev/null
		fi
	fi
fi

echo "[DONE] Results in ${VARIANT_DIR}" >&2
exit 0
