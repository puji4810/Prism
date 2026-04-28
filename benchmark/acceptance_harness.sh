#!/usr/bin/env bash
#
# acceptance_harness.sh — Unified before/after acceptance harness for Prism kv_bench.
#
# Runs sync and async benchmark variants, captures throughput, latency percentiles,
# observed inflight, scheduler/worker configuration, and profiler metadata.
# Produces output consumable by benchmark/compare_perf.py.
#
# Usage:
#   ./acceptance_harness.sh [OPTIONS]
#
# Options:
#   --output-dir DIR       Root directory for results (default: acceptance_results)
#   --variants V1,V2,...   Comma-separated variant names to run (default: all)
#   --kv-bench PATH        Path to kv_bench binary (default: auto-detect)
#   --build-first          Build kv_bench before running (default: skip if binary exists)
#   --skip-perf            Skip perf stat capture (still runs benchmarks)
#   --help                 Show this help message
#
# Variants:
#   sync-small       Sync mixed, c2/w2, ops=1000, vs=100
#   async-small      Async mixed, c2/w2, ops=1000, vs=100
#   sync-medium      Sync mixed, c4/w4, ops=5000, vs=1000
#   async-medium     Async mixed, c4/w4, ops=5000, vs=1000
#   async-inflight   Async mixed, c4/w4, ops=5000, vs=1000, inflight=4
#   async-latency    Async mixed, c2/w2, ops=500, vs=100 (with p50/p95 collection)
#
# Output structure (per-variant directory, compatible with compare_perf.py):
#   {output-dir}/
#     {variant-name}/
#       stdout.txt           — raw kv_bench stdout (contains ops/s, p50/p95, inflight)
#       perf-stat.txt        — perf stat counters (or MISSING_PERF marker)
#       exitcode.txt         — kv_bench exit code
#       metadata.txt         — variant params, timestamp, hostname
#     summary.txt            — aggregated results table
#
# Environment:
#   KV_BENCH                 Override kv_bench binary path
#   ACCEPTANCE_SKIP_PERF     Set to 1 to skip perf stat capture
#
# Exit codes:
#   0  — All variants completed successfully
#   1  — One or more variants failed
#   2  — kv_bench binary missing and build failed
#   3  — Invalid arguments
#
# Rerunnability:
#   Each run creates a unique subdirectory under --output-dir using a timestamp.
#   No hardcoded temp paths. Safe to run multiple times.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ------------------------------------------------------------------
# Defaults
# ------------------------------------------------------------------
OUTPUT_DIR=""
VARIANT_FILTER=""
KV_BENCH="${KV_BENCH:-}"
BUILD_FIRST=0
SKIP_PERF="${ACCEPTANCE_SKIP_PERF:-0}"

# ------------------------------------------------------------------
# Variant definitions
# Format: name|run_mode|clients|workers|ops|value_size|read_ratio|inflight|extra_flags
# ------------------------------------------------------------------
declare -a ALL_VARIANTS=(
	"sync-small|sync|2|2|1000|100|100|1|--rounds=1 --prefill=1 --no_latency"
	"async-small|async|2|2|1000|100|100|1|--rounds=1 --prefill=1 --no_latency"
	"sync-medium|sync|4|4|5000|1000|100|1|--rounds=1 --prefill=1 --no_latency"
	"async-medium|async|4|4|5000|1000|100|1|--rounds=1 --prefill=1 --no_latency"
	"async-inflight|async|4|4|5000|1000|100|4|--rounds=1 --prefill=1 --no_latency"
	"async-latency|async|2|2|500|100|100|1|--rounds=1 --prefill=1"
)

# ------------------------------------------------------------------
# Help
# ------------------------------------------------------------------
usage() {
	sed -n '2,/^$/s/^# \?//p' "$0" >&2
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
		--output-dir)
			if [[ -z "${2:-}" ]]; then
				echo "[ERROR] --output-dir requires a value" >&2
				exit 3
			fi
			OUTPUT_DIR="$2"
			shift 2
			;;
		--variants)
			if [[ -z "${2:-}" ]]; then
				echo "[ERROR] --variants requires a value" >&2
				exit 3
			fi
			VARIANT_FILTER="$2"
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
		--build-first)
			BUILD_FIRST=1
			shift
			;;
		--skip-perf)
			SKIP_PERF=1
			shift
			;;
		*)
			echo "[ERROR] Unknown argument: $1" >&2
			echo "Use --help for usage." >&2
			exit 3
			;;
	esac
done

# ------------------------------------------------------------------
# Resolve output directory (unique per run)
# ------------------------------------------------------------------
if [[ -z "${OUTPUT_DIR}" ]]; then
	OUTPUT_DIR="${PROJECT_ROOT}/acceptance_results"
fi

RUN_TIMESTAMP="$(date +%Y%m%d_%H%M%S 2>/dev/null || echo 'run')"
RUN_DIR="${OUTPUT_DIR}/${RUN_TIMESTAMP}"
mkdir -p "${RUN_DIR}"
RUN_DIR="$(cd "${RUN_DIR}" && pwd)"

echo "[INFO] Results will be in: ${RUN_DIR}" >&2

# ------------------------------------------------------------------
# Resolve kv_bench binary
# ------------------------------------------------------------------
if [[ -z "${KV_BENCH}" ]]; then
	KV_BENCH="${PROJECT_ROOT}/build/linux/x86_64/release/kv_bench"
fi

if [[ "${BUILD_FIRST}" == "1" ]] || [[ ! -x "${KV_BENCH}" ]]; then
	if [[ ! -x "${KV_BENCH}" ]]; then
		echo "[INFO] kv_bench not found at ${KV_BENCH}, building..." >&2
	fi
	cd "${PROJECT_ROOT}"
	if ! xmake f -m release >/dev/null 2>&1 || ! xmake build kv_bench >/dev/null 2>&1; then
		echo "[ERROR] Binary not found and build failed" >&2
		exit 2
	fi
fi

if [[ ! -x "${KV_BENCH}" ]]; then
	echo "[ERROR] kv_bench still missing after build at ${KV_BENCH}" >&2
	exit 2
fi

echo "[INFO] Using kv_bench: ${KV_BENCH}" >&2

# ------------------------------------------------------------------
# Detect perf availability
# ------------------------------------------------------------------
PERF_AVAILABLE=0
if [[ "${SKIP_PERF}" != "1" ]] && command -v perf >/dev/null 2>&1; then
	if perf stat -o /dev/null sleep 0.1 >/dev/null 2>&1; then
		PERF_AVAILABLE=1
	else
		echo "[WARN] perf found but perf_event_open denied" >&2
	fi
else
	if [[ "${SKIP_PERF}" == "1" ]]; then
		echo "[INFO] --skip-perf set, skipping perf capture" >&2
	else
		echo "[WARN] perf not found, skipping perf capture" >&2
	fi
fi

# ------------------------------------------------------------------
# Filter variants
# ------------------------------------------------------------------
should_run_variant() {
	local name="$1"
	if [[ -z "${VARIANT_FILTER}" ]]; then
		return 0
	fi
	IFS=',' read -ra FILTERED <<< "${VARIANT_FILTER}"
	for f in "${FILTERED[@]}"; do
		if [[ "${f}" == "${name}" ]]; then
			return 0
		fi
	done
	return 1
}

# ------------------------------------------------------------------
# Run one variant
# ------------------------------------------------------------------
run_variant() {
	local entry="$1"
	IFS='|' read -r NAME RUN_MODE CLIENTS WORKERS OPS VALUE_SIZE READ_RATIO INFLIGHT EXTRA_FLAGS <<< "${entry}"

	if ! should_run_variant "${NAME}"; then
		echo "[SKIP] ${NAME} (not in --variants filter)" >&2
		return 0
	fi

	local VARIANT_DIR="${RUN_DIR}/${NAME}"
	mkdir -p "${VARIANT_DIR}"

	local STDOUT_FILE="${VARIANT_DIR}/stdout.txt"
	local PERF_FILE="${VARIANT_DIR}/perf-stat.txt"
	local EXITCODE_FILE="${VARIANT_DIR}/exitcode.txt"
	local METADATA_FILE="${VARIANT_DIR}/metadata.txt"
	local MISSING_MARKER="${VARIANT_DIR}/MISSING_PERF"

	# Build unique db_dir to avoid collisions between runs
	local DB_DIR="/tmp/prism_acceptance_${NAME}_$$"

	# Compose kv_bench arguments
	local ARGS=(
		"--run=${RUN_MODE}"
		"--bench=mixed"
		"--clients=${CLIENTS}"
		"--workers=${WORKERS}"
		"--ops=${OPS}"
		"--value_size=${VALUE_SIZE}"
		"--read_ratio=${READ_RATIO}"
		"--inflight_per_client=${INFLIGHT}"
		"--db_dir=${DB_DIR}"
		"--keep_db=0"
	)
	# Append extra flags (space-separated)
	if [[ -n "${EXTRA_FLAGS}" ]]; then
		read -ra EXTRA_ARRAY <<< "${EXTRA_FLAGS}"
		ARGS+=("${EXTRA_ARRAY[@]}")
	fi

	# Write metadata
	{
		echo "variant=${NAME}"
		echo "run_mode=${RUN_MODE}"
		echo "clients=${CLIENTS}"
		echo "workers=${WORKERS}"
		echo "ops_per_client=${OPS}"
		echo "value_size=${VALUE_SIZE}"
		echo "read_ratio=${READ_RATIO}"
		echo "inflight_per_client=${INFLIGHT}"
		echo "extra_flags=${EXTRA_FLAGS}"
		echo "db_dir=${DB_DIR}"
		echo "timestamp=$(date --iso-8601=seconds 2>/dev/null || date -u '+%Y-%m-%dT%H:%M:%S%z')"
		echo "hostname=$(hostname 2>/dev/null || echo 'unknown')"
		echo "kv_bench=${KV_BENCH}"
		echo "perf_available=${PERF_AVAILABLE}"
	} >"${METADATA_FILE}"

	echo "[RUN] ${NAME}: ${RUN_MODE} c=${CLIENTS} w=${WORKERS} ops=${OPS} vs=${VALUE_SIZE} inflight=${INFLIGHT}" >&2

	rm -f "${MISSING_MARKER}"

	local EXITCODE=0
	if [[ "${PERF_AVAILABLE}" == "1" ]]; then
		perf stat -d -o "${PERF_FILE}" "${KV_BENCH}" "${ARGS[@]}" >"${STDOUT_FILE}" 2>&1 || EXITCODE=$?
	else
		echo "# MISSING_PERF: perf not available or denied" >"${PERF_FILE}"
		touch "${MISSING_MARKER}"
		"${KV_BENCH}" "${ARGS[@]}" >"${STDOUT_FILE}" 2>&1 || EXITCODE=$?
	fi

	echo "${EXITCODE}" >"${EXITCODE_FILE}"

	if [[ "${EXITCODE}" -ne 0 ]]; then
		echo "[FAIL] ${NAME} exited with code ${EXITCODE}" >&2
		return 1
	fi

	# Parse and report key metrics from stdout
	local OPS_LINE
	OPS_LINE=$(grep -oP 'ops/s=\K[0-9.]+' "${STDOUT_FILE}" | tail -n1 || true)
	local P50
	P50=$(grep -oP 'p50_us=\K[0-9.]+' "${STDOUT_FILE}" | tail -n1 || echo "N/A")
	local P95
	P95=$(grep -oP 'p95_us=\K[0-9.]+' "${STDOUT_FILE}" | tail -n1 || echo "N/A")
	local MAX_INFLIGHT
	MAX_INFLIGHT=$(grep -oP 'max_inflight=\K[0-9]+' "${STDOUT_FILE}" | tail -n1 || echo "N/A")
	local MAX_CLIENT_INFLIGHT
	MAX_CLIENT_INFLIGHT=$(grep -oP 'max_client_inflight=\K[0-9]+' "${STDOUT_FILE}" | tail -n1 || echo "N/A")
	local TIME_S
	TIME_S=$(grep -oP 'time=\K[0-9.]+' "${STDOUT_FILE}" | tail -n1 || echo "N/A")

	echo "[OK]   ${NAME} ops/s=${OPS_LINE:-?} time=${TIME_S:-?}s p50=${P50:-?}us p95=${P95:-?}us max_inflight=${MAX_INFLIGHT:-?} max_client_inflight=${MAX_CLIENT_INFLIGHT:-?}" >&2

	# Cleanup temp db dir
	rm -rf "${DB_DIR}" 2>/dev/null || true

	return 0
}

# ------------------------------------------------------------------
# Main execution
# ------------------------------------------------------------------
OVERALL_STATUS=0
SUMMARY_FILE="${RUN_DIR}/summary.txt"

{
	echo "# Acceptance Harness Results"
	echo "# Run: ${RUN_TIMESTAMP}"
	echo "# kv_bench: ${KV_BENCH}"
	echo "# perf_available: ${PERF_AVAILABLE}"
	echo "#"
	printf "%-20s %-6s %5s %5s %7s %6s %8s %10s %10s %10s %10s %6s\n" \
		"variant" "mode" "c" "w" "ops" "vs" "rr" "ops/s" "p50_us" "p95_us" "max_infl" "exit"
	echo "# ---------------------------------------------------------------------------------------------------------------"
} >"${SUMMARY_FILE}"

for entry in "${ALL_VARIANTS[@]}"; do
	IFS='|' read -r NAME RUN_MODE CLIENTS WORKERS OPS VALUE_SIZE READ_RATIO INFLIGHT EXTRA_FLAGS <<< "${entry}"

	if ! should_run_variant "${NAME}"; then
		continue
	fi

	VARIANT_DIR="${RUN_DIR}/${NAME}"
	STDOUT_FILE="${VARIANT_DIR}/stdout.txt"

	if run_variant "${entry}"; then
		# Extract metrics for summary
		OPS_LINE=$(grep -oP 'ops/s=\K[0-9.]+' "${STDOUT_FILE}" 2>/dev/null | tail -n1 || echo "N/A")
		P50=$(grep -oP 'p50_us=\K[0-9.]+' "${STDOUT_FILE}" 2>/dev/null | tail -n1 || echo "N/A")
		P95=$(grep -oP 'p95_us=\K[0-9.]+' "${STDOUT_FILE}" 2>/dev/null | tail -n1 || echo "N/A")
		MAX_INFLIGHT=$(grep -oP 'max_inflight=\K[0-9]+' "${STDOUT_FILE}" 2>/dev/null | tail -n1 || echo "N/A")
		EXITCODE=$(cat "${VARIANT_DIR}/exitcode.txt" 2>/dev/null || echo "?")

		printf "%-20s %-6s %5s %5s %7s %6s %8s %10s %10s %10s %10s %6s\n" \
			"${NAME}" "${RUN_MODE}" "${CLIENTS}" "${WORKERS}" "${OPS}" "${VALUE_SIZE}" \
			"${READ_RATIO}" "${OPS_LINE:-N/A}" "${P50:-N/A}" "${P95:-N/A}" "${MAX_INFLIGHT:-N/A}" "${EXITCODE}" \
			>>"${SUMMARY_FILE}"
	else
		OVERALL_STATUS=1
		EXITCODE=$(cat "${VARIANT_DIR}/exitcode.txt" 2>/dev/null || echo "?")
		printf "%-20s %-6s %5s %5s %7s %6s %8s %10s %10s %10s %10s %6s\n" \
			"${NAME}" "${RUN_MODE}" "${CLIENTS}" "${WORKERS}" "${OPS}" "${VALUE_SIZE}" \
			"${READ_RATIO}" "FAILED" "N/A" "N/A" "N/A" "${EXITCODE}" \
			>>"${SUMMARY_FILE}"
	fi
done

echo "" >>"${SUMMARY_FILE}"
echo "# compare_perf.py compatible: each variant dir has stdout.txt + perf-stat.txt" >>"${SUMMARY_FILE}"
echo "# To compare two runs:" >>"${SUMMARY_FILE}"
echo "#   python3 benchmark/compare_perf.py RUN_A/variant-name RUN_B/variant-name" >>"${SUMMARY_FILE}"

echo "" >&2
echo "[DONE] Results in ${RUN_DIR}" >&2
echo "[DONE] Summary: ${SUMMARY_FILE}" >&2

# Print summary to stderr for immediate visibility
cat "${SUMMARY_FILE}" >&2

exit "${OVERALL_STATUS}"
