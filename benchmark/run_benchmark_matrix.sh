#!/usr/bin/env bash
#
# run_benchmark_matrix.sh — Run the canonical 5-variant benchmark matrix for Prism kv_bench.
#
# Usage:
#   ./run_benchmark_matrix.sh [OUTPUT_DIR]
#
# Arguments:
#   OUTPUT_DIR    Directory to store results (default: benchmark_results)
#
# Environment:
#   KV_BENCH      Path to kv_bench binary (default: build/linux/x86_64/release/kv_bench)
#   SKIP_PERF     Set to 1 to skip perf stat capture (still runs benchmarks)
#
# Output structure (deterministic):
#   OUTPUT_DIR/
#     v1-c24w24-vs1000/
#       stdout.txt
#       perf-stat.txt        (or MISSING_PERF if perf unavailable)
#       exitcode.txt
#     v2-c24w8-vs1000/
#       ...
#     ...
#
# Exit codes:
#   0  — All benchmarks completed successfully (perf is optional)
#   1  — One or more benchmarks failed (non-zero exit from kv_bench)
#   2  — kv_bench binary missing and build failed
#
# Supported environments:
#   - Linux with xmake, bash 4.2+, and optional perf (linux-tools)
#   - Non-root perf works if /proc/sys/kernel/perf_event_paranoid <= 2
#   - Missing perf is handled gracefully: MISSING_PERF marker written
#
# Failure modes:
#   - "Binary not found and build failed" — xmake could not produce kv_bench
#   - "perf not available" — perf binary missing; MISSING_PERF marker written
#   - "perf_event_open failed" — kernel denies perf; MISSING_PERF marker written
#   - "Disk quota exceeded" — tmpfs full (V5 needs disk-backed dir)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

OUTPUT_DIR="${1:-benchmark_results}"
OUTPUT_DIR="$(cd "${PROJECT_ROOT}" && mkdir -p "${OUTPUT_DIR}" && cd "${OUTPUT_DIR}" && pwd)"

KV_BENCH="${KV_BENCH:-${PROJECT_ROOT}/build/linux/x86_64/release/kv_bench}"
SKIP_PERF="${SKIP_PERF:-0}"

if [[ ! -x "${KV_BENCH}" ]]; then
	echo "[INFO] kv_bench not found at ${KV_BENCH}, building..." >&2
	cd "${PROJECT_ROOT}"
	if ! xmake f -m release >/dev/null 2>&1 || ! xmake build kv_bench >/dev/null 2>&1; then
		echo "[ERROR] Binary not found and build failed" >&2
		exit 2
	fi
fi

if [[ ! -x "${KV_BENCH}" ]]; then
	echo "[ERROR] kv_bench still missing after build" >&2
	exit 2
fi

PERF_AVAILABLE=0
if [[ "${SKIP_PERF}" != "1" ]] && command -v perf >/dev/null 2>&1; then
	if perf stat -o /dev/null sleep 0.1 >/dev/null 2>&1; then
		PERF_AVAILABLE=1
	else
		echo "[WARN] perf found but perf_event_open denied (check /proc/sys/kernel/perf_event_paranoid)" >&2
	fi
else
	if [[ "${SKIP_PERF}" == "1" ]]; then
		echo "[INFO] SKIP_PERF=1, skipping perf capture" >&2
	else
		echo "[WARN] perf not found, skipping perf capture" >&2
	fi
fi

FIXED_FLAGS=(
	--run=async
	--bench=mixed
	--ops=50000
	--prefill=1
	--no_latency
	--rounds=1
	--read_ratio=100
	--keep_db=0
)

# Variant format: name|clients|workers|value_size|db_dir[|inflight]
# If inflight is omitted, defaults to 1

declare -a VARIANTS=(
	"v1-c24w24-vs1000|24|24|1000|/tmp/prism_bench_c24w24_vs1000"
	"v2-c24w8-vs1000|24|8|1000|/tmp/prism_bench_c24w8_vs1000"
	"v3-c48w24-vs1000|48|24|1000|/tmp/prism_bench_c48w24_vs1000"
	"v4-c24w24-vs100|24|24|100|/tmp/prism_bench_c24w24_vs100"
	"v5-c24w24-vs10000|24|24|10000|/home/puji/prism_bench_c24w24_vs10000"
	"v6-c24w24-vs1000-inf16|24|24|1000|/tmp/prism_bench_v6|16"
	"v7-c24w8-vs1000-inf16|24|8|1000|/tmp/prism_bench_v7|16"
	"v8-c48w24-vs1000-inf16|48|24|1000|/tmp/prism_bench_v8|16"
	"v9-c24w24-vs10000-inf16|24|24|10000|/home/puji/prism_bench_v9|16"
)

OVERALL_STATUS=0

for entry in "${VARIANTS[@]}"; do
	IFS='|' read -r VARIANT_ID CLIENTS WORKERS VALUE_SIZE DB_DIR INFLIGHT <<< "${entry}"
	INFLIGHT="${INFLIGHT:-1}"

	VARIANT_DIR="${OUTPUT_DIR}/${VARIANT_ID}"
	mkdir -p "${VARIANT_DIR}"

	STDOUT_FILE="${VARIANT_DIR}/stdout.txt"
	PERF_FILE="${VARIANT_DIR}/perf-stat.txt"
	EXITCODE_FILE="${VARIANT_DIR}/exitcode.txt"
	MISSING_MARKER="${VARIANT_DIR}/MISSING_PERF"

	echo "[RUN] ${VARIANT_ID}: clients=${CLIENTS} workers=${WORKERS} value_size=${VALUE_SIZE} inflight=${INFLIGHT}" >&2

	rm -f "${MISSING_MARKER}"

	ARGS=(
		"${FIXED_FLAGS[@]}"
		--clients="${CLIENTS}"
		--workers="${WORKERS}"
		--value_size="${VALUE_SIZE}"
		--db_dir="${DB_DIR}"
		--inflight_per_client="${INFLIGHT}"
	)

	EXITCODE=0
	if [[ "${PERF_AVAILABLE}" == "1" ]]; then
		perf stat -d -o "${PERF_FILE}" "${KV_BENCH}" "${ARGS[@]}" >"${STDOUT_FILE}" 2>&1 || EXITCODE=$?
	else
		echo "# MISSING_PERF: perf not available or denied" >"${PERF_FILE}"
		touch "${MISSING_MARKER}"
		"${KV_BENCH}" "${ARGS[@]}" >"${STDOUT_FILE}" 2>&1 || EXITCODE=$?
	fi

	echo "${EXITCODE}" >"${EXITCODE_FILE}"

	if [[ "${EXITCODE}" -ne 0 ]]; then
		echo "[FAIL] ${VARIANT_ID} exited with code ${EXITCODE}" >&2
		OVERALL_STATUS=1
	else
		OPS=$(grep -oP 'ops/s=\K[0-9]+' "${STDOUT_FILE}" | tail -n1 || true)
		if [[ -n "${OPS}" ]]; then
			echo "[OK]   ${VARIANT_ID} ops/s=${OPS}" >&2
		else
			echo "[OK]   ${VARIANT_ID} (no ops/s parsed)" >&2
		fi
	fi
done

echo "[DONE] Results in ${OUTPUT_DIR}" >&2
exit "${OVERALL_STATUS}"
