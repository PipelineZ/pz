#!/usr/bin/env bash
# Fixed-dataset macro throughput harness.
#
# Builds Release, generates a deterministic CSV (fixed row count + seed -- pure awk arithmetic, no RNG,
# so the file is byte-identical across machines and runs), then runs `pz run` twice against the SAME
# data: once with the engine's default native path (localfiles' native_scan/native_copy), once with
# engine.force_universal: true (the arrow_stream universal path) -- and prints rows/sec for each, so the
# two tiers documented as "behaviorally interchangeable" can also be compared for throughput.
#
# Then runs a companion probe, scripts/gate-serialization-probe.cs, which quantifies the
# DuckSession gate's serialization cost (the correctness fix from the full-suite-parallel-flake
# investigation -- see DuckSession._gate's doc comment) directly against production DuckSession code.
#
# Usage: scripts/macro-bench.sh [row_count]   (default 1,000,000)
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROW_COUNT="${1:-1000000}"
SEED=42
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

echo "== PipelineZ macro benchmark harness =="
echo "rows: ${ROW_COUNT} (seed ${SEED})"
echo "work dir: ${WORK_DIR}"
echo

echo "-- Building Release --"
dotnet build "${ROOT_DIR}/Pz.slnx" -c Release --nologo -v quiet
echo "build OK"
echo

echo "-- Generating deterministic CSV (${ROW_COUNT} rows) --"
mkdir -p "${WORK_DIR}/project/data"
CSV_PATH="${WORK_DIR}/project/data/orders.csv"
# Every value is a pure function of the row index and the fixed seed -- no awk rand(), which differs
# between awk implementations -- so the file is byte-identical on every machine and every run.
awk -v n="${ROW_COUNT}" -v seed="${SEED}" 'BEGIN {
    print "id,customer_id,amount,status";
    split("shipped,pending,returned,cancelled", statuses, ",");
    for (i = 1; i <= n; i++) {
        customer_id = ((i * 2654435761 + seed) % 100000);
        amount = ((i * 97 + seed) % 100000) / 100.0;
        status = statuses[(i % 4) + 1];
        printf "%d,%d,%.2f,%s\n", i, customer_id, amount, status;
    }
}' > "${CSV_PATH}"
echo "wrote ${CSV_PATH} ($(wc -l < "${CSV_PATH}") lines incl. header)"
echo

mkdir -p "${WORK_DIR}/project/pipelines"

# One connections.yml: a connection is a place, an entity is a thing in that place, the
# direction is the function the pipeline calls.
# `columns:` stays on the read: csv only gets the native tier with a contract (performance.md,
# "Many small files" lever 1).
cat > "${WORK_DIR}/project/connections.yml" << 'EOF'
bench:
  connector: localfiles
  entities:
    orders:
      read:
        path: data/orders.csv
        format: csv
        columns:
          id: bigint
          customer_id: bigint
          amount: double
          status: varchar

lake:
  connector: localfiles
  root: out
EOF

cat > "${WORK_DIR}/project/pipelines/orders_out.sql" << 'EOF'
INSERT INTO {{ sink('lake', 'orders_out', format: 'csv', strategy: 'replace') }}
select * from {{ source('bench', 'orders') }}
EOF

run_variant() {
    local variant_name="$1"
    local extra_engine_yaml="$2"

    local project_dir="${WORK_DIR}/${variant_name}"
    mkdir -p "${project_dir}"
    cp -r "${WORK_DIR}/project/"* "${project_dir}/"

    cat > "${project_dir}/project.yml" << EOF
name: macro_bench_${variant_name}
version: 0.1.0

connectors:
  - package: Pz.Connector.LocalFiles
    version: 0.1.0

engine:
  threads: 4
${extra_engine_yaml}
EOF

    local start_ns end_ns elapsed_s
    start_ns=$(date +%s%N)
    dotnet run -c Release --project "${ROOT_DIR}/src/Pz.Cli" --no-build -- run --project "${project_dir}" >"${project_dir}/run.log" 2>&1
    end_ns=$(date +%s%N)
    elapsed_s=$(awk -v s="${start_ns}" -v e="${end_ns}" 'BEGIN { printf "%.4f", (e - s) / 1000000000.0 }')
    LAST_ELAPSED="${elapsed_s}"

    local rows_per_sec
    rows_per_sec=$(awk -v rows="${ROW_COUNT}" -v secs="${elapsed_s}" 'BEGIN { if (secs <= 0) { print "n/a" } else { printf "%.0f", rows / secs } }')

    echo "  ${variant_name}: ${elapsed_s}s elapsed, ~${rows_per_sec} rows/sec"
}

echo "-- Running pz run (native path: localfiles native_scan + native_copy) --"
run_variant native ""
NATIVE_S="${LAST_ELAPSED}"

echo "-- Running pz run (engine.force_universal: true) --"
run_variant universal "  force_universal: true"
UNIVERSAL_S="${LAST_ELAPSED}"
echo

echo "-- Passthrough floor: fused COPY (SELECT * FROM read_csv(...)) TO ..., production DuckSession --"
echo "   (spec 2026-07-31-passthrough-floor-bench: the one statement a native-fusion planner would"
echo "    emit -- no compile/plan/staging, so this is fusion's MAXIMUM win, not its expected win)"
FLOOR_LINE=$(dotnet "${ROOT_DIR}/scripts/passthrough-floor-probe.cs" "${CSV_PATH}" "${WORK_DIR}/floor_out.csv")
echo "  ${FLOOR_LINE}"
FLOOR_S=$(awk -v line="${FLOOR_LINE}" 'BEGIN { split(line, a, " "); gsub(/s$/, "", a[2]); print a[2] }')
echo

echo "== Summary (${ROW_COUNT} rows) =="
awk -v rows="${ROW_COUNT}" -v n="${NATIVE_S}" -v u="${UNIVERSAL_S}" -v f="${FLOOR_S}" 'BEGIN {
    printf "  native:      %8.4fs  ~%d rows/sec\n", n, rows / n;
    printf "  universal:   %8.4fs  ~%d rows/sec\n", u, rows / u;
    printf "  fused floor: %8.4fs  ~%d rows/sec\n", f, rows / f;
    printf "  max fusion win vs native: %.0f%%\n", (n - f) / n * 100;
}'
echo

echo "-- DuckSession gate serialization cost --"
echo "   (two concurrent source-style ingests + a deliberately slow sink-style egress query, all"
echo "    against ONE shared DuckSession -- the same contention RunOrchestrator's concurrent node"
echo "    dispatch creates in a real multi-node run; see scripts/gate-serialization-probe.cs)"
echo "   This IS the slow-sink concurrent-vs-sequential scenario the connection-strategy spike"
echo "   measured: re-run this after any change to DuckSession's connection handling and compare"
echo "   the printed ratio against the recorded semaphore baseline (0.94-1.04) and the"
echo "   connection-per-operation measurement (0.85-0.94, i.e. no real speedup either way)."
echo
dotnet "${ROOT_DIR}/scripts/gate-serialization-probe.cs"
