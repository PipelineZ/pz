#!/usr/bin/env bash
# Fixed-dataset macro throughput harness.
#
# Builds Release, generates a deterministic CSV (fixed row count + seed -- pure awk arithmetic, no RNG,
# so the file is byte-identical across machines and runs), then runs `pz run` against the SAME data:
# once with the engine's default native path (localfiles' native_scan/native_copy), once with
# engine.force_universal: true (the in-proc arrow_stream universal path), and once against the SAME
# localfiles logic reached out of process over PCP (the PcpFakeConnector fixture, staged as a
# runtime:"process" package) also under engine.force_universal: true.
#
# The process-hosting throughput gate (spec invariant 7: process-hosted universal must stay >= 80% of
# in-proc universal throughput) is about protocol shape -- per-row/per-batch wire cost -- not about
# process startup, so it is measured MARGINALLY, not end to end: the universal and process_universal
# variants each run twice, once at a tiny calibration row count and once at the full row count, and the
# gate compares (full - tiny) on both sides. That difference cancels the one-time process-spawn +
# control-channel handshake cost every fresh child pays regardless of how much data it moves, leaving
# only the steady-state per-row cost the invariant actually governs. The fixed cost itself (tiny
# process_universal elapsed minus tiny universal elapsed) is printed for visibility but never asserted --
# it is a real, dotnet-runtime-plus-control-channel-cold-start number, but it is a property of this
# machine's process-spawn speed, not of the wire protocol, so there is nothing here to hold it to. The
# raw end-to-end ratio (full-N elapsed only, spawn cost included) is also printed, clearly labeled, so a
# reader can still see the number an actual single `pz run` would experience -- it just is not what gates
# the script. A ratio below the marginal gate prints "BUDGET FAIL" and exits 1 -- process hosting exists
# to move WHERE a connector runs, not to make its steady-state throughput slower than in-proc.
#
# Then runs a companion probe, scripts/gate-serialization-probe.cs, which quantifies the
# DuckSession gate's serialization cost (the correctness fix from the full-suite-parallel-flake
# investigation -- see DuckSession._gate's doc comment) directly against production DuckSession code.
#
# Usage: scripts/macro-bench.sh [row_count]   (default 1,000,000)
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROW_COUNT="${1:-1000000}"
# Calibration row count for the marginal-throughput gate -- small enough to be dominated by fixed
# process-spawn/handshake cost (the number the gate must NOT be measuring), large enough that `pz run`'s
# own fixed per-invocation cost (compile/plan/staging open) is not itself the dominant term on either side.
TINY=1000
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

# Every value is a pure function of the row index and the fixed seed -- no awk rand(), which differs
# between awk implementations -- so the file is byte-identical on every machine and every run.
generate_csv() {
    local row_count="$1"
    local out_path="$2"
    awk -v n="${row_count}" -v seed="${SEED}" 'BEGIN {
        print "id,customer_id,amount,status";
        split("shipped,pending,returned,cancelled", statuses, ",");
        for (i = 1; i <= n; i++) {
            customer_id = ((i * 2654435761 + seed) % 100000);
            amount = ((i * 97 + seed) % 100000) / 100.0;
            status = statuses[(i % 4) + 1];
            printf "%d,%d,%.2f,%s\n", i, customer_id, amount, status;
        }
    }' > "${out_path}"
}

# One connections.yml shape shared by every variant: a connection is a place, an entity is a thing in
# that place, the direction is the function the pipeline calls. `columns:` stays on the read: csv only
# gets the native tier with a contract (performance.md, "Many small files" lever 1). Only the connector
# name varies between the builtin and process-hosted trees.
build_project_tree() {
    local target_dir="$1"
    local connector_name="$2"
    local csv_source="$3"

    mkdir -p "${target_dir}/data" "${target_dir}/pipelines"
    cp "${csv_source}" "${target_dir}/data/orders.csv"

    cat > "${target_dir}/connections.yml" << EOF
bench:
  connector: ${connector_name}
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
  connector: ${connector_name}
  root: out
EOF

    cat > "${target_dir}/pipelines/orders_out.sql" << 'EOF'
INSERT INTO {{ sink('lake', 'orders_out', format: 'csv', strategy: 'replace') }}
select * from {{ source('bench', 'orders') }}
EOF
}

echo "-- Generating deterministic CSVs (${ROW_COUNT} rows full, ${TINY} rows calibration) --"
CSV_PATH="${WORK_DIR}/orders_full.csv"
TINY_CSV_PATH="${WORK_DIR}/orders_tiny.csv"
generate_csv "${ROW_COUNT}" "${CSV_PATH}"
generate_csv "${TINY}" "${TINY_CSV_PATH}"
echo "wrote ${CSV_PATH} ($(wc -l < "${CSV_PATH}") lines incl. header)"
echo "wrote ${TINY_CSV_PATH} ($(wc -l < "${TINY_CSV_PATH}") lines incl. header)"
echo

# Four source trees: {full, tiny} rows x {builtin, process-hosted} connector. Native and the fused
# floor probe below only ever run at full N (unaffected by the marginal-ratio calibration), so they
# reuse the plain "project"/"project_pcp" trees the process-hosting case already needed.
build_project_tree "${WORK_DIR}/project" localfiles "${CSV_PATH}"
build_project_tree "${WORK_DIR}/project_tiny" localfiles "${TINY_CSV_PATH}"
build_project_tree "${WORK_DIR}/project_pcp" localfiles-pcp "${CSV_PATH}"
build_project_tree "${WORK_DIR}/project_pcp_tiny" localfiles-pcp "${TINY_CSV_PATH}"

PKG_ID="LocalFilesPcp"
PKG_VERSION="1.0.0"
FIXTURE_EXE="${ROOT_DIR}/tests/fixtures/PcpFakeConnector/bin/Release/net10.0/PcpFakeConnector"
RID="$(dotnet --info | awk '/^ RID:/ { print $2 }')"

if [[ ! -x "${FIXTURE_EXE}" ]]; then
    echo "error: PcpFakeConnector fixture not found at ${FIXTURE_EXE} (did the Release build above succeed?)" >&2
    exit 1
fi

# Materializes what `pz restore` would have left behind for a runtime:"process" connector package --
# mirrors tests/Pz.EndToEnd.Tests/ProcessHostParityTests.cs's WriteProcessPackage exactly (package
# layout, manifest shape, lock shape) so this exercises the same lock-verified load path a real run
# takes, not a shortcut around it.
stage_process_package() {
    local project_dir="$1"
    local pkg_dir="${project_dir}/.pz/packages/${PKG_ID}/${PKG_VERSION}"
    local bin_dir="${pkg_dir}/bin"
    mkdir -p "${bin_dir}"

    cat > "${bin_dir}/connector" << EOF
#!/bin/sh
exec "${FIXTURE_EXE}" "\$@"
EOF
    chmod +x "${bin_dir}/connector"

    cat > "${pkg_dir}/pz.connector.json" << EOF
{"name":"localfiles-pcp","protocolMajorMin":1,"protocolMajorMax":1,"capabilities":["NativeScan","NativeCopy","ReplaceWrites","BoundedWindow","PartitionedRead"],"projectDirectoryAnchor":true,"runtime":"process","entrypoints":{"${RID}":"bin/connector"}}
EOF

    cat > "${project_dir}/pz.lock.json" << EOF
{
  "version": 2,
  "rid": "${RID}",
  "packages": [
    {
      "id": "${PKG_ID}",
      "version": "${PKG_VERSION}",
      "sha512": "sha512-macro-bench-fixture",
      "requested": true,
      "assets": {
        "lib": [],
        "native": []
      }
    }
  ]
}
EOF
}

run_variant() {
    local variant_name="$1"
    local extra_engine_yaml="$2"
    local source_project_dir="${3:-${WORK_DIR}/project}"
    local connectors_yaml="${4:-  - package: Pz.Connector.LocalFiles
    version: 0.1.0}"
    local is_process="${5:-false}"
    local row_count="${6:-${ROW_COUNT}}"

    local project_dir="${WORK_DIR}/${variant_name}"
    mkdir -p "${project_dir}"
    cp -r "${source_project_dir}/"* "${project_dir}/"

    cat > "${project_dir}/project.yml" << EOF
name: macro_bench_${variant_name}
version: 0.1.0

connectors:
${connectors_yaml}

engine:
  threads: 4
${extra_engine_yaml}
EOF

    if [[ "${is_process}" == "true" ]]; then
        stage_process_package "${project_dir}"
    fi

    local start_ns end_ns elapsed_s
    start_ns=$(date +%s%N)
    dotnet run -c Release --project "${ROOT_DIR}/src/Pz.Cli" --no-build -- run --project "${project_dir}" >"${project_dir}/run.log" 2>&1
    end_ns=$(date +%s%N)
    elapsed_s=$(awk -v s="${start_ns}" -v e="${end_ns}" 'BEGIN { printf "%.4f", (e - s) / 1000000000.0 }')
    LAST_ELAPSED="${elapsed_s}"

    local rows_per_sec
    rows_per_sec=$(awk -v rows="${row_count}" -v secs="${elapsed_s}" 'BEGIN { if (secs <= 0) { print "n/a" } else { printf "%.0f", rows / secs } }')

    echo "  ${variant_name}: ${elapsed_s}s elapsed, ~${rows_per_sec} rows/sec (${row_count} rows)"
}

PCP_CONNECTORS_YAML="  - package: ${PKG_ID}
    version: ${PKG_VERSION}"

echo "-- Running pz run (native path: localfiles native_scan + native_copy) --"
run_variant native ""
NATIVE_S="${LAST_ELAPSED}"

echo "-- Running pz run (engine.force_universal: true, calibration N=${TINY}) --"
run_variant universal_tiny "  force_universal: true" "${WORK_DIR}/project_tiny" "" false "${TINY}"
UNIVERSAL_TINY_S="${LAST_ELAPSED}"

echo "-- Running pz run (engine.force_universal: true) --"
run_variant universal "  force_universal: true"
UNIVERSAL_S="${LAST_ELAPSED}"
echo

echo "-- Running pz run (process-hosted universal: localfiles-pcp over PCP, calibration N=${TINY}) --"
run_variant process_universal_tiny "  force_universal: true" "${WORK_DIR}/project_pcp_tiny" \
    "${PCP_CONNECTORS_YAML}" true "${TINY}"
PROCESS_UNIVERSAL_TINY_S="${LAST_ELAPSED}"

echo "-- Running pz run (process-hosted universal: localfiles-pcp over PCP, engine.force_universal: true) --"
run_variant process_universal "  force_universal: true" "${WORK_DIR}/project_pcp" "${PCP_CONNECTORS_YAML}" true
PROCESS_UNIVERSAL_S="${LAST_ELAPSED}"
echo

# Fixed process-hosting overhead: the tiny run is small enough that its elapsed time is almost entirely
# process-spawn + control-channel handshake on both sides, so subtracting the (also-tiny) in-proc
# universal baseline isolates it. Reported, never asserted -- see the header comment for why.
FIXED_OVERHEAD_S=$(awk -v pt="${PROCESS_UNIVERSAL_TINY_S}" -v ut="${UNIVERSAL_TINY_S}" \
    'BEGIN { printf "%.4f", pt - ut }')

# Marginal throughput ratio: (full - tiny) elapsed on each side isolates the steady-state, per-row cost
# from the one-time spawn/handshake cost both full-N runs also pay -- this is what spec invariant 7
# actually governs. Guarded per the same "can only happen at absurdly small N" carve-out the brief gives
# the raw-ratio zero-denominator case: a process_universal run that took no longer at full N than at
# tiny N has no marginal cost to measure, so it trivially passes rather than dividing by <= 0.
MARGINAL_LINE=$(awk -v uf="${UNIVERSAL_S}" -v ut="${UNIVERSAL_TINY_S}" -v pf="${PROCESS_UNIVERSAL_S}" -v pt="${PROCESS_UNIVERSAL_TINY_S}" 'BEGIN {
    denom = pf - pt;
    if (denom <= 0) { printf "1.0000 pass(denominator<=0)"; }
    else { printf "%.4f measured", (uf - ut) / denom; }
}')
MARGINAL_RATIO="${MARGINAL_LINE%% *}"
MARGINAL_NOTE="${MARGINAL_LINE#* }"
echo "  marginal (steady-state) throughput ratio: ${MARGINAL_RATIO} [${MARGINAL_NOTE}]"
echo "  fixed process-hosting overhead (reported, not gated): ${FIXED_OVERHEAD_S}s"

# Raw end-to-end ratio at full N only -- includes the one-time spawn cost above, so it is NOT what the
# gate below checks; printed only so a reader can see the number one actual `pz run` invocation would
# experience.
RAW_RATIO=$(awk -v u="${UNIVERSAL_S}" -v p="${PROCESS_UNIVERSAL_S}" \
    'BEGIN { if (p <= 0) { print "0.0000" } else { printf "%.4f", u / p } }')
echo "  raw end-to-end ratio (includes one-time spawn cost, NOT gated): ${RAW_RATIO}"
echo

echo "-- Passthrough floor: fused COPY (SELECT * FROM read_csv(...)) TO ..., production DuckSession --"
echo "   (spec 2026-07-31-passthrough-floor-bench: the one statement a native-fusion planner would"
echo "    emit -- no compile/plan/staging, so this is fusion's MAXIMUM win, not its expected win)"
FLOOR_LINE=$(dotnet "${ROOT_DIR}/scripts/passthrough-floor-probe.cs" "${CSV_PATH}" "${WORK_DIR}/floor_out.csv")
echo "  ${FLOOR_LINE}"
FLOOR_S=$(awk -v line="${FLOOR_LINE}" 'BEGIN { split(line, a, " "); gsub(/s$/, "", a[2]); print a[2] }')
echo

echo "== Summary (${ROW_COUNT} rows, calibration N=${TINY}) =="
awk -v rows="${ROW_COUNT}" -v n="${NATIVE_S}" -v u="${UNIVERSAL_S}" -v pu="${PROCESS_UNIVERSAL_S}" \
    -v f="${FLOOR_S}" -v mr="${MARGINAL_RATIO}" -v mn="${MARGINAL_NOTE}" -v rr="${RAW_RATIO}" \
    -v fixed="${FIXED_OVERHEAD_S}" 'BEGIN {
    printf "  native:              %8.4fs  ~%d rows/sec\n", n, rows / n;
    printf "  universal:           %8.4fs  ~%d rows/sec\n", u, rows / u;
    printf "  process universal:   %8.4fs  ~%d rows/sec\n", pu, rows / pu;
    printf "  fused floor:         %8.4fs  ~%d rows/sec\n", f, rows / f;
    printf "  max fusion win vs native: %.0f%%\n", (n - f) / n * 100;
    printf "  marginal (steady-state) throughput ratio: %s [%s] (floor 0.80, spec invariant 7 -- GATED)\n", mr, mn;
    printf "  fixed process-hosting overhead: %ss (reported only, not gated -- process-spawn/handshake cold start)\n", fixed;
    printf "  raw end-to-end ratio: %s (includes the fixed overhead above, NOT gated)\n", rr;
}'
echo

# Spec invariant 7 gate: process hosting's STEADY-STATE (marginal) throughput must not cost more than
# 20% versus in-proc universal. Checked after the summary prints so a failure still leaves the full set
# of numbers on screen. Deliberately gates the marginal ratio, not the raw one -- see the header comment.
RATIO_OK=$(awk -v r="${MARGINAL_RATIO}" 'BEGIN { print (r + 0 >= 0.80) ? "1" : "0" }')
if [[ "${RATIO_OK}" != "1" ]]; then
    echo "BUDGET FAIL: marginal (steady-state) process-hosted universal throughput ratio ${MARGINAL_RATIO} is" \
        "below the 0.80 floor (spec invariant 7) -- full-N universal ${UNIVERSAL_S}s -> process ${PROCESS_UNIVERSAL_S}s," \
        "tiny-N (N=${TINY}) universal ${UNIVERSAL_TINY_S}s -> process ${PROCESS_UNIVERSAL_TINY_S}s" >&2
    exit 1
fi

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
