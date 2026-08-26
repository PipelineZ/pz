#!/usr/bin/env bash
# Head-to-head throughput comparison: the process-hosted Rust `deltalake-rs` connector (this repo,
# rust/pz-connector-deltalake) vs the in-proc .NET `deltalake` connector (a sibling repo,
# pz-connector-deltalake). This is the user's acceptance test for the whole process-connector-protocol
# feature -- the deliverable is the printed table, not a pass/fail gate.
#
# Structured like macro-bench.sh: deterministic awk CSV generation (no rand(), byte-identical across
# machines/runs), `set -euo pipefail`, a `mktemp -d` work directory (honors TMPDIR), a Release build
# step first. Unlike macro-bench.sh there is no throughput floor asserted here -- delta.net writes
# in-proc, delta-rs writes across a process boundary that pays a fixed per-run spawn cost (control-plane
# handshake, protocol negotiation) regardless of row count, and the two connectors use genuinely
# different write paths (delta-rs: DataFusion-backed Overwrite in a separate process; delta.net:
# DeltaLake.Net's own Overwrite path in-proc). The ratio this prints is a measurement to report, not a
# threshold to enforce.
#
# Four cases, three timed `pz run`s each, MEDIAN (not mean -- delta commits are noisy) rows/sec:
#   delta.net  no partition | delta.net  partition_by(status)
#   delta-rs   no partition | delta-rs   partition_by(status)
# `partition_by` is exercised because ConnectorCapabilities.ColumnPartitionedWrites is a hard part of
# the spec both connectors declare (rust: pz.connector.json capabilities; .NET: DeltaLakeConnector.
# Capabilities) -- ColumnPartitionedWrites is what lets a bare `partition_by: 'status'` (no `path:`,
# no date tokens) compile at all (Pz.Engine/Planning/ExecutionPlanner.cs's sink-side capability gate).
#
# Every case writes `strategy: 'replace'`, which is what makes a bare rerun of the SAME project safe to
# time three times in a row: replace is a full-table overwrite, not an incremental append keyed off
# stored watermark state, so there is no cursor/lock state that would turn run #2 or #3 into a no-op --
# unlike an incremental pipeline, nothing here needs resetting between samples. `.pz/runs` is still
# cleared between samples (matching macro-bench.sh) purely so repeated sampling does not grow disk usage
# beyond one run's artifacts; it has no bearing on correctness.
#
# Correctness gate BEFORE the numbers count: each table's LIVE (added, not later removed) row count,
# summed from `_delta_log/*.json` add-file `stats.numRecords` (the Delta protocol's own row-count
# bookkeeping -- see rust/pz-connector-deltalake/src/sink.rs's own _delta_log-parsing tests and
# tests/Pz.EndToEnd.Tests/DeltaRsRestoreTests.cs's CountLiveParquetRowsAsync for the same live-file
# add/remove tracking pattern), must equal the input row count. A mismatch fails the whole script before
# any throughput number is trusted.
#
# Packaging: this repo's rust/pz-connector-deltalake is packed via scripts/pack-deltalake-rs.sh (same
# nupkg shape RestoreDeltaRsTests.cs installs from, gated on PZ_DELTALAKE_RS_NUPKG there but built fresh
# here). The sibling .NET connector repo is NOT part of this repo (read-only, never modified here) --
# it has no dedicated pack script of its own for CI use (its own scripts/verify-external-connector.sh
# runs a plain `dotnet pack` on its csproj and uses that as its local feed, so this script mirrors that
# exact approach) and is packed the same way: `dotnet pack src/Pz.Connector.DeltaLake/*.csproj`. Packing
# it needs network (NuGet restore of DeltaLake.Net, ~220 MB, per that repo's own README) -- acceptable
# here since this script is a benchmark harness, not a CI test.
#
# Usage: scripts/delta-bench.sh [row_count]   (default 1,000,000)
# Env:   PZ_DELTALAKE_DOTNET_REPO   path to the sibling pz-connector-deltalake checkout
#                                   (default: $HOME/incubator/pz-connector-deltalake)
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROW_COUNT="${1:-1000000}"
SEED=42
SIBLING_REPO="${PZ_DELTALAKE_DOTNET_REPO:-$HOME/incubator/pz-connector-deltalake}"

if [[ ! -d "${SIBLING_REPO}" ]]; then
    echo "error: the sibling .NET Delta Lake connector repo was not found at '${SIBLING_REPO}'" >&2
    echo "  this benchmark compares deltalake-rs (in this repo) against Pz.Connector.DeltaLake" >&2
    echo "  (a separate repo), so both need to be checked out." >&2
    echo "  clone it:" >&2
    echo "    git clone git@github.com:PipelineZ/pz-connector-deltalake.git '${SIBLING_REPO}'" >&2
    echo "  or point PZ_DELTALAKE_DOTNET_REPO at wherever you already have it checked out." >&2
    exit 2
fi

# Kept shallow deliberately -- a process-hosted connector's control socket lives at
# <projectDir>/.pz/runs/<runId>/sockets/pcp-XXXXXXXX/control.sock, capped at ~104 bytes total
# (src/Pz.Cli/ProcessSocketRoot.cs); a project dir buried too deep silently falls back to a system-temp
# socket root instead of failing, but there is no reason to spend that budget here.
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT
FEED_DIR="${WORK_DIR}/feed"
mkdir -p "${FEED_DIR}"

echo "== delta-rs vs delta.net bench =="
echo "rows: ${ROW_COUNT} (seed ${SEED})"
echo "work dir: ${WORK_DIR}"
echo "sibling repo: ${SIBLING_REPO}"
echo

echo "-- Building pz Release --"
dotnet build "${ROOT_DIR}/Pz.slnx" -c Release --nologo -v quiet
echo "build OK"
echo

echo "-- Packing deltalake-rs (Rust, process-hosted) --"
"${ROOT_DIR}/scripts/pack-deltalake-rs.sh" "${FEED_DIR}"
RS_NUPKG="$(find "${FEED_DIR}" -maxdepth 1 -name 'Pz.Connector.DeltaLakeRs.*.nupkg' | head -1)"
[[ -n "${RS_NUPKG}" ]] || { echo "error: pack-deltalake-rs.sh did not produce a .nupkg" >&2; exit 1; }
RS_VERSION="$(basename "${RS_NUPKG}" .nupkg | sed 's/^Pz\.Connector\.DeltaLakeRs\.//')"
echo "packed Pz.Connector.DeltaLakeRs ${RS_VERSION}"
echo

echo "-- Packing Pz.Connector.DeltaLake (.NET, in-proc) from ${SIBLING_REPO} --"
dotnet pack "${SIBLING_REPO}/src/Pz.Connector.DeltaLake/Pz.Connector.DeltaLake.csproj" \
    -c Release -o "${FEED_DIR}" --nologo -v quiet
NET_NUPKG="$(find "${FEED_DIR}" -maxdepth 1 -name 'Pz.Connector.DeltaLake.*.nupkg' | head -1)"
[[ -n "${NET_NUPKG}" ]] || { echo "error: dotnet pack did not produce a Pz.Connector.DeltaLake .nupkg" >&2; exit 1; }
NET_VERSION="$(basename "${NET_NUPKG}" .nupkg | sed 's/^Pz\.Connector\.DeltaLake\.//')"
echo "packed Pz.Connector.DeltaLake ${NET_VERSION}"
echo

# Every value is a pure function of the row index and the fixed seed -- byte-identical on every
# machine and every run. `status` is the low-cardinality column both partition_by cases partition on.
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

echo "-- Generating deterministic CSV (${ROW_COUNT} rows) --"
CSV_PATH="${WORK_DIR}/orders.csv"
generate_csv "${ROW_COUNT}" "${CSV_PATH}"
echo "wrote ${CSV_PATH} ($(wc -l < "${CSV_PATH}") lines incl. header)"
echo

# One connections.yml/pipeline shape shared by every case -- a connection is a place with credentials
# (`root:`), an entity is a table in it, and the direction is the function the pipeline calls. Only the
# connector name, the lake root, and whether the sink call carries partition_by vary, so the two
# projects (delta.net / delta-rs) stay semantically identical except for the connector under test.
build_project() {
    local project_dir="$1" connector_name="$2" lake_root="$3" partitioned="$4"
    local package_id="$5" package_version="$6"

    mkdir -p "${project_dir}/data" "${project_dir}/pipelines"
    cp "${CSV_PATH}" "${project_dir}/data/orders.csv"

    cat > "${project_dir}/connections.yml" << EOF
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
  connector: ${connector_name}
  root: ${lake_root}
EOF

    cat > "${project_dir}/project.yml" << EOF
name: delta_bench_$(basename "${project_dir}")
version: 0.1.0

connectors:
  - package: Pz.Connector.LocalFiles
    version: 0.1.0
  - package: ${package_id}
    version: ${package_version}

engine:
  threads: 4
EOF

    local sink_call="sink('lake', 'orders', strategy: 'replace')"
    if [[ "${partitioned}" == "true" ]]; then
        sink_call="sink('lake', 'orders', strategy: 'replace', partition_by: 'status')"
    fi

    cat > "${project_dir}/pipelines/orders.sql" << EOF
INSERT INTO {{ ${sink_call} }}
select * from {{ source('bench', 'orders') }}
EOF
}

build_project "${WORK_DIR}/net_base" deltalake    "${WORK_DIR}/lake-net-base" false Pz.Connector.DeltaLake   "${NET_VERSION}"
build_project "${WORK_DIR}/net_part" deltalake    "${WORK_DIR}/lake-net-part" true  Pz.Connector.DeltaLake   "${NET_VERSION}"
build_project "${WORK_DIR}/rs_base"  deltalake-rs "${WORK_DIR}/lake-rs-base"  false Pz.Connector.DeltaLakeRs "${RS_VERSION}"
build_project "${WORK_DIR}/rs_part"  deltalake-rs "${WORK_DIR}/lake-rs-part"  true  Pz.Connector.DeltaLakeRs "${RS_VERSION}"

# nuget.org is needed alongside the local feed: DeltaLake.Net (the .NET connector's own transitive
# dependency, ~220 MB) is not vendored into the local feed, only Pz.Connector.DeltaLake's own nupkg is
# -- --feeds overrides the built-in nuget.org default entirely (RestoreCommand's own doc comment), so
# both have to be listed explicitly, exactly as pz-connector-deltalake/scripts/verify-external-
# connector.sh does for the same reason. deltalake-rs needs no such fallback (its nupkg is
# self-contained, no NuGet dependencies) but passing both feeds for every project is uniform and harmless.
restore_project() {
    local project_dir="$1"
    if ! dotnet run -c Release --project "${ROOT_DIR}/src/Pz.Cli" --no-build -- \
        restore --project "${project_dir}" --feeds "${FEED_DIR}" --feeds "https://api.nuget.org/v3/index.json" \
        > "${project_dir}/restore.log" 2>&1
    then
        echo "error: pz restore failed for ${project_dir}; see below" >&2
        cat "${project_dir}/restore.log" >&2
        exit 1
    fi
}

echo "-- Restoring 4 projects (delta.net's DeltaLake.Net dependency is a ~220 MB download if not" \
    "already cached) --"
restore_project "${WORK_DIR}/net_base"
restore_project "${WORK_DIR}/net_part"
restore_project "${WORK_DIR}/rs_base"
restore_project "${WORK_DIR}/rs_part"
echo "restore OK"
echo

declare -A CASE_MEDIAN_S
declare -A CASE_RPS

# Runs `pz run` 3 times against project_dir and reports each elapsed time plus the MEDIAN rows/sec
# (median, not mean -- delta commit latency is noisy, and a median is robust to one slow outlier in
# either direction, unlike macro-bench.sh's MIN-of-3 which only ever discards slowness). Each of the
# three is a genuinely fresh `pz run` -- see the header comment for why `strategy: 'replace'` makes that
# safe to repeat with no state to reset between samples.
run_case() {
    local case_name="$1" project_dir="$2" row_count="$3"
    local -a elapsed=()
    local i start_ns end_ns secs
    for i in 1 2 3; do
        start_ns=$(date +%s%N)
        if ! dotnet run -c Release --project "${ROOT_DIR}/src/Pz.Cli" --no-build -- \
            run --project "${project_dir}" > "${project_dir}/run_${i}.log" 2>&1
        then
            echo "error: pz run #${i} failed for ${case_name}; see below" >&2
            cat "${project_dir}/run_${i}.log" >&2
            exit 1
        fi
        end_ns=$(date +%s%N)
        secs=$(awk -v s="${start_ns}" -v e="${end_ns}" 'BEGIN { printf "%.4f", (e - s) / 1000000000.0 }')
        elapsed+=("${secs}")
        rm -rf "${project_dir}/.pz/runs"
    done

    local sorted median rps
    sorted="$(printf '%s\n' "${elapsed[@]}" | sort -n)"
    median="$(echo "${sorted}" | awk 'NR==2')"
    rps=$(awk -v rows="${row_count}" -v secs="${median}" \
        'BEGIN { if (secs <= 0) { print "n/a" } else { printf "%.0f", rows / secs } }')

    echo "  ${case_name}: runs=[${elapsed[0]}s, ${elapsed[1]}s, ${elapsed[2]}s]  median=${median}s  ~${rps} rows/sec"
    CASE_MEDIAN_S["${case_name}"]="${median}"
    CASE_RPS["${case_name}"]="${rps}"
}

echo "== Timed runs (3 each, ${ROW_COUNT} rows, median rows/sec) =="
echo "-- delta.net, no partition --"
run_case net_base "${WORK_DIR}/net_base" "${ROW_COUNT}"
echo "-- delta.net, partition_by(status) --"
run_case net_part "${WORK_DIR}/net_part" "${ROW_COUNT}"
echo "-- delta-rs, no partition --"
run_case rs_base "${WORK_DIR}/rs_base" "${ROW_COUNT}"
echo "-- delta-rs, partition_by(status) --"
run_case rs_part "${WORK_DIR}/rs_part" "${ROW_COUNT}"
echo

# Correctness gate BEFORE the numbers count -- see the header comment. Reads _delta_log directly (no
# DuckDB delta extension, no network), tracking which files are still LIVE (added, not later removed)
# the same way DeltaRsRestoreTests.cs's CountLiveParquetRowsAsync does, but sums each live file's own
# recorded `stats.numRecords` instead of re-reading the parquet bytes -- exactly the check the brief
# calls for and a strictly cheaper one at 1M rows.
sum_live_records() {
    local table_dir="$1"
    python3 - "${table_dir}" << 'PYEOF'
import json, sys, os

table_dir = sys.argv[1]
log_dir = os.path.join(table_dir, "_delta_log")
live = {}  # path -> numRecords
for name in sorted(f for f in os.listdir(log_dir) if f.endswith(".json")):
    with open(os.path.join(log_dir, name)) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            commit = json.loads(line)
            if "add" in commit:
                add = commit["add"]
                stats = json.loads(add["stats"]) if add.get("stats") else {}
                live[add["path"]] = stats.get("numRecords", 0)
            elif "remove" in commit:
                live.pop(commit["remove"]["path"], None)

print(sum(live.values()))
PYEOF
}

echo "-- Correctness gate: sum(numRecords) over live _delta_log adds == ${ROW_COUNT} --"
CORRECTNESS_FAIL=0
for pair in "net_base:${WORK_DIR}/lake-net-base" "net_part:${WORK_DIR}/lake-net-part" \
            "rs_base:${WORK_DIR}/lake-rs-base" "rs_part:${WORK_DIR}/lake-rs-part"; do
    name="${pair%%:*}"
    lake_root="${pair#*:}"
    table_dir="${lake_root}/orders"
    if [[ ! -d "${table_dir}/_delta_log" ]]; then
        echo "  ${name}: NO _delta_log at ${table_dir} -- correctness check FAILED" >&2
        CORRECTNESS_FAIL=1
        continue
    fi
    count="$(sum_live_records "${table_dir}")"
    if [[ "${count}" == "${ROW_COUNT}" ]]; then
        echo "  ${name}: ${count} rows (matches input) OK"
    else
        echo "  ${name}: ${count} rows, expected ${ROW_COUNT} -- MISMATCH" >&2
        CORRECTNESS_FAIL=1
    fi
done
echo

if [[ "${CORRECTNESS_FAIL}" != "0" ]]; then
    echo "CORRECTNESS FAILURE: at least one table's live row count does not match the input; the" >&2
    echo "throughput numbers above are not meaningful until this is fixed." >&2
    exit 1
fi

# The deliverable. No pass/fail threshold: delta.net writes in-proc and delta-rs writes across a
# process boundary that pays a fixed per-run spawn cost regardless of row count, and the two connectors
# take genuinely different write paths -- the ratio is what the user asked to measure, not a gate.
echo "== Summary (${ROW_COUNT} rows) =="
printf "  %-10s %-20s %16s %14s\n" "connector" "mode" "median rows/sec" "ratio rs/net"
awk -v nb="${CASE_RPS[net_base]}" -v rb="${CASE_RPS[rs_base]}" \
    -v np="${CASE_RPS[net_part]}" -v rp="${CASE_RPS[rs_part]}" 'BEGIN {
    printf "  %-10s %-20s %16s %14s\n", "delta.net", "no partition", nb, "-";
    printf "  %-10s %-20s %16s %14.4f\n", "delta-rs", "no partition", rb, rb / nb;
    printf "  %-10s %-20s %16s %14s\n", "delta.net", "partition_by(status)", np, "-";
    printf "  %-10s %-20s %16s %14.4f\n", "delta-rs", "partition_by(status)", rp, rp / np;
}'
echo
echo "delta-rs pays a fixed process-spawn + control-plane handshake cost per \`pz run\` that delta.net" \
    "does not (reported honestly above, not isolated out -- see the header comment)."
