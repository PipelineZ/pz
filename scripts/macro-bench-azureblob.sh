#!/usr/bin/env bash
# LocalFiles CSV <-> Azure Blob (Azurite) macro throughput, mirroring scripts/macro-bench-s3.sh.
# The azureblob connector is native-tier both ways (read: native-only scan; write: native COPY over
# the DuckDB azure extension unless partitioned), so the pz legs measure that plus pz's
# orchestration; the raw-DuckDB yardstick runs the same statements through the duckdb CLI alone.
# Needs docker (Azurite >= 3.35.0 -- earlier versions reject the azure extension's requests) and
# the az CLI (container creation); SKIPs cleanly when either is missing.
# Usage: scripts/macro-bench-azureblob.sh [row_count]   (default 1,000,000)
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROW_COUNT="${1:-1000000}"
SEED=42
CONTAINER="pz-bench-azurite"
PORT=11000
BUCKET="bench"
# Azurite's well-known devstore account (public documented key, not a secret).
AZURE_CS="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:${PORT}/devstoreaccount1;"

command -v docker >/dev/null || { echo "SKIP: docker not available"; exit 0; }
command -v az >/dev/null || { echo "SKIP: az CLI not available (needed to create the container)"; exit 0; }
docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true
WORK_DIR="$(mktemp -d)"
trap 'docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true; rm -rf "${WORK_DIR}"' EXIT

echo "== PipelineZ azureblob (Azurite) macro benchmark =="
echo "rows: ${ROW_COUNT} (seed ${SEED})"

echo "-- Building Release --"
dotnet build "${ROOT_DIR}/Pz.slnx" -c Release --nologo -v quiet
PZ=(dotnet "${ROOT_DIR}/src/Pz.Cli/bin/Release/net10.0/Pz.Cli.dll")

echo "-- Starting Azurite container --"
docker run -d --name "${CONTAINER}" -p "${PORT}:10000" \
  mcr.microsoft.com/azure-storage/azurite:3.35.0 azurite-blob --blobHost 0.0.0.0 --skipApiVersionCheck >/dev/null
ready=0
for _ in $(seq 1 60); do
  if az storage container create --name "${BUCKET}" --connection-string "${AZURE_CS}" --only-show-errors >/dev/null 2>&1; then
    ready=1; break
  fi
  sleep 1
done
[[ "${ready}" == 1 ]] || { echo "FAIL: Azurite did not become ready" >&2; exit 1; }

echo "-- Generating deterministic CSV (${ROW_COUNT} rows) --"
mkdir -p "${WORK_DIR}/data"
CSV_PATH="${WORK_DIR}/data/orders.csv"
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

HAVE_DUCKDB=0
if command -v duckdb >/dev/null && duckdb -c "INSTALL azure; LOAD azure;" >/dev/null 2>&1; then
  HAVE_DUCKDB=1
fi
DUCK_SECRET="CREATE OR REPLACE SECRET pz_bench (TYPE azure, CONNECTION_STRING '${AZURE_CS}')"

verify_blob() { # $1 = blob name, $2 = read function (read_csv / read_parquet)
  if [[ "${HAVE_DUCKDB}" != 1 ]]; then
    az storage blob show --container-name "${BUCKET}" --name "$1" --connection-string "${AZURE_CS}" \
      --only-show-errors >/dev/null || { echo "FAIL: blob $1 missing" >&2; exit 1; }
    return
  fi
  local n
  # tail -n1: CREATE SECRET echoes its own "true" row before the count arrives.
  n="$(duckdb -noheader -list -c "LOAD azure; ${DUCK_SECRET}; SELECT count(*) FROM $2('az://${BUCKET}/$1');" | tail -n1)"
  [[ "${n}" == "${ROW_COUNT}" ]] || { echo "FAIL: $1 has ${n} rows, expected ${ROW_COUNT}" >&2; exit 1; }
}

write_up_project() { # $1 = format, $2 = project dir  (local csv -> azure blob)
  local dir="$2"
  mkdir -p "${dir}/pipelines"
  printf 'name: bench\nversion: "1.0"\n' > "${dir}/project.yml"
  cat > "${dir}/connections.yml" <<EOF
src:
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

blob:
  connector: azureblob
  auth: connection_string
  connection_string: \${BENCH_AZURE_CS}
  entities:
    orders_out:
      write:
        container: ${BUCKET}
        format: $1
        strategy: replace
EOF
  cp -r "${WORK_DIR}/data" "${dir}/data"
  cat > "${dir}/pipelines/orders_out.sql" <<'EOF'
INSERT INTO {{ sink('blob', 'orders_out') }}
select * from {{ source('src', 'orders') }}
EOF
}

write_down_project() { # $1 = project dir  (azure blob csv -> local csv)
  local dir="$1"
  mkdir -p "${dir}/pipelines"
  printf 'name: bench\nversion: "1.0"\n' > "${dir}/project.yml"
  cat > "${dir}/connections.yml" <<EOF
blob:
  connector: azureblob
  auth: connection_string
  connection_string: \${BENCH_AZURE_CS}
  entities:
    orders_in:
      read:
        container: ${BUCKET}
        path: orders_out.csv
        format: csv

out:
  connector: localfiles
  root: out
  entities:
    orders_local:
      write:
        format: csv
        strategy: replace
EOF
  cat > "${dir}/pipelines/orders_local.sql" <<'EOF'
INSERT INTO {{ sink('out', 'orders_local') }}
select * from {{ source('blob', 'orders_in') }}
EOF
}

time_pz() { # $1 = label, $2 = project dir
  local start end secs rate
  start=$(date +%s.%N)
  BENCH_AZURE_CS="${AZURE_CS}" "${PZ[@]}" run --all --project "$2" >"$2/run.log"
  end=$(date +%s.%N)
  secs=$(echo "${end} ${start}" | awk '{printf "%.1f", $1-$2}')
  rate=$(echo "${ROW_COUNT} ${secs}" | awk '{ if ($2 <= 0) print "n/a"; else printf "%.0f", $1/$2 }')
  echo "${1}: ${secs}s (${rate} rows/sec)"
}

echo "-- Scenarios (pz: native tier both ways over the azure extension) --"
DIR_CSV="${WORK_DIR}/proj-up-csv";      write_up_project csv "${DIR_CSV}"
DIR_PARQ="${WORK_DIR}/proj-up-parquet"; write_up_project parquet "${DIR_PARQ}"
DIR_DOWN="${WORK_DIR}/proj-down";       write_down_project "${DIR_DOWN}"
time_pz "pz csv -> az csv        " "${DIR_CSV}"
verify_blob "orders_out.csv" read_csv
time_pz "pz csv -> az parquet    " "${DIR_PARQ}"
verify_blob "orders_out.parquet" read_parquet
time_pz "pz az csv -> local csv  " "${DIR_DOWN}"
down_rows=$(( $(wc -l < "${DIR_DOWN}/out/orders_local/orders_local.csv") - 1 ))
[[ "${down_rows}" == "${ROW_COUNT}" ]] || { echo "FAIL: downloaded ${down_rows} rows, expected ${ROW_COUNT}" >&2; exit 1; }

echo "-- DuckDB azure extension comparison (raw-DuckDB yardstick) --"
if [[ "${HAVE_DUCKDB}" != 1 ]]; then
  echo "SKIP: duckdb CLI (or its azure extension) not available"
else
  READ_CSV="read_csv('${CSV_PATH}', header=true, columns={'id': 'BIGINT', 'customer_id': 'BIGINT', 'amount': 'DOUBLE', 'status': 'VARCHAR'})"
  run_ext() { # $1 = label, $2 = full COPY statement
    local start end secs rate
    start=$(date +%s.%N)
    duckdb -c "LOAD azure; ${DUCK_SECRET}; $2;" >/dev/null
    end=$(date +%s.%N)
    secs=$(echo "${end} ${start}" | awk '{printf "%.1f", $1-$2}')
    rate=$(echo "${ROW_COUNT} ${secs}" | awk '{ if ($2 <= 0) print "n/a"; else printf "%.0f", $1/$2 }')
    echo "${1}: ${secs}s (${rate} rows/sec)"
  }
  run_ext "ext csv -> az csv       " "COPY (SELECT * FROM ${READ_CSV}) TO 'az://${BUCKET}/ext_out.csv' (format csv, header)"
  verify_blob "ext_out.csv" read_csv
  run_ext "ext csv -> az parquet   " "COPY (SELECT * FROM ${READ_CSV}) TO 'az://${BUCKET}/ext_out.parquet' (format parquet)"
  verify_blob "ext_out.parquet" read_parquet
  run_ext "ext az csv -> local csv " "COPY (SELECT * FROM read_csv('az://${BUCKET}/orders_out.csv')) TO '${WORK_DIR}/ext_down.csv' (format csv, header)"
fi
echo "== DONE =="
