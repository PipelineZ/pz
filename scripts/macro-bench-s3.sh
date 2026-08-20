#!/usr/bin/env bash
# LocalFiles CSV -> S3 (MinIO) macro throughput, mirroring scripts/macro-bench-mssql.sh's
# conventions. The s3 sink is native-only (every output is a DuckDB COPY over httpfs with a
# scoped CREATE SECRET), so the pz legs measure that native COPY plus pz's
# orchestration around it; the raw-DuckDB yardstick runs the same COPY statement through the
# duckdb CLI alone. Row counts are verified through an independent httpfs read after each leg.
# Standalone docker; SKIPs cleanly when docker is unavailable.
# Usage: scripts/macro-bench-s3.sh [row_count]   (default 1,000,000)
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROW_COUNT="${1:-1000000}"
SEED=42
MINIO_USER="pzbench"
MINIO_PASSWORD="pz-bench-$(date +%s)"
CONTAINER="pz-bench-minio"
PORT=19000
BUCKET="bench"

command -v docker >/dev/null || { echo "SKIP: docker not available"; exit 0; }
docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true
WORK_DIR="$(mktemp -d)"
trap 'docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true; rm -rf "${WORK_DIR}"' EXIT

echo "== PipelineZ s3 (MinIO) macro benchmark =="
echo "rows: ${ROW_COUNT} (seed ${SEED})"

echo "-- Building Release --"
dotnet build "${ROOT_DIR}/Pz.slnx" -c Release --nologo -v quiet
PZ=(dotnet "${ROOT_DIR}/src/Pz.Cli/bin/Release/net10.0/Pz.Cli.dll")

echo "-- Starting MinIO container --"
docker run -d --name "${CONTAINER}" -e "MINIO_ROOT_USER=${MINIO_USER}" \
  -e "MINIO_ROOT_PASSWORD=${MINIO_PASSWORD}" -p "${PORT}:9000" \
  minio/minio:RELEASE.2025-09-07T16-13-09Z server /data >/dev/null
for _ in $(seq 1 60); do
  if docker exec "${CONTAINER}" mc alias set local http://127.0.0.1:9000 "${MINIO_USER}" "${MINIO_PASSWORD}" >/dev/null 2>&1; then break; fi
  sleep 1
done
docker exec "${CONTAINER}" mc mb "local/${BUCKET}" >/dev/null || { echo "FAIL: MinIO did not become ready" >&2; exit 1; }

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
if command -v duckdb >/dev/null && duckdb -c "INSTALL httpfs; LOAD httpfs;" >/dev/null 2>&1; then
  HAVE_DUCKDB=1
fi
DUCK_SECRET="CREATE OR REPLACE SECRET pz_bench (TYPE s3, KEY_ID '${MINIO_USER}', SECRET '${MINIO_PASSWORD}', REGION 'us-east-1', ENDPOINT '127.0.0.1:${PORT}', URL_STYLE 'path', USE_SSL false)"

verify_object() { # $1 = object name, $2 = read function (read_csv / read_parquet)
  if [[ "${HAVE_DUCKDB}" != 1 ]]; then
    docker exec "${CONTAINER}" mc stat "local/${BUCKET}/$1" >/dev/null \
      || { echo "FAIL: object $1 missing" >&2; exit 1; }
    return
  fi
  local n
  # tail -n1: CREATE SECRET echoes its own "true" row before the count arrives.
  n="$(duckdb -noheader -list -c "LOAD httpfs; ${DUCK_SECRET}; SELECT count(*) FROM $2('s3://${BUCKET}/$1');" | tail -n1)"
  [[ "${n}" == "${ROW_COUNT}" ]] || { echo "FAIL: $1 has ${n} rows, expected ${ROW_COUNT}" >&2; exit 1; }
}

write_project() { # $1 = format, $2 = project dir
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

lake:
  connector: s3
  root: ${BUCKET}
  access_key: ${MINIO_USER}
  secret_key: \${BENCH_MINIO_PASSWORD}
  region: us-east-1
  endpoint: 127.0.0.1:${PORT}
  url_style: path
  use_ssl: false
  entities:
    orders_out:
      write:
        format: $1
        strategy: replace
EOF
  cp -r "${WORK_DIR}/data" "${dir}/data"
  cat > "${dir}/pipelines/orders_out.sql" <<'EOF'
INSERT INTO {{ sink('lake', 'orders_out') }}
select * from {{ source('src', 'orders') }}
EOF
}

run_scenario() { # $1 = label, $2 = format
  local dir="${WORK_DIR}/proj-$2"
  write_project "$2" "${dir}"
  local start end secs rate
  start=$(date +%s.%N)
  BENCH_MINIO_PASSWORD="${MINIO_PASSWORD}" "${PZ[@]}" run --all --project "${dir}" >"${dir}/run.log"
  end=$(date +%s.%N)
  secs=$(echo "${end} ${start}" | awk '{printf "%.1f", $1-$2}')
  rate=$(echo "${ROW_COUNT} ${secs}" | awk '{ if ($2 <= 0) print "n/a"; else printf "%.0f", $1/$2 }')
  verify_object "orders_out.$2" "$( [[ "$2" == parquet ]] && echo read_parquet || echo read_csv )"
  echo "${1}: ${secs}s (${rate} rows/sec)"
}

echo "-- Scenarios (pz: localfiles native_scan -> s3 native COPY) --"
run_scenario "pz csv -> s3 csv        " csv
run_scenario "pz csv -> s3 parquet    " parquet

echo "-- DuckDB httpfs comparison (raw-DuckDB yardstick) --"
if [[ "${HAVE_DUCKDB}" != 1 ]]; then
  echo "SKIP: duckdb CLI (or its httpfs extension) not available"
else
  READ_CSV="read_csv('${CSV_PATH}', header=true, columns={'id': 'BIGINT', 'customer_id': 'BIGINT', 'amount': 'DOUBLE', 'status': 'VARCHAR'})"
  run_ext() { # $1 = label, $2 = COPY target clause
    local start end secs rate
    start=$(date +%s.%N)
    duckdb -c "LOAD httpfs; ${DUCK_SECRET}; COPY (SELECT * FROM ${READ_CSV}) TO $2;" >/dev/null
    end=$(date +%s.%N)
    secs=$(echo "${end} ${start}" | awk '{printf "%.1f", $1-$2}')
    rate=$(echo "${ROW_COUNT} ${secs}" | awk '{ if ($2 <= 0) print "n/a"; else printf "%.0f", $1/$2 }')
    echo "${1}: ${secs}s (${rate} rows/sec)"
  }
  run_ext "ext csv -> s3 csv       " "'s3://${BUCKET}/ext_out.csv' (format csv, header)"
  verify_object "ext_out.csv" read_csv
  run_ext "ext csv -> s3 parquet   " "'s3://${BUCKET}/ext_out.parquet' (format parquet)"
  verify_object "ext_out.parquet" read_parquet
fi
echo "== DONE =="
