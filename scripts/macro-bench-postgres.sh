#!/usr/bin/env bash
# Postgres -> Postgres macro throughput, mirroring scripts/macro-bench-mssql.sh:
# pz scenarios (read partitions 1 vs 4 with an append sink, then merge) against a seeded
# public.src, each verified to have landed exactly the seeded row count; then the optional
# raw-DuckDB yardstick -- the same table read and written through DuckDB's own postgres
# extension (single process, no pz orchestration).
# Standalone docker; SKIPs cleanly when docker is unavailable.
# Usage: scripts/macro-bench-postgres.sh [row_count]   (default 1,000,000)
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROW_COUNT="${1:-1000000}"
PG_PASSWORD="pz-bench-$(date +%s)"
CONTAINER="pz-bench-postgres"
PORT=54320

command -v docker >/dev/null || { echo "SKIP: docker not available"; exit 0; }
docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true
WORK_DIR="$(mktemp -d)"
trap 'docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true; rm -rf "${WORK_DIR}"' EXIT

echo "== PipelineZ postgres macro benchmark =="
echo "rows: ${ROW_COUNT}"

echo "-- Building Release --"
dotnet build "${ROOT_DIR}/Pz.slnx" -c Release --nologo -v quiet
PZ=(dotnet "${ROOT_DIR}/src/Pz.Cli/bin/Release/net10.0/Pz.Cli.dll")

echo "-- Starting Postgres container --"
docker run -d --name "${CONTAINER}" -e "POSTGRES_PASSWORD=${PG_PASSWORD}" -e POSTGRES_DB=bench \
  -p "${PORT}:5432" postgres:16-alpine >/dev/null

PSQL=(docker exec "${CONTAINER}" psql -U postgres -d bench -q)
for _ in $(seq 1 60); do
  if "${PSQL[@]}" -c "select 1" >/dev/null 2>&1; then break; fi
  sleep 1
done
"${PSQL[@]}" -c "select 1" >/dev/null || { echo "FAIL: Postgres did not become ready" >&2; exit 1; }

echo "-- Seeding ${ROW_COUNT} rows --"
"${PSQL[@]}" -c "
  create table public.src (id int not null primary key, customer_id int not null,
    amount numeric(18,2) not null, status varchar(16) not null, updated timestamp not null);
  insert into public.src
  select i, (i * 2654435761) % 100000, ((i * 97) % 100000) / 100.0,
    case i % 4 when 0 then 'shipped' when 1 then 'pending' when 2 then 'returned' else 'cancelled' end,
    timestamp '2026-01-01' + make_interval(secs => i % 86400)
  from generate_series(1, ${ROW_COUNT}) as i;" >/dev/null
actual_rows="$("${PSQL[@]}" -t -A -c "select count(*) from public.src")"
if [[ "${actual_rows}" != "${ROW_COUNT}" ]]; then
  echo "FAIL: seeded ${actual_rows} rows, expected ${ROW_COUNT}" >&2
  exit 1
fi
echo "seeded ${actual_rows} rows (verified)"

write_project() { # $1 = partitions, $2 = write strategy, $3 = project dir
  local dir="$3"
  mkdir -p "${dir}/pipelines"
  printf 'name: bench\nversion: "1.0"\n' > "${dir}/project.yml"
  cat > "${dir}/connections.yml" <<EOF
bench:
  connector: postgres
  host: 127.0.0.1
  port: ${PORT}
  database: bench
  user: postgres
  password: \${BENCH_PG_PASSWORD}
  entities:
    public.src:
      read:
        partition_column: id
        partitions: $1
    public.mart_out:
      write:
        strategy: $2
$( [[ "$2" == "merge" ]] && printf '        keys: [id]\n' )
EOF
  cat > "${dir}/pipelines/mart.sql" <<'EOF'
INSERT INTO {{ sink('bench', 'public.mart_out') }}
select id, customer_id, amount, status, updated
from {{ source('bench', 'public.src') }}
EOF
}

verify_count() { # $1 = table (must hold exactly ROW_COUNT rows after a scenario)
  local n
  n="$("${PSQL[@]}" -t -A -c "select count(*) from $1")"
  [[ "${n}" == "${ROW_COUNT}" ]] || { echo "FAIL: $1 has ${n} rows, expected ${ROW_COUNT}" >&2; exit 1; }
}

run_scenario() { # $1 = label, $2 = partitions, $3 = strategy
  local dir="${WORK_DIR}/proj-$2-$3"
  write_project "$2" "$3" "${dir}"
  "${PSQL[@]}" -c "drop table if exists public.mart_out" >/dev/null 2>&1
  local start end secs rate
  start=$(date +%s.%N)
  BENCH_PG_PASSWORD="${PG_PASSWORD}" "${PZ[@]}" run --all --project "${dir}" >"${dir}/run.log"
  end=$(date +%s.%N)
  secs=$(echo "${end} ${start}" | awk '{printf "%.1f", $1-$2}')
  rate=$(echo "${ROW_COUNT} ${secs}" | awk '{ if ($2 <= 0) print "n/a"; else printf "%.0f", $1/$2 }')
  verify_count public.mart_out
  echo "${1}: ${secs}s (${rate} rows/sec)"
}

echo "-- Scenarios --"
run_scenario "read x1 partition, append" 1 append
run_scenario "read x4 partitions, append" 4 append
run_scenario "read x1 partition, merge " 1 merge

echo "-- DuckDB postgres extension comparison (raw-DuckDB yardstick) --"
if ! command -v duckdb >/dev/null; then
  echo "SKIP: duckdb CLI not available"
elif ! duckdb -c "INSTALL postgres; LOAD postgres;" >/dev/null 2>&1; then
  echo "SKIP: postgres extension unavailable (offline?)"
else
  EXT_DB="${WORK_DIR}/ext.duckdb"
  EXT_ATTACH="ATTACH 'host=127.0.0.1 port=${PORT} dbname=bench user=postgres password=${PG_PASSWORD}' AS pg (TYPE postgres)"
  run_ext() { # $1 = label, $2 = SQL (runs after LOAD + ATTACH against the persistent ext.duckdb)
    local start end secs rate
    start=$(date +%s.%N)
    duckdb "${EXT_DB}" -c "LOAD postgres; ${EXT_ATTACH}; $2" >/dev/null
    end=$(date +%s.%N)
    secs=$(echo "${end} ${start}" | awk '{printf "%.1f", $1-$2}')
    rate=$(echo "${ROW_COUNT} ${secs}" | awk '{ if ($2 <= 0) print "n/a"; else printf "%.0f", $1/$2 }')
    echo "${1}: ${secs}s (${rate} rows/sec)"
  }
  "${PSQL[@]}" -c "drop table if exists public.ext_out" >/dev/null 2>&1
  # Read: land the whole table into a disk-backed DuckDB file (the analogue of pz's SourceLoad
  # into staging.duckdb). Write: CTAS back into Postgres. Both timings include duckdb CLI
  # startup + ATTACH, as the pz legs include dotnet startup.
  run_ext "ext read  (CTAS pg -> local) " "CREATE OR REPLACE TABLE local_src AS FROM pg.public.src;"
  run_ext "ext write (CTAS local -> pg) " "CREATE TABLE pg.public.ext_out AS FROM local_src;"
  verify_count public.ext_out
fi
echo "== DONE =="
