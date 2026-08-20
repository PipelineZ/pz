#!/usr/bin/env bash
# SQL Server -> SQL Server macro throughput.
# Scenarios: read partitions 1 vs 4 (append sink), then merge vs append (single partition).
# Standalone docker; SKIPs cleanly when docker is unavailable.
#
# Optional comparison leg: when the `duckdb` CLI is on PATH and the community `mssql` extension
# installs, the same seeded table is read and written through DuckDB's own extension (single
# process, no pz orchestration) -- the "how close to raw DuckDB are we" yardstick.
#
# Usage: scripts/macro-bench-mssql.sh [row_count]   (default 1,000,000)
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROW_COUNT="${1:-1000000}"
SA_PASSWORD="Pz-bench-$(date +%s)!"
CONTAINER="pz-bench-mssql"
PORT=14330

command -v docker >/dev/null || { echo "SKIP: docker not available"; exit 0; }
docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true
WORK_DIR="$(mktemp -d)"
trap 'docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true; rm -rf "${WORK_DIR}"' EXIT

echo "== PipelineZ mssql macro benchmark =="
echo "rows: ${ROW_COUNT}"

echo "-- Building Release --"
dotnet build "${ROOT_DIR}/Pz.slnx" -c Release --nologo -v quiet
PZ=(dotnet "${ROOT_DIR}/src/Pz.Cli/bin/Release/net10.0/Pz.Cli.dll")

echo "-- Starting SQL Server container --"
docker run -d --name "${CONTAINER}" -e ACCEPT_EULA=Y -e "MSSQL_SA_PASSWORD=${SA_PASSWORD}" \
  -p "${PORT}:1433" mcr.microsoft.com/mssql/server:2022-latest >/dev/null

SQLCMD=(docker exec -e "SQLCMDPASSWORD=${SA_PASSWORD}" "${CONTAINER}" /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa)
for _ in $(seq 1 60); do
  if "${SQLCMD[@]}" -Q "select 1" >/dev/null 2>&1; then break; fi
  sleep 2
done
"${SQLCMD[@]}" -Q "select 1" >/dev/null || { echo "FAIL: SQL Server did not become ready" >&2; exit 1; }

echo "-- Seeding ${ROW_COUNT} rows --"
"${SQLCMD[@]}" -Q "create database bench" >/dev/null
"${SQLCMD[@]}" -d bench -Q "
  create table dbo.src (id int not null primary key, customer_id int not null,
    amount decimal(18,2) not null, status nvarchar(16) not null, updated datetime2(6) not null);
  with n as (select top (${ROW_COUNT}) row_number() over (order by (select null)) as i
             from sys.all_objects a cross join sys.all_objects b cross join sys.all_objects c)
  insert into dbo.src select i, (i * 2654435761) % 100000, ((i * 97) % 100000) / 100.0,
    case i % 4 when 0 then N'shipped' when 1 then N'pending' when 2 then N'returned' else N'cancelled' end,
    dateadd(second, i % 86400, '2026-01-01') from n;" >/dev/null
actual_rows="$("${SQLCMD[@]}" -d bench -h -1 -Q "set nocount on; select count(*) from dbo.src" | tr -d '[:space:]')"
if [[ "${actual_rows}" != "${ROW_COUNT}" ]]; then
  echo "FAIL: seeded ${actual_rows} rows, expected ${ROW_COUNT} (sys.all_objects cross-join undershoot?)" >&2
  exit 1
fi
echo "seeded ${actual_rows} rows (verified)"

write_project() { # $1 = partitions, $2 = write strategy, $3 = project dir
  local dir="$3"
  mkdir -p "${dir}/pipelines"
  printf 'name: bench\nversion: "1.0"\n' > "${dir}/project.yml"
  cat > "${dir}/connections.yml" <<EOF
bench:
  connector: sqlserver
  host: 127.0.0.1
  port: ${PORT}
  database: bench
  user: sa
  password: \${BENCH_SA_PASSWORD}
  trust_server_certificate: true
  entities:
    dbo.src:
      read:
        partition_column: id
        partitions: $1
    dbo.mart_out:
      write:
        strategy: $2
$( [[ "$2" == "merge" ]] && printf '        keys: [id]\n' )
EOF
  cat > "${dir}/pipelines/mart.sql" <<'EOF'
INSERT INTO {{ sink('bench', 'dbo.mart_out') }}
select id, customer_id, amount, status, updated
from {{ source('bench', 'dbo.src') }}
EOF
}

verify_count() { # $1 = table (must hold exactly ROW_COUNT rows after a scenario)
  local n
  n="$("${SQLCMD[@]}" -d bench -h -1 -Q "set nocount on; select count(*) from $1" | tr -d '[:space:]')"
  [[ "${n}" == "${ROW_COUNT}" ]] || { echo "FAIL: $1 has ${n} rows, expected ${ROW_COUNT}" >&2; exit 1; }
}

run_scenario() { # $1 = label, $2 = partitions, $3 = strategy
  local dir="${WORK_DIR}/proj-$2-$3"
  write_project "$2" "$3" "${dir}"
  "${SQLCMD[@]}" -d bench -Q "drop table if exists dbo.mart_out" >/dev/null
  local start end secs rate
  start=$(date +%s.%N)
  BENCH_SA_PASSWORD="${SA_PASSWORD}" "${PZ[@]}" run --all --project "${dir}" >"${dir}/run.log"
  end=$(date +%s.%N)
  secs=$(echo "${end} ${start}" | awk '{printf "%.1f", $1-$2}')
  rate=$(echo "${ROW_COUNT} ${secs}" | awk '{ if ($2 <= 0) print "n/a"; else printf "%.0f", $1/$2 }')
  verify_count dbo.mart_out
  echo "${1}: ${secs}s (${rate} rows/sec)"
}

echo "-- Scenarios --"
run_scenario "read x1 partition, append" 1 append
run_scenario "read x4 partitions, append" 4 append
run_scenario "read x1 partition, merge " 1 merge

echo "-- DuckDB mssql community extension comparison (raw-DuckDB yardstick) --"
if ! command -v duckdb >/dev/null; then
  echo "SKIP: duckdb CLI not available"
elif ! duckdb -c "INSTALL mssql FROM community; LOAD mssql;" >/dev/null 2>&1; then
  echo "SKIP: community mssql extension unavailable (offline?)"
else
  EXT_DB="${WORK_DIR}/ext.duckdb"
  EXT_ATTACH="ATTACH 'Server=127.0.0.1,${PORT};Database=bench;User Id=sa;Password=${SA_PASSWORD}' AS ms (TYPE mssql)"
  run_ext() { # $1 = label, $2 = SQL (runs after LOAD + ATTACH against the persistent ext.duckdb)
    local start end secs rate
    start=$(date +%s.%N)
    duckdb "${EXT_DB}" -c "LOAD mssql; ${EXT_ATTACH}; $2" >/dev/null
    end=$(date +%s.%N)
    secs=$(echo "${end} ${start}" | awk '{printf "%.1f", $1-$2}')
    rate=$(echo "${ROW_COUNT} ${secs}" | awk '{ if ($2 <= 0) print "n/a"; else printf "%.0f", $1/$2 }')
    echo "${1}: ${secs}s (${rate} rows/sec)"
  }
  "${SQLCMD[@]}" -d bench -Q "drop table if exists dbo.ext_out" >/dev/null
  # Read: land the whole table into a disk-backed DuckDB file (the analogue of pz's SourceLoad
  # into staging.duckdb). Write: CTAS back into SQL Server (parallel BCP, the extension default).
  # Both timings include duckdb CLI startup + ATTACH, as the pz legs include dotnet startup.
  run_ext "ext read  (CTAS ms -> local) " "CREATE OR REPLACE TABLE local_src AS FROM ms.dbo.src;"
  run_ext "ext write (CTAS local -> ms) " "CREATE TABLE ms.dbo.ext_out AS FROM local_src;"
  verify_count dbo.ext_out
fi
echo "== DONE =="
