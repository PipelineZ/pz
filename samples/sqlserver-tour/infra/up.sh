#!/usr/bin/env bash
# Start (or reuse) the local SQL Server container and (re)seed the pz database.
# Idempotent: safe to re-run; re-running resets all source AND mart tables.
set -euo pipefail
cd "$(dirname "$0")"
source ./env.sh

NAME=pz-mssql-tour
if ! docker ps -a --format '{{.Names}}' | grep -qx "$NAME"; then
  docker run -d --name "$NAME" \
    -e ACCEPT_EULA=Y \
    -e MSSQL_SA_PASSWORD="$PZ_MSSQL_PASSWORD" \
    -p "$PZ_MSSQL_PORT:1433" \
    mcr.microsoft.com/mssql/server:2022-latest >/dev/null
  echo "started container $NAME on port $PZ_MSSQL_PORT"
else
  docker start "$NAME" >/dev/null
  echo "reusing container $NAME"
fi

echo -n "waiting for sql server"
ok=
for _ in $(seq 1 60); do
  if docker exec "$NAME" /opt/mssql-tools18/bin/sqlcmd -C -S localhost \
      -U sa -P "$PZ_MSSQL_PASSWORD" -Q "select 1" >/dev/null 2>&1; then
    ok=1; break
  fi
  echo -n .
  sleep 2
done
echo
[ -n "$ok" ] || { echo "sql server did not come up within 120s (docker logs $NAME)"; exit 1; }

docker cp seed.sql "$NAME":/tmp/seed.sql
docker exec "$NAME" /opt/mssql-tools18/bin/sqlcmd -C -S localhost \
  -U sa -P "$PZ_MSSQL_PASSWORD" -i /tmp/seed.sql
echo "seeded database '$PZ_MSSQL_DB'. Next: source infra/env.sh"
