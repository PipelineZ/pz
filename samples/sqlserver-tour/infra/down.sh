#!/usr/bin/env bash
# Remove the SQL Server container (all data is lost; ./up.sh recreates + reseeds).
set -euo pipefail
docker rm -f pz-mssql-tour >/dev/null 2>&1 || true
echo "removed container pz-mssql-tour"
