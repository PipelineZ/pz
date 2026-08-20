# source this before running any scenario: `source infra/env.sh`
export PZ_MSSQL_HOST=localhost
export PZ_MSSQL_PORT=14333   # note: also hardcoded as port: 14333 in every project YAML (schema wants an integer; env interpolation yields strings)
export PZ_MSSQL_DB=pz
export PZ_MSSQL_USER=sa
export PZ_MSSQL_PASSWORD='Pz!Passw0rd_2026'
