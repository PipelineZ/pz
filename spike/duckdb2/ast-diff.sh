#!/usr/bin/env bash
# Replays ast-corpus.jsonl (every statement DuckDbSqlAstReader sent to DuckDB during the AST-related test
# suites on 1.5.6) through the 1.5.6 and v2.0-cyanoptera CLIs and writes the differences to
# artifacts/duckdb2-ast-diff.txt.
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
cli="$root/artifacts/duckdb2-cli"
mkdir -p "$cli/v15" "$cli/v20"
[ -x "$cli/v15/duckdb" ] || {
  curl -fsSL -o "$cli/v15/cli.zip" https://github.com/duckdb/duckdb/releases/download/v1.5.6/duckdb_cli-linux-amd64.zip
  unzip -oq "$cli/v15/cli.zip" -d "$cli/v15"
}
[ -x "$cli/v20/duckdb" ] || {
  curl -fsSL https://artifacts.duckdb.org/v2.0-cyanoptera/duckdb-cli-linux-amd64.tar.gz | tar -xz -C "$cli/v20"
}
"$cli/v20/duckdb" -no-agent -c "select library_version from pragma_version()"
python3 -I "$root/spike/duckdb2/ast_diff.py" "$root/spike/duckdb2/ast-corpus.jsonl" \
  "$cli/v15/duckdb" "$cli/v20/duckdb" "$root/artifacts/duckdb2-ast-diff.txt"
