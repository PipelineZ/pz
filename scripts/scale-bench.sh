#!/usr/bin/env bash
# Project-size scaling harness.
#
# Generates N independent chains (csv source -> pipeline -> parquet sink, 3N nodes) and one chain of depth N
# for N in 10/50/200, runs `pz run --all` against the built Pz.Cli.dll, and reports wall time plus the
# run_completed phase breakdown (compileMs among them). Per-node cost shows up as superlinear growth across N.
#
# Each case takes the MINIMUM of 3 samples: scheduler/IO interference only ever ADDS time to a run's true
# cost, never subtracts it, so the minimum -- not the mean -- is the noise-robust estimate.
#
# Usage: scripts/scale-bench.sh   (PZ_DLL overrides the CLI under test)
set -euo pipefail

PZ_DLL="${PZ_DLL:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/src/Pz.Cli/bin/Release/net10.0/Pz.Cli.dll}"
OUT="$(mktemp -d)"
trap 'rm -rf "$OUT"' EXIT

gen_wide() { # N independent chains
  local n=$1 d=$OUT/wide_$n
  mkdir -p "$d/data" "$d/pipelines"
  printf 'name: wide_%s\nversion: 0.1.0\nconnectors:\n  - package: Pz.Connector.LocalFiles\n    version: 0.1.0\n' "$n" > "$d/project.yml"
  { echo "src:"; echo "  connector: localfiles"; echo "  entities:";
    for i in $(seq 1 "$n"); do
      echo "    t$i:"; echo "      read: { path: data/t$i.csv, format: csv }"
    done
    echo "out:"; echo "  connector: localfiles"; echo "  root: out"; echo "  entities:";
    for i in $(seq 1 "$n"); do echo "    o$i:"; echo "      write: { format: parquet, strategy: replace }"; done
  } > "$d/connections.yml"
  for i in $(seq 1 "$n"); do
    awk -v i="$i" 'BEGIN{print "id,v"; for(r=1;r<=100;r++) printf "%d,%d\n", r, r*i}' > "$d/data/t$i.csv"
    printf "INSERT INTO {{ sink('out', 'o%s') }}\nselect id, v * 2 as v2 from {{ source('src', 't%s') }}\n" "$i" "$i" > "$d/pipelines/p$i.sql"
  done
}

gen_deep() { # one source, chain of N pipelines via ref(), one sink
  local n=$1 d=$OUT/deep_$n
  mkdir -p "$d/data" "$d/pipelines"
  printf 'name: deep_%s\nversion: 0.1.0\nconnectors:\n  - package: Pz.Connector.LocalFiles\n    version: 0.1.0\n' "$n" > "$d/project.yml"
  printf 'src:\n  connector: localfiles\n  entities:\n    t:\n      read: { path: data/t.csv, format: csv }\nout:\n  connector: localfiles\n  root: out\n  entities:\n    o:\n      write: { format: parquet, strategy: replace }\n' > "$d/connections.yml"
  awk 'BEGIN{print "id,v"; for(r=1;r<=100;r++) printf "%d,%d\n", r, r}' > "$d/data/t.csv"
  printf "select id, v from {{ source('src', 't') }}\n" > "$d/pipelines/p1.sql"
  for i in $(seq 2 "$n"); do printf "select id, v + 1 as v from {{ ref('p%s') }}\n" "$((i-1))" > "$d/pipelines/p$i.sql"; done
  printf "INSERT INTO {{ sink('out', 'o') }}\nselect * from {{ ref('p%s') }}\n" "$n" > "$d/pipelines/final.sql"
}

run_one() {
  local d=$1 label=$2 best=999999 line=""
  for s in 1 2 3; do
    rm -rf "$d/.pz/runs" "$d/out"
    local t0=$(date +%s%N)
    dotnet "$PZ_DLL" run --project "$d" --all --log-format json > "$d/run.ndjson" 2> "$d/run.err" || { echo "$label FAILED"; tail -5 "$d/run.err"; tail -3 "$d/run.ndjson"; return; }
    local t1=$(date +%s%N); local ms=$(( (t1-t0)/1000000 ))
    if (( ms < best )); then best=$ms; line=$(grep '"run_completed"' "$d/run.ndjson" | python3 -c 'import sys,json;e=json.loads(sys.stdin.read());print(e.get("succeeded"), "nodes; engine", e["durationMs"],"ms; phases", e.get("phases"))'); fi
  done
  echo "$label: wall ${best}ms (min of 3) | $line"
}

echo "-- startup --"
for s in 1 2 3; do t0=$(date +%s%N); dotnet "$PZ_DLL" --version >/dev/null; echo "pz --version: $(( ($(date +%s%N)-t0)/1000000 ))ms"; done
for n in 10 50 200; do gen_wide "$n"; run_one "$OUT/wide_$n" "wide N=$n (3N nodes)"; done
for n in 10 50 200; do gen_deep "$n"; run_one "$OUT/deep_$n" "deep N=$n"; done
