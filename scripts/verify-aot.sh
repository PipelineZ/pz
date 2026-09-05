#!/usr/bin/env bash
# End-to-end proof that the Native AOT publish of `pz` actually WORKS, not just compiles.
#
# The AOT compile only proves the trimmed/AOT-compiled binary links; the third-party assemblies
# whose internals produce trim/AOT rollup warnings (see Pz.Cli.csproj's PublishAot block) can only be
# proven safe by running the paths that cross them. Each step below exists to cross one:
#
#   pz init + pz run (sample)  -> Scriban render, YamlDotNet load, DuckDB.NET Arrow interop,
#                                 Sylvan CSV read, Parquet.Net write, run artifacts (the whole hub)
#   pz restore (local feed)    -> NuGet.Protocol/NuGet.Packaging + Newtonsoft.Json (rooted assemblies)
#   pz run after that restore  -> the PZ0360 refusal is a clean coded error, not an AOT crash
#   pz connectors (PCP pkg)    -> ProcessConnectorHost spawn, Grpc.Net.Client + protobuf Hello over UDS
#   pz run (gcs SDK sink)      -> the Google stack (rooted assemblies): service-account key JSON
#                                 parse (Newtonsoft over Google.Apis DTOs), StorageClient build, and
#                                 an attempted upload against a dead endpoint failing as a CLASSIFIED
#                                 node error, never an AOT MissingMetadata crash
#   pz mcp                     -> the MCP SDK's stdio server: initialize handshake + tools/list
#
# Linux only (the PCP fixture serves unix domain sockets; AOT cross-OS compiles are impossible anyway).
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK_DIR="$(mktemp -d)"
PUBLISH_DIR="${WORK_DIR}/publish"
FEED_DIR="${WORK_DIR}/feed"
trap 'rm -rf "${WORK_DIR}"' EXIT

RID="linux-x64"
if [[ "$(uname -m)" == "aarch64" ]]; then RID="linux-arm64"; fi

echo "== PipelineZ Native AOT verification (${RID}) =="
echo "work dir: ${WORK_DIR}"
echo

echo "-- Publishing Pz.Cli with PublishAot --"
dotnet publish "${ROOT_DIR}/src/Pz.Cli" -c Release -r "${RID}" -p:PublishAot=true \
  -o "${PUBLISH_DIR}" --nologo -v quiet
PZ="${PUBLISH_DIR}/Pz.Cli"
[[ -x "${PZ}" ]] || { echo "FAIL: no native binary at ${PZ}"; exit 1; }
# A CoreCLR apphost would also pass -x; a native image has no .NET runtime config beside it.
[[ ! -f "${PUBLISH_DIR}/Pz.Cli.runtimeconfig.json" ]] || { echo "FAIL: runtimeconfig.json present — this is not an AOT image"; exit 1; }

echo "-- pz --version --"
"${PZ}" --version

echo "-- pz init + pz run (sample template, offline) --"
(cd "${WORK_DIR}" && "${PZ}" init hello --template sample >/dev/null)
(cd "${WORK_DIR}/hello" && "${PZ}" run orders_enriched)
[[ -f "${WORK_DIR}/hello/out/orders_curated/orders_curated.parquet" ]] || { echo "FAIL: parquet output missing"; exit 1; }
grep -q '"status":"success"' "${WORK_DIR}/hello/.pz/runs"/*/run_results.json || { echo "FAIL: run_results not success"; exit 1; }

# xlsx rides DuckDB's excel extension, downloaded on first use -- skip offline (CI's build-test job,
# and any other offline invocation) rather than fail on a network dependency this script doesn't own.
if [[ "${PZ_TESTS_OFFLINE:-0}" != "1" ]]; then
  echo "-- pz run with an xlsx sink (DuckDB excel extension install+load under AOT) --"
  cat > "${WORK_DIR}/hello/pipelines/orders_xlsx.sql" <<'EOF'
INSERT INTO {{ sink('lake', 'orders_xlsx', strategy: 'replace', format: 'xlsx') }}
select id, customer_id, amount, status from {{ ref('stg_orders') }}
EOF
  (cd "${WORK_DIR}/hello" && "${PZ}" run orders_xlsx)
  [[ -f "${WORK_DIR}/hello/out/orders_xlsx/orders_xlsx.xlsx" ]] || { echo "FAIL: xlsx output missing"; exit 1; }
else
  echo "-- skipping xlsx step (PZ_TESTS_OFFLINE=1) --"
fi

echo "-- pz restore from a local feed (NuGet + Newtonsoft under AOT) --"
dotnet pack "${ROOT_DIR}/tests/fixtures/connector-host/FakeTransitiveDep" -c Release -o "${FEED_DIR}" --nologo -v quiet
dotnet pack "${ROOT_DIR}/tests/fixtures/connector-host/FakeSourceConnector" -c Release \
  -p:FakeSourceConnectorVersion=1.2.3 -o "${FEED_DIR}" --nologo -v quiet
RESTORE_DIR="${WORK_DIR}/restore-smoke"
mkdir -p "${RESTORE_DIR}"
printf 'name: aot_restore\nversion: 0.1.0\n\nconnectors:\n  - package: FakeSourceConnector\n    version: 1.2.3\n' \
  > "${RESTORE_DIR}/project.yml"
(cd "${RESTORE_DIR}" && "${PZ}" restore --feeds "${FEED_DIR}")
[[ -f "${RESTORE_DIR}/.pz/packages/FakeSourceConnector/1.2.3/pz.connector.json" ]] || { echo "FAIL: restore did not materialize"; exit 1; }

echo "-- pz run refuses the dotnet-runtime package with PZ0360 (coded error, exit 2) --"
set +e
RUN_STDERR="$(cd "${RESTORE_DIR}" && "${PZ}" run 2>&1 >/dev/null)"
RUN_EXIT=$?
set -e
[[ "${RUN_EXIT}" -eq 2 ]] || { echo "FAIL: expected exit 2, got ${RUN_EXIT}"; exit 1; }
grep -q "PZ0360" <<<"${RUN_STDERR}" || { echo "FAIL: no PZ0360 in stderr: ${RUN_STDERR}"; exit 1; }

echo "-- pz connectors spawns a PCP connector (gRPC over unix sockets under AOT) --"
dotnet build "${ROOT_DIR}/tests/fixtures/PcpFakeConnector" -c Release --nologo -v quiet
FIXTURE_EXE="$(find "${ROOT_DIR}/tests/fixtures/PcpFakeConnector/bin/Release" -maxdepth 2 -name PcpFakeConnector -type f | head -1)"
[[ -n "${FIXTURE_EXE}" ]] || { echo "FAIL: PcpFakeConnector binary not found"; exit 1; }
PCP_DIR="${WORK_DIR}/pcp-smoke"
PKG_DIR="${PCP_DIR}/.pz/packages/LocalFilesPcp/1.0.0"
mkdir -p "${PKG_DIR}/bin"
printf '#!/bin/sh\nexec "%s" "$@"\n' "${FIXTURE_EXE}" > "${PKG_DIR}/bin/connector"
chmod +x "${PKG_DIR}/bin/connector"
HOST_RID="$(dotnet --info | sed -n 's/^ *RID: *//p' | head -1)"
cat > "${PKG_DIR}/pz.connector.json" <<PCPEOF
{"name":"localfiles-pcp","protocolMajorMin":1,"protocolMajorMax":1,
 "capabilities":["NativeScan","NativeCopy","ReplaceWrites","BoundedWindow","PartitionedRead"],
 "runtime":"process","entrypoints":{"${HOST_RID}":"bin/connector"}}
PCPEOF
printf 'name: aot_pcp\nversion: 0.1.0\n\nconnectors:\n  - package: LocalFilesPcp\n    version: 1.0.0\n' \
  > "${PCP_DIR}/project.yml"
python3 - "$PCP_DIR" "$HOST_RID" <<'PYEOF'
import json, sys
d, rid = sys.argv[1], sys.argv[2]
lock = {"version": 2, "rid": rid, "packages": [
    {"id": "LocalFilesPcp", "version": "1.0.0", "sha512": "sha512-aot-smoke",
     "assets": {"lib": [], "native": []}, "requested": True}]}
open(f"{d}/pz.lock.json", "w").write(json.dumps(lock, indent=2) + "\n")
PYEOF
LISTING="$(cd "${PCP_DIR}" && "${PZ}" connectors)"
grep -q "localfiles-pcp" <<<"${LISTING}" || { echo "FAIL: PCP connector missing from listing"; exit 1; }
grep -q "localfiles-pcp.*native+universal" <<<"${LISTING}" || { echo "FAIL: PCP Hello capabilities not reflected"; exit 1; }

echo "-- pz run with a gcs SDK sink fails classified against a dead endpoint (Google stack under AOT) --"
GCS_DIR="${WORK_DIR}/gcs-smoke"
mkdir -p "${GCS_DIR}/data" "${GCS_DIR}/pipelines"
# A structurally valid service-account key with a throwaway RSA key; token_uri points at a dead
# local port so the SDK's pre-upload token fetch fails fast and offline.
GCS_KEY_PEM="$(openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 2>/dev/null | awk 'BEGIN{ORS="\\n"} {print}')"
printf 'name: aot_gcs\nversion: 0.1.0\n\nconnectors:\n  - package: Pz.Connector.LocalFiles\n    version: 0.1.0\n  - package: Pz.Connector.Gcs\n    version: 0.1.0\n' \
  > "${GCS_DIR}/project.yml"
printf 'id,name\n1,a\n' > "${GCS_DIR}/data/rows.csv"
cat > "${GCS_DIR}/connections.yml" <<GCSEOF
files:
  connector: localfiles
  entities:
    rows:
      read:
        path: data/rows.csv
        format: csv
        columns:
          id: bigint
          name: varchar

lake:
  connector: gcs
  auth: service_account
  key_json: '{"type":"service_account","project_id":"aot-smoke","private_key_id":"0000000000000000000000000000000000000000","private_key":"${GCS_KEY_PEM}","client_email":"aot@aot-smoke.iam.gserviceaccount.com","client_id":"0","token_uri":"http://127.0.0.1:1/token"}'
  endpoint: "http://127.0.0.1:1/storage/v1/"
  root: aot-smoke-bucket
GCSEOF
printf "INSERT INTO {{ sink('lake', 'rows_out', strategy: 'replace', format: 'json') }}\nselect * from {{ source('files', 'rows') }}\n" \
  > "${GCS_DIR}/pipelines/rows_out.sql"
set +e
(cd "${GCS_DIR}" && timeout 120 "${PZ}" run >/dev/null 2>&1)
GCS_EXIT=$?
set -e
[[ "${GCS_EXIT}" -eq 1 ]] || { echo "FAIL: expected exit 1 (node failure), got ${GCS_EXIT}"; exit 1; }
grep -q '"status":"failed"' "${GCS_DIR}/.pz/runs"/*/run_results.json || { echo "FAIL: gcs sink node not marked failed"; exit 1; }
grep -q 'upload failed' "${GCS_DIR}/.pz/runs"/*/run_results.json || { echo "FAIL: failure is not the classified gcs upload error"; exit 1; }

echo "-- pz mcp: initialize handshake + tools/list --"
MCP_OUT="$( (printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"verify-aot","version":"0"}}}' \
  '{"jsonrpc":"2.0","method":"notifications/initialized"}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/list"}'; sleep 3) \
  | (cd "${WORK_DIR}/hello" && timeout 30 "${PZ}" mcp 2>/dev/null) )"
grep -q '"serverInfo":{"name":"pz"' <<<"${MCP_OUT}" || { echo "FAIL: MCP initialize gave no pz serverInfo"; exit 1; }
grep -q 'pz_validate' <<<"${MCP_OUT}" || { echo "FAIL: tools/list missing pz_validate"; exit 1; }

echo
echo "OK: native AOT pz publishes, initializes, runs a pipeline, restores, refuses PZ0360 cleanly,"
echo "    spawns a PCP connector over gRPC, fails a gcs SDK write classified, serves MCP, and"
echo "    writes xlsx through the excel extension."
