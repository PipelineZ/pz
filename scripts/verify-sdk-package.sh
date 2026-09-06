#!/usr/bin/env bash
# End-to-end proof that a connector packaged through Pz.Connectors.Sdk's targets is installable and
# runnable by pz the way a stranger's connector would be: publish (AOT, then self-contained) ->
# pack into a local feed -> `pz restore` from that feed -> `pz run` both directions ->
# `pz connector test` against the materialized package.
#
# The fixture (tests/fixtures/PcpFakeConnector) is the connector under test: it is a real
# LocalFilesConnector served by the real SDK, so a green run here proves the SDK, its targets, the
# manifest self-description and the host's restore layout agree end to end.
#
# Linux only: Native AOT cannot cross-compile between OSes and the host's runner is Linux.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT

RID="linux-x64"
if [[ "$(uname -m)" == "aarch64" ]]; then RID="linux-arm64"; fi
FIXTURE="${ROOT_DIR}/tests/fixtures/PcpFakeConnector"
PKG_ID="PcpFakeConnector"
PKG_VERSION="1.0.0"

echo "== Pz.Connectors.Sdk packaging verification (${RID}) =="
echo "work dir: ${WORK_DIR}"

echo "-- Building pz --"
dotnet build "${ROOT_DIR}/src/Pz.Cli" -c Release --nologo -v quiet
PZ="${ROOT_DIR}/src/Pz.Cli/bin/Release/net10.0/Pz.Cli"
[[ -x "${PZ}" ]] || { echo "FAIL: no pz binary at ${PZ}"; exit 1; }

verify_mode() {
  local mode="$1"
  local mode_dir="${WORK_DIR}/${mode}"
  local stage="${mode_dir}/stage/"
  local feed="${mode_dir}/feed"
  local proj="${mode_dir}/project"
  mkdir -p "${feed}" "${proj}/data" "${proj}/pipelines"

  echo
  echo "== mode: ${mode} =="
  echo "-- dotnet publish -r ${RID} (PzPackaging=${mode}) --"
  dotnet publish "${FIXTURE}" -c Release -r "${RID}" -p:PzPackaging="${mode}" -p:PzNativeStaging="${stage}" \
    --nologo -v quiet
  [[ -x "${stage}${RID}/PcpFakeConnector" ]] || { echo "FAIL: no staged binary at ${stage}${RID}/PcpFakeConnector"; exit 1; }
  if [[ "${mode}" == "aot" ]]; then
    [[ ! -f "${stage}${RID}/PcpFakeConnector.dll" ]] || { echo "FAIL: AOT publish staged a managed dll"; exit 1; }
  fi

  echo "-- dotnet pack --"
  # PzRuntimeIdentifiers is narrowed to the one RID this machine can publish: the missing-RID warning
  # (PZSDK002) is an error under this repo's TreatWarningsAsErrors, and here it would be a false alarm.
  # MinVerVersionOverride, not PackageVersion: MinVer computes the version in a target and would
  # otherwise stamp the git-height prerelease over anything set on the command line.
  dotnet pack "${FIXTURE}" -c Release -p:PzPackaging="${mode}" -p:PzNativeStaging="${stage}" \
    -p:PzRuntimeIdentifiers="${RID}" -p:IsPackable=true -p:MinVerVersionOverride="${PKG_VERSION}" \
    -o "${feed}" --nologo -v quiet
  local nupkg="${feed}/${PKG_ID}.${PKG_VERSION}.nupkg"
  [[ -f "${nupkg}" ]] || { echo "FAIL: no nupkg at ${nupkg}"; exit 1; }

  echo "-- nupkg layout --"
  local listing
  listing="$(unzip -Z1 "${nupkg}")"
  grep -qx "runtimes/${RID}/native/PcpFakeConnector" <<<"${listing}" || { echo "FAIL: binary missing from runtimes/${RID}/native/"; echo "${listing}"; exit 1; }
  grep -qx "pz.connector.json" <<<"${listing}" || { echo "FAIL: manifest missing from nupkg root"; exit 1; }
  ! grep -q '^lib/' <<<"${listing}" || { echo "FAIL: lib/ must not be packed"; exit 1; }
  ! unzip -p "${nupkg}" "${PKG_ID}.nuspec" | grep -q '<dependency ' || { echo "FAIL: nuspec declares dependencies"; exit 1; }
  local manifest
  manifest="$(unzip -p "${nupkg}" pz.connector.json)"
  grep -q '"runtime": "process"' <<<"${manifest}" || { echo "FAIL: manifest runtime"; echo "${manifest}"; exit 1; }
  grep -q "\"${RID}\": \"native/PcpFakeConnector\"" <<<"${manifest}" || { echo "FAIL: manifest entrypoint"; echo "${manifest}"; exit 1; }
  grep -q '"name": "localfiles-pcp"' <<<"${manifest}" || { echo "FAIL: manifest name"; echo "${manifest}"; exit 1; }
  grep -q '"PartitionedRead"' <<<"${manifest}" || { echo "FAIL: manifest capabilities"; echo "${manifest}"; exit 1; }

  echo "-- pz restore from the local feed --"
  printf 'name: sdk_verify\nversion: 0.1.0\n\nconnectors:\n  - package: %s\n    version: %s\nengine:\n  threads: 2\n' \
    "${PKG_ID}" "${PKG_VERSION}" > "${proj}/project.yml"
  cat > "${proj}/connections.yml" <<'EOF'
files:
  connector: localfiles-pcp
  entities:
    orders:
      read:
        path: data/orders.csv
        format: csv
        columns:
          id: bigint
          customer: varchar
          amount: double

lake:
  connector: localfiles-pcp
  entities:
    orders_copy:
      write:
        strategy: replace
        format: parquet
        path: out/
EOF
  printf 'INSERT INTO {{ sink('"'"'lake'"'"', '"'"'orders_copy'"'"') }}\nselect id, customer, amount\nfrom {{ source('"'"'files'"'"', '"'"'orders'"'"') }}\norder by id\n' \
    > "${proj}/pipelines/orders_copy.sql"
  printf 'id,customer,amount\n1,ann,10.5\n2,bob,20.25\n3,ann,5.25\n4,cy,100.0\n' > "${proj}/data/orders.csv"
  (cd "${proj}" && "${PZ}" restore --feeds "${feed}")
  local pkg_dir="${proj}/.pz/packages/${PKG_ID}/${PKG_VERSION}"
  # Existence, not the executable bit: a nupkg carries no unix mode, and the host sets +x when it
  # reads the manifest to spawn the connector -- which is what the post-run assertion below checks.
  [[ -f "${pkg_dir}/native/PcpFakeConnector" ]] || { echo "FAIL: restore did not materialize native/PcpFakeConnector"; ls -R "${pkg_dir}"; exit 1; }
  [[ ! -d "${pkg_dir}/lib" ]] || { echo "FAIL: restore materialized a lib/ directory"; exit 1; }

  echo "-- pz run (source and sink both through the packaged connector) --"
  (cd "${proj}" && "${PZ}" run)
  [[ -x "${pkg_dir}/native/PcpFakeConnector" ]] || { echo "FAIL: the host did not make the entrypoint executable"; exit 1; }
  compgen -G "${proj}/out/*.parquet" >/dev/null || compgen -G "${proj}/out/**/*.parquet" >/dev/null \
    || { echo "FAIL: no parquet output under ${proj}/out"; ls -R "${proj}/out" 2>/dev/null; exit 1; }

  echo "-- pz connector test against the materialized package --"
  cat > "${proj}/probe.yml" <<EOF
connection:
  root: ${proj}
read:
  dataset: orders
  path: data/orders.csv
  format: csv
  columns:
    id: bigint
    customer: varchar
    amount: double
write:
  output: customer_totals
  mode: replace
  format: parquet
  path: out-probe/
EOF
  local report
  report="$(cd "${proj}" && "${PZ}" connector test "${pkg_dir}" --config probe.yml)"
  echo "${report}" | tail -n 5
  ! grep -q "FAIL" <<<"${report}" || { echo "FAIL: a conformance vector failed"; echo "${report}"; exit 1; }
  echo "mode ${mode}: OK"
}

verify_mode aot
verify_mode self-contained

echo
echo "== PASS: Pz.Connectors.Sdk packaging verified in both modes =="
