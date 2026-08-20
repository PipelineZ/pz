#!/usr/bin/env bash
# End-to-end proof that `pz` is actually installable and runnable as a real NuGet-distributed .NET
# tool, not just buildable in-repo.
#
# Pack everything -> local folder feed -> `dotnet tool install --tool-path <clean tmp dir>` -> run the
# INSTALLED tool binary, completely offline, against `pz init`'s own scaffolded output -> assert
# success + real output files -> cleanup.
#
# Both templates are exercised: the default (minimal) for its shape, and `--sample` for the run
# proof, since only the sample ships pipelines to run (builtin connectors only, so no
# `pz restore`/NuGet touch at run time).
#
# The install step must be hermetic. `dotnet tool install --add-source <feed>` is
# ADDITIVE -- the machine's ambient NuGet sources (nuget.org, any configured private feeds, etc.) are
# still consulted alongside the local feed, so a package-name collision or a flaky/slow remote source
# could silently change what gets installed, or just slow this down. `--configfile <throwaway
# NuGet.Config with ONLY the local feed>` replaces the entire ambient config lookup instead of adding to
# it, so "this install only ever touches the local feed" is a structural property of the command, not a
# hope about the machine's NuGet.Config.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=lib/packable-ids.sh
source "${ROOT_DIR}/scripts/lib/packable-ids.sh"
WORK_DIR="$(mktemp -d)"
FEED_DIR="${WORK_DIR}/feed"
TOOL_DIR="${WORK_DIR}/tool"
INIT_DIR="${WORK_DIR}/init-smoke"
MINIMAL_DIR="${WORK_DIR}/init-minimal-smoke"
NUGET_CONFIG="${WORK_DIR}/nuget.config"
trap 'rm -rf "${WORK_DIR}"' EXIT

mkdir -p "${FEED_DIR}" "${TOOL_DIR}"

echo "== PipelineZ tool-install verification =="
echo "work dir: ${WORK_DIR}"
echo

echo "-- Building Release --"
dotnet build "${ROOT_DIR}/Pz.slnx" -c Release --nologo -v quiet
echo "build OK"
echo

echo "-- Packing every packable project to a local folder feed --"
dotnet pack "${ROOT_DIR}/Pz.slnx" -c Release -o "${FEED_DIR}" --nologo -v quiet
find "${FEED_DIR}" -maxdepth 1 -name '*.nupkg' -printf '  %f\n' | sort
pz_packable_ids "${ROOT_DIR}"
pz_assert_feed_matches "${FEED_DIR}"
echo

echo "-- Writing a throwaway NuGet.Config that lists ONLY the local feed --"
cat > "${NUGET_CONFIG}" <<EOF
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <clear />
    <add key="local-feed" value="${FEED_DIR}" />
  </packageSources>
</configuration>
EOF
echo "wrote ${NUGET_CONFIG}"
echo

echo "-- Installing Pz.Cli as a local tool (clean tool-path, local-feed-only config) --"
dotnet tool install Pz.Cli --tool-path "${TOOL_DIR}" --configfile "${NUGET_CONFIG}" --prerelease
PZ="${TOOL_DIR}/pz"
if [[ ! -x "${PZ}" ]]; then
  echo "FAIL: expected an executable pz shim at ${PZ}" >&2
  exit 1
fi
echo

echo "-- pz --version --"
version_output="$("${PZ}" --version)"
echo "version: ${version_output}"
if [[ -z "${version_output}" ]]; then
  echo "FAIL: pz --version printed nothing" >&2
  exit 1
fi
echo

# Force offline for both init and run: unset any proxy env vars so a network call would fail loudly
# rather than silently succeed if this invariant ever regresses. `pz init` writes only builtin
# (Pz.Connector.LocalFiles) sources/sinks, so neither verb ever touches NuGet/pz.lock.json here.
echo "-- pz init smoke (offline, default template) --"
if ! env -u HTTP_PROXY -u HTTPS_PROXY -u http_proxy -u https_proxy \
  "${PZ}" init "${MINIMAL_DIR}"; then
  echo "FAIL: pz init exited non-zero" >&2
  exit 1
fi
# The default is the MINIMAL project: project.yml + connections.yml and nothing else. Asserted from
# the installed binary because the template set is chosen from embedded resources -- a packaging
# mistake that shipped only one of the two template directories would otherwise surface as a
# stranger's first command scaffolding the wrong project.
if [[ ! -f "${MINIMAL_DIR}/project.yml" || ! -f "${MINIMAL_DIR}/connections.yml" ]]; then
  echo "FAIL: expected project.yml and connections.yml after pz init" >&2
  exit 1
fi
if [[ -d "${MINIMAL_DIR}/pipelines" || -d "${MINIMAL_DIR}/data" ]]; then
  echo "FAIL: default pz init scaffolded sample content; expected the minimal project" >&2
  exit 1
fi
echo "init OK: ${MINIMAL_DIR} holds the minimal project (project.yml + connections.yml)"
echo

echo "-- pz init --sample smoke (offline, builtin connectors only) --"
if ! env -u HTTP_PROXY -u HTTPS_PROXY -u http_proxy -u https_proxy \
  "${PZ}" init "${INIT_DIR}" --sample; then
  echo "FAIL: pz init --sample exited non-zero" >&2
  exit 1
fi
if [[ ! -f "${INIT_DIR}/project.yml" ]]; then
  echo "FAIL: expected ${INIT_DIR}/project.yml to exist after pz init --sample" >&2
  exit 1
fi
echo "init OK: ${INIT_DIR}/project.yml exists"
echo

echo "-- cd smoke && pz run --all (offline) --"
if ! (
  cd "${INIT_DIR}"
  env -u HTTP_PROXY -u HTTPS_PROXY -u http_proxy -u https_proxy "${PZ}" run --all
); then
  echo "FAIL: pz run --all exited non-zero" >&2
  exit 1
fi

curated="${INIT_DIR}/out/orders_curated/orders_curated.parquet"
totals="${INIT_DIR}/out/order_totals/order_totals.csv"
catalog="${INIT_DIR}/out/product_catalog/product_catalog.csv"
if [[ ! -s "${curated}" || ! -s "${totals}" || ! -s "${catalog}" ]]; then
  echo "FAIL: expected sink output files at ${curated}, ${totals}, ${catalog}" >&2
  exit 1
fi
echo "run OK: ${curated}, ${totals}, ${catalog} all exist and are non-empty"
echo

echo "== PASS: pack -> tool install -> offline init -> offline run all succeeded =="
