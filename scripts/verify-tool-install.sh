#!/usr/bin/env bash
# End-to-end proof that `pz` is actually installable and runnable as a real NuGet-distributed .NET
# tool, not just buildable in-repo.
#
# Pack everything -> local folder feed -> `dotnet tool install --tool-path <clean tmp dir>` -> run the
# INSTALLED tool binary, completely offline, against `pz init`'s own scaffolded output -> assert
# success + real output files -> cleanup.
#
# Both templates are exercised: the default (minimal) for its shape, and `--template sample` for the run
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

# Hybrid AOT tool packaging (see Pz.Cli.csproj): the bare solution pack above emits the `pz` POINTER
# package, which an install can only resolve through a RID sub-package. Pack this host's Native AOT
# sub-package -- so the install below proves the real per-platform artifact a release ships -- plus
# the CoreCLR `any` fallback. Packed AFTER the set-equality check above, which counts one package per
# packable project and would (rightly) refuse these extra sub-package ids.
echo "-- Packing the host-RID Native AOT and 'any' fallback tool sub-packages --"
HOST_RID="$(dotnet --info | sed -n 's/^ *RID: *//p' | head -1)"
dotnet pack "${ROOT_DIR}/src/Pz.Cli" -c Release -r "${HOST_RID}" -o "${FEED_DIR}" --nologo -v quiet
dotnet pack "${ROOT_DIR}/src/Pz.Cli" -c Release -r any -p:PublishAot=false -o "${FEED_DIR}" --nologo -v quiet
echo "packed pz.${HOST_RID} (Native AOT) and pz.any (CoreCLR fallback)"
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

# `pz` is the package id (Pz.Cli.csproj's <PackageId>), not just the command name -- installing by
# the project name would fail, which is the whole point of proving the real install line here.
echo "-- Installing pz as a local tool (clean tool-path, local-feed-only config) --"
dotnet tool install pz --tool-path "${TOOL_DIR}" --configfile "${NUGET_CONFIG}" --prerelease
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
# The default is the MINIMAL project: project.yml and connections.yml, with no pipelines/ or data/
# directory (that content belongs to the sample template). Asserted from the installed binary
# because the template set is chosen from embedded resources -- a packaging mistake that shipped
# only one of the template directories would otherwise surface as a stranger's first command
# scaffolding the wrong project.
if [[ ! -f "${MINIMAL_DIR}/project.yml" || ! -f "${MINIMAL_DIR}/connections.yml" ]]; then
  echo "FAIL: expected project.yml and connections.yml after pz init" >&2
  exit 1
fi
if [[ -d "${MINIMAL_DIR}/pipelines" || -d "${MINIMAL_DIR}/data" ]]; then
  echo "FAIL: default pz init scaffolded sample content; expected the minimal project" >&2
  exit 1
fi
# A dotfile dropped by the glob, the embed, or NuGet packaging fails silently -- the scaffold still
# succeeds, just without the file. This is the only check that runs against a genuinely packed and
# installed binary, which is exactly where that failure would first appear.
if [[ ! -f "${MINIMAL_DIR}/README.md" || ! -f "${MINIMAL_DIR}/.gitignore" ]]; then
  echo "FAIL: expected README.md and .gitignore after pz init" >&2
  exit 1
fi
echo "init OK: ${MINIMAL_DIR} holds the minimal project (project.yml, connections.yml, README.md, .gitignore)"
echo

echo "-- pz init --template sample smoke (offline, builtin connectors only) --"
if ! env -u HTTP_PROXY -u HTTPS_PROXY -u http_proxy -u https_proxy \
  "${PZ}" init "${INIT_DIR}" --template sample; then
  echo "FAIL: pz init --template sample exited non-zero" >&2
  exit 1
fi
if [[ ! -f "${INIT_DIR}/project.yml" ]]; then
  echo "FAIL: expected ${INIT_DIR}/project.yml to exist after pz init --template sample" >&2
  exit 1
fi
echo "init OK: ${INIT_DIR}/project.yml exists"
echo

echo "-- pz init --list-templates smoke (offline) --"
LIST_OUT="$(env -u HTTP_PROXY -u HTTPS_PROXY -u http_proxy -u https_proxy "${PZ}" init --list-templates)" || {
  echo "FAIL: pz init --list-templates exited non-zero" >&2
  exit 1
}
for id in minimal sample incremental http sqlserver; do
  if ! grep -q "${id}" <<<"${LIST_OUT}"; then
    echo "FAIL: --list-templates output does not name '${id}'" >&2
    exit 1
  fi
done
echo "list OK: all five templates named"
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
