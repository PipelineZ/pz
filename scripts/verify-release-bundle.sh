#!/usr/bin/env bash
# Proves the bundle zip is self-sufficient: build bundle -> unzip -> tool install from the bundle's
# own nuget.config (local feed ONLY) -> offline pz init + pz run. Linux-runnable (install.ps1 is
# Windows-only; this exercises the same feed + config the ps1 uses).
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT

"${ROOT_DIR}/scripts/make-release-bundle.sh" "${WORK_DIR}/out"

BUNDLE_DIR="${WORK_DIR}/bundle"
mkdir -p "${BUNDLE_DIR}"
unzip -q "${WORK_DIR}"/out/pz-bundle-*.zip -d "${BUNDLE_DIR}"

for expected in feed nuget.config install.ps1 run-pz.ps1; do
  if [[ ! -e "${BUNDLE_DIR}/${expected}" ]]; then
    echo "FAIL: bundle is missing ${expected}" >&2
    exit 1
  fi
done

echo "-- Installing from the bundle's own nuget.config --"
dotnet tool install pz --tool-path "${WORK_DIR}/tool" \
  --configfile "${BUNDLE_DIR}/nuget.config" --prerelease
PZ="${WORK_DIR}/tool/pz"
if [[ ! -x "${PZ}" ]]; then
  echo "FAIL: expected an executable pz shim at ${PZ}" >&2
  exit 1
fi

version_output="$("${PZ}" --version)"
echo "version: ${version_output}"
if [[ -z "${version_output}" ]]; then
  echo "FAIL: pz --version printed nothing" >&2
  exit 1
fi

echo "-- Offline init + run smoke --"
env -u HTTP_PROXY -u HTTPS_PROXY -u http_proxy -u https_proxy "${PZ}" init "${WORK_DIR}/smoke"
(cd "${WORK_DIR}/smoke" &&
  env -u HTTP_PROXY -u HTTPS_PROXY -u http_proxy -u https_proxy "${PZ}" run --all)

curated="${WORK_DIR}/smoke/out/curated/orders_curated.parquet"
totals="${WORK_DIR}/smoke/out/totals/order_totals.csv"
catalog="${WORK_DIR}/smoke/out/catalog/product_catalog.csv"
if [[ ! -s "${curated}" || ! -s "${totals}" || ! -s "${catalog}" ]]; then
  echo "FAIL: expected sink output files at ${curated}, ${totals}, ${catalog}" >&2
  exit 1
fi
echo "run OK: ${curated}, ${totals}, ${catalog} all exist and are non-empty"

echo "== PASS: bundle -> install -> offline init/run all succeeded =="
