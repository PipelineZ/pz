#!/usr/bin/env bash
# Build the offline install bundle for a VM. No public NuGet:
# distribution = this zip, copied to the machine. Same pack->local-feed recipe verify-tool-install.sh
# proves, plus the VM-side scripts. Usage: scripts/make-release-bundle.sh [output-dir]
set -euo pipefail

command -v zip >/dev/null || { echo "FAIL: 'zip' is required" >&2; exit 1; }
# GNU find required (-printf); macOS users: run in a Linux container or install findutils.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=lib/packable-ids.sh
source "${ROOT_DIR}/scripts/lib/packable-ids.sh"
OUT_DIR="${1:-${ROOT_DIR}/artifacts}"
STAGE_DIR="$(mktemp -d)"
trap 'rm -rf "${STAGE_DIR}"' EXIT
FEED_DIR="${STAGE_DIR}/feed"
mkdir -p "${FEED_DIR}" "${OUT_DIR}"

echo "-- Building + packing Release --"
dotnet build "${ROOT_DIR}/Pz.slnx" -c Release --nologo -v quiet
dotnet pack "${ROOT_DIR}/Pz.slnx" -c Release -o "${FEED_DIR}" --nologo -v quiet

pz_packable_ids "${ROOT_DIR}"
pz_assert_feed_matches "${FEED_DIR}"

# The tool package's id is `pz`, lowercase, while every other id starts with a capital `Pz.` -- the
# glob is case-sensitive, so it cannot pick up a connector package by accident.
cli_nupkg="$(find "${FEED_DIR}" -maxdepth 1 -name 'pz.*.nupkg' -printf '%f\n')"
version="$(sed -E 's/^pz\.(.+)\.nupkg$/\1/' <<< "${cli_nupkg}")"
echo "bundle version: ${version}"

# The feed path is RELATIVE: NuGet resolves it against the nuget.config location, so the
# bundle works from any extraction directory without rewriting.
cat > "${STAGE_DIR}/nuget.config" <<'EOF'
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <clear />
    <add key="local-feed" value="feed" />
  </packageSources>
</configuration>
EOF

cp "${ROOT_DIR}/scripts/bundle/install.ps1" "${STAGE_DIR}/install.ps1"
cp "${ROOT_DIR}/scripts/bundle/run-pz.ps1" "${STAGE_DIR}/run-pz.ps1"

bundle_zip="${OUT_DIR}/pz-bundle-${version}.zip"
rm -f "${bundle_zip}"
(cd "${STAGE_DIR}" && zip -qr "${bundle_zip}" .)
echo "== PASS: wrote ${bundle_zip} =="
