#!/usr/bin/env bash
# Builds the Rust SDK's `memory_sink` example and runs the host's black-box PCP conformance verb
# (`pz connector test`) against it -- the SDK's real contract is "the host accepts what this crate
# produces on the wire", not anything provable from Rust-side unit tests alone.
#
# SKIPs cleanly (exit 0) when either toolchain this needs is missing, matching every other
# docker/toolchain-gated script in this directory: `cargo` (builds the example) and `dotnet` (runs the
# conformance verb). Any conformance vector failing -- or the verb's own exit code 2 for a setup/config
# problem -- fails this script.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if ! command -v cargo >/dev/null; then
  echo "SKIP: cargo not available"
  exit 0
fi

if ! command -v dotnet >/dev/null; then
  echo "SKIP: dotnet not available"
  exit 0
fi

echo "building the memory_sink example..."
cargo build --example memory_sink --manifest-path "${ROOT_DIR}/rust/pz-connector/Cargo.toml"

ENTRYPOINT="${ROOT_DIR}/rust/target/debug/examples/memory_sink"
if [[ ! -x "${ENTRYPOINT}" ]]; then
  echo "error: expected the built example at '${ENTRYPOINT}'" >&2
  exit 1
fi

# TMPDIR, not the repo tree: unix socket paths have a short sun_path limit (~104 bytes), and a config
# file lives alongside so a run-scoped directory is deleted as a unit afterward.
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pzrs.XXXXXX")"
trap 'rm -rf "${WORK_DIR}"' EXIT

CONFIG_FILE="${WORK_DIR}/conformance.yml"
cat >"${CONFIG_FILE}" <<'YAML'
connection: {}
write:
  output: conformance_probe
  mode: replace
  schema_policy: match
YAML

echo "running pz connector test against the memory_sink example..."
if dotnet run --project "${ROOT_DIR}/src/Pz.Cli" -c Release -- connector test "${ENTRYPOINT}" --config "${CONFIG_FILE}"; then
  echo "rust-conformance: PASS"
else
  status=$?
  echo "rust-conformance: FAILED (pz connector test exited ${status})" >&2
  exit 1
fi
