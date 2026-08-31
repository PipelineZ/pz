#!/usr/bin/env bash
# Builds the `pz-deltalake` binary (rust/pz-connector-deltalake) and runs the host's black-box PCP
# conformance verb (`pz connector test`) against it -- the sibling of rust-conformance.sh, which does
# the same thing for the SDK's own memory_sink example. Kept as a separate script (not a leg bolted
# onto rust-conformance.sh) because this one needs a real filesystem `root:` to write a Delta table
# into, not an empty `connection: {}`.
#
# The probe write mode is `replace`, not `merge`: the conformance CLI's --config `write:` block has
# no way to populate `OutputSpec.Keys` (ConnectorTestCommand.LoadProbeConfig folds every key besides
# `output`/`mode`/`schema_policy` into Options, never Keys), and this connector correctly refuses a
# keyless merge -- `replace` exercises the same commit/abort/premature-commit/transient-error/
# control-plane-size protocol paths merge would, just without that harness gap. merge mode's own
# correctness (keys, matched-update, not-matched-insert) is covered by `cargo test`'s
# `merge_updates_matching_keys_and_inserts_new_ones`, which drives the WriteSession trait directly and
# so is not subject to the same --config limitation.
#
# SKIPs cleanly (exit 0) when either toolchain this needs is missing, matching every other
# docker/toolchain-gated script in this directory: `cargo` (builds the binary) and `dotnet` (runs the
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

echo "building pz-deltalake..."
cargo build --bin pz-deltalake --manifest-path "${ROOT_DIR}/rust/pz-connector-deltalake/Cargo.toml"

ENTRYPOINT="${ROOT_DIR}/rust/target/debug/pz-deltalake"
if [[ ! -x "${ENTRYPOINT}" ]]; then
  echo "error: expected the built binary at '${ENTRYPOINT}'" >&2
  exit 1
fi

# TMPDIR, not the repo tree: unix socket paths have a short sun_path limit (~104 bytes), a Delta
# table needs a real writable directory tree, and a config file lives alongside so a run-scoped
# directory is deleted as a unit afterward.
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pzdl.XXXXXX")"
trap 'rm -rf "${WORK_DIR}"' EXIT

CONFIG_FILE="${WORK_DIR}/conformance.yml"
cat >"${CONFIG_FILE}" <<YAML
connection:
  root: ${WORK_DIR}/lake
write:
  output: conformance_probe
  mode: replace
  schema_policy: match
YAML

echo "running pz connector test against pz-deltalake..."
if dotnet run --project "${ROOT_DIR}/src/Pz.Cli" -c Release -- connector test "${ENTRYPOINT}" --config "${CONFIG_FILE}"; then
  echo "rust-conformance-deltalake: PASS"
else
  status=$?
  echo "rust-conformance-deltalake: FAILED (pz connector test exited ${status})" >&2
  exit 1
fi
