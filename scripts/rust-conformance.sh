#!/usr/bin/env bash
# Builds the Rust SDK's `memory_sink` example and runs two checks against it: the host's black-box
# PCP conformance verb (`pz connector test`) -- the SDK's real contract is "the host accepts what
# this crate produces on the wire", not anything provable from Rust-side unit tests alone -- and the
# `Category=RustPcp` xunit facts, which prove the SDK's OTel spans actually land under the host's
# span end to end.
#
# SKIPs cleanly (exit 0) when either toolchain this needs is missing, matching every other
# docker/toolchain-gated script in this directory: `cargo` (builds the example) and a `dotnet` that runs the
# SDK global.json requires (runs the conformance verb and the telemetry test); under CI a missing SDK fails. Any conformance vector failing, the verb's own exit code
# 2 for a setup/config problem, or a failing RustPcp test fails this script.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if ! command -v cargo >/dev/null; then
  echo "SKIP: cargo not available"
  exit 0
fi

# The first `dotnet` on PATH may be a runtime-only or older install that cannot run the SDK global.json asks
# for: try every `dotnet` on PATH, then $DOTNET_ROOT and ~/.dotnet, and use the first whose `dotnet --version`
# succeeds from the repo root. None: SKIP locally, but fail under CI, which must never quietly skip this check.
find_dotnet() {
  local candidate dir
  local -a candidates=()
  if [[ -n "${DOTNET_ROOT:-}" ]]; then candidates+=("${DOTNET_ROOT}/dotnet"); fi
  IFS=: read -r -a dirs <<<"${PATH}"
  for dir in "${dirs[@]}"; do candidates+=("${dir}/dotnet"); done
  candidates+=("${HOME}/.dotnet/dotnet")
  for candidate in "${candidates[@]}"; do
    if [[ -x "${candidate}" ]] && (cd "${ROOT_DIR}" && DOTNET_ROOT="$(dirname "${candidate}")" "${candidate}" --version >/dev/null 2>&1); then
      echo "${candidate}"
      return 0
    fi
  done
  return 1
}

if ! DOTNET="$(find_dotnet)"; then
  message="no dotnet that can run the .NET SDK global.json requires was found (tried every dotnet on PATH, \$DOTNET_ROOT and ~/.dotnet)"
  if [[ -n "${CI:-}" ]]; then
    echo "error: ${message}" >&2
    exit 1
  fi
  echo "SKIP: ${message}"
  exit 0
fi
export DOTNET_ROOT="$(dirname "${DOTNET}")"
dotnet() { "${DOTNET}" "$@"; }

echo "building the memory_sink example..."
cargo build --example memory_sink --manifest-path "${ROOT_DIR}/rust/pz-connector/Cargo.toml"

ENTRYPOINT="${ROOT_DIR}/rust/target/debug/examples/memory_sink"
# Git Bash on Windows: cargo names the example memory_sink.exe.
if [[ -f "${ENTRYPOINT}.exe" ]]; then
  ENTRYPOINT="${ENTRYPOINT}.exe"
fi
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

# --blame-hang: these facts drive a real connector process, and a connector that never answers would
# otherwise sit silent until whatever runs this script gives up, naming nothing. The whole filter
# takes well under a minute, so five minutes only ever ends a run that was never going to finish.
echo "running the Rust telemetry e2e (Category=RustPcp) against the memory_sink example..."
if dotnet test "${ROOT_DIR}/tests/Pz.PackageManagement.Tests" -c Release --filter "Category=RustPcp" \
  --blame-hang --blame-hang-timeout 5m --blame-hang-dump-type mini; then
  echo "rust-telemetry: PASS"
else
  status=$?
  echo "rust-telemetry: FAILED (dotnet test exited ${status})" >&2
  exit 1
fi
