#!/usr/bin/env bash
# Fails when any ELF file in a Linux RID tool package needs a newer glibc than the baseline pz
# promises to start on. A Native AOT binary links against the glibc of the machine that built it,
# so a release built on a newer runner silently stops starting on older hosts: 0.13.0 and 0.14.0
# were packed on ubuntu-latest after it moved to Ubuntu 26.04 and needed GLIBC_2.43, which fails on
# Ubuntu 24.04 (2.39) and Debian 12 (2.36). release.yml runs this right after `dotnet pack`, before
# anything is uploaded or pushed.
#
# Usage: check-glibc-baseline.sh <package.nupkg> <max-glibc>
#   <package.nupkg>  e.g. packages/pz.linux-x64.0.14.1.nupkg
#   <max-glibc>      the newest glibc version a binary may require, e.g. 2.39
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <package.nupkg> <max-glibc>" >&2
  exit 2
fi

package="$1"
baseline="$2"
[[ -f "${package}" ]] || { echo "check-glibc-baseline: no package at '${package}'" >&2; exit 2; }
command -v objdump >/dev/null || { echo "check-glibc-baseline: objdump not found (binutils)" >&2; exit 2; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
unzip -q "${package}" -d "${work}"

checked=0
failed=0
while IFS= read -r -d '' file; do
  head -c 4 "${file}" | grep -q $'\x7fELF' || continue
  checked=$((checked + 1))
  needed="$(objdump -T "${file}" 2>/dev/null | grep -o 'GLIBC_[0-9][0-9.]*' | sed 's/GLIBC_//' | sort -uV | tail -1 || true)"
  name="${file#"${work}/"}"
  if [[ -z "${needed}" ]]; then
    echo "  ${name}: no versioned glibc symbols"
    continue
  fi
  newest="$(printf '%s\n%s\n' "${needed}" "${baseline}" | sort -V | tail -1)"
  if [[ "${newest}" != "${baseline}" ]]; then
    echo "::error::${name} needs GLIBC_${needed}, newer than the GLIBC_${baseline} baseline. Build Linux RIDs on a runner no newer than the baseline (see the pack-aot matrix)."
    failed=1
  else
    echo "  ${name}: GLIBC_${needed} (baseline ${baseline})"
  fi
done < <(find "${work}" -type f ! -name '*.dbg' -print0)

if [[ "${checked}" -eq 0 ]]; then
  echo "::error::no ELF files in ${package}; is this a Linux RID package?"
  exit 1
fi

exit "${failed}"
