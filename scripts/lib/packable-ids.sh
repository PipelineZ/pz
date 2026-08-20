#!/usr/bin/env bash
# Shared by scripts/verify-tool-install.sh and scripts/make-release-bundle.sh.
#
# The expected package set is DERIVED, never hardcoded: every project under src/ and connectors/
# that does not opt out with <IsPackable>false</IsPackable> must produce exactly one .nupkg, and its
# package id is its project name (nothing under src/ or connectors/ overrides <PackageId>).
#
# Hardcoded counts have gone stale twice: once when the MySQL and SQLite connectors landed and took
# the set from 10 to 12, and again when the eight builtin connectors stopped publishing and took it
# from 12 to 4. Deriving costs one find, reports *which* id is missing rather than a bare count, and
# needs no edit the next time the set changes. Both callers share this file so the two cannot drift
# apart from each other either.

# Populates the global `expected_ids` array with every packable project's id, sorted.
pz_packable_ids() {
  local root="$1" proj
  expected_ids=()
  while IFS= read -r proj; do
    grep -q '<IsPackable>false</IsPackable>' "${proj}" && continue
    expected_ids+=("$(basename "${proj}" .csproj)")
  done < <(find "${root}/src" "${root}/connectors" -name '*.csproj' | sort)
}

# Fails unless the feed holds exactly one .nupkg per expected id and nothing more.
pz_assert_feed_matches() {
  local feed="$1" id missing=() nupkg_count
  for id in "${expected_ids[@]}"; do
    compgen -G "${feed}/${id}.*.nupkg" >/dev/null || missing+=("${id}")
  done
  if [[ "${#missing[@]}" -gt 0 ]]; then
    echo "FAIL: packable projects produced no package: ${missing[*]}" >&2
    return 1
  fi
  # Every expected id is present, so an over-count means pack emitted something unexpected.
  nupkg_count="$(find "${feed}" -maxdepth 1 -name '*.nupkg' | wc -l | tr -d ' ')"
  if [[ "${nupkg_count}" -ne "${#expected_ids[@]}" ]]; then
    echo "FAIL: expected ${#expected_ids[@]} packages, found ${nupkg_count}" >&2
    return 1
  fi
  echo "packages: ${nupkg_count} (${expected_ids[*]})"
}
