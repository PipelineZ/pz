#!/usr/bin/env bash
# Fails unless CHANGELOG.md has a Keep a Changelog `## [X.Y.Z]` heading for the version a release
# tag names. release.yml runs this BEFORE anything is packed or pushed, so a tag cut without its
# changelog entry never reaches nuget.org.
#
# Usage: check-changelog-entry.sh <tag-or-version> [changelog-path]
#   <tag-or-version>  e.g. "v0.7.0", "0.7.0", or a pre-release tag "v0.7.0-rc.1"
#   [changelog-path]  defaults to CHANGELOG.md in the current directory
#
# The `v` MinVerTagPrefix (Directory.Build.props) is stripped if present; a bare version passes
# through unchanged.
#
# Pre-release tags (v0.7.0-rc.1, v0.7.0-beta.2, build metadata like +sha) are checked against their
# BASE version's heading ("## [0.7.0]"), not an exact rc heading. This repo's CHANGELOG.md tracks
# shipped releases, not individual release-candidate tags (no `v*-rc.*` tag has ever been cut here,
# and Keep a Changelog itself has no rc convention) -- the base version's entry, written before the
# first candidate, is what a reader wants regardless of which candidate tag triggered the build.
# See CONTRIBUTING.md's "Release process" for the full tag/version contract.
#
# The match is anchored to the START of a heading line ("^## [") and requires the closing "]"
# immediately after the version, so:
#   - "0.7.0" never matches a heading for "0.7.01" or "0.7.0-rc.1" (the char after "0.7.0" in the
#     bracket must be "]", not "1" or "-").
#   - "## [Unreleased]" never matches any numeric version.
#   - a footer link reference ("[0.7.0]: https://github.com/...") never counts -- it does not start
#     with "## ".
set -euo pipefail

raw="${1:?usage: check-changelog-entry.sh <tag-or-version> [changelog-path]}"
changelog="${2:-CHANGELOG.md}"

# Strip the MinVerTagPrefix ("v") if present.
version="${raw#v}"

# Strip build metadata (+...) then a pre-release suffix (-rc.1, -beta.2, ...) to get the base
# semver X.Y.Z this repo's CHANGELOG.md tracks headings for.
base_version="${version%%+*}"
base_version="${base_version%%-*}"

if [[ ! "${base_version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "FAIL: '${raw}' is not a vX.Y.Z-style tag/version (parsed base version '${base_version}')" >&2
  exit 1
fi

if [[ ! -f "${changelog}" ]]; then
  echo "FAIL: ${changelog} not found" >&2
  exit 1
fi

# Escape the dots so they match literal dots, not "any character" -- otherwise a heading like
# "## [0X7X0]" would (in principle) satisfy a pattern meant only for "0.7.0".
escaped_version="${base_version//./\\.}"

if grep -qE "^## \[${escaped_version}\]" "${changelog}"; then
  echo "OK: ${changelog} has a heading for ${base_version} (from tag/version '${raw}')"
  exit 0
fi

echo "FAIL: ${changelog} has no '## [${base_version}]' heading for tag/version '${raw}'" >&2
echo "Add one under CHANGELOG.md's release process before tagging -- see CONTRIBUTING.md." >&2
exit 1
