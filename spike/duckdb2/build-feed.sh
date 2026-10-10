#!/usr/bin/env bash
# Builds DuckDB.NET's nightly-builds branch against DuckDB's v2.0-cyanoptera native libraries and packs
# DuckDB.NET.Data.Full / DuckDB.NET.Bindings.Full 2.0.0-spike.1 into artifacts/duckdb2-feed, the local
# source nuget.config points at. Host RID only (DuckDB.NET's nightly mode downloads one native library).
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
work="$root/artifacts/duckdb2-work"
feed="$root/artifacts/duckdb2-feed"
sha="${DUCKDB_NET_SHA:-39bfd657d7bee62ff7492d8e8bc0453d2bd5e5f7}"

rm -rf "$work" "$feed"
mkdir -p "$work" "$feed"
git clone -q https://github.com/Giorgi/DuckDB.NET.git "$work/DuckDB.NET"
git -C "$work/DuckDB.NET" checkout -q "$sha"

export DuckDbArtifactRoot=https://artifacts.duckdb.org/v2.0-cyanoptera
export NightlyBuild=true
cd "$work/DuckDB.NET"
# CI=true (set on every GitHub runner) makes DuckDB.NET reference GitVersion, which overrides Version.
pack() {
  env -u CI dotnet pack "$1" -c Release -p:BuildType=Full -p:Version=2.0.0-spike.1 -o "$feed" | tail -3
}
# The native library is packed through a glob evaluated before the build step downloads it, so the
# first pack only fetches it; the bindings package is packed again once the file is on disk.
pack DuckDB.NET.Bindings/Bindings.csproj
rm -f "$feed"/DuckDB.NET.Bindings.Full.*.nupkg
pack DuckDB.NET.Bindings/Bindings.csproj
pack DuckDB.NET.Data/Data.csproj

# A nupkg is a zip; python is on every runner image, unzip is not (windows Git Bash).
py="$(command -v python3 || command -v python)"
"$py" -I -m zipfile -l "$feed"/DuckDB.NET.Bindings.Full.2.0.0-spike.1.nupkg | grep -q 'runtimes/.*/native/' \
  || { echo "native library missing from the bindings package" >&2; exit 1; }
# A cached copy of the same version would shadow the freshly built one.
rm -rf "${NUGET_PACKAGES:-$HOME/.nuget/packages}"/duckdb.net.{data,bindings}.full/2.0.0-spike.1
ls -la "$feed"
