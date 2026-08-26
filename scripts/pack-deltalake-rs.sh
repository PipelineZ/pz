#!/usr/bin/env bash
# Packs the `pz-deltalake` binary (rust/pz-connector-deltalake) as a NuGet package a real `pz restore`
# can install: rust/pz-connector-deltalake/pack/pz.connector.json becomes the package's manifest,
# rust/pz-connector-deltalake/pack/Pz.Connector.DeltaLakeRs.nuspec its metadata, and the built release
# binary lands at the NuGet native-asset convention path `runtimes/linux-x64/native/pz-deltalake` --
# that convention is what makes NuGetResolver.SelectNativeAssets (src/Pz.PackageManagement/Restore/
# NuGetResolver.cs) recognize and lock the binary as a RID-specific asset in the first place.
#
# linux-x64 only, matching rust-conformance-deltalake.sh's own scope: the Rust SDK ships UDS-only, so
# a win-x64 entrypoint would need a named-pipe transport this crate does not implement. A Windows host
# restoring this package gets PZ0354 ("ships no binary for RID") with a clear message, not a silent
# wrong-platform binary.
#
# No `nuget`/`dotnet nuget` CLI is assumed to be on PATH. The .nupkg is instead built by a throwaway
# .NET 10 file-based app driving NuGet.Packaging.PackageBuilder directly -- the exact library
# src/Pz.PackageManagement's restore path (NuGetResolver, PackageMaterializer) reads .nupkg files
# with, so a package built this way is provably readable by the real restore path, not merely "looks
# like a valid zip". The package version installed in this repo's NuGet cache is pinned by version so
# building it never needs network access.
#
# SKIPs cleanly (exit 0) when either toolchain this needs is missing, matching every other
# docker/toolchain-gated script in this directory: `cargo` (builds the binary) and `dotnet` (builds the
# .nupkg).
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONNECTOR_DIR="${ROOT_DIR}/rust/pz-connector-deltalake"
PACK_DIR="${CONNECTOR_DIR}/pack"
PACKAGE_ID="Pz.Connector.DeltaLakeRs"

if ! command -v cargo >/dev/null; then
  echo "SKIP: cargo not available"
  exit 0
fi

if ! command -v dotnet >/dev/null; then
  echo "SKIP: dotnet not available"
  exit 0
fi

VERSION="$(command grep -m1 '^version' "${CONNECTOR_DIR}/Cargo.toml" | sed -E 's/version = "([^"]*)"/\1/')"
if [[ -z "${VERSION}" ]]; then
  echo "error: could not read [package].version from ${CONNECTOR_DIR}/Cargo.toml" >&2
  exit 1
fi

OUT_DIR="${1:-${ROOT_DIR}/rust/target/nupkg}"
mkdir -p "${OUT_DIR}"

echo "building pz-deltalake (release)..."
cargo build --release --bin pz-deltalake --manifest-path "${CONNECTOR_DIR}/Cargo.toml"

BINARY="${ROOT_DIR}/rust/target/release/pz-deltalake"
if [[ ! -x "${BINARY}" ]]; then
  echo "error: expected the built binary at '${BINARY}'" >&2
  exit 1
fi

# TMPDIR, not the repo tree: the builder .cs file below is a file-based app, and file-based apps
# inherit global.json/Directory.Build.props/nuget.config from ANCESTOR directories -- staging outside
# the repo keeps it clear of this repo's TreatWarningsAsErrors and central package management, neither
# of which this one-off packing step needs to satisfy.
STAGE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pzdlpack.XXXXXX")"
trap 'rm -rf "${STAGE_DIR}"' EXIT

mkdir -p "${STAGE_DIR}/runtimes/linux-x64/native"
cp "${BINARY}" "${STAGE_DIR}/runtimes/linux-x64/native/pz-deltalake"
# No `chmod +x` here: a .nupkg is a zip archive, and PackageBuilder.Save() (like a plain zip writer)
# does not carry the source file's Unix executable bit into the entry's external attributes -- so
# setting it on this staged copy would be a no-op that could not survive being zipped anyway. The
# restored package's entrypoint is instead made executable at resolve time, in
# ManifestReader.ResolveEntrypoint (src/Pz.PackageManagement/Hosting/ManifestReader.cs), which is the
# one place every caller that spawns a process-runtime connector's binary goes through.
cp "${PACK_DIR}/pz.connector.json" "${STAGE_DIR}/pz.connector.json"

NUPKG_PATH="${OUT_DIR}/${PACKAGE_ID}.${VERSION}.nupkg"
rm -f "${NUPKG_PATH}"

# Not byte-reproducible: PackageBuilder.Save() stamps a random package/services/metadata/
# core-properties/*.psmdcp GUID and the current time into every .nupkg it writes, the same as
# `nuget pack` itself does -- pz's own "byte-stable .pz artifacts" determinism contract
# (CLAUDE.md's Binding conventions) is about what pz WRITES, not about a third-party packaging
# format's own OPC ceremony, so this is a bounded, deliberate exception rather than a gap in it.
BUILDER_CS="${STAGE_DIR}/build-nupkg.cs"
cat > "${BUILDER_CS}" <<'CSHARP'
#:package NuGet.Packaging@7.6.0
using System.Xml.Linq;
using NuGet.Packaging;
using NuGet.Packaging.Licenses;
using NuGet.Versioning;

// args: <stageDir> <nuspecPath> <version> <outputPath>
var stageDir = args[0];
var nuspecPath = args[1];
var version = args[2];
var outputPath = args[3];

var ns = XNamespace.Get("http://schemas.microsoft.com/packaging/2013/05/nuspec.xsd");
var doc = XDocument.Load(nuspecPath);
var metadata = doc.Root!.Element(ns + "metadata")!;

var builder = new PackageBuilder
{
    Id = metadata.Element(ns + "id")!.Value,
    // The nuspec's own <version> is the template's documentation copy; the version actually stamped
    // on the package is the one this script just read from Cargo.toml, so the two can never drift.
    Version = new NuGetVersion(version),
    Description = metadata.Element(ns + "description")!.Value,
};
foreach (var author in metadata.Element(ns + "authors")!.Value.Split(',', StringSplitOptions.TrimEntries))
{
    builder.Authors.Add(author);
}

// Every <metadata> child the nuspec declares beyond id/version/authors/description is carried
// through too -- the nuspec's own claim to be "the human-readable record of what the build
// produces" (its own header comment) is only true if nothing it lists is silently dropped here.
if (metadata.Element(ns + "projectUrl") is { } projectUrlElement)
{
    builder.ProjectUrl = new Uri(projectUrlElement.Value);
}

if (metadata.Element(ns + "license") is { } licenseElement
    && licenseElement.Attribute("type")?.Value == "expression")
{
    var expression = NuGetLicenseExpression.Parse(licenseElement.Value);
    builder.LicenseMetadata = new LicenseMetadata(
        LicenseType.Expression, licenseElement.Value, expression, warningsAndErrors: null,
        LicenseMetadata.CurrentVersion);
}

foreach (var file in doc.Root.Element(ns + "files")!.Elements(ns + "file"))
{
    builder.Files.Add(new PhysicalPackageFile
    {
        SourcePath = Path.Combine(stageDir, file.Attribute("src")!.Value),
        TargetPath = file.Attribute("target")!.Value,
    });
}

Directory.CreateDirectory(Path.GetDirectoryName(outputPath)!);
using var stream = File.Create(outputPath);
builder.Save(stream);
Console.WriteLine($"wrote {outputPath}");
CSHARP

dotnet "${BUILDER_CS}" -- "${STAGE_DIR}" "${PACK_DIR}/${PACKAGE_ID}.nuspec" "${VERSION}" "${NUPKG_PATH}"

echo "pack-deltalake-rs: PASS (${NUPKG_PATH})"
