#!/usr/bin/env bash
# End-to-end proof that a connector packaged through Pz.Connectors.Sdk's targets is installable and
# runnable by pz the way a stranger's connector would be: publish (AOT, then self-contained) ->
# pack into a local feed -> `pz restore` from that feed -> `pz run` both directions ->
# `pz connector test` against the materialized package.
#
# The fixture (tests/fixtures/PcpFakeConnector) is the connector under test: it is a real
# LocalFilesConnector served by the real SDK, so a green run here proves the SDK, its targets, the
# manifest self-description and the host's restore layout agree end to end.
#
# The fixture imports the SDK's props/targets by path, though, which is not how a connector outside
# this repository gets them: it references the SDK as a NuGet package, and its packaging files then
# arrive through obj/*.nuget.g.{props,targets} -- files NuGet excludes while it evaluates the restore
# graph. The last section packs the SDK into a feed and publishes a consumer against it, so the one
# path every real connector takes (restore, publish, pack, all through the package) is proven too.
#
# Linux only: Native AOT cannot cross-compile between OSes and the host's runner is Linux.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK_DIR="$(mktemp -d)"
# Delete the staged publish/nupkg/restore only on success: a CI failure is exactly when a human needs
# to inspect what got built, so a failing run keeps the directory and says where it is.
cleanup() {
  local status=$?
  if [[ ${status} -eq 0 ]]; then
    rm -rf "${WORK_DIR}"
  else
    echo "FAIL: leaving work dir for inspection: ${WORK_DIR}" >&2
  fi
}
trap cleanup EXIT

RID="linux-x64"
if [[ "$(uname -m)" == "aarch64" ]]; then RID="linux-arm64"; fi
FIXTURE="${ROOT_DIR}/tests/fixtures/PcpFakeConnector"
PKG_ID="PcpFakeConnector"
PKG_VERSION="1.0.0"

echo "== Pz.Connectors.Sdk packaging verification (${RID}) =="
echo "work dir: ${WORK_DIR}"

echo "-- Building pz --"
dotnet build "${ROOT_DIR}/src/Pz.Cli" -c Release --nologo -v quiet
PZ="${ROOT_DIR}/src/Pz.Cli/bin/Release/net10.0/Pz.Cli"
[[ -x "${PZ}" ]] || { echo "FAIL: no pz binary at ${PZ}"; exit 1; }

# A <PzPackaging> set in the PROJECT FILE must win over the SDK's default exactly as a -p: global
# property does. The two publish/pack runs below pass -p:PzPackaging, which cannot tell the two
# apart, so this evaluates a throwaway consumer that sets the value in its body and asks MSBuild
# what PublishAot/PublishSingleFile became. Evaluation only: no restore, no build.
verify_project_level_packaging() {
  local probe="${WORK_DIR}/probe"
  mkdir -p "${probe}"
  cat > "${probe}/Probe.csproj" <<EOF
<Project Sdk="Microsoft.NET.Sdk">
  <Import Project="${ROOT_DIR}/src/Pz.Connectors.Sdk/build/Pz.Connectors.Sdk.props" />
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net10.0</TargetFramework>
    <PzPackaging>self-contained</PzPackaging>
  </PropertyGroup>
  <Import Project="${ROOT_DIR}/src/Pz.Connectors.Sdk/build/Pz.Connectors.Sdk.targets" />
</Project>
EOF
  echo "-- project-level <PzPackaging>self-contained</PzPackaging> --"
  local aot single
  aot="$(dotnet msbuild "${probe}/Probe.csproj" -getProperty:PublishAot -p:RuntimeIdentifier="${RID}" --nologo)"
  single="$(dotnet msbuild "${probe}/Probe.csproj" -getProperty:PublishSingleFile -p:RuntimeIdentifier="${RID}" --nologo)"
  [[ "${aot}" == "false" ]] || { echo "FAIL: project-level PzPackaging=self-contained left PublishAot='${aot}'"; exit 1; }
  [[ "${single}" == "true" ]] || { echo "FAIL: project-level PzPackaging=self-contained left PublishSingleFile='${single}'"; exit 1; }
  echo "ok: PublishAot=${aot} PublishSingleFile=${single}"
}

verify_mode() {
  local mode="$1"
  local mode_dir="${WORK_DIR}/${mode}"
  local stage="${mode_dir}/stage/"
  local feed="${mode_dir}/feed"
  local proj="${mode_dir}/project"
  mkdir -p "${feed}" "${proj}/data" "${proj}/pipelines"

  echo
  echo "== mode: ${mode} =="
  echo "-- dotnet publish -r ${RID} (PzPackaging=${mode}) --"
  dotnet publish "${FIXTURE}" -c Release -r "${RID}" -p:PzPackaging="${mode}" -p:PzNativeStaging="${stage}" \
    --nologo -v quiet
  [[ -x "${stage}${RID}/PcpFakeConnector" ]] || { echo "FAIL: no staged binary at ${stage}${RID}/PcpFakeConnector"; exit 1; }
  if [[ "${mode}" == "aot" ]]; then
    [[ ! -f "${stage}${RID}/PcpFakeConnector.dll" ]] || { echo "FAIL: AOT publish staged a managed dll"; exit 1; }
  else
    # Single-file bundles the runtime into the executable; a loose CoreLib means it did not apply.
    [[ ! -f "${stage}${RID}/System.Private.CoreLib.dll" ]] || { echo "FAIL: self-contained publish is not single-file"; exit 1; }
  fi

  echo "-- dotnet pack --"
  # PzRuntimeIdentifiers is narrowed to the one RID this machine can publish: the missing-RID warning
  # (PZSDK002) is an error under this repo's TreatWarningsAsErrors, and here it would be a false alarm.
  # MinVerVersionOverride, not PackageVersion: MinVer computes the version in a target and would
  # otherwise stamp the git-height prerelease over anything set on the command line.
  dotnet pack "${FIXTURE}" -c Release -p:PzPackaging="${mode}" -p:PzNativeStaging="${stage}" \
    -p:PzRuntimeIdentifiers="${RID}" -p:IsPackable=true -p:MinVerVersionOverride="${PKG_VERSION}" \
    -o "${feed}" --nologo -v quiet
  local nupkg="${feed}/${PKG_ID}.${PKG_VERSION}.nupkg"
  [[ -f "${nupkg}" ]] || { echo "FAIL: no nupkg at ${nupkg}"; exit 1; }

  echo "-- nupkg layout --"
  local listing
  listing="$(unzip -Z1 "${nupkg}")"
  grep -qx "runtimes/${RID}/native/PcpFakeConnector" <<<"${listing}" || { echo "FAIL: binary missing from runtimes/${RID}/native/"; echo "${listing}"; exit 1; }
  grep -qx "pz.connector.json" <<<"${listing}" || { echo "FAIL: manifest missing from nupkg root"; exit 1; }
  ! grep -q '^lib/' <<<"${listing}" || { echo "FAIL: lib/ must not be packed"; exit 1; }
  local nuspec
  nuspec="$(unzip -p "${nupkg}" "${PKG_ID}.nuspec")"
  [[ -n "${nuspec}" ]] || { echo "FAIL: could not read ${PKG_ID}.nuspec out of the nupkg"; exit 1; }
  ! grep -q '<dependency ' <<<"${nuspec}" || { echo "FAIL: nuspec declares dependencies"; echo "${nuspec}"; exit 1; }
  local manifest
  manifest="$(unzip -p "${nupkg}" pz.connector.json)"
  grep -q '"runtime": "process"' <<<"${manifest}" || { echo "FAIL: manifest runtime"; echo "${manifest}"; exit 1; }
  grep -q "\"${RID}\": \"native/PcpFakeConnector\"" <<<"${manifest}" || { echo "FAIL: manifest entrypoint"; echo "${manifest}"; exit 1; }
  grep -q '"name": "localfiles-pcp"' <<<"${manifest}" || { echo "FAIL: manifest name"; echo "${manifest}"; exit 1; }
  grep -q '"PartitionedRead"' <<<"${manifest}" || { echo "FAIL: manifest capabilities"; echo "${manifest}"; exit 1; }

  echo "-- pz restore from the local feed --"
  printf 'name: sdk_verify\nversion: 0.1.0\n\nconnectors:\n  - package: %s\n    version: %s\nengine:\n  threads: 2\n' \
    "${PKG_ID}" "${PKG_VERSION}" > "${proj}/project.yml"
  cat > "${proj}/connections.yml" <<'EOF'
files:
  connector: localfiles-pcp
  entities:
    orders:
      read:
        path: data/orders.csv
        format: csv
        columns:
          id: bigint
          customer: varchar
          amount: double

lake:
  connector: localfiles-pcp
  entities:
    orders_copy:
      write:
        strategy: replace
        format: parquet
        path: out/
EOF
  printf 'INSERT INTO {{ sink('"'"'lake'"'"', '"'"'orders_copy'"'"') }}\nselect id, customer, amount\nfrom {{ source('"'"'files'"'"', '"'"'orders'"'"') }}\norder by id\n' \
    > "${proj}/pipelines/orders_copy.sql"
  printf 'id,customer,amount\n1,ann,10.5\n2,bob,20.25\n3,ann,5.25\n4,cy,100.0\n' > "${proj}/data/orders.csv"
  (cd "${proj}" && "${PZ}" restore --feeds "${feed}")
  local pkg_dir="${proj}/.pz/packages/${PKG_ID}/${PKG_VERSION}"
  # Existence, not the executable bit: a nupkg carries no unix mode, and the host sets +x when it
  # reads the manifest to spawn the connector -- which is what the post-run assertion below checks.
  [[ -f "${pkg_dir}/native/PcpFakeConnector" ]] || { echo "FAIL: restore did not materialize native/PcpFakeConnector"; ls -R "${pkg_dir}"; exit 1; }
  [[ ! -d "${pkg_dir}/lib" ]] || { echo "FAIL: restore materialized a lib/ directory"; exit 1; }

  echo "-- pz run (source and sink both through the packaged connector) --"
  (cd "${proj}" && "${PZ}" run)
  [[ -x "${pkg_dir}/native/PcpFakeConnector" ]] || { echo "FAIL: the host did not make the entrypoint executable"; exit 1; }
  compgen -G "${proj}/out/*.parquet" >/dev/null || compgen -G "${proj}/out/**/*.parquet" >/dev/null \
    || { echo "FAIL: no parquet output under ${proj}/out"; ls -R "${proj}/out" 2>/dev/null; exit 1; }

  echo "-- pz connector test against the materialized package --"
  cat > "${proj}/probe.yml" <<EOF
connection:
  root: ${proj}
read:
  dataset: orders
  path: data/orders.csv
  format: csv
  columns:
    id: bigint
    customer: varchar
    amount: double
write:
  output: customer_totals
  mode: replace
  format: parquet
  path: out-probe/
EOF
  local report
  report="$(cd "${proj}" && "${PZ}" connector test "${pkg_dir}" --config probe.yml)"
  echo "${report}" | tail -n 5
  ! grep -q "FAIL" <<<"${report}" || { echo "FAIL: a conformance vector failed"; echo "${report}"; exit 1; }
  echo "mode ${mode}: OK"
}

# The consumer below is what a stranger's connector project looks like: the SDK and the ABI as
# PackageReferences from a feed, nothing imported by path, the project file as the README documents
# it. Written outside the repository so none of its Directory.Build.* applies.
SDK_PKG_VERSION="0.0.0-sdk-verify"
CONSUMER_ROOT="${WORK_DIR}/consumer"

write_consumer() {
  local dir="$1" properties="$2"
  mkdir -p "${dir}"
  cat > "${dir}/SdkConsumer.csproj" <<EOF
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>${SDK_TFM}</TargetFramework>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
    <Version>1.0.0</Version>
${properties}
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Pz.Connectors.Sdk" Version="${SDK_PKG_VERSION}" />
    <PackageReference Include="Pz.Connectors.Abstractions" Version="${SDK_PKG_VERSION}" />
  </ItemGroup>
</Project>
EOF
  cat > "${dir}/Program.cs" <<'EOF'
using Pz.Connectors.Sdk;

return await PzConnectorHost.RunAsync(args, new SdkConsumer.NullSinkConnector());
EOF
  cat > "${dir}/NullSinkConnector.cs" <<'EOF'
using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace SdkConsumer;

// The smallest sink the ABI admits: counts what it is handed and keeps nothing.
public sealed class NullSinkConnector : ISinkConnector
{
    public ConnectorInfo Info => new("nullsink", "1.0.0", ProtocolVersion.Major);
    public ConnectorCapabilities Capabilities => ConnectorCapabilities.None;
    public string ConnectionConfigSchema => "{}";
    public string DatasetConfigSchema => "{}";
    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
        ValueTask.FromResult(ValidationResult.Success);
    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        ValueTask.FromResult(new ConnectionCheck(true));
    public ValueTask<ISink> OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        ValueTask.FromResult<ISink>(new NullSink());
}

internal sealed class NullSink : ISink
{
    public bool TryGetNativeCopy(OutputSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeCopy? copy)
    {
        copy = null;
        return false;
    }

    public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct) =>
        ValueTask.FromResult<ISinkWriteSession>(new NullSession());

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

internal sealed class NullSession : ISinkWriteSession
{
    private long _rows, _batches;

    public ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
    {
        _rows += batch.Length;
        _batches++;
        return ValueTask.CompletedTask;
    }

    public ValueTask<WriteResult> CommitAsync(CancellationToken ct) => ValueTask.FromResult(new WriteResult(_rows, _batches));
    public ValueTask AbortAsync(CancellationToken ct) => ValueTask.CompletedTask;
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
EOF
}

# Exact-ish ELF check without depending on file(1): the four magic bytes.
is_elf() { [[ "$(head -c 4 "$1" | od -An -tx1 | tr -d ' \n')" == "7f454c46" ]]; }
ilcompiler_rid_pack_count() { grep -c -i "\"runtime.${RID}.microsoft.dotnet.ilcompiler/" "$1/obj/project.assets.json" || true; }

verify_consumer_through_package() {
  local feed="${CONSUMER_ROOT}/feed"
  mkdir -p "${feed}"
  echo
  echo "== consumer through the packaged SDK (${SDK_PKG_VERSION}) =="
  echo "-- dotnet pack Pz.Connectors.Abstractions + Pz.Connectors.Sdk --"
  # One MinVerVersionOverride for both: the SDK's nuspec depends on the ABI at the version the same
  # override gives it, so the consumer resolves both from this feed and nothing else.
  dotnet pack "${ROOT_DIR}/src/Pz.Connectors.Abstractions" -c Release -p:MinVerVersionOverride="${SDK_PKG_VERSION}" \
    -o "${feed}" --nologo -v quiet
  dotnet pack "${ROOT_DIR}/src/Pz.Connectors.Sdk" -c Release -p:MinVerVersionOverride="${SDK_PKG_VERSION}" \
    -o "${feed}" --nologo -v quiet
  [[ -f "${feed}/Pz.Connectors.Sdk.${SDK_PKG_VERSION}.nupkg" ]] || { echo "FAIL: no SDK nupkg in ${feed}"; ls "${feed}"; exit 1; }
  # The global package cache keys on id+version, so a copy an earlier run left there would shadow
  # the pack just made. The version string is one only this script produces.
  local cache="${NUGET_PACKAGES:-${HOME}/.nuget/packages}"
  rm -rf "${cache}/pz.connectors.sdk/${SDK_PKG_VERSION}" "${cache}/pz.connectors.abstractions/${SDK_PKG_VERSION}"
  cat > "${CONSUMER_ROOT}/NuGet.Config" <<EOF
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <clear />
    <add key="sdk-verify" value="${feed}" />
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
  </packageSources>
</configuration>
EOF
  SDK_TFM="$(dotnet msbuild "${ROOT_DIR}/src/Pz.Connectors.Sdk" -getProperty:TargetFramework --nologo)"
  [[ -n "${SDK_TFM}" ]] || { echo "FAIL: could not read the SDK's TargetFramework"; exit 1; }

  # (1) The documented project: PublishAot in the project body, PzPackaging left at its default.
  local aot="${CONSUMER_ROOT}/aot" stage
  write_consumer "${aot}" "    <PublishAot>true</PublishAot>"
  echo "-- documented project, PzPackaging default: dotnet publish -r ${RID} --"
  dotnet publish "${aot}" -c Release -r "${RID}" --nologo -v quiet
  local packs
  packs="$(ilcompiler_rid_pack_count "${aot}")"
  [[ "${packs}" -gt 0 ]] || { echo "FAIL: restore did not bring runtime.${RID}.Microsoft.DotNet.ILCompiler into project.assets.json"; exit 1; }
  stage="${aot}/bin/pz-native/${RID}"
  is_elf "${stage}/SdkConsumer" || { echo "FAIL: staged SdkConsumer is not an ELF binary"; ls -la "${stage}"; exit 1; }
  ! compgen -G "${stage}/*.dll" >/dev/null || { echo "FAIL: AOT publish staged a managed dll"; ls "${stage}"; exit 1; }
  [[ ! -f "${stage}/libcoreclr.so" ]] || { echo "FAIL: AOT publish staged the CoreCLR runtime"; ls "${stage}"; exit 1; }
  echo "ok: ${packs} ILCompiler RID-pack assets entries; staged $(ls "${stage}" | tr '\n' ' ')"

  echo "-- same project, dotnet pack --"
  dotnet pack "${aot}" -c Release -p:PzRuntimeIdentifiers="${RID}" -o "${CONSUMER_ROOT}/out" --nologo -v quiet
  local nupkg="${CONSUMER_ROOT}/out/SdkConsumer.1.0.0.nupkg" listing
  [[ -f "${nupkg}" ]] || { echo "FAIL: no nupkg at ${nupkg}"; exit 1; }
  listing="$(unzip -Z1 "${nupkg}")"
  grep -qx "runtimes/${RID}/native/SdkConsumer" <<<"${listing}" || { echo "FAIL: binary missing from runtimes/${RID}/native/"; echo "${listing}"; exit 1; }
  grep -qx "pz.connector.json" <<<"${listing}" || { echo "FAIL: manifest missing from nupkg root"; exit 1; }
  ! grep -q '^lib/' <<<"${listing}" || { echo "FAIL: lib/ must not be packed"; echo "${listing}"; exit 1; }
  grep -q '"name": "nullsink"' <<<"$(unzip -p "${nupkg}" pz.connector.json)" || { echo "FAIL: manifest name"; exit 1; }
  echo "ok: nupkg layout"

  # (2) The same project switched to self-contained from the command line, as a -p: global
  # property: single-file CoreCLR, PublishAot off, exactly what a project-level opt-out yields.
  echo "-- documented project, -p:PzPackaging=self-contained --"
  dotnet publish "${aot}" -c Release -r "${RID}" -p:PzPackaging=self-contained --nologo -v quiet
  [[ -x "${stage}/SdkConsumer" ]] || { echo "FAIL: no staged binary after the self-contained publish"; exit 1; }
  [[ ! -f "${stage}/System.Private.CoreLib.dll" ]] || { echo "FAIL: -p:PzPackaging=self-contained is not single-file"; ls "${stage}"; exit 1; }
  echo "ok: single-file; staged $(ls "${stage}" | wc -l) files"

  # (3) A project that forgot PublishAot: restore cannot have brought the compiler pack, and the
  # publish must say so (PZSDK005) rather than stage a CoreCLR layout as if it were native.
  local plain="${CONSUMER_ROOT}/plain" output
  write_consumer "${plain}" ""
  echo "-- project without PublishAot: dotnet publish must fail with PZSDK005 --"
  if output="$(dotnet publish "${plain}" -c Release -r "${RID}" --nologo -v quiet 2>&1)"; then
    echo "FAIL: publish without PublishAot succeeded; staged: $(ls "${plain}/bin/pz-native/${RID}" 2>/dev/null | wc -l) files"; exit 1
  fi
  grep -q "PZSDK005" <<<"${output}" || { echo "FAIL: publish failed for another reason"; echo "${output}"; exit 1; }
  [[ "$(ilcompiler_rid_pack_count "${plain}")" == "0" ]] || { echo "FAIL: expected no ILCompiler pack in a restore that never saw PublishAot"; exit 1; }
  [[ ! -d "${plain}/bin/pz-native" ]] || { echo "FAIL: a failed publish still staged output"; exit 1; }
  echo "ok: PZSDK005"

  # (4) The project-level opt-out kafka ships with: no PublishAot anywhere, single-file CoreCLR.
  local sc="${CONSUMER_ROOT}/self-contained"
  write_consumer "${sc}" "    <PzPackaging>self-contained</PzPackaging>"
  echo "-- project-level <PzPackaging>self-contained</PzPackaging>: dotnet publish -r ${RID} --"
  dotnet publish "${sc}" -c Release -r "${RID}" --nologo -v quiet
  stage="${sc}/bin/pz-native/${RID}"
  [[ -x "${stage}/SdkConsumer" ]] || { echo "FAIL: no staged binary at ${stage}/SdkConsumer"; exit 1; }
  [[ ! -f "${stage}/System.Private.CoreLib.dll" ]] || { echo "FAIL: self-contained publish is not single-file"; ls "${stage}"; exit 1; }
  echo "ok: single-file; staged $(ls "${stage}" | wc -l) files"
}

verify_project_level_packaging
verify_mode aot
verify_mode self-contained
verify_consumer_through_package

echo
echo "== PASS: Pz.Connectors.Sdk packaging verified in both modes, by path and through the package =="
