#Requires -Version 5.1
# Installs/updates pz as a dotnet tool from THIS bundle's local feed only (no network).
# Prerequisite: .NET 10 SDK on the machine (`dotnet tool install` needs the SDK, not just a runtime).
param(
    [string]$ToolPath = "C:\pz\tool"
)
$ErrorActionPreference = "Stop"

$bundleDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$nugetConfig = Join-Path $bundleDir "nuget.config"
if (-not (Test-Path (Join-Path $bundleDir "feed"))) {
    throw "feed/ not found next to install.ps1 - run this from the extracted bundle directory."
}

dotnet --version | Out-Null
if ($LASTEXITCODE -ne 0) { throw "The .NET SDK is required but 'dotnet --version' failed (exit $LASTEXITCODE)." }

# A tool path written by a pre-rename bundle holds the old `Pz.Cli` package, which claims the same
# `pz` shim name -- installing the current `pz` id over it fails on that collision instead of
# replacing it. Removing the old id first is a silent no-op when it was never installed; all streams
# go to $null so its "tool not installed" message is not mistaken for a failure of this script.
dotnet tool uninstall Pz.Cli --tool-path $ToolPath *> $null

if (Test-Path (Join-Path $ToolPath "pz.exe")) {
    dotnet tool update pz --tool-path $ToolPath --configfile $nugetConfig --prerelease
} else {
    dotnet tool install pz --tool-path $ToolPath --configfile $nugetConfig --prerelease
}
if ($LASTEXITCODE -ne 0) { throw "dotnet tool install/update failed with exit code $LASTEXITCODE" }

& (Join-Path $ToolPath "pz.exe") --version
if ($LASTEXITCODE -ne 0) { throw "pz --version failed with exit code $LASTEXITCODE" }
Write-Host "== PASS: pz installed at $ToolPath =="
