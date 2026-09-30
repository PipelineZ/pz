# Packages a win-x64 native publish folder as the GitHub Release / winget zip.
# The assembly is Pz.Cli, so the native image is Pz.Cli.exe; it is renamed to the command people type.
# The exe loads native libraries (duckdb.dll and others) from its own directory, so the zip carries
# every published file except debug symbols, and the renamed exe must run from there before zipping.
param(
    [Parameter(Mandatory)][string]$PublishDir,
    [Parameter(Mandatory)][string]$ZipPath
)
$ErrorActionPreference = 'Stop'

$staging = Join-Path ([IO.Path]::GetTempPath()) "pz-win-x64-$([guid]::NewGuid().ToString('N'))"
New-Item -ItemType Directory $staging | Out-Null
try {
    Get-ChildItem -LiteralPath $PublishDir -File |
        Where-Object { $_.Extension -ne '.pdb' } |
        Copy-Item -Destination $staging
    Rename-Item -LiteralPath (Join-Path $staging 'Pz.Cli.exe') -NewName 'pz.exe'
    Get-ChildItem -LiteralPath $staging | Format-Table Name, Length -AutoSize | Out-String | Write-Host

    & (Join-Path $staging 'pz.exe') --version
    if ($LASTEXITCODE -ne 0) { throw "pz.exe --version exited $LASTEXITCODE" }

    Compress-Archive -Path (Join-Path $staging '*') -DestinationPath $ZipPath -Force
}
finally {
    Remove-Item -LiteralPath $staging -Recurse -Force -ErrorAction SilentlyContinue
}
