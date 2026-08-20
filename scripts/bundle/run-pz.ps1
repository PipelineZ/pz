#Requires -Version 5.1
# Task Scheduler wrapper: runs `pz run --all` for one project, capturing NDJSON stdout to a dated
# `.ndjson` log and raw stderr to a sibling `.stderr.log`, prunes old logs, and exits with pz's own
# exit code (0 ok, 1 node failures, 2 config/validation, 3 fatal) so the scheduled task's Last Run
# Result mirrors the run outcome. Uses Start-Process file redirection (not `2>&1 | Out-File`):
# under Windows PowerShell 5.1, piping merges native stderr into ErrorRecord objects (mangling the
# raw text worst exactly in the exit-3 fatal case ops must diagnose), and Out-File -Encoding utf8
# adds a BOM and CRLF line endings that NDJSON consumers don't expect.
param(
    [Parameter(Mandatory = $true)][string]$ProjectDir,
    [string]$PzExe = "C:\pz\tool\pz.exe",
    [string]$LogDir = "D:\pz\logs",
    [ValidateRange(1, 3650)][int]$KeepLogsDays = 30
)
$ErrorActionPreference = "Continue"  # pz reporting failures is data, not a script error

# a trailing backslash would escape the embedded closing quote below
$ProjectDir = $ProjectDir.TrimEnd('\')

if (-not (Test-Path $PzExe)) { Write-Error "pz executable not found at $PzExe"; exit 3 }

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
$name = Split-Path -Leaf $ProjectDir
$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$log = Join-Path $LogDir "$name-$stamp.ndjson"
$errLog = Join-Path $LogDir "$name-$stamp.stderr.log"

# `"..."` around $ProjectDir: PS 5.1 joins -ArgumentList with spaces and does NO quoting,
# so a path containing a space would otherwise split into two argv entries.
$proc = Start-Process -FilePath $PzExe `
    -ArgumentList @('run', '--all', '--project', "`"$ProjectDir`"", '--log-format', 'json') `
    -NoNewWindow -Wait -PassThru `
    -RedirectStandardOutput $log -RedirectStandardError $errLog
if (-not $proc) { Write-Error "failed to start $PzExe"; exit 3 }
$exit = $proc.ExitCode

if ((Test-Path $errLog) -and (Get-Item $errLog).Length -eq 0) { Remove-Item $errLog }

# Anchor the prune to this project's exact stamp shape so "orders" never prunes "orders-eu" logs.
$prunePattern = "^$([regex]::Escape($name))-\d{8}-\d{6}\.(ndjson|stderr\.log)$"
Get-ChildItem $LogDir -File |
    Where-Object { $_.Name -match $prunePattern -and $_.LastWriteTime -lt (Get-Date).AddDays(-$KeepLogsDays) } |
    Remove-Item -Force

exit $exit
