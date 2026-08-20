#!/usr/bin/env python3
"""Run a scenario N times, tracking .pz growth and run-dir count (retention behaviour)."""
import os, pathlib, shutil, subprocess, sys

W = pathlib.Path(os.environ.get("PZ_STRESS_ROOT", "/tmp/pz-stress"))
PZ = os.environ.get("PZ_CLI_DLL", "src/Pz.Cli/bin/Release/net10.0/Pz.Cli.dll")
name, n = sys.argv[1], int(sys.argv[2])
proj = W / "scen" / name
shutil.rmtree(proj / ".pz", ignore_errors=True)


def dsize(p):
    return sum(f.stat().st_size for f in p.rglob("*") if f.is_file()) if p.exists() else 0


for i in range(1, n + 1):
    subprocess.run(["dotnet", PZ, "run", "--project", str(proj)],
                   capture_output=True, timeout=3600)
    runs = proj / ".pz" / "runs"
    ndirs = len(list(runs.iterdir())) if runs.exists() else 0
    print(f"run {i:3d}: .pz={dsize(proj / '.pz') / 1048576:8.1f} MB  run_dirs={ndirs}")
    sys.stdout.flush()
