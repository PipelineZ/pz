#!/usr/bin/env python3
"""driver.py <scenario> [<scenario>...]  — plan + run each scenario under stressmon, append to results.jsonl"""
import json, os, pathlib, shutil, subprocess, sys

W = pathlib.Path(os.environ.get("PZ_STRESS_ROOT", "/tmp/pz-stress"))
PZ = os.environ.get("PZ_CLI_DLL", "src/Pz.Cli/bin/Release/net10.0/Pz.Cli.dll")
RESULTS = W / "results.jsonl"
HERE = pathlib.Path(__file__).resolve().parent

EXTRA = os.environ.get("PZ_EXTRA_ARGS", "").split()


def budget_for(proj):
    """Run `pz plan` and read plan.json's memoryBudget."""
    r = subprocess.run(["dotnet", PZ, "plan", "--project", str(proj)],
                       capture_output=True, text=True, timeout=900)
    plan = None
    cands = sorted((proj / ".pz").rglob("plan.json"), key=lambda p: p.stat().st_mtime) if (proj / ".pz").exists() else []
    if cands:
        try:
            plan = json.loads(cands[-1].read_text())
        except Exception:
            pass
    mb = (plan or {}).get("memoryBudget")
    nodes = len((plan or {}).get("nodes", []))
    return {"plan_rc": r.returncode, "memory_budget": mb, "nodes": nodes,
            "plan_stderr": r.stderr[-400:] if r.returncode else ""}


for name in sys.argv[1:]:
    proj = W / "scen" / name
    if not proj.exists():
        print(f"SKIP {name}: no such scenario"); continue
    shutil.rmtree(proj / ".pz", ignore_errors=True)
    shutil.rmtree(proj / "out", ignore_errors=True)

    b = budget_for(proj)
    shutil.rmtree(proj / ".pz", ignore_errors=True)

    cmd = ["python3", str(HERE / "stressmon.py"), name, str(proj), "--",
           "dotnet", PZ, "run", "--project", str(proj)] + EXTRA
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)
    rec = {}
    for line in r.stdout.splitlines():
        if line.startswith("RESULT "):
            rec = json.loads(line[7:])
    rec.update(b)
    outdir = proj / "out"
    rec["out_mb"] = round(sum(f.stat().st_size for f in outdir.rglob("*") if f.is_file()) / 1048576, 1) if outdir.exists() else 0
    log = (W / "samples" / f"{name}.log")
    tail = log.read_text(errors="replace").splitlines()[-6:] if log.exists() else []
    rec["tail"] = tail
    if rec.get("memory_budget"):
        tb = rec["memory_budget"].get("totalBytes")
        if tb:
            rec["budget_mb"] = round(tb / 1048576, 1)
            rec["rss_over_budget_pct"] = round((rec["peak_rss_mb"] - rec["budget_mb"]) / rec["budget_mb"] * 100, 1)
    with open(RESULTS, "a") as f:
        f.write(json.dumps(rec) + "\n")
    print(json.dumps({k: v for k, v in rec.items() if k not in ("memory_budget",)}, indent=None))
    sys.stdout.flush()
