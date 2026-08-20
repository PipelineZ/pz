#!/usr/bin/env python3
"""stressmon.py LABEL PROJECT_DIR -- cmd...

Runs cmd; samples the whole process tree's RSS and the project's .pz staging dir every 200ms.
Reports peak RSS (from wait4 ru_maxrss, which covers the tree), CPU seconds, wall time,
peak/final staging bytes, and writes a JSON timeseries.
"""
import json, os, resource, subprocess, sys, time, pathlib

label = sys.argv[1]
proj = pathlib.Path(sys.argv[2])
assert sys.argv[3] == "--"
cmd = sys.argv[4:]

outdir = pathlib.Path(os.environ.get(
    "STRESS_OUT", os.path.join(os.environ.get("PZ_STRESS_ROOT", "/tmp/pz-stress"), "samples")))
outdir.mkdir(parents=True, exist_ok=True)


def dir_bytes(p):
    total = 0
    try:
        for root, _dirs, files in os.walk(p):
            for f in files:
                try:
                    total += os.lstat(os.path.join(root, f)).st_size
                except OSError:
                    pass
    except OSError:
        pass
    return total


def tree_rss_kb(pid):
    """Sum RSS over pid + descendants via /proc."""
    total = 0
    try:
        pids = [pid]
        seen = set()
        while pids:
            p = pids.pop()
            if p in seen:
                continue
            seen.add(p)
            try:
                with open(f"/proc/{p}/statm") as fh:
                    total += int(fh.read().split()[1]) * (os.sysconf("SC_PAGE_SIZE") // 1024)
                with open(f"/proc/{p}/task/{p}/children") as fh:
                    pids.extend(int(c) for c in fh.read().split())
            except (OSError, ValueError, IndexError):
                pass
    except Exception:
        pass
    return total


logf = open(outdir / f"{label}.log", "wb")
start = time.time()
proc = subprocess.Popen(cmd, stdout=logf, stderr=subprocess.STDOUT)

series = []
peak_disk = 0
peak_sampled_rss = 0
pzdir = proj / ".pz"
status = ru = None
while True:
    wpid, wstatus, wru = os.wait4(proc.pid, os.WNOHANG)
    if wpid != 0:
        status, ru = wstatus, wru
        break
    t = time.time() - start
    rss = tree_rss_kb(proc.pid)
    d = dir_bytes(pzdir)
    peak_disk = max(peak_disk, d)
    peak_sampled_rss = max(peak_sampled_rss, rss)
    series.append({"t": round(t, 2), "rss_mb": round(rss / 1024, 1), "pz_mb": round(d / 1048576, 1)})
    time.sleep(0.2)
proc.returncode = os.waitstatus_to_exitcode(status)
elapsed = time.time() - start
logf.close()
rc = os.waitstatus_to_exitcode(status)
final_disk = dir_bytes(pzdir)

rec = {
    "label": label,
    "rc": rc,
    "elapsed_s": round(elapsed, 2),
    "peak_rss_mb": round(ru.ru_maxrss / 1024, 1),
    "peak_sampled_rss_mb": round(peak_sampled_rss / 1024, 1),
    "user_cpu_s": round(ru.ru_utime, 2),
    "sys_cpu_s": round(ru.ru_stime, 2),
    "cpu_pct": round((ru.ru_utime + ru.ru_stime) / elapsed * 100) if elapsed > 0 else 0,
    "peak_pz_mb": round(peak_disk / 1048576, 1),
    "final_pz_mb": round(final_disk / 1048576, 1),
    "major_faults": ru.ru_majflt,
    "blk_in": ru.ru_inblock,
    "blk_out": ru.ru_oublock,
}
(outdir / f"{label}.series.json").write_text(json.dumps(series))
(outdir / f"{label}.result.json").write_text(json.dumps(rec, indent=2))
print("RESULT " + json.dumps(rec))
sys.exit(rc)
