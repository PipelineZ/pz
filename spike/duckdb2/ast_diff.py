"""Replays captured json_serialize_sql / json_deserialize_sql statements through two DuckDB CLIs
and reports where the results differ (structurally for JSON, textually otherwise)."""
import json, os, subprocess, sys
from collections import Counter

corpus, cli_old, cli_new, out = sys.argv[1:5]
env = {k: v for k, v in os.environ.items() if k not in ("AI_AGENT", "CLAUDECODE")}

def run(cli, sql):
    p = subprocess.run([cli, "-json", "-c", sql], capture_output=True, text=True, env=env)
    if p.returncode != 0 or not p.stdout.strip():
        return ("ERROR", (p.stderr or p.stdout).strip())
    rows = json.loads(p.stdout)
    return ("OK", list(rows[0].values())[0])

def norm(v):
    try:
        return json.loads(v)
    except Exception:
        return v

def paths(a, b, p="$"):
    """Yield JSON paths where a and b differ."""
    if type(a) != type(b):
        yield f"{p}: {json.dumps(a)[:80]} -> {json.dumps(b)[:80]}"
    elif isinstance(a, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a: yield f"{p}.{k}: (added) {json.dumps(b[k])[:80]}"
            elif k not in b: yield f"{p}.{k}: (removed)"
            else: yield from paths(a[k], b[k], f"{p}.{k}")
    elif isinstance(a, list):
        if len(a) != len(b): yield f"{p}: len {len(a)} -> {len(b)}"
        for i, (x, y) in enumerate(zip(a, b)): yield from paths(x, y, f"{p}[{i}]")
    elif a != b:
        yield f"{p}: {json.dumps(a)[:80]} -> {json.dumps(b)[:80]}"

stmts = list(dict.fromkeys(json.loads(l) for l in open(corpus)))
same = 0
keyhist = Counter()
with open(out, "w") as f:
    for s in stmts:
        o, n = run(cli_old, s), run(cli_new, s)
        if o == n:
            same += 1
            continue
        f.write("=" * 100 + "\nSQL: " + s[:600] + "\n")
        if o[0] != n[0] or o[0] == "ERROR":
            f.write(f"OLD {o[0]}: {str(o[1])[:400]}\nNEW {n[0]}: {str(n[1])[:400]}\n")
            keyhist[f"status {o[0]}->{n[0]}"] += 1
            continue
        a, b = norm(o[1]), norm(n[1])
        if isinstance(a, str) or isinstance(b, str):
            f.write(f"OLD: {o[1][:400]}\nNEW: {n[1][:400]}\n")
            keyhist["text differs"] += 1
            continue
        for d in paths(a, b):
            f.write("  " + d + "\n")
            # Generalise the path (drop indices) to histogram the kinds of drift.
            import re
            keyhist[re.sub(r"\[\d+\]", "[]", d.split(":")[0])] += 1
print(f"unique statements: {len(stmts)}  identical: {same}  differing: {len(stmts) - same}")
for k, c in keyhist.most_common(40):
    print(f"{c:5d}  {k}")
