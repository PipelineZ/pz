#!/usr/bin/env python3
"""Minimal stdio JSON-RPC client for `pz mcp`. Usage: mcp_client.py <project> [--allow-run] [tool [json_args]]..."""
import json, subprocess, sys, threading, time, os

PZ = os.environ.get("PZ_CLI_DLL", "src/Pz.Cli/bin/Release/net10.0/Pz.Cli.dll")

project = sys.argv[1]
rest = sys.argv[2:]
allow_run = "--allow-run" in rest
if allow_run:
    rest.remove("--allow-run")

cmd = ["dotnet", PZ, "mcp", "--project", project] + (["--allow-run"] if allow_run else [])
p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)

_id = [0]


def call(method, params=None, timeout=600):
    _id[0] += 1
    msg = {"jsonrpc": "2.0", "id": _id[0], "method": method}
    if params is not None:
        msg["params"] = params
    p.stdin.write(json.dumps(msg) + "\n")
    p.stdin.flush()
    deadline = time.time() + timeout
    while time.time() < deadline:
        line = p.stdout.readline()
        if not line:
            return {"error": "eof", "stderr": p.stderr.read()[-2000:]}
        line = line.strip()
        if not line:
            continue
        try:
            d = json.loads(line)
        except json.JSONDecodeError:
            continue
        if d.get("id") == _id[0]:
            return d
    return {"error": "timeout"}


def notify(method, params=None):
    msg = {"jsonrpc": "2.0", "method": method}
    if params is not None:
        msg["params"] = params
    p.stdin.write(json.dumps(msg) + "\n")
    p.stdin.flush()


init = call("initialize", {"protocolVersion": "2024-11-05", "capabilities": {},
                           "clientInfo": {"name": "stressmon", "version": "1"}})
print("INIT:", json.dumps(init.get("result", init))[:400])
notify("notifications/initialized")

tools = call("tools/list")
names = [t["name"] for t in tools.get("result", {}).get("tools", [])]
print(f"TOOLS ({len(names)}):", ", ".join(names))

i = 0
while i < len(rest):
    tool = rest[i]
    args = {}
    if i + 1 < len(rest) and rest[i + 1].startswith("{"):
        args = json.loads(rest[i + 1]); i += 1
    i += 1
    t0 = time.time()
    r = call("tools/call", {"name": tool, "arguments": args})
    dt = time.time() - t0
    payload = json.dumps(r.get("result", r.get("error", r)))
    print(f"\n=== {tool} ({dt:.2f}s, {len(payload)} bytes) ===")
    print(payload[:3000])

p.stdin.close()
try:
    p.wait(timeout=15)
except subprocess.TimeoutExpired:
    p.kill()
