#!/usr/bin/env python3
"""Generate bigrec scenarios at a range of single-cell sizes to find the universal-path row-size limit."""
import os, pathlib, sys

W = pathlib.Path(os.environ.get("PZ_STRESS_ROOT", "/tmp/pz-stress"))
SCEN, DATA = W / "scen", W / "data"

SIZES = [(1, 1 << 10), (2, 1 << 11), (4, 1 << 12), (8, 1 << 13)]
uni = sys.argv[1] == "uni" if len(sys.argv) > 1 else True

for kb, cell in SIZES:
    name = f"row-{kb}kb-{'uni' if uni else 'native'}"
    csv = DATA / f"row_{cell}.csv"
    if not csv.exists():
        with open(csv, "w", newline="\n") as f:
            f.write("id,payload\n")
            for i in range(20):
                f.write(f"{i},{'x' * cell}\n")
    d = SCEN / name
    (d / "pipelines").mkdir(parents=True, exist_ok=True)
    (d / "project.yml").write_text(f"""name: {name.replace('-', '_')}
version: 0.1.0

connectors:
  - package: Pz.Connector.LocalFiles
    version: 0.1.0

engine:
  threads: 2
{'  force_universal: true' if uni else ''}
  duckdb:
    memory_limit: 1GiB
""")
    (d / "connections.yml").write_text(f"""bench:
  connector: localfiles
  entities:
    big:
      read:
        path: {csv}
        format: csv
        columns:
          id: bigint
          payload: varchar

lake:
  connector: localfiles
  root: out
""")
    (d / "pipelines" / "big_out.sql").write_text(
        "INSERT INTO {{ sink('lake', 'big_out', format: 'csv', strategy: 'replace') }}\n"
        "select * from {{ source('bench', 'big') }}\n")
    print(name)
