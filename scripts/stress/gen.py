#!/usr/bin/env python3
"""Generate PipelineZ stress scenario projects under $W/scen/<name>."""
import os, pathlib, sys, random

W = pathlib.Path(os.environ.get("PZ_STRESS_ROOT", "/tmp/pz-stress"))
SCEN = W / "scen"
DATA = W / "data"   # shared generated data, symlinked/pointed at by projects
SCEN.mkdir(parents=True, exist_ok=True)
DATA.mkdir(parents=True, exist_ok=True)

ORDERS_COLS = "id: bigint\n          customer_id: bigint\n          amount: double\n          status: varchar"


def gen_orders_csv(path, n):
    if path.exists() and path.stat().st_size > 0:
        return
    statuses = ["shipped", "pending", "returned", "cancelled"]
    with open(path, "w", newline="\n") as f:
        f.write("id,customer_id,amount,status\n")
        buf = []
        for i in range(1, n + 1):
            buf.append(f"{i},{(i * 2654435761 + 42) % 100000},{((i * 97 + 42) % 100000) / 100.0:.2f},{statuses[i % 4]}\n")
            if len(buf) >= 100000:
                f.writelines(buf); buf = []
        f.writelines(buf)


def gen_wide_csv(path, cols, rows):
    if path.exists() and path.stat().st_size > 0:
        return
    with open(path, "w", newline="\n") as f:
        f.write(",".join(f"c{i}" for i in range(cols)) + "\n")
        buf = []
        for r in range(rows):
            buf.append(",".join(str((r * 31 + c) % 100000) for c in range(cols)) + "\n")
            if len(buf) >= 500:
                f.writelines(buf); buf = []
        f.writelines(buf)


def gen_bigrec_csv(path, rows, cell_bytes):
    """rows, each with one huge quoted text cell of cell_bytes."""
    if path.exists() and path.stat().st_size > 0:
        return
    blob = "x" * cell_bytes
    with open(path, "w", newline="\n") as f:
        f.write("id,payload\n")
        for i in range(rows):
            f.write(f"{i},{blob}\n")


def write(p, text):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)


def project(name, engine_yaml, connections, pipelines, connectors=("Pz.Connector.LocalFiles",)):
    d = SCEN / name
    (d / "pipelines").mkdir(parents=True, exist_ok=True)
    conn_yaml = "".join(f"  - package: {c}\n    version: 0.1.0\n" for c in connectors)
    write(d / "project.yml", f"""name: {name.replace('-', '_')}
version: 0.1.0

connectors:
{conn_yaml}
engine:
{engine_yaml}
""")
    write(d / "connections.yml", connections)
    for fn, sql in pipelines.items():
        write(d / "pipelines" / fn, sql)
    return d


ENGINE_BASE = "  threads: 2\n  duckdb:\n    memory_limit: 1GiB\n"


def orders_conn(csv_path, with_contract):
    contract = f"\n        columns:\n          {ORDERS_COLS}" if with_contract else ""
    return f"""bench:
  connector: localfiles
  entities:
    orders:
      read:
        path: {csv_path}
        format: csv{contract}

lake:
  connector: localfiles
  root: out
"""


PASSTHRU = """INSERT INTO {{ sink('lake', 'orders_out', format: 'csv', strategy: 'replace') }}
select * from {{ source('bench', 'orders') }}
"""

which = sys.argv[1] if len(sys.argv) > 1 else "all"


def want(g):
    return which in ("all", g)


# --- A: progressive scale, native tier -------------------------------------
if want("scale"):
    for label, n in [("scale-100k", 100_000), ("scale-1m", 1_000_000), ("scale-5m", 5_000_000)]:
        csv = DATA / f"orders_{n}.csv"
        gen_orders_csv(csv, n)
        project(label, ENGINE_BASE, orders_conn(csv, False), {"orders_out.sql": PASSTHRU})

# --- B: universal tier (forces the .NET Arrow path) ------------------------
if want("uni"):
    for label, n in [("uni-1m", 1_000_000), ("uni-5m", 5_000_000)]:
        csv = DATA / f"orders_{n}.csv"
        gen_orders_csv(csv, n)
        project(label, ENGINE_BASE + "  force_universal: true\n",
                orders_conn(csv, True), {"orders_out.sql": PASSTHRU})

# --- C: very wide ----------------------------------------------------------
if want("wide"):
    for label, cols, rows, uni in [("wide-1000-native", 1000, 20_000, False),
                                   ("wide-1000-uni", 1000, 20_000, True),
                                   ("wide-4000-native", 4000, 5_000, False)]:
        csv = DATA / f"wide_{cols}x{rows}.csv"
        gen_wide_csv(csv, cols, rows)
        contract = "\n        columns:\n" + "".join(f"          c{i}: bigint\n" for i in range(cols)) if uni else ""
        conn = f"""bench:
  connector: localfiles
  entities:
    wide:
      read:
        path: {csv}
        format: csv{contract}

lake:
  connector: localfiles
  root: out
"""
        sql = """INSERT INTO {{ sink('lake', 'wide_out', format: 'csv', strategy: 'replace') }}
select * from {{ source('bench', 'wide') }}
"""
        project(label, ENGINE_BASE + ("  force_universal: true\n" if uni else ""), conn, {"wide_out.sql": sql})

# --- D: huge individual records -------------------------------------------
if want("bigrec"):
    for label, rows, cell, uni in [("bigrec-1mb-uni", 200, 1 << 20, True),
                                   ("bigrec-16mb-uni", 32, 16 << 20, True),
                                   ("bigrec-64mb-uni", 8, 64 << 20, True),
                                   ("bigrec-64mb-native", 8, 64 << 20, False)]:
        csv = DATA / f"bigrec_{rows}x{cell}.csv"
        gen_bigrec_csv(csv, rows, cell)
        contract = "\n        columns:\n          id: bigint\n          payload: varchar"
        conn = f"""bench:
  connector: localfiles
  entities:
    big:
      read:
        path: {csv}
        format: csv{contract}

lake:
  connector: localfiles
  root: out
"""
        sql = """INSERT INTO {{ sink('lake', 'big_out', format: 'csv', strategy: 'replace') }}
select * from {{ source('bench', 'big') }}
"""
        project(label, ENGINE_BASE + ("  force_universal: true\n" if uni else ""), conn, {"big_out.sql": sql})

# --- E: many batches (tiny batch_bytes) ------------------------------------
if want("manybatch"):
    for label, bb in [("manybatch-64k", 65536), ("manybatch-1m", 1 << 20)]:
        csv = DATA / "orders_1000000.csv"
        gen_orders_csv(csv, 1_000_000)
        project(label, ENGINE_BASE + f"  force_universal: true\n  batch_bytes: {bb}\n",
                orders_conn(csv, True), {"orders_out.sql": PASSTHRU})

# --- F: many datasets / many small files -----------------------------------
if want("manyfiles"):
    N = 300
    for i in range(N):
        p = DATA / "small" / f"part_{i:04d}.csv"
        p.parent.mkdir(parents=True, exist_ok=True)
        if not p.exists():
            with open(p, "w", newline="\n") as f:
                f.write("id,customer_id,amount,status\n")
                for r in range(100):
                    f.write(f"{i * 100 + r},{r},{r}.50,shipped\n")
    ents = "".join(
        f"""    p{i:04d}:
      read:
        path: {DATA / 'small' / f'part_{i:04d}.csv'}
        format: csv
        columns:
          {ORDERS_COLS}
""" for i in range(N))
    conn = f"""bench:
  connector: localfiles
  entities:
{ents}
lake:
  connector: localfiles
  root: out
"""
    union = "\nunion all\n".join("select * from {{ source('bench', 'p%04d') }}" % i for i in range(N))
    sql = "INSERT INTO {{ sink('lake', 'all_out', format: 'csv', strategy: 'replace') }}\n" + union + "\n"
    project("manyfiles-300", ENGINE_BASE, conn, {"all_out.sql": sql})

# --- G: expensive transformation, forced spill ------------------------------
if want("expensive"):
    csv = DATA / "orders_2000000.csv"
    gen_orders_csv(csv, 2_000_000)
    conn = orders_conn(csv, False)
    sql = """INSERT INTO {{ sink('lake', 'heavy_out', format: 'csv', strategy: 'replace') }}
select
  o.id,
  o.customer_id,
  o.amount,
  o.status,
  sum(o.amount) over (partition by o.customer_id order by o.id) as running_total,
  row_number() over (order by o.amount desc, o.id) as amount_rank,
  md5(o.status || cast(o.id as varchar)) as h
from {{ source('bench', 'orders') }} o
order by h
"""
    project("expensive-spill", "  threads: 2\n  duckdb:\n    memory_limit: 512MB\n", conn, {"heavy_out.sql": sql})

# --- H: concurrent pipelines ------------------------------------------------
if want("concurrent"):
    N = 8
    csvs = []
    for i in range(N):
        p = DATA / f"conc_{i}.csv"
        gen_orders_csv(p, 500_000)
        csvs.append(p)
    ents = "".join(
        f"""    s{i}:
      read:
        path: {csvs[i]}
        format: csv
""" for i in range(N))
    conn = f"""bench:
  connector: localfiles
  entities:
{ents}
lake:
  connector: localfiles
  root: out
"""
    pipes = {}
    for i in range(N):
        pipes[f"flow_{i}.sql"] = (
            "INSERT INTO {{ sink('lake', 'out_%d', format: 'csv', strategy: 'replace') }}\n"
            "select customer_id, count(*) as n, sum(amount) as total from {{ source('bench', 's%d') }} "
            "group by customer_id\n" % (i, i))
    project("concurrent-8", "  threads: 8\n  duckdb:\n    memory_limit: 1GiB\n", conn, pipes)

print("generated:", sorted(p.name for p in SCEN.iterdir()))
print("data dir bytes:", sum(f.stat().st_size for f in DATA.rglob("*") if f.is_file()))
