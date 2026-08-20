# PipelineZ (`pz`)

**Write SQL. Get a data pipeline. Run it anywhere.**

[![CI](https://github.com/coccor/pz/actions/workflows/ci.yml/badge.svg)](https://github.com/coccor/pz/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

`pz` is a lightweight, developer-first engine for batch ETL/ELT, powered by
[DuckDB](https://duckdb.org). You describe your pipeline in plain SQL files.

## 🚀 Try it in five commands

```console
$ dotnet tool install --global Pz.Cli
$ pz init demo
$ cd demo
$ pz run --all
$ cat out/order_totals/*.csv
```

No Docker, no database, no network calls after the install. All you need is the .NET 10 SDK. Here's
what the run prints:

```console
$ pz run --all
ok src_raw__customers 3 rows 28ms
ok src_raw__orders 5 rows 20ms
ok src_raw__products 3 rows 7ms
ok stg_orders 3 rows 20ms
ok product_catalog 3 rows 12ms
ok orders_enriched 3 rows 14ms
ok order_totals 2 rows 14ms
ok check_orders_enriched_not_null_id_email 0 rows 3ms
ok check_orders_enriched_unique_id 0 rows 4ms
ok lake.orders_curated 3 rows 18ms
ok lake.product_catalog 3 rows 27ms
ok lake.order_totals 2 rows 1ms
run 20260819T193054712Z-04a1: 12 succeeded, 0 failed, 0 skipped
```

Sources loaded, pipelines ran in dependency order, data-quality checks ran inline, sinks wrote.

Full walkthrough: [the quickstart](https://pipelinez.dev/quickstart/).

## What a project looks like

A pipeline is a SQL file. Three template calls give `pz` everything it needs to build the graph:
`source()` reads from outside, `ref()` reads another pipeline, `sink()` writes back out.

```sql
-- pipelines/orders_enriched.sql
INSERT INTO {{ sink('lake', 'orders_curated', format: 'parquet', strategy: 'replace') }}
select o.id, o.amount, c.email
from {{ ref('stg_orders') }} as o
join {{ source('crm', 'customers', path: 'data/customers.csv', format: 'csv') }} as c
  on c.id = o.customer_id
```

`connections.yml` says where those places are and how to get in. It's a CSV folder here, but the
same two lines of SQL work against Postgres or S3 once you point the connection somewhere else:

```yaml
crm:
  connector: localfiles
  entities:
    orders:
      read: { path: data/orders.csv, format: csv }

lake:
  connector: localfiles
  root: out
```

Every read and write option can live in the YAML, as `orders` does above, or right at the
`source()`/`sink()` call that uses it. Your choice, but never both, so there's no precedence puzzle
to solve. That's the whole project. No `models/`, no profiles directory, no adapter to install.

## ⚡ Speed

DuckDB does the work, and often your data never even enters .NET: when both sides of an edge can
speak SQL, `pz` hands DuckDB a fragment and steps out of the way. When they can't, data streams
through .NET as zero-copy Arrow batches, never row by row.

Moving **1,000,000 rows**, end to end through `pz run`, next to raw DuckDB doing the same move by
itself:

| Move | `pz` | Raw DuckDB | |
|---|---|---|---|
| SQL Server → SQL Server | **10.5 s** | 16.2 s | 🏆 `pz` is 1.5× faster |
| CSV → Parquet on S3 | 1.4 s | 0.5 s | ~0.9 s of that is startup |
| CSV → Parquet on Azure Blob | 1.0 s | 0.5 s | ~0.5 s of that is startup |
| Local CSV → CSV | 1.9 s | 0.4 s | staged, not fused |
| Postgres → Postgres | 4.9 s | 1.9 s | the .NET driver floor |

`pz` beats DuckDB's own SQL Server extension because its sink measures your text columns and sizes
the target table to fit, then bulk-copies into it. Everywhere else the gap is a flat startup cost
that gets smaller as your data gets bigger, not a per-row tax.

Under the hood, on the same laptop: **~806k rows/sec** into DuckDB, **~960k rows/sec** back out.

> Every number here is one laptop (i7-8665U, 4 cores, 15 GiB), measured, not tuned. Re-run them on
> your own hardware with the harnesses documented in [Performance](https://pipelinez.dev/performance/), which also
> explains the ones where `pz` loses, and why.

And you know the memory cost before you run: `pz plan` prints a static budget computed from your
config alone.

## 🔌 What's in the box

Eight connectors ship built in, with no packages to install:

| | Read | Write | Notes |
|---|---|---|---|
| **Local files** | ✅ | ✅ | csv, NDJSON, parquet |
| **Postgres** | ✅ | ✅ | incremental, merge, CDC |
| **SQL Server** | ✅ | ✅ | incremental, merge, CDC |
| **MySQL** | ✅ | ✅ | incremental; append/replace |
| **SQLite** | ✅ | ✅ | incremental; append/replace |
| **S3** | ✅ | ✅ | any S3-compatible store, incl. [GCS](https://pipelinez.dev/how-to/gcs/) |
| **Azure Blob** | ✅ | ✅ | csv, NDJSON, parquet |
| **HTTP APIs** | ✅ | ✅ | REST; append/merge |

Need another? Connectors are ordinary NuGet packages behind a small, versioned interface.
See [Author a connector](https://pipelinez.dev/how-to/author-a-connector/).

## Why you might like it

- **It's just files.** Your pipeline reviews like code, diffs like code, and rolls back like code.
- **One tool, no moving parts.** No daemon, no scheduler, no service to keep alive. The same project
  runs on your laptop, in CI, and in a container without changing a line.
- **Nothing runs twice by accident.** Watermarks only advance after every destination has committed,
  and `pz retry` re-runs the failed nodes while reusing the data that already landed.
- **Errors that tell you what to do.** Every failure has a code, names the file, and suggests a next
  step. Validation reports *all* the problems at once, not the first one.
- **Look before you leap.** `pz compile`, `pz plan`, and `pz validate` show you the graph, the
  strategy, and the memory budget without touching a single connection.
- **Your agent can drive it.** `pz mcp` exposes the whole thing as an MCP server.

## Commands

| Verb | Does |
|---|---|
| `pz init <name>` | Scaffold a new runnable project |
| `pz run [name]` | Run a flow, or `--all` for everything |
| `pz test` | Run only the data-quality checks |
| `pz retry` | Re-run just what didn't succeed last time |
| `pz plan` | Show the execution strategy and memory budget |
| `pz validate [--connect]` | Check config and SQL; with `--connect`, probe live connections too |
| `pz compile` | Render pipelines and build the DAG, without running it |
| `pz ls` | List every node in dependency order |
| `pz connectors` | List installed connectors and what they can do |
| `pz restore` | Resolve third-party connector packages |
| `pz mcp` | Serve the project to an AI agent over MCP |

## Documentation

Start at [pipelinez.dev](https://pipelinez.dev/docs/). Worth a look:

- [Quickstart](https://pipelinez.dev/quickstart/): the five-minute version, with real output
- [Architecture overview](https://pipelinez.dev/concepts/architecture-overview/): the full design and why
- [Performance](https://pipelinez.dev/performance/): every benchmark, and how to re-run it
- [Event contract](https://pipelinez.dev/events/): the `--log-format json` NDJSON stream, field by field
- [Author a connector](https://pipelinez.dev/how-to/author-a-connector/): build your own

## Status

Pre-release (**v0.x**). The CLI, engine, eight connectors, and the MCP server are built and tested,
with around 3,000 tests gating every pull request. Expect breaking changes between v0.x minors;
The [versioning policy](https://pipelinez.dev/versioning/) spells out what v0.x promises and what `v1.0.0` will
freeze.

## License

MIT. See [LICENSE](LICENSE).
