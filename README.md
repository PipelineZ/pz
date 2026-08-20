# [PipelineZ (`pz`)](https://pipelinez.dev)

**Write SQL. Get a data pipeline. Run it anywhere.**

[![CI](https://github.com/coccor/pz/actions/workflows/ci.yml/badge.svg)](https://github.com/coccor/pz/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

`pz` is a lightweight, developer-first engine for batch ETL/ELT, powered by
[DuckDB](https://duckdb.org). You describe your pipeline in plain SQL files.

## 🚀 Try it in five commands

```console
$ dotnet tool install --global pz
$ pz init demo --template sample
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
-- pipelines/orders_curated.sql
INSERT INTO {{ sink('lake', 'orders_curated') }}
SELECT
    id,
    amount,
    customer_id
FROM {{ source('crm', 'orders') }}
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
  entities:
    orders_curated:
      write: { format: parquet, strategy: replace }
```

Every read and write option can live in the YAML, as they do above, or right at the
`source()`/`sink()` call that uses it. Your choice, but never both, so there's no precedence puzzle
to solve. That's the whole project. No `models/`, no profiles directory, no adapter to install.

## ⚡ Speed

DuckDB does the work, and often your data never even enters .NET: when both sides of an edge can
speak SQL, `pz` hands DuckDB a fragment and steps out of the way. When they can't, data streams
through .NET as zero-copy Arrow batches, never row by row.

**A million rows, SQL Server to SQL Server, end to end through `pz run`: 10.5 s — 1.5× faster than
raw DuckDB doing the same move by itself.** The sink measures your text columns, sizes the target
table to fit, and bulk-copies into it.

| Moving 1,000,000 rows | `pz` | |
|---|---|---|
| SQL Server → SQL Server | **10.5 s** | 🏆 1.5× faster than raw DuckDB (16.2 s) |
| CSV → Parquet on Azure Blob | **1.0 s** | ~0.5 s of that is process startup |
| CSV → Parquet on S3 | **1.4 s** | ~0.9 s of that is process startup |
| Local CSV → CSV | **1.9 s** | |

Startup is a flat cost that gets smaller as your data gets bigger, not a per-row tax. Sustained
throughput on the same machine: **~806k rows/sec** into DuckDB, **~960k rows/sec** back out.

> Every number here is one laptop (i7-8665U, 4 cores, 15 GiB), measured, not tuned. Re-run them on
> your own hardware with the harnesses documented in [Performance](https://pipelinez.dev/performance/), which also
> covers the moves where raw DuckDB still wins, and why.

And you know the memory cost before you run: `pz plan` prints a static budget computed from your
config alone.

## Commands

| Verb | Does |
|---|---|
| `pz init <name>` | Scaffold a new project; `--template <id>` picks one of five, `--list-templates` shows them |
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
