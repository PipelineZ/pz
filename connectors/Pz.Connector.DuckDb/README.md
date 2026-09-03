# `duckdb` connector

DuckDB-database-file source + sink for PipelineZ (`pz`), registered under the connector name
`duckdb`. **The engine's own DuckDB session is the entire data plane**: every read and write is a
plain SQL statement against one `attach if not exists '<path>' as <alias>` of the file, run over the
[native scan/copy data-plane tier](https://pipelinez.dev/concepts/data-plane/), never the universal
Arrow-stream tier. The connector ships **zero drivers and does no data-plane I/O of its own** — it is
pure SQL-fragment generation, like the sqlite and mysql connectors before it.

Source: `connectors/Pz.Connector.DuckDb/`. Contract test suite:
[`tests/Pz.Connector.DuckDb.Tests`](../../tests/Pz.Connector.DuckDb.Tests) — no Docker, no network:
duckdb is a file, so even the end-to-end suite runs against real temp `.duckdb` files created by the
DuckDB engine that ships with `pz` itself.

This page is the connector's own reference. For the *why* behind the ABI it implements, see
[Connectors](https://pipelinez.dev/concepts/connectors/); for the authoring surface (`connections.yml`,
`source()`/`sink()`), see [Project structure](https://pipelinez.dev/concepts/project-structure/).

## Capabilities

Declared in `DuckDbConnector.Capabilities`:

| Flag | What it means here |
|---|---|
| `NativeScan` | every read is a scan fragment over the connection's attach alias |
| `NativeCopy` | every write is native DuckDB SQL against the same attach alias — `INSERT`/`CREATE OR REPLACE TABLE`/`MERGE INTO` |
| `ReplaceWrites` | the sink supports `strategy: replace` |
| `Merge` | the sink supports `strategy: merge` (DuckDB's own `MERGE INTO`, unlike sqlite/mysql) |
| `Transactional` | each generated statement commits atomically inside the attached file |
| `BoundedWindow` | `cursor <= upper` is applied alongside `cursor > lower` — required for the windowed-incremental trio (`initial`/`max_window`/`until`) |
| `InclusiveWatermarkBound` | the connector honors an inclusive lower-bound watermark comparison (`>=` instead of `>`) when the engine asks for one |

Plus the `INativeOnlySource`/`INativeOnlySink` marker interfaces: `engine.force_universal` fails at
**plan time** with `PZ0312` on either direction — there is no universal-tier fallback to fall back to.

**Not declared:** `ColumnPruning`/`PredicatePushdown` (those drive universal-tier `ReadHints`;
pruning happens anyway, inside the scan fragment itself), `PartitionedRead`, `ChangeCapture`,
`GatedOperations`/`SyncState`/`PathTemplating`/`TextLengthStats`.

## Connection (`connections.yml`)

```yaml
lakedb:
  connector: duckdb
  path: ./data/warehouse.duckdb   # required — the database file
  entities:
    events:
      read:
        sync: { mode: incremental, cursor: updated_at }
```

`path` is the only user-facing key (`ConnectionConfigSchema` rejects anything else). A relative path
resolves against the **project directory** (the same `base_dir` mechanism localfiles and sqlite use,
injected internally by the host — never write `base_dir` yourself); an absolute path passes through.
The **entity is `table` or `schema.table`**, named the way duckdb names it — no separate
`schema:`/`table:` options (PZ0348).

Under `pz mcp`, a `path:` that resolves outside the project directory is refused with `PZ0606`,
exactly like localfiles and sqlite — the agent surface operates only on files inside the project.
The connector's own cross-field check additionally refuses a `path` that resolves inside the
project's `.pz/` directory (the run's own staging/state area) under the plain CLI too, whether or
not `base_dir` has been injected yet.

## Reading data

Every read is a scan of the qualified table, wrapped in a subquery only when there is something to
add —

```sql
(select "id", "updated_at" from pz_duckdb_lakedb_a1b2c3d4."events"
  where "updated_at" > '2026-08-01 10:00:00' and "updated_at" <= '2026-08-15 00:00:00')
```

- A declared `columns:` contract **prunes the read** — only declared columns are projected.
  Contract-less reads take the table as duckdb declares it, but also mean `pz validate --connect`
  cannot probe a schema for that dataset (there is no offline driver to ask — the contract *is* the
  schema).
- The plain incremental watermark **is pushed into the fragment** (the database-source rule); the
  windowed pair (`initial`/`max_window`/`until`) is MUST-apply.

## Writing data

One read-write attach per connection, shared by every read and write against that connection
(`attach if not exists '<path>' as pz_duckdb_<name>_<hash>`); the engine issues each setup
statement once per run, and every node that needs it shares that one execution. Then:

- `strategy: append` — `create table if not exists … as select * from {{source}} limit 0;` +
  `insert into … select * from {{source}};` (first run needs no pre-created table). At-least-once:
  an incremental source feeding an append sink still requires `write: { duplicates: accept }`
  (PZ0214).
- `strategy: replace` — `create or replace table … as select * from {{source}}`. Runs as one
  DuckDB statement.
- `strategy: merge` — `create table if not exists … as select * from {{source}} limit 0;` +
  `merge into … as t using {{source}} as s on <keys match> when matched then update when not
  matched then insert;`. Requires at least one declared key column (refused at compile time
  otherwise).

The connector does not create schemas: a `schema.table` entity's schema must already exist inside
the attached file before a read or write against it.

## Behaviours to know

- **One writer per file.** A DuckDB database file accepts one writer at a time; a `pz run` holds the
  file attached read-write for the run's duration. Another process (a second `pz run`, `duckdb` CLI,
  or anything else) holding the same file open when the attach happens makes the setup statement
  fail (`PZ0311`) — close it first, or point the run at a copy.
- **A read against a missing file is refused, not silently materialized.** The shared attach alias
  is read-write (writes must be able to create the file on first use), so an unguarded attach of a
  path that does not exist would otherwise create an empty database and "succeed" at reading zero
  rows — indistinguishable from an empty table and a likely `path` typo. The connector checks the
  file exists before returning a native scan and refuses at plan time (`PZ0353`) if it does not; run
  a write against that `path` first, or fix the connection. The refusal applies to nodes the run
  executes: when one flow of a project writes the file and another reads it, `pz run <writer>`
  plans the reader as refused-and-deferred and runs, so the project bootstraps without a second
  project; `pz run --all` on the fresh file still refuses, because the reader would execute.
- **Two connections, one file, is a conflict — not two writers.** Two separate `duckdb` connections
  (two `connections.yml` blocks) pointing at the same file fail at execute time with DuckDB's
  "Unique file handle conflict" error, because each connection gets its own attach alias and DuckDB
  will not attach one file twice in one session. Use one connection for both the reads and the
  writes of a given file.
- **Merge does not deduplicate the staged side.** `MERGE INTO`'s `on` clause matches the target
  against the staged relation row by row; if the staged relation itself carries duplicate key
  values, DuckDB does not error the way SQL Server's `MERGE` does — it inserts (or updates from) each
  duplicate in turn, silently producing duplicate rows in the target. Dedupe the pipeline SQL feeding
  a merge output if the source can carry duplicate keys.
- **Transactions are DuckDB's own, one snapshot per statement.** Each generated statement (the
  `create table if not exists` + `insert`/`merge` pair included) runs and commits as DuckDB provides
  transactions for a single connection — there is no cross-statement transaction wrapping multiple
  sink outputs together, and no isolation guarantee beyond what one DuckDB session gives a
  same-process reader mid-write.

## Control plane

- `pz validate --connect` runs a **real local check**: an existing file must carry the DuckDB header
  magic (`DUCK` at byte offset 8); a missing file passes with an explicit "will be created on first
  write" note; a missing parent directory fails (`ATTACH` will not create directories).
- The `--connect` schema precheck works only for datasets with a declared `columns:` contract (the
  contract IS the schema); contract-less datasets get a clear refusal. Plain `pz validate`,
  `pz run`, and the `on_source_drift` gate (which baselines from the staged DESCRIBE) are
  unaffected.
