# `sqlite` connector

SQLite source + sink for PipelineZ (`pz`), registered under the connector name `sqlite`.
**DuckDB's own `sqlite` extension is the entire data plane**, on both the read and write side, and
the connector ships with **zero .NET SQLite driver dependency** — it is pure SQL-fragment generation
over the [native scan/copy data-plane tier](https://pipelinez.dev/concepts/data-plane/), never the universal
Arrow-stream tier. It is the second connector on the native-path-only pattern the MySQL connector
proved out, and the simpler of the two: there is no server, no credential, and no second SQL dialect
(everything, scan and copy alike, is parsed by DuckDB itself).

Source: `connectors/Pz.Connector.Sqlite/`. Contract test suite:
[`tests/Pz.Connector.Sqlite.Tests`](../../tests/Pz.Connector.Sqlite.Tests) — **no Docker needed at
all**: sqlite is a file, so even the end-to-end suite runs against temp files (it skips only under
`PZ_TESTS_OFFLINE=1`, because `install sqlite` needs DuckDB's extension repo on first run).

This page is the connector's own reference. For the *why* behind the ABI it implements, see
[Connectors](https://pipelinez.dev/concepts/connectors/); for the authoring surface (`connections.yml`,
`source()`/`sink()`), see [Project structure](https://pipelinez.dev/concepts/project-structure/).

## Capabilities

Declared in `SqliteConnector.Capabilities`:

| Flag | What it means here |
|---|---|
| `NativeScan` | every read is a `sqlite_scan('<path>', '<table>')` DuckDB native scan — self-contained, no attach, no alias |
| `NativeCopy` | every write is native DuckDB SQL against one rw attach of the database file — `INSERT`/`CREATE OR REPLACE` |
| `ReplaceWrites` | the sink supports `strategy: replace` |
| `BoundedWindow` | `cursor <= upper` is applied alongside `cursor > lower` — required for the windowed-incremental trio (`initial`/`max_window`/`until`) |
| `InclusiveWatermarkBound` | the connector honors an inclusive lower-bound watermark comparison (`>=` instead of `>`) when the engine asks for one |

Plus the `INativeOnlySource`/`INativeOnlySink` marker interfaces: `engine.force_universal` fails at
**plan time** with `PZ0312` on either direction — there is no universal-tier fallback to fall back to.

**Not declared:** `ColumnPruning`/`PredicatePushdown` (those drive universal-tier `ReadHints`;
pruning happens anyway, inside the scan fragment itself), `PartitionedRead`, `Merge` (the DuckDB
`sqlite` catalog has no upsert — `strategy: merge` is refused at plan time, `PZ0324`),
`Transactional` (the sink's `replace` is drop+create on the sqlite side), `ChangeCapture`,
`GatedOperations`/`SyncState`/`PathTemplating`/`TextLengthStats`.

## Connection (`connections.yml`)

```yaml
appdb:
  connector: sqlite
  connection:
    path: ./data/app.db   # required — the database file
  entities:
    events:
      read:
        sync: { mode: incremental, cursor: updated_at }
```

`path` is the only key (`ConnectionConfigSchema` rejects anything else). A relative path resolves
against the **project directory** (the same `base_dir` mechanism localfiles uses), an absolute path
passes through. The **entity is the table**, named the way sqlite names it — no `schema:`/`table:`
options (PZ0348; sqlite has no schemas).

Under `pz mcp`, a `path:` that resolves outside the project directory is refused with `PZ0606`,
exactly like a localfiles root — the agent surface operates only on files inside the project.

## Reading data

Every read is one native scan: `sqlite_scan('<path>', '<table>')`, wrapped in a subquery only when
there is something to add —

```sql
(select "id", "name" from sqlite_scan('/proj/data/app.db', 'events')
  where "updated_at" > '2026-08-01 10:00:00' and "updated_at" <= '2026-08-15 00:00:00')
```

- A declared `columns:` contract **prunes the read** — only declared columns are projected (the
  csv/json/mysql rule). Contract-less reads take the table as sqlite declares it.
- The plain incremental watermark **is pushed into the fragment** (the database-source rule), and
  DuckDB's sqlite scanner pushes projection and filters into the file scan.
- **No `query:` option**, deliberately: the passthrough mechanism it would need
  (`sqlite_query(...)`) fails unusably in the bundled DuckDB, and there is no server-side execution
  to preserve — put the transformation in your pipeline SQL, which runs in the same process anyway.

**Type mapping (read side):** the scanner maps by *declared* sqlite column type — a database whose
schema says `DATE`/`DATETIME`/`TIMESTAMP` surfaces real DuckDB `DATE`/`TIMESTAMP` columns;
`BOOLEAN` surfaces as `BIGINT` (0/1) and `NUMERIC(p,s)` as `DOUBLE`. Text-typed timestamp columns
surface as `VARCHAR`; a `VARCHAR` cursor still windows correctly as long as the stored text is
sqlite's conventional space-separated ISO form (`2026-08-19 12:30:00`) — that comparison is lexical,
and the watermark literal is rendered in exactly that form.

## Writing data

One read-write attach per sink connection (`attach if not exists '<path>' as pz_sqlite_snk_… (type
sqlite)`), created automatically if the file does not exist. Then:

- `strategy: append` — `create table if not exists … as select * from {{source}} limit 0;` +
  `insert into … select * from {{source}};` (first run needs no pre-created table). At-least-once:
  an incremental source feeding an append sink still requires `write: { duplicates: accept }`
  (PZ0214).
- `strategy: replace` — `create or replace table … as select * from {{source}}`. Effectively-once
  from the pipeline's perspective, but the sqlite-side swap is drop+create, not atomic — a reader
  concurrent with the copy can observe the table absent.
- `strategy: merge` — not supported (`PZ0324`).

**Type flattening (write side):** sqlite has no DATE/TIMESTAMP/BOOLEAN/DECIMAL storage classes, and
the extension stores them as TEXT/TEXT/BIGINT/TEXT. A table *created by a pz sink* therefore reads
back with those flattened types — the **values** round-trip losslessly (ISO date strings, exact
decimal digits, 0/1 booleans); only the declared type is flattened. Pre-existing tables keep
whatever their schema declares.

pz makes no cross-process locking claims: concurrent *external* writers to the same file contend on
sqlite's own file locking. Within a run there is no self-contention: pz drives one DuckDB
connection per run, serialized by a semaphore, so a run never races itself.

## Control plane

- `pz validate --connect` runs a **real local check**: an existing file must start with the 16-byte
  SQLite header magic (`SQLite format 3\0`); a missing file passes with an explicit "will be created
  on first write" note; a missing parent directory fails (sqlite will not create directories).
- The `--connect` schema precheck works only for datasets with a declared `columns:` contract (the
  contract IS the schema); contract-less datasets get a clear refusal. Plain `pz validate`,
  `pz run`, and the `on_source_drift` gate (which baselines from the staged DESCRIBE) are
  unaffected.
