# `motherduck` connector

MotherDuck database source + sink for PipelineZ (`pz`), registered under the connector name
`motherduck`. **DuckDB's own `motherduck` extension is the entire data plane**: the engine's session
attaches the database once per connection and every read/write is a plain SQL statement against that
attach, over the [native scan/copy data-plane tier](https://pipelinez.dev/concepts/data-plane/), never
the universal Arrow-stream tier. The connector ships **zero drivers** — it is pure SQL-fragment
generation, exactly like the duckdb/ducklake/quack connectors before it.

Source: `connectors/Pz.Connector.MotherDuck/`. Contract test suite:
[`tests/Pz.Connector.MotherDuck.Tests`](../../tests/Pz.Connector.MotherDuck.Tests): sqlgen,
connector, planner, and secret-redaction suites run offline; `MotherDuckLiveTests` is the only proof
of the documentation-derived behaviors (alias-less attach, the session token setting, real
`MERGE INTO`) and runs only when `PZ_MOTHERDUCK_TOKEN`/`PZ_MOTHERDUCK_DATABASE` are set — never in
CI.

This page is the connector's own reference. For the *why* behind the ABI it implements, see
[Connectors](https://pipelinez.dev/concepts/connectors/); for the authoring surface (`connections.yml`,
`source()`/`sink()`), see [Project structure](https://pipelinez.dev/concepts/project-structure/).

## Capabilities

Declared in `MotherDuckConnector.Capabilities`:

| Flag | What it means here |
|---|---|
| `NativeScan` | every read is a scan fragment over the connection's attach |
| `NativeCopy` | every write is native DuckDB SQL against the same attach — `INSERT`/`CREATE OR REPLACE TABLE`/`MERGE INTO` |
| `ReplaceWrites` | the sink supports `strategy: replace` |
| `Merge` | the sink supports `strategy: merge` — a real `MERGE INTO` executed by MotherDuck, see below |
| `BoundedWindow` | `cursor <= upper` is applied alongside `cursor > lower` — required for the windowed-incremental trio (`initial`/`max_window`/`until`) |
| `InclusiveWatermarkBound` | the connector honors an inclusive lower-bound watermark comparison (`>=` instead of `>`) when the engine asks for one |

Plus the `INativeOnlySource`/`INativeOnlySink` marker interfaces: `engine.force_universal` fails at
**plan time** with `PZ0312` on either direction — there is no universal-tier fallback to fall back to.

**Not declared:** `Transactional` (commit semantics belong to MotherDuck, not this connection),
`ColumnPruning`/`PredicatePushdown` (those drive universal-tier `ReadHints`; pruning happens anyway,
inside the scan fragment itself), `PartitionedRead`, `ChangeCapture`,
`GatedOperations`/`SyncState`/`PathTemplating`/`TextLengthStats`.

## Connection (`connections.yml`)

```yaml
lake:
  connector: motherduck
  database: my_db
  token: ${MOTHERDUCK_TOKEN}
  entities:
    events:
      read:
        sync: { mode: incremental, cursor: updated_at }
```

`database` and `token` are the only user-facing keys — a flat shape, no nesting
(`ConnectionConfigSchema` rejects anything else, `additionalProperties: false`). Both are required;
`ValidateAsync` performs no cross-field checks beyond the schema, since both values are only ever
used as `''`-escaped literals in the statements that carry them.

The **entity is `table` or `schema.table`**, named the way MotherDuck names it — no separate
`schema:`/`table:` options (PZ0348).

**Not path-scoped.** Unlike localfiles/sqlite/duckdb/ducklake, a motherduck connection names no
local file or directory — there is no `base_dir` injection and no `.pz/` guard to apply, because
nothing about this connector resolves against the project directory.

## Reading data

An entity in `entities:` (or the same options at a `source()` call site) names the table and any
read options:

```yaml
lake:
  connector: motherduck
  database: my_db
  token: ${MOTHERDUCK_TOKEN}
  entities:
    events:
      read:
        columns: { id: bigint, updated_at: timestamp, amount: double }
        sync: { mode: incremental, cursor: updated_at }
```

Every read is a scan of the qualified table, filtered inline, wrapped in a subquery only when there
is something to add —

```sql
(select "id", "updated_at" from "my_db"."events"
  where "updated_at" > '2026-08-01 10:00:00' and "updated_at" <= '2026-08-15 00:00:00')
```

- A declared `columns:` contract **prunes the read** — only declared columns are projected.
  Contract-less reads take the table as MotherDuck declares it, but also mean
  `pz validate --connect` cannot probe a schema for that dataset (there is no offline driver to ask
  — the contract *is* the schema).
- The plain incremental watermark **is pushed into the fragment** (the database-source rule); the
  windowed pair (`initial`/`max_window`/`until`) is MUST-apply.

## Writing data

One attach per connection, shared by every read and write against that connection, setup idempotent
under the engine's per-run once-only re-issue rule (`install`/`load` are no-ops on repeat; the
session token setting and the attach are each issued exactly once — see "One MotherDuck token per
run" below). Then:

- `strategy: append` — `create table if not exists … as select * from {{source}} limit 0;` +
  `insert into … select * from {{source}};` (first run needs no pre-created table). At-least-once:
  an incremental source feeding an append sink still requires `write: { duplicates: accept }`
  (PZ0214).
- `strategy: replace` — `create or replace table … as select * from {{source}}`. Runs as one
  statement, mechanism `motherduck create-or-replace`.
- `strategy: merge` — a real `merge into … using {{source}} as s on … when matched then update when
  not matched then insert`, mechanism `motherduck merge`, executed by MotherDuck server-side.
  Requires at least one declared key column, refused at compile time otherwise (PZ0209; the
  connector throws too, as ABI defense-in-depth).

The connector does not create databases or schemas: the database must already exist in the account,
and a `schema.table` entity's schema must already exist on it before the first read or write.

## `pz validate --connect` behaviour

Zero drivers and no offline probe: `CheckConnectionAsync` always reports **"not checked"** — there
is nothing to reach without authenticating, and authenticating is exactly what the first run does.
The `--connect` schema precheck works only for datasets with a declared `columns:` contract (the
contract IS the schema); contract-less datasets get a clear refusal. Plain `pz validate`, `pz run`,
and the `on_source_drift` gate (which baselines from the staged DESCRIBE) are unaffected.

## Behaviours to know

- **The token rides `set motherduck_token`, never the attach string.** The extension's session
  setting is described by the engine without echoing it; a failed `attach` therefore echoes only
  `md:<database>`, never a credential. A wrong token fails the SET as a redacted `PZ0311` — the
  message never contains the token, whether the token was wrong or the carrier statement itself was
  malformed.
- **One MotherDuck token per run.** The extension accepts `set motherduck_token` only before its
  first attach and refuses any later SET — so this connector relies on the engine's per-run
  once-only re-issue rule (`NativeSetupLedger`, keyed by exact statement text): connections sharing
  the same `database` and `token` share one setup and one attach, while a second connection with a
  *different* token issues its own `set motherduck_token` and fails as a redacted `PZ0311`, because
  the extension has already locked in the first connection's token for the run.
- **No alias.** MotherDuck refuses an alias on a database the user owns, so the attach name IS the
  database name — there is no read-only/read-write split. References are
  `"<database>"."schema"."table"` (or `"<database>"."table"` with no schema).
- **`pz validate --connect` performs no probe.** It always reports "not checked"; credentials are
  exercised only by the first run's attach.
- **Merge is a real `MERGE INTO`, executed by MotherDuck.** Matched rows update, unmatched rows
  insert, and an empty source batch leaves the target untouched — there is no pull-rewrite-push round
  trip and no blast radius on the target's constraints or indexes (unlike quack's merge-by-replace).
  **Keep each batch key-unique.** MERGE matches every source row independently against the target as
  it stood *before* the statement ran: duplicates of a key the target already holds all update it —
  which value survives is not defined; duplicates of a key the target lacks are all inserted — the
  sink does not collapse duplicate keys within a batch, and the connector's generated SQL makes no
  promise that it will. Requires at least one declared key column (PZ0209 at compile).
- **The connector does not create databases or schemas.** The database must exist in the account
  before the first read or write, and a `schema.table` entity's schema must exist on it too.
- **Not path-scoped.** No `base_dir`, no `.pz` guard — a motherduck connection names a database in an
  account, not a location inside the project directory, so neither the `base_dir` injection nor the
  `.pz/` containment check that localfiles/sqlite/duckdb/ducklake apply has anything to attach to
  here.
