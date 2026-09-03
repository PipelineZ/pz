# `quack` connector

Remote DuckDB server source + sink for PipelineZ (`pz`), registered under the connector name
`quack`. **A quack server reached over the Quack protocol is the entire data plane**: the engine's
session attaches the server once per connection through the `quack` extension, and every read/write
is a plain SQL statement against that alias, executed by the server, over the
[native scan/copy data-plane tier](https://pipelinez.dev/concepts/data-plane/), never the universal
Arrow-stream tier. The connector ships **zero drivers** — it is pure SQL-fragment generation, exactly
like the sqlite/duckdb/ducklake connectors before it.

Source: `connectors/Pz.Connector.Quack/`. Contract test suite:
[`tests/Pz.Connector.Quack.Tests`](../../tests/Pz.Connector.Quack.Tests): sqlgen, connector, planner,
and secret-redaction suites run offline; the end-to-end suite runs against an in-process quack test
server (`QuackTestServer`), no Docker, gated only on `PZ_TESTS_OFFLINE`.

This page is the connector's own reference. For the *why* behind the ABI it implements, see
[Connectors](https://pipelinez.dev/concepts/connectors/); for the authoring surface (`connections.yml`,
`source()`/`sink()`), see [Project structure](https://pipelinez.dev/concepts/project-structure/).

## Capabilities

Declared in `QuackConnector.Capabilities`:

| Flag | What it means here |
|---|---|
| `NativeScan` | every read is a scan fragment over the connection's attach alias |
| `NativeCopy` | every write is native DuckDB SQL against the same attach alias — `INSERT`/`CREATE OR REPLACE TABLE`/merge-by-replace |
| `ReplaceWrites` | the sink supports `strategy: replace` |
| `Merge` | the sink supports `strategy: merge` — merge-by-replace, see below |
| `BoundedWindow` | `cursor <= upper` is applied alongside `cursor > lower` — required for the windowed-incremental trio (`initial`/`max_window`/`until`) |
| `InclusiveWatermarkBound` | the connector honors an inclusive lower-bound watermark comparison (`>=` instead of `>`) when the engine asks for one |

Plus the `INativeOnlySource`/`INativeOnlySink` marker interfaces: `engine.force_universal` fails at
**plan time** with `PZ0312` on either direction — there is no universal-tier fallback to fall back to.

**Not declared:** `Transactional` (commit semantics belong to the remote server, not this
connection), `ColumnPruning`/`PredicatePushdown` (those drive universal-tier `ReadHints`; pruning
happens anyway, inside the scan fragment itself), `PartitionedRead`, `ChangeCapture`,
`GatedOperations`/`SyncState`/`PathTemplating`/`TextLengthStats`.

## Connection (`connections.yml`)

```yaml
wh:
  connector: quack
  uri: quack:host:port          # or quack:host, or quack://host[:port]
  token: ${QUACK_TOKEN}
  entities:
    events:
      read:
        sync: { mode: incremental, cursor: updated_at }
```

`uri` and `token` are the only user-facing keys (`ConnectionConfigSchema` rejects anything else,
`additionalProperties: false`). All three uri spellings — `quack:host`, `quack:host:port`, and
`quack://host[:port]` — are accepted and normalized to one canonical `quack:host:port` form (default
port `9494`) before it lands in either the attach string or the secret's scope, so every spelling
attaches identically. `ValidateAsync` refuses offline, aggregate: a `uri` that doesn't parse, and a
`token` shorter than four characters (the server itself refuses shorter tokens, so this is caught
before the first run rather than echoed back from a remote failure).

The **entity is `table` or `schema.table`**, named the way the remote server names it — no separate
`schema:`/`table:` options (PZ0348).

**Not path-scoped.** Unlike localfiles/sqlite/duckdb/ducklake, a quack connection names no local
file or directory — there is no `base_dir` injection and no `.pz/` guard to apply, because nothing
about this connector resolves against the project directory.

## Reading data

An entity in `entities:` (or the same options at a `source()` call site) names the table and any
read options:

```yaml
wh:
  connector: quack
  uri: quack:host:port
  token: ${QUACK_TOKEN}
  entities:
    events:
      read:
        columns: { id: bigint, updated_at: timestamp, amount: double }
        sync: { mode: incremental, cursor: updated_at }
```

Every read is a scan of the qualified table, filtered inline, wrapped in a subquery only when there
is something to add —

```sql
(select "id", "updated_at" from pz_quack_wh_a1b2c3d4."events"
  where "updated_at" > '2026-08-01 10:00:00' and "updated_at" <= '2026-08-15 00:00:00')
```

- A declared `columns:` contract **prunes the read** — only declared columns are projected.
  Contract-less reads take the table as the server declares it, but also mean
  `pz validate --connect` cannot probe a schema for that dataset (there is no offline driver to ask
  — the contract *is* the schema).
- The plain incremental watermark **is pushed into the fragment** (the database-source rule); the
  windowed pair (`initial`/`max_window`/`until`) is MUST-apply.

## Writing data

One read-write attach per connection, shared by every read and write against that connection; the
engine issues each setup statement once per run, and a node retry re-issues one that failed (extension
install/load are no-ops on repeat, `create or replace secret` is last-wins, `attach if not exists`
skips an existing alias). Then:

- `strategy: append` — `create table if not exists … as select * from {{source}} limit 0;` +
  `insert into … select * from {{source}};` (first run needs no pre-created table). At-least-once:
  an incremental source feeding an append sink still requires `write: { duplicates: accept }`
  (PZ0214).
- `strategy: replace` — `create or replace table … as select * from {{source}}`. Runs as one
  statement, mechanism `quack create-or-replace`.
- `strategy: merge` — **merge-by-replace**, mechanism `quack merge-by-replace`. A quack-attached
  table accepts only bulk `CREATE TABLE AS`/`INSERT` from the wire protocol — no row-level
  `UPDATE`/`DELETE`/`MERGE` — so a merge pulls the target through the client, computes the merged set
  locally (source rows win on key match; unmatched target rows are kept), and rewrites the whole
  remote table in one `create or replace table`. Requires at least one declared key column (refused
  at compile time otherwise). That rewrite is the full blast radius of merge-by-replace, every time:
  - Primary keys, `NOT NULL`/`DEFAULT` constraints and indexes on the target do not survive it — the
    replacement table has none of them.
  - The target's column order follows the source batch's order, not whatever order it had before.
  - A matched row is replaced wholesale by the source row, not column-patched: **a column the source
    batch omits comes back NULL on matched rows**, so keep the pipeline's column set (and order)
    stable across runs.
  - Duplicate keys within one source batch collapse to one connector-determined survivor before the
    rewrite runs.
  - The `create or replace table` itself is one statement executed by the server — whether it is
    atomic is the server's guarantee, not pz's. A failed rewrite can leave the target missing or
    partial until the next run, which recomputes the same result: the merge is idempotent.
  - An empty source batch still runs the rewrite: with zero source rows nothing ever matches, so
    every target row survives untouched (the table is recreated, not skipped). Cost grows with the
    target table's size, since the whole table crosses the wire on every merge.

The connector does not create schemas: a `schema.table` entity's schema must already exist on the
server before a read or write against it.

## `pz validate --connect` behaviour

Zero drivers, so the check is necessarily shallow — credentials are exercised only by the first
run's attach: a TCP reachability probe to the parsed `uri`'s host/port (a 5-second timeout).
Credentials are verified at run time, and a failed attach is a redacted `PZ0311` — the token never
appears in the error, in a `plan.json` `Reason`, or in any `NativeSetup` failure message.

The `--connect` schema precheck works only for datasets with a declared `columns:` contract (the
contract IS the schema); contract-less datasets get a clear refusal. Plain `pz validate`, `pz run`,
and the `on_source_drift` gate (which baselines from the staged DESCRIBE) are unaffected.

## Behaviours to know

- **The token rides a scoped secret, never the attach string.** `SetupStatements` builds a
  `type quack` DuckDB secret (`token '<token>', scope '<canonical uri>'`) before the attach; a
  failed `ATTACH` therefore echoes only the canonical `quack:host:port` uri, never a credential.
  The secret's scope and the attach target are built from the same canonicalized uri deliberately —
  a mismatch would make the scoped secret silently fail to match.
- **`pz validate --connect` is a TCP reachability probe, nothing more.** It proves the host and port
  accept a connection; it does not exercise the token. Credentials are verified at first run, and a
  bad token fails as a redacted `PZ0311` (the message never contains the token, whether the token was
  wrong or the carrier statement itself was malformed).
- **TLS is the reverse proxy's job.** The connector has no TLS configuration of its own — put a
  reverse proxy in front of the quack server for anything that isn't a trusted loopback/private
  network.
- **Merge is merge-by-replace.** The quack extension exposes no row-level DML on attached tables, so
  a merge pulls the target through the client, computes the merged set locally (source rows win on
  key match; unmatched target rows are kept), and rewrites the whole remote table in one
  `create or replace table`. That rewrite is the full blast radius, every time: primary keys,
  `NOT NULL`/`DEFAULT` constraints and indexes on the target do not survive it; the target's column
  order follows the source batch's order; a matched row is replaced wholesale, so a column the source
  batch omits becomes NULL on matched rows — keep the pipeline's column set stable; duplicate keys
  within one source batch collapse to one connector-determined survivor; and whether the rewrite
  itself is atomic is the server's guarantee, not pz's — a failed rewrite can leave the target
  missing or partial until the next run, which recomputes the same result (the merge is idempotent).
  Cost grows with the target table's size.
- **Not path-scoped.** No `base_dir`, no `.pz` guard — a quack connection names a remote server, not
  a location inside the project directory, so neither the `base_dir` injection nor the `.pz/`
  containment check that localfiles/sqlite/duckdb/ducklake apply has anything to attach to here.
- **One read-write attach per connection.** Every read and write against a connection shares the
  same alias; the connector does not split reads and writes onto separate aliases.
