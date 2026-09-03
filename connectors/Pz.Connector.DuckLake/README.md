# `ducklake` connector

DuckLake lakehouse source + sink for PipelineZ (`pz`), registered under the connector name
`ducklake`. **DuckDB's own `ducklake` extension is the entire data plane**: the engine's session
attaches the lake once per connection and every read/write is a plain SQL statement against that
alias, over the [native scan/copy data-plane tier](https://pipelinez.dev/concepts/data-plane/), never
the universal Arrow-stream tier. The connector ships **zero drivers** — it is pure SQL-fragment
generation, exactly like the sqlite and duckdb connectors before it, over a metadata *catalog* (one
of five backends) and a separate *data path* (local directory or object store) where DuckLake writes
its Parquet files.

Source: `connectors/Pz.Connector.DuckLake/`. Contract test suite:
[`tests/Pz.Connector.DuckLake.Tests`](../../tests/Pz.Connector.DuckLake.Tests): sqlgen, catalog
matrix, connector, planner, and secret-redaction suites run offline; end-to-end suites cover the
duckdb and sqlite file catalogs and a quack-served catalog (an in-process quack test server) with no
Docker, plus a postgres catalog suite that skips cleanly without Docker
(`Xunit.SkippableFact`/`DockerFacts`).

This page is the connector's own reference. For the *why* behind the ABI it implements, see
[Connectors](https://pipelinez.dev/concepts/connectors/); for the authoring surface (`connections.yml`,
`source()`/`sink()`), see [Project structure](https://pipelinez.dev/concepts/project-structure/).

## Capabilities

Declared in `DuckLakeConnector.Capabilities`:

| Flag | What it means here |
|---|---|
| `NativeScan` | every read is a scan fragment over the connection's attach alias |
| `NativeCopy` | every write is native DuckDB SQL against the same attach alias — `INSERT`/`CREATE OR REPLACE TABLE`/`MERGE INTO` |
| `ReplaceWrites` | the sink supports `strategy: replace` |
| `Merge` | the sink supports `strategy: merge` (DuckDB's own `MERGE INTO`, matched on declared key columns) |
| `Transactional` | each generated statement commits as one DuckLake snapshot |
| `BoundedWindow` | `cursor <= upper` is applied alongside `cursor > lower` — required for the windowed-incremental trio (`initial`/`max_window`/`until`) |
| `InclusiveWatermarkBound` | the connector honors an inclusive lower-bound watermark comparison (`>=` instead of `>`) when the engine asks for one |

Plus the `INativeOnlySource`/`INativeOnlySink` marker interfaces: `engine.force_universal` fails at
**plan time** with `PZ0312` on either direction — there is no universal-tier fallback to fall back
to.

**Not declared:** `ColumnPruning`/`PredicatePushdown` (those drive universal-tier `ReadHints`;
pruning happens anyway, inside the scan fragment itself), `PartitionedRead`, `ChangeCapture`,
`GatedOperations`/`SyncState`/`PathTemplating`/`TextLengthStats`.

## Connection (`connections.yml`)

Every connection names a **catalog** (metadata backend, `catalog:` — defaults to `duckdb` when
omitted) and, for every catalog but a bare local `duckdb` one, a **`data_path`** (where DuckLake
writes Parquet). `ConnectionConfigSchema` rejects any key outside the matrix below
(`additionalProperties: false`); `DuckLakeCatalog.Validate` then enforces which keys each catalog
requires and which belong to a different catalog, aggregate — every stray or missing key comes back
as one error naming the catalog it belongs to, so a whole block is fixed in one pass.

### `duckdb` — a DuckDB file catalog (the default)

```yaml
lake:
  connector: ducklake
  catalog: duckdb        # optional — this is the default
  path: ./data/lake.ducklake   # required — the catalog file
  data_path: ./data/lake/      # optional for this catalog — omitted, DuckLake picks its own default
```

Requires `path`. Forbids every `postgres`/`quack` key (`host`, `port`, `database`, `user`,
`password`, `uri`, `token`).

### `sqlite` — a SQLite file catalog

```yaml
lake:
  connector: ducklake
  catalog: sqlite
  path: ./data/lake.sqlite    # required — the catalog file
  data_path: ./data/lake/     # required for this catalog
```

Requires `path` and `data_path`. Forbids every `postgres`/`quack` key.

### `postgres` — a Postgres-backed catalog, secret-indirected

```yaml
lake:
  connector: ducklake
  catalog: postgres
  host: pg.internal
  port: 5432                  # optional — defaults to 5432
  database: lake_catalog      # required
  user: pz                    # optional
  password: ${LAKE_PG_PASSWORD}   # optional
  data_path: ./data/lake/     # required for this catalog
```

Requires `host`, `database`, `data_path`. Forbids `path` and every `quack` key (`uri`, `token`).
Credentials never ride the attach string: they build a `type postgres` DuckDB secret first, which a
`type ducklake` secret (empty `metadata_path`, its `metadata_parameters` pointing at that secret)
references by name — a failed attach therefore echoes only the lake's data path, never a
host/user/password.

### `quack` — a remote DuckDB catalog server reached over Quack

```yaml
lake:
  connector: ducklake
  catalog: quack
  uri: "quack:quack.internal:9494"   # required — quack:host, quack:host:port, or quack://host[:port]
  token: ${LAKE_QUACK_TOKEN}         # required
  data_path: ./data/lake/            # required for this catalog
```

Requires `uri`, `token`, `data_path`. All three spellings — `quack:host`, `quack:host:port`, and
`quack://host[:port]` — are accepted and normalized to one canonical `quack:host:port` form (default
port 9494) before it lands in either the attach string or the secret's scope, so every spelling
attaches identically. Forbids `path` and every `postgres` key. The token rides a `type quack` secret
scoped to that canonical URI, never the attach string.

### `motherduck` — a MotherDuck-hosted database

```yaml
lake:
  connector: ducklake
  catalog: motherduck
  database: my_md_db          # required
  token: ${MOTHERDUCK_TOKEN}  # required
  data_path: ./data/lake/     # required for this catalog
```

Requires `database`, `token`, `data_path`. Forbids `path`, `uri`, and every `postgres` key except
`database` (which motherduck reuses for its own database name). The token is set as a session
`motherduck_token` variable, never the attach string; the attach targets
`ducklake:md:__ducklake_metadata_<database>`.

### Optional: S3-compatible storage credentials

```yaml
lake:
  connector: ducklake
  catalog: duckdb
  path: ./data/lake.ducklake
  data_path: "s3://my-bucket/lake/"   # object-store data_path — required to use storage_* keys
  storage_key_id: ${LAKE_S3_KEY}
  storage_secret_key: ${LAKE_S3_SECRET}
  storage_region: us-east-1           # optional — defaults to us-east-1
  storage_endpoint: minio.internal:9000  # optional — for an S3-compatible endpoint
  storage_url_style: path             # optional — "vhost" (default) or "path"
  storage_use_ssl: true                # optional — defaults to true
```

`storage_key_id` and `storage_secret_key` must be declared together, and only when `data_path` is an
object-store URL (a value containing `://`) — declaring either alone, or declaring any `storage_*`
key without both credentials, is a validation error; so is declaring `storage_key_id`/
`storage_secret_key` against a local `data_path`. When present, they build a `type s3` DuckDB secret
**scoped to the exact `data_path`**, so the credentials apply only to this lake's files and nothing
else in the session — defaults match the `s3` connector's own.

### Entities and paths

The **entity is `table` (the lake's `main` schema) or `schema.table`**, named the way DuckLake names
it — no separate `schema:`/`table:` options (PZ0348). A relative `path`/`data_path` resolves against
the **project directory** (the same `base_dir` mechanism localfiles/sqlite/duckdb use, injected
internally by the host — never write `base_dir` yourself); an absolute path passes through. Neither
`path` nor a local `data_path` may resolve inside the project's `.pz/` directory (the run's own
staging/state area) — refused at validate time whether or not `base_dir` has been injected yet; an
object-store `data_path` is exempt from that check by construction.

Under `pz mcp`, a `path:` or `data_path:` that resolves outside the project directory is refused
with `PZ0606`, exactly like localfiles/sqlite/duckdb — the agent surface operates only on files
inside the project. A `data_path` naming an object store (any value containing `://`) is skipped by
that guard: it is never a project-relative path to begin with.

## Reading data

An entity in `entities:` (or the same options at a `source()` call site) names the table and any
read options:

```yaml
lake:
  connector: ducklake
  path: ./data/lake.ducklake
  entities:
    events:
      read:
        columns: { id: bigint, updated_at: timestamp, amount: double }
        sync: { mode: incremental, cursor: updated_at }
    snapshot_events:
      read:
        columns: { id: bigint, updated_at: timestamp }
        version: 42          # or timestamp: "2026-08-15 00:00:00" — never both
```

Every read is a scan of the qualified table, time-travelled and filtered inline, wrapped in a
subquery only when there is something to add —

```sql
(select "id", "updated_at" from pz_ducklake_lake_a1b2c3d4."events" at (version => 42)
  where "updated_at" > '2026-08-01 10:00:00' and "updated_at" <= '2026-08-15 00:00:00')
```

- A declared `columns:` contract **prunes the read** — only declared columns are projected.
  Contract-less reads take the table as DuckLake declares it, but also mean `pz validate --connect`
  cannot probe a schema for that dataset (there is no offline driver to ask — the contract *is* the
  schema).
- The plain incremental watermark **is pushed into the fragment** (the database-source rule); the
  windowed pair (`initial`/`max_window`/`until`) is MUST-apply.
- **Time travel**: a dataset may declare `version:` (a non-negative snapshot id) or `timestamp:` (the
  latest snapshot at or before an instant — a string DuckDB's own parser validates, or a
  `DateTime`/`DateTimeOffset` reachable through the Scriban kwarg surface, rendered invariantly
  regardless of host culture), never both — declaring both fails at plan time.

## Writing data

One read-write attach per connection, shared by every read and write against that connection —
DuckLake's own catalog cannot be attached twice in one session (a unique-file-handle conflict on the
metadata database for the duckdb/sqlite catalogs), so there is no read-only/read-write alias split.
Then:

- `strategy: append` — `create table if not exists … as select * from {{source}} limit 0;` +
  `insert into … select * from {{source}};` (first run needs no pre-created table). At-least-once:
  an incremental source feeding an append sink still requires `write: { duplicates: accept }`
  (PZ0214).
- `strategy: replace` — `create or replace table … as select * from {{source}}`. Runs as one
  statement.
- `strategy: merge` — `create table if not exists … as select * from {{source}} limit 0;` +
  `merge into … as t using (select s.* from {{source}} as s qualify row_number() over (partition by
  <keys>) = 1) as s on <keys match> when matched then update when not matched then insert;`. The
  staged side is keyed unique first because DuckDB's MERGE matches every source row independently
  against the pre-statement target, so duplicates of a key the target lacks would all insert; one
  connector-determined survivor per key is the sink contract, and the engine warns (PZ0522) with
  counts when a batch carried duplicates. Requires at least one declared key column (refused at
  compile time otherwise).

Each generated statement commits as **one DuckLake snapshot** — there is no cross-statement
transaction wrapping multiple sink outputs together. The connector does not create schemas: a
`schema.table` entity's schema must already exist inside the lake before a read or write against it.

## `pz validate --connect` behaviour

Zero drivers, so the check per catalog is necessarily shallow — credentials are exercised only by
the first run's attach:

| Catalog | Check |
|---|---|
| `duckdb` | the `path` file, if it exists, must carry the DuckDB header magic (`DUCK` at byte offset 8); a missing file passes with a "will be created on first write" note; a missing parent directory fails |
| `sqlite` | the `path` file, if it exists, must carry the 16-byte SQLite header magic (`SQLite format 3\0`); same missing-file/missing-directory behaviour |
| `postgres` | TCP reachability to `host:port` only (a 5-second timeout); credentials are verified at run time |
| `quack` | TCP reachability to the parsed `uri` host/port only; credentials are verified at run time |
| `motherduck` | **not checked** — "not checked: motherduck has no offline probe; the first run authenticates" |

The `--connect` schema precheck works only for datasets with a declared `columns:` contract (the
contract IS the schema); contract-less datasets get a clear refusal. Plain `pz validate`, `pz run`,
and the `on_source_drift` gate (which baselines from the staged DESCRIBE) are unaffected.

## Behaviours to know

- **One read-write attach per connection.** Every read and write against a connection shares the
  same alias, exactly as the duckdb connector does, because DuckLake's catalog backends do not
  support attaching the same target twice in one session.
- **A read refuses a missing catalog file, for the two file-backed catalogs.** The shared alias is
  read-write (writes must be able to create the catalog), so an unguarded attach of a `path` that
  does not exist would otherwise create an empty catalog and "succeed" at reading zero rows —
  indistinguishable from an empty table and a likely `path` typo. The connector checks the file
  exists before returning a native scan for the `duckdb`/`sqlite` catalogs and refuses at plan time
  (`PZ0353`) if it does not; a server catalog (postgres/quack/motherduck) has no local file to check
  and is left to the attach itself. The refusal applies to nodes the run executes: `pz run <writer>`
  plans a same-project reader of the not-yet-written catalog as refused-and-deferred and runs.
- **Credentials never ride the attach string.** Postgres credentials build a `type postgres` DuckDB
  secret referenced from a `type ducklake` secret whose `metadata_path` is empty by construction; the
  quack token rides a `type quack` secret scoped to the canonical `quack:host:port` URI (every
  accepted spelling normalizes to this one form, so the secret's scope always matches the attach);
  the MotherDuck token is a session
  `motherduck_token` setting, not part of any attach; S3-compatible storage credentials build a
  `type s3` secret scoped to the exact `data_path`. A failed attach therefore echoes only a path, a
  URI, or a database name — never a credential.
- **`.pz/` paths are refused.** Neither `path` nor a local `data_path` may resolve inside the
  project's `.pz/` directory — attaching a run's own staging/state area to itself has no legitimate
  use, and the check runs on the connection as written, whether or not the host has injected
  `base_dir` yet.
- **A lake whose catalog file is shared by two connections conflicts.** Two separate `ducklake`
  connections (two `connections.yml` blocks) pointing the same `path` at the same duckdb/sqlite
  catalog file each get their own attach alias, and DuckDB refuses to attach one file twice in one
  session (a unique-file-handle conflict) — use one connection for both the reads and the writes of
  a given lake.
- **Storage credentials require an object-store `data_path`.** `storage_key_id`/
  `storage_secret_key` are refused unless `data_path` is a URL (contains `://`); declaring only one
  of the pair, or declaring any other `storage_*` key without both, is refused the same way.
- **Setup statements run once per run.** The engine issues each distinct setup statement once per
  run and every node that needs it shares that execution; a node retry re-issues a statement that
  failed, which the statements tolerate (extension install/load are no-ops on repeat,
  `create or replace secret` is last-wins, `attach if not exists` skips an existing alias). A
  `motherduck` catalog depends on this: its extension accepts a token only before its first attach.
- **A shared remote catalog wants an object-store `data_path`.** DuckLake's data files are read and
  written by the client, not by the catalog server — so when the catalog is shared (`postgres`,
  `quack`, or `motherduck`), `data_path` should normally be an object store too. A project-relative
  `data_path` is valid for a single client, but two machines attaching the same remote catalog would
  each resolve it against their own project directory, landing writes in two different places.
