# `snowflake` connector

Snowflake source + sink for PipelineZ (`pz`), registered under the connector name `snowflake` — one
of the nine builtin connectors (`src/Pz.Cli/BuiltinConnectors.cs`), so no `pz restore` step is
needed to use it. It runs entirely on the [universal Arrow-stream data-plane
tier](https://pipelinez.dev/concepts/data-plane/) — a typed, boxing-free reader on the source side
(`SnowflakeArrowReader`) and a spool→PUT→COPY load on the sink side — via
[`Snowflake.Data`](https://www.nuget.org/packages/Snowflake.Data). Key-pair (JWT) authentication
only; there is no password-auth surface.

Source: `connectors/Pz.Connector.Snowflake/`. Contract test suite:
[`tests/Pz.Connector.Snowflake.Tests`](../../tests/Pz.Connector.Snowflake.Tests) — env-gated live
acceptance (below), since no Snowflake container/emulator exists for Testcontainers.

This page is the connector's own reference. For the *why* behind the ABI it implements, see
[Connectors](https://pipelinez.dev/concepts/connectors/); for the authoring surface (`connections.yml`,
`source()`/`sink()`), see [Project structure](https://pipelinez.dev/concepts/project-structure/).

## Capabilities

Declared in `SnowflakeConnector.Capabilities`:

| Flag | What it means here |
|---|---|
| `ColumnPruning` | table-mode reads project only the columns `ReadHints` requests, not `select *` |
| `PredicatePushdown` | the engine's predicate SQL is ANDed into the generated `WHERE` |
| `BoundedWindow` | `cursor <= upper` is applied alongside `cursor > lower` — required for the windowed-incremental trio (`initial`/`max_window`/`until`) |
| `InclusiveWatermarkBound` | the connector honors an inclusive lower-bound watermark comparison (`>=` instead of `>`) when the engine asks for one |
| `Merge` | the sink supports `strategy: merge` |
| `ReplaceWrites` | the sink supports `strategy: replace` |
| `Transactional` | the sink's write session commits or aborts as a whole — see "Writing data" below for what that means here, since Snowflake auto-commits DDL |

**Not declared:** `PartitionedRead` (every read plans exactly one partition —
`SnowflakeSource.PlanReadAsync` always returns a single-element list; native range partitioning is
future work), `NativeScan`/`NativeCopy` (both `TryGetNativeScan`/`TryGetNativeCopy` decline
unconditionally — every read and write moves through the universal Arrow-stream tier), `ChangeCapture`
and `ApplyDeletes` (no cdc support), `CheckpointableReads`/`CheckpointableWrites`.

## Connection (`connections.yml`)

```yaml
wh:
  connector: snowflake
  account: myorg-myaccount              # required
  user: pz_svc                          # required
  private_key_path: /secrets/rsa_key.p8 # required
  database: analytics                   # required
  warehouse: compute_wh                 # required
  private_key_passphrase: ...           # optional, if the key is encrypted
  role: transformer                     # optional
```

`account`/`user`/`private_key_path`/`database`/`warehouse` are the required keys
(`ConnectionConfigSchema` rejects anything else — `additionalProperties: false`). There is no
`password` key: `SnowflakeConnector.BuildConnectionString` always sets
`authenticator=snowflake_jwt` and points the driver at the private key file — key-pair auth is the
only supported credential shape. `application=pz` is always stamped.

See [Secure connection config](https://pipelinez.dev/how-to/secure-connection-config/) for keeping
credentials (and the private key path) out of `connections.yml` (env var interpolation, secret
files).

## Naming an entity

No `schema:`/`table:` option — the dataset/output *name* is the object name, split on its own dot
(`SfDdl.SplitEntity`). An unqualified name defaults to the `PUBLIC` schema:

```sql
from {{ source('wh', 'RAW.ORDERS', ...) }}   -- schema RAW, table ORDERS
from {{ source('wh', 'ORDERS', ...) }}       -- same: unqualified defaults to PUBLIC
```

Every identifier this connector generates — schema and table names included — is double-quoted
(`SfDdl.Quote`). Snowflake folds an *unquoted* identifier to uppercase but preserves a *quoted*
one exactly as written, so an entity name is always case-sensitive here, byte-for-byte: `orders`
and `ORDERS` name different objects unless the underlying table was itself created quoted-lowercase.

## Reading data

Two mutually exclusive dataset modes.

### Table mode (default)

No `query:` option — the dataset name (`SCHEMA.TABLE`) is read directly. Supports column pruning
and predicate/watermark pushdown. The generated SQL is always
`select <cols> from "<schema>"."<table>" [where (<predicate>) and (<watermark>) and (<upper bound>)]`
— every predicate term is self-parenthesized before the join so a disjunctive engine pushdown can't
bind into the watermark's `AND`.

### Query mode (`query:`)

```yaml
entities:
  recent_orders:
    read:
      query: "select * from raw.orders where status = 'open'"
```

The SQL runs verbatim — **no pushdown of any kind** (column pruning, predicate, watermark, or
window bound). Schema resolution wraps it (`select * from (<query>) as pz_probe limit 0`), so the
query's own `LIMIT`/`ORDER BY` stay intact.

## Incremental extraction and bounded windows

No `sync:` block is required — the `where` clause in your own SQL *is* the incremental declaration:

```sql
from {{ source('wh', 'RAW.ORDERS', ...) }}
where "UPDATED_AT" > {{ watermark('wh', 'RAW.ORDERS') }}
```

pz extracts the cursor column and comparison from that clause; there's no separate `sync.cursor` to
keep in sync with it. The connector applies `cursor > watermark` (or `>=` when the engine asks for
an inclusive lower bound — `InclusiveWatermarkBound`) as a `WHERE` term, with the watermark value
rendered as a quoted string literal.

**Bounded windows** (`initial`/`max_window`/`until` alongside the cursor) additionally apply
`cursor <= upper` — the connector declares `BoundedWindow`, so `pz` allows a windowed dataset on
this connector (`PZ0313` would otherwise refuse it). See [Backfill in bounded
slices](https://pipelinez.dev/how-to/backfill-in-slices/) for how to declare the window.

Watermark/incremental behavior is generic engine mechanism, not snowflake-specific — see
[Connectors: incremental extraction and watermarks](https://pipelinez.dev/concepts/connectors/#incremental-extraction-and-watermarks)
for the full contract (commit-gated advancement, late-arriving-data caveat, etc).

## Writing data

```sql
INSERT INTO {{ sink('wh', 'RAW.ORDERS_CURRENT', strategy: 'merge', keys: ['id']) }}
select ...
```

**Spool → PUT → COPY → one commit statement.** Batches spool to gzip-compressed CSV files in a
local temp directory as `WriteBatchAsync` is called (no connection open yet, no server-side work
yet), rolling to a new file once the current one exceeds ~100 MB compressed so one batch is never
split across files. `CommitAsync` is where everything server-side happens, in order:

1. Ensure the target table exists (create it) or enforce `schema_policy` against what's already
   there (below).
2. `create temporary stage` — a session-scoped stage, dropped automatically when the connection
   closes.
3. One `PUT` per spool file.
4. `create temporary table` — a staging table matching the write schema (plus, for merge only, a
   trailing `_pz_seq` sequence column).
5. `COPY INTO` the staging table from the stage.
6. **The one statement that ever touches the target** — an `insert into`, `insert overwrite into`,
   or `merge into` depending on write mode.

Snowflake auto-commits DDL, so this sink never relies on a multi-statement transaction against the
target — only step 6 touches it, which is what makes `Transactional` an honest declaration: `Abort`
(before any connection opens) deletes the spool files and is a `DiscardsAll`; a failure at any step
after that leaves the target's *rows* exactly as it was, because no earlier step wrote to it. One
caveat: if step 1 had to create the target (it didn't already exist) and any later step then fails,
that empty table is left behind — CREATE auto-commits the moment it runs, so there is no undoing it
short of a later run's own `EnsureTargetAsync` finding it and proceeding normally against it.

| Mode | Behavior | Delivery guarantee |
|---|---|---|
| `append` | `insert into <target> select * from <staging>` | at-least-once |
| `replace` | `insert overwrite into <target> select * from <staging>` | effectively-once |
| `merge` | key-deduped upsert into `<target>` from `<staging>` | effectively-once |

See [Delivery guarantees](https://pipelinez.dev/concepts/delivery-guarantees/) for what each
guarantee means on commit/crash.

**`merge`'s staging path**: every row written to the spool file also gets a trailing,
session-monotonic `_pz_seq` value (a real value written into the CSV, not a target-side
autoincrement — Snowflake's `COPY` can load a stage's files in parallel, so an autoincrement's fill
order would not reliably track write order across files). The generated `MERGE` sources its `USING`
side from `select ... from <staging> qualify row_number() over (partition by <keys> order by
"_pz_seq" desc) = 1` — a last-writer-wins dedup of the batch by key, done in one set-based query,
before the `when matched`/`when not matched` upsert runs. `_pz_seq` is a reserved column name: a
write schema that already has one is a named, non-transient error before any connection opens.

### `schema_policy`

- **`fail_on_change`** (the default): if the target already exists, every declared column's name
  and canonical type is compared against `information_schema.columns` (database-qualified — the
  connection string sets a default database but no default schema), aggregating every mismatch into
  one error rather than failing on the first. A missing target is created instead.
- **`evolve`** is rejected outright — a named, non-transient error before any connection opens. This
  sink has no `ALTER`/drift-repair machinery, so pretending to support it would silently skip real
  schema evolution; the fix is to align the target table by hand, or drop it and let the sink
  recreate it.

Every string column writes as unsized `VARCHAR` and every numeric/temporal column writes with the
`TIMESTAMP_NTZ(6)`/`NUMBER(p,s)` shape `SfTypeMap.ToSnowflakeDdl` derives from the Arrow type — there
is no `columns:` write option on this connector to override sizing or type per column.

## Type mapping

`SfTypeMap` — Snowflake's reported type name (both the driver's logical spellings, `FIXED`/`TEXT`/
`REAL`, and SQL spellings, `NUMBER`/`VARCHAR`/`DOUBLE`, are accepted on the read side) to Arrow type:

| Snowflake type(s) | Arrow type |
|---|---|
| `FIXED`/`NUMBER`/`DECIMAL`/`NUMERIC`, scale 0, precision ≤ 9 | `Int32` |
| `FIXED`/`NUMBER`/`DECIMAL`/`NUMERIC`, scale 0, precision ≤ 18 | `Int64` |
| `FIXED`/`NUMBER`/`DECIMAL`/`NUMERIC`, otherwise | `Decimal128(precision, scale)` |
| `REAL`, `FLOAT`, `DOUBLE` | `Double` |
| `TEXT`, `VARCHAR`, `STRING`, `CHAR` | `Utf8` |
| `BOOLEAN` | `Boolean` |
| `DATE` | `Date32` |
| `TIMESTAMP_NTZ`, `TIMESTAMP_LTZ`, `TIMESTAMP_TZ`, `DATETIME` | `Timestamp(µs, no zone)` |

An unrecognized source type fails schema resolution loudly, naming the column and hinting a
`query:`-side cast or a `columns:` exclusion, rather than falling back to a lossy default. On the
sink side, a `Decimal128` value at the high end of the v0 matrix's precision (up to 38 digits) can
be too wide for the CLR `decimal` (96-bit) the CSV writer renders it through; that overflow is caught
and reported as a named write error, not left to surface as a raw, columnless
`OverflowException` — same pattern as the read side's own decimal128-overflow guard.

## Errors and retries

Every driver exception this connector raises is wrapped as a `PzConnectorException` naming the
dataset/output and the underlying message. Transience (`SfErrors.IsTransient`) is what lets the
engine's retry policy decide whether to retry — the connector never retries internally:

- Network shapes (`TimeoutException`, `IOException`, `HttpRequestException`, and a `SnowflakeDbException`
  whose `SqlState` starts with `08` — connection exceptions) are transient.
- Auth failures, SQL compilation errors, and missing-object errors carry other `SqlState`s and are
  permanent.

A handful of shapes are checked before any connection opens and always classify as non-transient (a
validation problem, not a database hiccup): a missing required connection key, an unsupported write
mode, a merge write schema missing a declared key column or carrying the reserved `_pz_seq` name, a
malformed entity name (not `SCHEMA.TABLE` or `TABLE`), `schema_policy: evolve`.

## Platform notes

- **Linux: prefer glibc.** `Snowflake.Data`'s documented Linux support is glibc-based
  (Debian/Ubuntu/RHEL-family); musl distros (Alpine) aren't officially validated, even though the
  pinned 6.0.0 package does ship `linux-musl-x64`/`linux-musl-arm64` native assets — until Snowflake
  documents musl support, run this connector on a glibc Linux rather than an Alpine container image.
- **No GCP regional endpoint support.** The driver has no option analogous to other Snowflake
  connectors' regional-endpoint configuration for GCS-backed stages; this only matters for accounts
  hosted on GCP with a regional endpoint requirement.

## Package layout

```
connectors/Pz.Connector.Snowflake/
├── SnowflakeConnector.cs      # IConnector/ISourceConnector/ISinkConnector, JWT connection string, manifest identity
├── SnowflakeSource.cs         # ISource: SELECT generation, schema probe, single-partition read
├── SnowflakeArrowReader.cs    # typed, boxing-free DbDataReader → Arrow RecordBatch reader
├── SnowflakeSink.cs           # ISink: spool → PUT → COPY → one commit statement, per write mode
├── SfDdl.cs                   # identifier quoting, entity-name splitting, DDL/MERGE SQL generation, schema_policy drift check
├── SfCsv.cs                   # sink's spool-file CSV encoding + matching COPY FILE_FORMAT clause
├── SfTypeMap.cs               # Snowflake ↔ Arrow type matrix (read resolution + DDL/information_schema rendering)
├── SfErrors.cs                # transience classification for engine retries
└── pz.connector.json           # manifest: name "snowflake", protocol major range, capabilities [source, sink]
```

The package embeds `pz.connector.json` (readable without loading the assembly — an incompatible
protocol version is rejected before any package code runs) and is one of the repo's first-party
connectors bundled into the `pz` tool package itself; see [Connectors: discovery, packaging, and
restore](https://pipelinez.dev/concepts/connectors/#discovery-packaging-and-restore).

## Testing

[`tests/Pz.Connector.Snowflake.Tests`](../../tests/Pz.Connector.Snowflake.Tests) runs the shared
[`Pz.Connectors.TestKit`](../../src/Pz.Connectors.TestKit) acceptance suite against a real Snowflake
account — there is no container or local emulator for Snowflake, so the docker-fixture SKIP
convention becomes an env-var SKIP convention instead. The suite activates only when every required
variable is set:

- `PZ_SNOWFLAKE_ACCOUNT`, `PZ_SNOWFLAKE_USER`, `PZ_SNOWFLAKE_PRIVATE_KEY_PATH`,
  `PZ_SNOWFLAKE_DATABASE`, `PZ_SNOWFLAKE_WAREHOUSE` — required.
- `PZ_SNOWFLAKE_ROLE`, `PZ_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` — optional.

Without them, `dotnet test` skips cleanly (same convention as the docker-backed suites) — see
`SnowflakeFacts.SkipUnlessConfigured`. With them:

```bash
PZ_SNOWFLAKE_ACCOUNT=... PZ_SNOWFLAKE_USER=... PZ_SNOWFLAKE_PRIVATE_KEY_PATH=... \
PZ_SNOWFLAKE_DATABASE=... PZ_SNOWFLAKE_WAREHOUSE=... \
dotnet test tests/Pz.Connector.Snowflake.Tests -c Release
```

The source suite reads a pre-seeded `PZ_TESTKIT.ORDERS` table that the fixture does not create
(seeding SQL is in `SnowflakeSourceAcceptance`'s doc comment); the sink suite creates its own target
tables on demand and needs only the `PZ_TESTKIT` schema to already exist.

## See also

- [Connectors](https://pipelinez.dev/concepts/connectors/) — the ABI these types implement, and why.
- [Backfill in bounded slices](https://pipelinez.dev/how-to/backfill-in-slices/) — bounded windows from
  the user's side.
- [Secure connection config](https://pipelinez.dev/how-to/secure-connection-config/) — keeping
  credentials and the private key path out of `connections.yml`.
- [Author a connector](https://pipelinez.dev/how-to/author-a-connector/) — the ABI this connector
  implements, for anyone writing a new one.
