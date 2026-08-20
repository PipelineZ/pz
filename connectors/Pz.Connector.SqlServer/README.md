# `sqlserver` connector

Microsoft SQL Server source + sink for PipelineZ (`pz`), registered under the connector name
`sqlserver`. It runs entirely on the [universal Arrow-stream data-plane
tier](https://pipelinez.dev/concepts/data-plane/) — a typed, boxing-free reader on the source side
(`SqlServerArrowReader`) and `SqlBulkCopy` on the sink side — via
[`Microsoft.Data.SqlClient`](https://www.nuget.org/packages/Microsoft.Data.SqlClient). It works
against on-prem SQL Server, Azure SQL Database, and Azure SQL Managed Instance identically; the
only difference is what you put in `authentication`.

Source: `connectors/Pz.Connector.SqlServer/`. Contract test suite:
[`tests/Pz.Connector.SqlServer.Tests`](../../tests/Pz.Connector.SqlServer.Tests) (Testcontainers —
`Xunit.SkippableFact`, skips cleanly without Docker).
Worked example: [`samples/mssql-mart`](../../samples/mssql-mart).

This page is the connector's own reference. For the *why* behind the ABI it implements, see
[Connectors](https://pipelinez.dev/concepts/connectors/); for the authoring surface (`connections.yml`,
`source()`/`sink()`), see [Project structure](https://pipelinez.dev/concepts/project-structure/).

## Capabilities

Declared in `SqlServerConnector.Capabilities`:

| Flag | What it means here |
|---|---|
| `ColumnPruning` | table-mode reads project only the columns `ReadHints` requests, not `select *` |
| `PredicatePushdown` | the engine's predicate SQL is ANDed into the generated `WHERE` |
| `PartitionedRead` | `partition_column`/`partitions` splits a table-mode read into up to 16 concurrent range reads |
| `BoundedWindow` | `cursor <= upper` is applied alongside `cursor > lower` — required for the windowed-incremental trio (`initial`/`max_window`/`until`) |
| `InclusiveWatermarkBound` | the connector honors an inclusive lower-bound watermark comparison (`>=` instead of `>`) when the engine asks for one |
| `Merge` | the sink supports `strategy: merge` |
| `ReplaceWrites` | the sink supports `strategy: replace` |
| `Transactional` | every write session (append/replace/merge) runs inside one transaction; abort = rollback |
| `ChangeCapture` | `sync: {mode: cdc}` is supported on the source side, backed by SQL Server's native change tables |
| `ApplyDeletes` | a `merge` write session accepts delete-key batches (hard or soft) from an upstream cdc dataset, applied in the same transaction as the merge |

**Not declared:** `NativeScan`/`NativeCopy` — SQL Server has no built-in DuckDB extension the way
Postgres/S3/Azure Blob do, so every read and write moves through the universal Arrow-stream tier.
`TryGetNativeScan`/`TryGetNativeCopy` both decline unconditionally. (The community `mssql` DuckDB
extension is the designated future native tier — not wired up yet.)

## Connection (`connections.yml`)

```yaml
erp:
  connector: sqlserver
  host: myserver.database.windows.net   # required
  database: mart                        # required
  port: 1433                            # optional, default SqlClient's own (1433)
  user: ...                             # optional
  password: ...                        # optional
  authentication: ...                   # optional — SqlClient passthrough, see below
  encrypt: true                         # optional boolean
  trust_server_certificate: false       # optional boolean
```

`host`/`database` are the only required keys (`ConnectionConfigSchema` rejects anything else —
`additionalProperties: false`). Values are read by `SqlServerConnector.BuildConnectionString`
straight into a `SqlConnectionStringBuilder`; `ApplicationName` is always stamped `pz`.

See [Secure connection config](https://pipelinez.dev/how-to/secure-connection-config/) for keeping
credentials out of `connections.yml` (env var interpolation, secret files).

### Entra ID / managed identity

Omit `password` and set `authentication` — the value is passed to `SqlClient` verbatim (any
documented mode works, e.g. `Active Directory Default`, which also picks up Azure CLI credentials
locally):

```yaml
# system-assigned managed identity (e.g. on an Azure VM)
erp:
  connector: sqlserver
  host: myserver.database.windows.net
  database: mart
  authentication: Active Directory Managed Identity
```

```yaml
# user-assigned managed identity: add its client id as `user`
erp:
  connector: sqlserver
  host: myserver.database.windows.net
  database: mart
  authentication: Active Directory Managed Identity
  user: <client-id-guid>
```

An unrecognized `authentication` string is a compile-time-adjacent connector error naming the bad
value, not a raw SqlClient exception. The database user must exist independently of pz —
`CREATE USER [identity-name] FROM EXTERNAL PROVIDER`, granted the roles the pipeline needs
(`db_datareader` to read, `db_datareader, db_datawriter, db_ddladmin` to write — the sink creates
tables but never schemas, so `CREATE SCHEMA <target>;` is a one-time manual step).

## Naming an entity

No `schema:`/`table:` option — the dataset/output *name* is the object name, split on its own dot
(`Pz.Connectors.Abstractions.Paths` / `MsDdl.SplitEntity`). An unqualified name defaults to the
`dbo` schema; a three-part name (`db.schema.table`) is refused rather than silently treated as one
quoted identifier:

```sql
from {{ source('erp', 'dbo.orders', ...) }}   -- schema 'dbo', table 'orders'
from {{ source('erp', 'orders', ...) }}       -- same: unqualified defaults to dbo
```

## Reading data

Three mutually exclusive dataset modes; declaring more than one option from a different mode on
the same dataset is a connector error.

### Table mode (default)

No `query:`/`procedure:` option — the dataset name (schema.table) is read directly. Supports
column pruning, predicate/watermark pushdown, and partitioned reads (below). The generated SQL is
always `select <cols> from <schema>.<table> [where (<predicate>) and (<watermark>) and (<upper bound>)]`
— every predicate term is self-parenthesized before the join so a disjunctive engine pushdown can't
bind into the watermark's `AND`.

### Query mode (`query:`)

```yaml
entities:
  recent_orders:
    read:
      query: "select * from dbo.orders where status = 'open'"
```

The SQL runs verbatim — **no pushdown of any kind** (column pruning, predicate, watermark, or
window bound). `partition_column`/`partitions` still work in query mode: the query is wrapped as a
derived table (`select * from (<query>) q where ...`) for both the min/max probe and the per-range
reads, the same as table mode.

### Stored procedure mode (`procedure:`)

```yaml
entities:
  recent_orders:
    read:
      procedure: dbo.get_recent_orders
      parameters:
        since: "$watermark"          # sentinel: binds the engine's watermark cursor value
        until: "$watermark_upper"    # sentinel: binds the bounded-window upper bound
        status: open                 # literal value, bound via SqlParameter's CLR-type inference
```

Runs as `CommandType.StoredProcedure` with typed `SqlParameter`s — never a hand-built `EXEC`
string, so there's no injection surface to guard beyond a defense-in-depth name check. The proc
*is* the pushdown: the connector applies no additional `WHERE`. `$watermark`/`$watermark_upper` are
reserved sentinel values — a `parameters:` entry set to literally that string is always bound as
the watermark cursor / window upper bound, never passed through as text — and both bind
`DBNull`/`NULL` on a run with no watermark yet (including every planning-time schema probe), so a
proc must treat a `NULL` bound as unbounded.

Schema normally comes from a `SchemaOnly` probe (SqlClient's legacy `SET FMTONLY ON`), which
**cannot** see through a procedure that stages its result in a `#temp` table or table variable —
`WITH RESULT SETS` on the `EXEC` doesn't rescue this either. For exactly that case, declare a
`columns:` contract on the dataset: it bypasses the probe entirely and builds the schema from the
declared types, and the *actual* result schema is verified against it (name, position, Arrow type)
before the first row streams — a mismatch is a named, non-transient error.

Partitioned reads are rejected for `procedure:` datasets (`partition_column`/`partitions` set on
one is a connector error) — re-running a non-deterministic proc per partition would break the
union-equals-one-read invariant every other read mode relies on.

### Partitioned reads (`partition_column`/`partitions`)

```yaml
# or as source() kwargs, e.g. partition_column: 'order_id', partitions: 4
entities:
  orders:
    read:
      partition_column: order_id
      partitions: 4          # 1-16; 1 (the default) means "don't partition"
```

`partitions > 1` probes `min(col)`/`max(col)` over the (already predicate/watermark-filtered)
read, then splits that range into `partitions` equal-width buckets
(`RangeBoundaries.ComputeLiterals`), each read by its own connection concurrently. Requirements
and edge cases:

- `partition_column` must be orderable — numeric (`int`/`bigint`/`float`/`decimal`) or temporal
  (`date`/`datetime2`/`datetimeoffset`); anything else is a named, non-transient error.
- The first partition's range absorbs a `NULL` bucket (`col is null or (lo <= col < hi)`) so rows
  with a null partition column are never silently dropped.
- If the probed min/max come back equal (or null — an empty read), partitioning collapses to a
  single unpartitioned read rather than emitting degenerate ranges.
- The last partition's upper bound is inclusive (`<= hi`); every other partition is `[lo, hi)`.

## Incremental extraction and bounded windows

No `sync:` block is required — the `where` clause in your
own SQL *is* the incremental declaration:

```sql
from {{ source('erp', 'dbo.orders', ...) }}
where updated_at > {{ watermark('erp', 'dbo.orders') }}
```

pz extracts the cursor column and comparison from that clause; there's no separate `sync.cursor`
to keep in sync with it. The connector applies `cursor > watermark` (or `>=` when the engine asks
for an inclusive lower bound — `InclusiveWatermarkBound`) as a `WHERE` term; the watermark literal
rides in untyped and unquoted-by-cast, so T-SQL's normal data-type precedence casts it to the
*column's* type and the comparison stays sargable (index-usable).

**Bounded windows** (`initial`/`max_window`/`until` alongside the cursor) additionally apply
`cursor <= upper` — the connector declares `BoundedWindow`, so `pz` allows a windowed dataset on
this connector (`PZ0313` would otherwise refuse it). See [Backfill in bounded
slices](https://pipelinez.dev/how-to/backfill-in-slices/) for how to declare the window, and
[`samples/mssql-mart`](../../samples/mssql-mart) for both the SQL-declared and YAML-declared forms
side by side.

Watermark/incremental behavior is generic engine mechanism, not sqlserver-specific — see
[Connectors: incremental extraction and watermarks](https://pipelinez.dev/concepts/connectors/#incremental-extraction-and-watermarks)
for the full contract (commit-gated advancement, late-arriving-data caveat, etc).

## Change Data Capture (CDC)

```yaml
entities:
  orders:
    read:
      # capture_instance: dbo_orders   # optional; default is "{schema}_{table}"
      sync:
        mode: cdc
```

Backed by SQL Server's native change-tracking tables (`cdc.fn_cdc_get_all_changes_<instance>`),
not a third-party log-reader. No `cursor:` — cdc needs none. Table-mode-only options
(`query`/`procedure`/`partition_column`/`partitions`) are rejected on a cdc dataset; a cdc read is
always a single sequential partition (one bounded LSN window per run).

Server-side prerequisites, checked (and reported with copy-pasteable remediation) both at read
time and via `pz cdc status`:

1. `EXEC sys.sp_cdc_enable_db` — database-level cdc.
2. `EXEC sys.sp_cdc_enable_table @source_schema = N'<schema>', @source_name = N'<table>', @role_name = NULL`
   — table-level capture (creates the instance named by `capture_instance`, or `{schema}_{table}`
   by default).
3. SQL Server Agent running (`MSSQL_AGENT_ENABLED=true` in a container) — cdc's capture job needs
   it.

The sync token is a `_pz_lsn` string: `{start_lsn: 20 uppercase hex}-{seqval: 20 uppercase hex}`,
no `0x` prefix. The first run snapshots the base table (`_pz_op = 'insert'`, all-zeros `_pz_lsn`);
every run after reads `[from, to]` through the change-table function, incrementing the prior token
via `sys.fn_cdc_increment_lsn` before use (the window's `@from` bound is inclusive, and the prior
token was already consumed), ordered `[__$start_lsn], [__$seqval]`. `_pz_changed_at` comes from
`sys.fn_cdc_map_lsn_to_time`.

`pz cdc drop` is deliberately a no-op on the server side — disabling cdc
(`sp_cdc_disable_table`) is a DBA-level, database-wide decision pz never makes unilaterally; the
command prints the exact statement and clears only pz's own sync-state entry.

This is one of two first-party cdc connectors (Postgres is the other) sharing one engine-side
contract — write-side `on_delete` handling, delivery semantics, and the full walkthrough live in
[Capture changes with CDC](https://pipelinez.dev/how-to/capture-changes-with-cdc/) (design:
`2026-07-24-cdc-design.md`); this section
covers only what's sqlserver-specific.

## Writing data

```sql
INSERT INTO {{ sink('mart', 'mart.orders_current', strategy: 'merge', keys: ['order_id'], on_delete: 'delete') }}
select ...
```

One connection + one transaction per write session — `AbortAsync` is always exactly `ROLLBACK`,
`CommitAsync` is exactly `COMMIT`, and the engine guarantees one or the other is called, never
both. `tablock` (dataset/output option, default `true`) controls whether the bulk load takes a
table lock (`SqlBulkCopyOptions.TableLock`); the merge staging `#temp` table is always
table-locked regardless, since it's session-private.

| Mode | Behavior |
|---|---|
| `append` | `SqlBulkCopy` straight into the target table — no staging. |
| `replace` | Clears the target, then bulk-loads it, in the same transaction. |
| `merge` | Bulk-loads into a `#temp` staging table, then one set-based `MERGE`. |

**`replace`'s clear step** picks `TRUNCATE` (metadata-only, O(1) log) when it can, but pre-checks
first rather than reacting to failure: a `TRUNCATE` blocked by an FK reference dooms the whole
transaction (`xact_state() = -1`) in a way no later `DELETE` can recover from, even from a
server-side `TRY/CATCH`. So the sink probes `sys.foreign_keys` (any FK referencing the target) and
`has_perms_by_name(..., 'ALTER')` (missing permission, or object invisible to the caller) up front
and falls back to a transactional `DELETE` when either is true — same observable result, different
speed/locking.

**`merge`'s staging path**: rows land in a heap `#temp` table with a trailing identity column
(`__pz_seq`) that autofills in arrival order — SqlBulkCopy's explicit-by-name column mappings never
touch it, so it's a free last-writer-wins tiebreaker for duplicate keys in the same batch. After
the load, one clustered index is built over `(<keys>, __pz_seq)` (sort-once beats maintaining a
clustered index row-by-row during the load), then a single `MERGE ... WITH (HOLDLOCK)` upserts into
the target, letting SQL Server's engine dedup the batch itself via the sort order the index already
provides.

**Deletes** (`on_delete: delete`/`soft`, only meaningful on a merge output fed by an upstream cdc
dataset) apply *after* the merge, in the same transaction: delete-key batches stream into their own
`#pz_del` staging table, then one join-based (not `MERGE`-based — a duplicate key from an idempotent
replay would make `MERGE`'s `WHEN MATCHED` raise error 8672) `DELETE ... FROM ... JOIN` or
`UPDATE ... SET _pz_deleted_at = sysutcdatetime() FROM ... JOIN` applies them.

`replace` and `merge` both require `ConnectorCapabilities.ReplaceWrites`/`Merge`, both declared —
see [Delivery guarantees](https://pipelinez.dev/concepts/delivery-guarantees/) for what each mode guarantees
on commit/crash, and merge key-column requirements (must exist in the write schema; the reserved
column name the staging path uses, `__pz_seq`, can't be a data column).

### Write column types

By default every Arrow `String` column creates as `nvarchar(max)`, which forces `SqlBulkCopy` onto
the slower LOB/PLP path. The `columns:` write option gives string (and other) columns real sizes,
resolved per column, in order:

1. **Declared** — a `columns:` entry naming that column.
2. **Derived** — for string columns with no declared entry, the engine measures the staged data's
   `max(length())` and rounds up to the smallest of `{16, 32, 64, 128, 256, 512, 1000, 2000, 4000}`
   that is at least 2× the observed length (headroom for later runs); an observed length over 4000
   resolves to `nvarchar(max)` rather than truncating real data to fit a bucket.
3. **Fallback** — `nvarchar(4000)` when nothing was observed (no rows, or an all-null column).

```yaml
entities:
  dbo.orders_mart:
    write:
      strategy: merge
      keys: [id]
      columns:
        status: nvarchar(20)
        note: nvarchar(200)
```

or the equivalent `sink()` kwarg — never both (`PZ0341`):

```sql
INSERT INTO {{ sink('mart', 'dbo.orders_mart', strategy: 'merge', keys: ['id'], columns: { status: 'nvarchar(20)', note: 'nvarchar(200)' }) }}
```

Accepted `columns:` types (case-insensitive, parsed to an AST and re-rendered — never interpolated
raw into DDL): `int`, `bigint`, `float`, `bit`, `date`, `datetime2(0..7)`, `decimal(p,s)` with
`1<=p<=38` and `0<=s<=p`, `nvarchar(1..4000|max)`, `varchar(1..8000|max)`. An unknown type, a
`columns:` key naming no column in the write schema, or a non-map `columns:` value is a named,
non-transient error before any connection opens.

The resolved type feeds `CREATE TABLE` for a missing target, the merge staging `#temp` table (whose
string columns mirror the *existing* target's actual types when the target already exists, so a
hand-sized or previously pz-created table governs what the bulk load pays), and the
`fail_on_change` column check: a **declared** column must match its declared type exactly, but an
**undeclared** string column accepts any `nvarchar`/`varchar` width on the target — old pz-created
`nvarchar(max)` tables and hand-sized tables both keep passing. Derived sizes only ever apply when
`pz` creates the table; **an existing table is never `ALTER`ed** for sizing.

If a later run's value doesn't fit — a declared/derived/fallback column too narrow, or a declared
type incompatible with the data — the bulk load fails loudly (SQL Server error 2628/8152 or a
conversion error) with a hint to widen the column or declare a larger type in `columns:`; there is
no silent truncation. A hand-created `varchar` target column code-page-converts Unicode silently
instead of failing loudly — unmappable characters become `?` — so declare `nvarchar` for Unicode
data.


## Type mapping

`MsTypeMap` — SQL Server's `DataTypeName` (case-insensitive, parameterized forms like
`decimal(18,2)` normalized by stripping everything from `(` on) to Arrow type:

| SQL Server type(s) | Arrow type |
|---|---|
| `int` | `Int32` |
| `tinyint`, `smallint` | `Int32` (widened) |
| `bigint` | `Int64` |
| `float` | `Double` |
| `real` | `Double` (widened) |
| `decimal`, `numeric`, `money`, `smallmoney` | `Decimal128(38, 9)` |
| `nvarchar`, `varchar`, `char`, `nchar`, `text`, `ntext` | `Utf8` |
| `uniqueidentifier` | `Utf8` (string form) |
| `bit` | `Boolean` |
| `date` | `Date32` |
| `datetime2`, `datetime`, `smalldatetime` | `Timestamp(µs, UTC)` |
| `datetimeoffset` | `Timestamp(µs, UTC)` |

`datetime`-family values carry no offset on the wire and are trusted as UTC. An unrecognized
source type fails schema resolution loudly rather than falling back to a lossy default. On the
sink side, a `decimal` value whose actual precision/scale would overflow the target column is
caught and reported as a named write error rather than truncated silently — see
`ArrowBatchDataReader`.

## Errors and retries

Every `SqlException` this connector raises is wrapped as a `PzConnectorException` naming the
dataset/output and the underlying message, classified via `SqlException.IsTransient` — the
connector never retries internally; `IsTransient` is what lets the engine's retry policy decide
whether to. A handful of shapes are checked before any connection opens and always classify as
non-transient (a validation problem, not a database hiccup): unrecognized `authentication` values,
`query`+`procedure` both set, an invalid `partitions` count, an unorderable `partition_column`, a
malformed procedure name, `parameters:` that isn't a mapping, an unknown `columns:` contract type.

## Package layout

```
connectors/Pz.Connector.SqlServer/
├── SqlServerConnector.cs      # IConnector/ISourceConnector/ISinkConnector, connection string, manifest identity
├── SqlServerSource.cs         # ISource: SELECT generation, schema probe, partitioned/table/query read
├── ProcedureDataset.cs        # procedure: mode — RPC command build, parameter sentinels, columns: escape hatch
├── SqlServerCdc.cs            # cdc helpers shared by the source and `pz cdc status`/`drop`
├── SqlServerCdcPartition.cs   # cdc's single IDatasetPartition (snapshot + windowed change reads)
├── SqlServerArrowReader.cs    # typed, boxing-free SqlDataReader → Arrow RecordBatch reader
├── ArrowBatchDataReader.cs    # Arrow RecordBatch → IDataReader adapter SqlBulkCopy reads from
├── SqlServerSink.cs           # ISink: append/replace/merge write sessions, delete application
├── MsDdl.cs                   # identifier quoting, entity-name splitting, DDL/MERGE SQL generation
├── MsTypeMap.cs                # SQL Server → Arrow type matrix
├── RangeBoundaries.cs          # equal-width partition-range literal computation
└── pz.connector.json           # manifest: name "sqlserver", protocol major range, capabilities [source, sink]
```

The package embeds `pz.connector.json` (readable without loading the assembly — an incompatible
protocol version is rejected before any package code runs) and is one of the repo's first-party
connectors bundled into the `pz` tool package itself; see [Connectors: discovery, packaging, and
restore](https://pipelinez.dev/concepts/connectors/#discovery-packaging-and-restore).

## Testing

[`tests/Pz.Connector.SqlServer.Tests`](../../tests/Pz.Connector.SqlServer.Tests) runs the shared
[`Pz.Connectors.TestKit`](../../src/Pz.Connectors.TestKit) acceptance suite plus connector-specific
tests against a real SQL Server container (`MsSqlContainerFixture`, Testcontainers). Every
docker-backed test is `[SkippableFact]` and skips cleanly when Docker isn't available:

```bash
dotnet test tests/Pz.Connector.SqlServer.Tests -c Release
```

## See also

- [`samples/mssql-mart`](../../samples/mssql-mart) — a runnable SQL Server → SQL Server mart
  project (partitioned incremental read, merge sink, Entra ID auth end to end).
- [Connectors](https://pipelinez.dev/concepts/connectors/) — the ABI these types implement, and why.
- [Capture changes with CDC](https://pipelinez.dev/how-to/capture-changes-with-cdc/) — the full,
  connector-agnostic CDC walkthrough.
- [Backfill in bounded slices](https://pipelinez.dev/how-to/backfill-in-slices/) — bounded windows from the
  user's side.
- [Secure connection config](https://pipelinez.dev/how-to/secure-connection-config/) — keeping credentials
  out of `connections.yml`.
- [Author a connector](https://pipelinez.dev/how-to/author-a-connector/) — the ABI this connector
  implements, for anyone writing a new one.
