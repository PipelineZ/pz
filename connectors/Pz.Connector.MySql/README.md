# `mysql` connector

MySQL source + sink for PipelineZ (`pz`), registered under the connector name `mysql`. It is the
native-path-only experiment: **DuckDB's own `mysql` extension is the entire data plane**, on both
the read and write side, and the connector ships with **zero .NET MySQL driver dependency** — it
is pure SQL-fragment generation over the [native scan/copy data-plane
tier](https://pipelinez.dev/concepts/data-plane/), never the universal Arrow-stream tier.

Source: `connectors/Pz.Connector.MySql/`. Contract test suite:
[`tests/Pz.Connector.MySql.Tests`](../../tests/Pz.Connector.MySql.Tests) (Testcontainers —
`Xunit.SkippableFact`, skips cleanly without Docker).

This page is the connector's own reference. For the *why* behind the ABI it implements, see
[Connectors](https://pipelinez.dev/concepts/connectors/); for the authoring surface (`connections.yml`,
`source()`/`sink()`), see [Project structure](https://pipelinez.dev/concepts/project-structure/).

## Capabilities

Declared in `MySqlConnector.Capabilities`:

| Flag | What it means here |
|---|---|
| `NativeScan` | every read is a `mysql_query('alias', '…')` DuckDB native scan — never a bare attached-table scan, never the universal tier |
| `NativeCopy` | every write is a native `COPY`-equivalent statement against the rw attach — `INSERT`/`CREATE OR REPLACE` |
| `ReplaceWrites` | the sink supports `strategy: replace` |
| `BoundedWindow` | `cursor <= upper` is applied alongside `cursor > lower` — required for the windowed-incremental trio (`initial`/`max_window`/`until`) |
| `InclusiveWatermarkBound` | the connector honors an inclusive lower-bound watermark comparison (`>=` instead of `>`) when the engine asks for one |

Plus the `INativeOnlySource`/`INativeOnlySink` marker interfaces: `engine.force_universal` fails at
**plan time** with `PZ0312` on either direction, rather than at run time — there is no universal-tier
fallback to fall back to.

**Not declared:** `ColumnPruning`/`PredicatePushdown` (those drive universal-tier `ReadHints`, which a
native-only source never receives — pruning happens anyway, just inside the `mysql_query(...)` SQL
text itself, see [Reading data](#reading-data) below), `PartitionedRead` (a native scan is one DuckDB
scan), `Merge` (the DuckDB `mysql` catalog has no upsert — `strategy: merge` is refused at plan time,
`PZ0324`), `Transactional` (the sink's `replace` swap is not atomic on the MySQL side — see
[Writing data](#writing-data)), `ChangeCapture` (`sync: {mode: cdc}` is a future cycle, ideas.md #32),
`GatedOperations`/`SyncState`/`PathTemplating`/`TextLengthStats` (native reads/writes perform no
gateable, syncable, path-templated, or sized-DDL-relevant .NET operations at all).

## Connection (`connections.yml`)

```yaml
warehouse:
  connector: mysql
  connection:
    host: db.example.com          # required
    database: analytics           # required
    port: 3306                    # optional, default 3306
    user: pz                      # optional
    password: ${MYSQL_PASSWORD}   # optional
    ssl_mode: required            # optional, passed through to the DuckDB mysql extension
```

`host`/`database` are the only required keys (`ConnectionConfigSchema` rejects anything else —
`additionalProperties: false`). Every value here rides a DuckDB `mysql` secret
(`create or replace secret … (type mysql, host '…', port …, database '…', user '…', password
'…', ssl_mode '…')`) — an ordinary, `''`-escaped SQL string literal per field, never the attach
path itself — so none of them are restricted in what characters they may contain; a password with
a space, a quote, or an `=` renders and round-trips correctly.

See [Secure connection config](https://pipelinez.dev/how-to/secure-connection-config/) for keeping
credentials out of `connections.yml` (`${VAR}` env interpolation).

The first `pz run`/`pz validate --connect` against a `mysql` connection needs network access:
`install mysql` pulls the extension from DuckDB's extension repository the first time it isn't
already cached locally.

## Naming an entity

No `schema:`/`table:` option (`PZ0348`) — the dataset/output *name* is the table name, read or
written exactly as MySQL names it. MySQL has no separate per-connection "schema" concept the way
Postgres/SQL Server do; the connection's `database` already plays that role, so there is nothing
left to split a dotted name into:

```sql
from {{ source('warehouse', 'orders') }}
```

## Reading data

Two dataset options, both optional and freely combined, declarable under `entities: <e>: read:` or
as `source()` keywords — never both on the same dataset (`PZ0341`):

- **`query:`** — an arbitrary MySQL `SELECT` that replaces the table as the read's base.
- **`columns:`** — the generic `columns:` contract. When declared it **prunes the read**: only the
  declared columns are projected (the csv/json rule), instead of `SELECT *`.

Every read is always the single `mysql_query('<alias>', '<SELECT …>')` scan fragment, so pruning and
the watermark window (below) execute **inside MySQL**, never client-side after a full-table fetch:

```sql
-- no query:, columns: { id: bigint, updated_at: timestamp } (connection "warehouse")
mysql_query('pz_mysql_src_warehouse_ae1fd358', 'SELECT `id`, `updated_at` FROM `orders`')

-- query: "select * from orders where status = 'open'", no columns:
mysql_query('pz_mysql_src_warehouse_ae1fd358', 'select * from orders where status = ''open''')
```

The alias's trailing hex suffix is a short, stable hash of the connection name (`pz_mysql_src_<sanitized name>_<hash>`, sink side `pz_mysql_snk_…`) — sanitizing non-alphanumerics to `_` alone would let two differently-punctuated connection names (`prod-db`/`prod_db`) collide onto the same alias, and `attach if not exists` is first-wins.

`query:`'s SQL runs verbatim only when nothing else needs to be added (no `columns:`, no watermark
predicate); otherwise it's wrapped as a derived table (`SELECT <cols|*> FROM (<query>) pzq WHERE …`).
Identifiers (the table name, cursor, contract columns) are backtick-quoted per MySQL's own grammar
— never DuckDB double-quoting — because this SQL text lives inside a string literal MySQL itself
parses, not DuckDB.

## Incremental extraction and bounded windows

No `sync:` block is required — the `where` clause in your
own SQL *is* the incremental declaration:

```sql
select id, customer_id, status, updated_at
from {{ source('warehouse', 'orders') }}
where updated_at > {{ watermark('warehouse', 'orders') }}
```

Unlike the file connectors — which deliberately skip unwindowed watermark pushdown because
re-reading a whole file is merely wasteful — mysql pushes the **plain, unwindowed** watermark down
too: re-scanning a whole production table defeats the point of incremental extraction, and
`DatasetSpec`'s contract explicitly permits it. The connector applies `cursor > watermark` (or
`>=` when the engine asks for an inclusive lower bound — `InclusiveWatermarkBound`) as a backtick-
quoted `WHERE` term inside the `mysql_query(...)` literal. The watermark literal renders from the
engine's canonical stored string by shape: all-digits (int/bigint/decimal) stays bare and unquoted;
anything else is single-quoted (with `''` escaping), and a canonical timestamp's `T` separator
becomes a space — MySQL's universally accepted literal form.

**Bounded windows** (`initial`/`max_window`/`until` alongside the cursor) additionally apply
`cursor <= upper` — the connector declares `BoundedWindow`, so `pz` allows a windowed dataset on
this connector (`PZ0313` would otherwise refuse it). See [Backfill in bounded
slices](https://pipelinez.dev/how-to/backfill-in-slices/) for how to declare the window.

Watermark/incremental behavior beyond the pushdown itself is generic engine mechanism, not
mysql-specific — see [Connectors: incremental extraction and
watermarks](https://pipelinez.dev/concepts/connectors/#incremental-extraction-and-watermarks) for the full
contract (commit-gated advancement, late-arriving-data caveat, etc).

No `partitions`/`partition_column` (a native scan is one DuckDB scan) and no `rate_limit` (native
reads perform no gateable .NET operations — the existing `INativeOnlySource` gate refuses it with
`PZ0317`).

## Writing data

```sql
INSERT INTO {{ sink('warehouse', 'orders_mart', strategy: 'append') }}
select ...
```

Two write strategies, both native `COPY`-equivalent SQL against the rw attach — no per-output table
override, since the entity already names the table. The table identifier is **double-quoted**
(`create table alias."orders_mart" ...`), not backtick-quoted: unlike the read side's inner
`SELECT`, this whole statement is parsed by **DuckDB itself**, and DuckDB's grammar has no
backtick-quoted-identifier production at all.

| Mode | Behavior |
|---|---|
| `append` | `create table if not exists <alias>."<table>" as select * from {{source}} limit 0;` then `insert into <alias>."<table>" select * from {{source}};` — one multi-statement batch, so a first run needs no pre-created table. At-least-once (incremental→append still requires `write: { duplicates: accept }`, `PZ0214`). |
| `replace` | `create or replace table <alias>."<table>" as select * from {{source}}` — one statement. |
| `merge` | **Not supported.** The DuckDB `mysql` catalog has no upsert; the planner's `PZ0324` refusal owns the error. |

**`replace` is not atomic on the MySQL side.** The extension's `CREATE OR REPLACE TABLE` is a
drop-then-create, and MySQL DDL is implicitly committed — a reader running concurrently with the
copy can observe the table momentarily absent. This is why `Transactional` is deliberately not
declared; it is documented behavior, not a bug. `replace` is otherwise effectively-once from the
pipeline's own perspective — see [Delivery guarantees](https://pipelinez.dev/concepts/delivery-guarantees/).

Column types on first materialization come from the DuckDB→MySQL default type mapping (e.g.
`VARCHAR`→`TEXT`). The sized-text-DDL machinery the sqlserver sink uses
(`TextLengthStats`/`MaxTextLengths`) is universal-tier only and does not apply to a native copy.

## Control plane: what zero-driver costs

The data plane needs no MySQL driver at all — the only thing a driver would buy is a credentialed
offline probe, and that trade is confined to two places:

- **`pz validate` / `pz run` connection check.** `CheckConnectionAsync` does a raw TCP **greeting
  probe**: a MySQL server sends its handshake packet unprompted on connect, so reachability, "this
  is a MySQL server", and the server version are all verifiable without a driver and without
  credentials. The check message says exactly that — credentials are verified only at run time,
  through the native scan/copy itself. A network failure (timeout, unreachable host) classifies
  transient; a connection refusal or a pre-auth error packet (e.g. a host-not-allowed rule)
  classifies permanent.
- **`pz validate --connect`'s schema fetch.** `GetSchemaAsync` — called only by this opt-in drift
  precheck, never by plain `validate`/`run`/the `on_source_drift` gate — can answer only when the
  dataset declares a `columns:` contract; the contract *is* the returned schema (the azureblob json
  precedent). A contract-less dataset gets a clear, permanent error naming the fix: declare
  `columns:`, or skip `--connect` for that dataset.

## Type mapping

`MySqlTypeNameMap` maps the generic `columns:` contract's type names to Arrow types — used only by
`GetSchemaAsync` above, since a native scan otherwise never needs a schema before DuckDB executes
the read:

| `columns:` type | Arrow type |
|---|---|
| `int` | `Int32` |
| `bigint` | `Int64` |
| `double` | `Double` |
| `decimal` | `Decimal128(38, 9)` |
| `varchar` | `Utf8` |
| `boolean` | `Boolean` |
| `date` | `Date32` |
| `timestamp` | `Timestamp(µs, UTC)` |

An unrecognized type name is a named, non-transient error before any connection opens.

## Errors and retries

`ValidateAsync` always succeeds — post-review fix, every connection value now rides the CREATE
SECRET statement as an ordinary escaped string literal (see [Connection](#connection-connectionsyml)
above), so there is no cross-field rule left for it to enforce. Everything else the connector can
fail with is a genuine thrown `PzConnectorException`, always `isTransient: false` (there is no
retryable failure mode in the connector's own code): `CreateSecretSql`'s missing-`host`/missing-
`database` guards (unreachable in practice once the connection schema's `required: [host,
database]` has already been enforced, present as defense-in-depth), the `PZ0312` refusal stubs
(`PlanReadAsync`/`BeginWriteAsync`, reached only if something forces the universal tier),
`MySqlTypeNameMap`'s unknown-`columns:`-type error, and `GetSchemaAsync`'s contract-less refusal
(reached only by `pz validate --connect`'s drift precheck). The one place this connector *does*
classify transient vs. permanent is the connectivity check itself (`MySqlGreeting.ProbeAsync` —
see [Control plane](#control-plane-what-zero-driver-costs) above), which returns a
`ConnectionCheck`, not a thrown exception.

Setup statements (`install`/`load`/`create secret`/`attach`) and the generated SQL are never
logged unredacted; any engine error routes through `NativeStatementRedactor` before surfacing.
Credentials ride a DuckDB secret, never the attach path (the S3/Azure precedent) — the attach path
is always the empty string, so even a raw connect-failure echo (DuckDB's own
`Failed to connect to MySQL database with parameters "…"` message, which quotes the attach path
verbatim) is credential-free by construction, not merely masked.

## Package layout

```
connectors/Pz.Connector.MySql/
├── MySqlConnector.cs      # IConnector/ISourceConnector/ISinkConnector, connection schema, capabilities
├── MySqlSql.cs            # all SQL/text generation: attach strings, scan fragments, DDL, quoting
├── MySqlSource.cs         # ISource: native scan (always succeeds), PZ0312 refusal stub, schema-from-contract
├── MySqlSink.cs           # ISink: native copy (append/replace), PZ0312 refusal stub
├── MySqlGreeting.cs       # the zero-driver TCP handshake-packet connectivity probe
├── MySqlTypeNameMap.cs    # columns: contract type name → Arrow type matrix
└── pz.connector.json      # manifest: name "mysql", protocol major range, capabilities [source, sink]
```

The package embeds `pz.connector.json` (readable without loading the assembly — an incompatible
protocol version is rejected before any package code runs) and is one of the repo's first-party
connectors bundled into the `pz` tool package itself; see [Connectors: discovery, packaging, and
restore](https://pipelinez.dev/concepts/connectors/#discovery-packaging-and-restore).

## Testing

[`tests/Pz.Connector.MySql.Tests`](../../tests/Pz.Connector.MySql.Tests) runs offline SQL-generation,
capability, and secret-redaction tests (always CI-safe) plus an end-to-end suite against a real
MySQL container (`MySqlContainerFixture`, Testcontainers). Every docker-backed test is
`[SkippableFact]` and skips cleanly when Docker isn't available (or `PZ_TESTS_OFFLINE=1` is set,
since `install mysql` needs network access to the extension repository):

```bash
dotnet test tests/Pz.Connector.MySql.Tests -c Release
```

## See also

- [`samples/mysql-native`](../../samples/mysql-native/) — a runnable MySQL → MySQL template
  showing both delivery shapes (incremental → append log, full → replace snapshot).
- [Connectors](https://pipelinez.dev/concepts/connectors/) — the ABI these types implement, and why.
- [Delivery guarantees](https://pipelinez.dev/concepts/delivery-guarantees/) — what `append`/`replace`
  guarantee on commit/crash.
- [Backfill in bounded slices](https://pipelinez.dev/how-to/backfill-in-slices/) — bounded windows from the
  user's side.
- [Secure connection config](https://pipelinez.dev/how-to/secure-connection-config/) — keeping credentials
  out of `connections.yml`.
- [Author a connector](https://pipelinez.dev/how-to/author-a-connector/) — the ABI this connector
  implements, for anyone writing a new one.
