# `iceberg` connector

Apache Iceberg source + sink for PipelineZ (`pz`), registered under the connector name `iceberg`.
**DuckDB's own `iceberg` extension is the entire data plane**: the engine's session attaches the
catalog once per connection and every read/write is a plain SQL statement against that alias, over
the [native scan/copy data-plane tier](https://pipelinez.dev/concepts/data-plane/), never the
universal Arrow-stream tier. The connector ships **zero drivers** — it is pure SQL-fragment
generation, exactly like the ducklake and duckdb connectors, over an Iceberg *catalog* (a REST
catalog, AWS Glue, Amazon S3 Tables — or none at all, for reading table directories straight from
storage) and the object store where the tables' Parquet and metadata files live.

Source: `connectors/Pz.Connector.Iceberg/`. Contract test suite:
[`tests/Pz.Connector.Iceberg.Tests`](../../tests/Pz.Connector.Iceberg.Tests): sqlgen, catalog
matrix, connector, planner, and secret-redaction suites run offline; an end-to-end suite drives a
real REST catalog (the upstream Apache `iceberg-rest-fixture` image) over a MinIO warehouse and skips
cleanly without Docker (`Xunit.SkippableFact`/`DockerFacts`).

This page is the connector's own reference. For the *why* behind the ABI it implements, see
[Connectors](https://pipelinez.dev/concepts/connectors/); for the authoring surface (`connections.yml`,
`source()`/`sink()`), see [Project structure](https://pipelinez.dev/concepts/project-structure/).

## Capabilities

Declared in `IcebergConnector.Capabilities`:

| Flag | What it means here |
|---|---|
| `NativeScan` | every read is a scan fragment over the connection's attach alias (or an `iceberg_scan` call for a `files` connection) |
| `NativeCopy` | every write is native DuckDB SQL against the same attach alias — `INSERT`/`DELETE`+`INSERT`/`MERGE INTO` |
| `ReplaceWrites` | the sink supports `strategy: replace` |
| `Merge` | the sink supports `strategy: merge` (DuckDB's own `MERGE INTO`, matched on declared key columns) |
| `Transactional` | the write applies as a unit — a replace's DELETE and INSERT land together or not at all, even though DuckDB's iceberg extension still records them as two snapshots (a delete then an append), never one combined `overwrite` snapshot |
| `BoundedWindow` | `cursor <= upper` is applied alongside `cursor > lower` — required for the windowed-incremental trio (`initial`/`max_window`/`until`) |
| `InclusiveWatermarkBound` | the connector honors an inclusive lower-bound watermark comparison (`>=` instead of `>`) when the engine asks for one |

Plus the `INativeOnlySource`/`INativeOnlySink` marker interfaces: `engine.force_universal` fails at
**plan time** with `PZ0312` on either direction — there is no universal-tier fallback to fall back
to.

**Not declared:** `ColumnPruning`/`PredicatePushdown` (those drive universal-tier `ReadHints`;
pruning happens anyway, inside the scan fragment itself), `PartitionedRead`, `ChangeCapture`,
`GatedOperations`/`SyncState`/`PathTemplating`/`TextLengthStats`.

## Connection (`connections.yml`)

Every connection names a **catalog** (`catalog:` — defaults to `rest` when omitted).
`ConnectionConfigSchema` rejects any key outside the matrix below (`additionalProperties: false`);
`IcebergCatalog.Validate` then enforces which keys each catalog requires and which belong to a
different catalog, aggregate — every stray or missing key comes back as one error naming the
catalog it belongs to, so a whole block is fixed in one pass.

### `rest` — an Iceberg REST catalog (the default)

```yaml
lake:
  connector: iceberg
  catalog: rest                          # optional — this is the default
  endpoint: https://catalog.example.com/api   # required — the REST catalog URI
  warehouse: my_warehouse                # optional — what the catalog calls the warehouse (a name or an id, never a URL)
  token: ${LAKE_TOKEN}                   # bearer token — or the OAuth2 pair below, never both
  # client_id: ${LAKE_CLIENT_ID}
  # client_secret: ${LAKE_CLIENT_SECRET}
  # oauth2_server_uri: https://catalog.example.com/api/v1/oauth/tokens   # optional, with the pair
  # oauth2_scope: PRINCIPAL_ROLE:ALL                                    # optional, with the pair
  nested_namespaces: false               # optional — set true for catalogs that nest namespaces
```

Requires `endpoint` (an `http://`/`https://` URL). Forbids `root`. Authentication is a bearer
`token` **or** a `client_id`/`client_secret` pair (declared together; `oauth2_server_uri` and
`oauth2_scope` tune the pair and mean nothing without it) — declaring both forms is an error;
declaring neither attaches unauthenticated (`authorization_type 'none'`, the right thing for a local
development catalog). Any of Polaris, Lakekeeper, Nessie, Cloudflare R2, Unity Catalog, Google
BigLake, or the Apache REST fixture is a `rest` catalog; `warehouse` is whatever that catalog wants
in the ATTACH string (a Polaris catalog name, an R2 warehouse id, a Lakekeeper warehouse name).

### `glue` — AWS Glue

```yaml
lake:
  connector: iceberg
  catalog: glue
  warehouse: "123456789012:my_catalog"   # optional — ':' (the caller's default catalog) when omitted
  storage_region: eu-central-1           # optional — defaults to us-east-1
```

Forbids `endpoint`, every `rest` credential key, and `root`. Signs with the ambient AWS credential
chain (environment, profile, instance role) unless `storage_key_id`/`storage_secret_key` are declared.
The `warehouse` forms Glue accepts: `:`, an account id, `account_id:catalog`, `catalog/sub_catalog`,
`account_id:catalog/sub_catalog`.

### `s3_tables` — Amazon S3 Tables

```yaml
lake:
  connector: iceberg
  catalog: s3_tables
  warehouse: "arn:aws:s3tables:us-east-1:123456789012:bucket/my-table-bucket"   # required
```

Requires `warehouse` (the table bucket ARN). Forbids the same keys as `glue`; same credential rule.

### `files` — no catalog: table directories under a root (read-only)

```yaml
lake:
  connector: iceberg
  catalog: files
  root: "s3://my-bucket/warehouse/"      # required — a local directory or an object-store URL
```

Requires `root`. Forbids `endpoint`, `warehouse`, `nested_namespaces` and every `rest` credential
key. Every read is `iceberg_scan('<root>/<namespace>/<table>', allow_moved_paths = true)`; there is
nothing to commit a write to, so a `files` connection used as a sink is refused at plan time
(`PZ0353`). A table without a `version-hint.text` (every table a REST catalog wrote) needs the
dataset option `metadata_version:` naming the metadata file to read (see below).

### Optional: S3-compatible storage credentials

```yaml
lake:
  connector: iceberg
  catalog: rest
  endpoint: http://minio-catalog:8181
  warehouse: dev                         # a NAME — validation refuses a URL-shaped warehouse (DuckDB would attach it read-only)
  storage_key_id: ${LAKE_S3_KEY}
  storage_secret_key: ${LAKE_S3_SECRET}
  storage_region: us-east-1              # optional — defaults to us-east-1
  storage_endpoint: minio.internal:9000  # optional — for an S3-compatible endpoint
  storage_url_style: path                # optional — "vhost" (default) or "path"
  storage_use_ssl: false                 # optional — defaults to true
```

`storage_key_id` and `storage_secret_key` must be declared together; `storage_endpoint`/
`storage_url_style`/`storage_use_ssl` require the pair (`storage_region` stands alone — it is also
the credential chain's region). When present they build a `type s3` DuckDB secret **scoped** to the
`files` root, so the credentials apply to that root's tables and nothing else in the session; a
catalog connection's secret is unscoped (the catalog hands out each table's location, so there is
nothing to scope to up front — DuckDB still prefers a longer-scoped secret, such as the s3
connector's own, for any path one covers). On a `rest` catalog the keys also switch credential vending off (`access_delegation_mode 'none'`): the keys ARE the
data-plane credential, and a catalog that cannot vend (a MinIO-backed development catalog, the
Apache REST fixture) would otherwise be asked to. Without them a REST catalog is expected to vend
storage credentials itself (Polaris, S3 Tables, Glue, R2 all do).

### Optional: Azure storage (`storage: azure`)

```yaml
lake:
  connector: iceberg
  catalog: rest
  endpoint: https://lakekeeper.internal/catalog
  warehouse: adls-wh                     # a NAME, as for every catalog
  token: ${LAKE_TOKEN}
  storage: azure                         # the tables' data files live on Azure Blob / ADLS Gen2
  storage_auth: service_principal        # or connection_string | account_key | credential_chain
  storage_tenant_id: ${AZ_TENANT}
  storage_client_id: ${AZ_CLIENT}
  storage_client_secret: ${AZ_SECRET}
  storage_account_name: mylakeaccount

raw:
  connector: iceberg
  catalog: files
  root: "abfss://lake@mylakeaccount.dfs.core.windows.net/warehouse/"   # az://, azure:// or abfss://
  storage_auth: credential_chain         # storage: azure is inferred from the root's scheme
  storage_account_name: mylakeaccount
  storage_chain: cli;env                 # optional
```

`storage` selects the key family: `s3` (the default, the keys above) or `azure`. Under `azure`
the keys mirror the azureblob connector's `auth` methods field-for-field, prefixed `storage_`
because `client_id`/`client_secret` already name a REST catalog's OAuth2 pair here:

| `storage_auth` | required | optional |
|---|---|---|
| `connection_string` | `storage_connection_string` | — |
| `account_key` | `storage_account_name`, `storage_account_key` | `storage_endpoint` (a custom Blob endpoint, e.g. Azurite) |
| `service_principal` | `storage_tenant_id`, `storage_client_id`, `storage_client_secret`, `storage_account_name` | — |
| `credential_chain` | `storage_account_name` | `storage_chain` (e.g. `cli;env`; managed identity is a link in the chain (a user-assigned identity's client id cannot be pinned here)) |

Every S3 key is refused under `azure` and every Azure key under `s3`; `storage: azure` is refused
on `glue`/`s3_tables`. A `files` root with an Azure scheme infers `storage: azure` and needs a
`storage_auth` (nothing vends credentials for a bare root); a `rest` catalog may omit
`storage_auth`, in which case the catalog is expected to vend Azure SAS credentials. The
connection loads DuckDB's `azure` extension and, when a method is declared, builds a `type azure`
secret — scoped to a `files` root, unscoped on a catalog — and switches the REST catalog's
credential vending off exactly as explicit S3 keys do. As with S3, DuckDB prefers a longer-scoped
secret for any path one covers, so an azureblob connection's scoped secret wins over an iceberg
catalog connection's unscoped one in the same session.

**Status of writes on Azure.** The `azure` extension DuckDB 1.5.5 installs implements the
directory and write operations the iceberg extension's insert needs, but DuckDB's own
documentation still lists REST catalogs as supported on S3, S3 Tables and GCS only, and no local
emulator can host an Azure-backed catalog (Azurite has no ADLS/DFS endpoint). This repository's CI
therefore proves `files` reads over `az://` (Azurite) and ships REST writes on Azure as
extension-supported but unproven; `tests/Pz.Connector.Iceberg.Tests/IcebergAzureRestTests.cs`
runs the write round-trip when `PZ_ICEBERG_AZURE_ENDPOINT`/`PZ_ICEBERG_AZURE_WAREHOUSE` (and
optionally `PZ_ICEBERG_AZURE_TOKEN`, `PZ_ICEBERG_AZURE_ACCOUNT_NAME`/`_KEY`) point at a real catalog.

### Entities and namespaces

The **entity is `namespace.table`** — an Iceberg table always lives in a namespace, so a bare
`table` is refused on a catalog connection (a `files` entity may be bare: a table directory directly
under `root`). Nested namespaces (`a.b.table`) are not supported. The namespace **`main`** cannot be
addressed: DuckDB's binder reserves that name for its own default schema and never asks the catalog
for it, so the connector refuses it up front rather than letting the read fail with a misleading
"schema not found". A relative local `root` resolves against the **project directory** (the same
`base_dir` mechanism localfiles/sqlite/duckdb/ducklake use, injected internally by the host — never
write `base_dir` yourself); it may not resolve inside the project's `.pz/` directory (the run's own
staging/state area).

Under `pz mcp`, a local `root:` that resolves outside the project directory is refused with
`PZ0606`, exactly like localfiles/sqlite/duckdb/ducklake; an object-store `root` (any value
containing `://`) is skipped by that guard.

## Reading data

An entity in `entities:` (or the same options at a `source()` call site) names the table and any
read options:

```yaml
lake:
  connector: iceberg
  endpoint: https://catalog.example.com/api
  token: ${LAKE_TOKEN}
  entities:
    raw.events:
      read:
        columns: { id: bigint, updated_at: timestamp, amount: double }
        sync: { mode: incremental, cursor: updated_at }
    raw.snapshot_events:
      read:
        columns: { id: bigint, updated_at: timestamp }
        version: 4830783628919130688    # a snapshot id — or timestamp: "2026-08-15 00:00:00", never both
```

Every read is a scan of the qualified table, time-travelled and filtered inline, wrapped in a
subquery only when there is something to add —

```sql
(select "id", "updated_at" from pz_iceberg_lake_a1b2c3d4."raw"."events" at (version => 4830783628919130688)
  where "updated_at" > '2026-08-01 10:00:00' and "updated_at" <= '2026-08-15 00:00:00')
```

- A declared `columns:` contract **prunes the read** — only declared columns are projected.
  Contract-less reads take the table as the catalog declares it, but also mean `pz validate
  --connect` cannot probe a schema for that dataset (there is no offline driver to ask — the
  contract *is* the schema).
- The plain incremental watermark **is pushed into the fragment** (the database-source rule); the
  windowed pair (`initial`/`max_window`/`until`) is MUST-apply.
- **Time travel**: a dataset may declare `version:` (a snapshot id) or `timestamp:` (the snapshot
  current at an instant — a string DuckDB's own parser validates, or a `DateTime`/`DateTimeOffset`
  reachable through the Scriban kwarg surface, rendered invariantly regardless of host culture),
  never both — declaring both fails at plan time.
- A **`files`** read is `iceberg_scan('<root>/<namespace>/<table>', allow_moved_paths = true, …)`
  with `version:` → `snapshot_from_id`, `timestamp:` → `snapshot_from_timestamp`, and
  `metadata_version:` → `version` (the metadata file to start from, e.g. `00003-<uuid>` for a
  catalog-written table whose directory carries no version hint; `metadata_version` is refused on a
  catalog connection, where the catalog resolves the current metadata itself).

## Writing data

One read-write attach per connection, shared by every read and write against that connection. Every
mode first ensures the namespace (`create schema if not exists`) and creates the target from the
staged shape (`create table if not exists … as select * from {{source}} limit 0`) so a first run
needs no pre-created namespace or table. Then:

- `strategy: append` — `insert into … select * from {{source}};` (one `append` snapshot).
  At-least-once: an incremental source feeding an append sink still requires
  `write: { duplicates: accept }` (PZ0214).
- `strategy: replace` — `begin transaction; delete from …; insert into … select * from {{source}};
  commit;`. DuckDB's iceberg extension commits one snapshot per DML statement — there is no
  single-snapshot `overwrite` it can be asked for — so this is always **two** new snapshots, a
  `delete` immediately followed by an `append`, never one combined commit. What the wrapping
  transaction buys instead: neither snapshot reaches the catalog until `commit`, so a concurrent
  reader sees the old rows and no new snapshot right up to that instant, then both the delete and
  the append snapshot at once — never an empty table, never one without the other (proven against
  the live REST fixture in `IcebergRestCatalogTests.Replace_is_invisible_to_other_readers_until_commit`).
  The table keeps its identity and history either way (DuckDB's iceberg extension has no
  `CREATE OR REPLACE`, and a drop-and-recreate would discard every earlier snapshot). The delete is
  merge-on-read (positional delete files), so a table replaced many times benefits from the
  catalog's compaction/maintenance.
- `strategy: merge` — `merge into … as t using (select s.* from {{source}} as s qualify
  row_number() over (partition by <keys>) = 1) as s on <keys match> when matched then update when
  not matched then insert;`. The staged side is keyed unique first because DuckDB's MERGE matches
  every source row independently against the pre-statement target, so duplicates of a key the target
  lacks would all insert; one connector-determined survivor per key is the sink contract, and the
  engine warns (PZ0522) with counts when a batch carried duplicates. Requires at least one declared
  key column (refused at compile time otherwise).

A `files` connection cannot write: only a catalog can commit new table metadata.

## `pz validate --connect` behaviour

Zero drivers, so the check per catalog is necessarily shallow — credentials are exercised only by
the first run's attach:

| Catalog | Check |
|---|---|
| `rest` | TCP reachability to the `endpoint` host/port only (a 5-second timeout); credentials are verified at run time |
| `glue`, `s3_tables` | **not checked** — "not checked: an AWS catalog has no offline probe; the first run authenticates" |
| `files` | a local `root` directory must exist (reads cannot create it); an object-store `root` is not checked |

The `--connect` schema precheck works only for datasets with a declared `columns:` contract (the
contract IS the schema); contract-less datasets get a clear refusal. Plain `pz validate`, `pz run`,
and the `on_source_drift` gate (which baselines from the staged DESCRIBE) are unaffected.

## Behaviours to know

- **Credentials never ride the attach string.** A bearer token or OAuth2 client pair builds a
  `type iceberg` DuckDB secret the attach references by name; AWS catalogs sign with a `type s3`
  secret (explicit keys, or `provider credential_chain`); storage keys build a `type s3` secret
  scoped as described above, or a `type azure` secret under `storage: azure`. A failed attach
  therefore echoes only the warehouse and the endpoint — never a credential — and a malformed
  carrier statement is redacted before it reaches a run result (`PZ0311`).
- **A `files` read refuses a missing local table directory** at plan time (`PZ0353`) — almost
  always an entity or `root` typo, and `iceberg_scan`'s own error would name the absolute path.
- **Setup statements run once per run.** The engine issues each distinct setup statement once per
  run and every node that needs it shares that execution; a node retry re-issues a statement that
  failed, which the statements tolerate (extension install/load are no-ops on repeat, `create or
  replace secret` is last-wins, `attach if not exists` skips an existing alias).
- **First use needs network access** to install the DuckDB `iceberg` and `httpfs` extensions
  (`azure` under `storage: azure`, `aws` for a credential-chain AWS catalog); the extension
  repository is consulted only when an extension is not yet installed.
- **A catalog and a `files` connection may point at the same warehouse.** They get separate aliases
  and separately scoped secrets; reading a table through `files` right after the catalog wrote it
  needs the newest `metadata_version:` — a `files` read never consults the catalog.
