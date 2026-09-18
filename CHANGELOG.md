# Changelog

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions are git
tags (`v*`), computed into package versions by MinVer. Breaking changes are listed
first in each release, each with a migration note — see
the [versioning policy](https://pipelinez.dev/versioning/).

## [Unreleased]

### Changed

- **Quoted YAML scalars are strings.** The loader typed every scalar by its
  text and ignored the quotes, so `password: "0123456"` reached the connector as
  `123456`, a connector `version: "1.10"` restored package `1.1`, and `"true"`
  became a boolean — undoing the quoting `pz mcp`'s authoring tools add around
  number-like strings. Only plain (unquoted) scalars are typed now; quoted and
  block (`|`, `>`) scalars stay text.
  *Migration:* a value that must be a number or a boolean must not be quoted in
  pz's own keys — `threads: "4"` is now refused with PZ0120 where it used to be
  read as `4`. Connector options are unaffected where the connector reads them
  through `ConnectorConfig.GetInt`/`GetBool`, which still accept `port: "5432"`.
- An unquoted decimal connector version (`version: 1.10`) is refused: YAML reads
  it as the number 1.1, a different package. Quote it.

### Added

- Connectors TestKit: `ColumnPruning_yields_exactly_the_hinted_columns_in_hint_order`,
  the acceptance fact for the `ColumnPruning` capability. It plans a read with a
  non-prefix, reordered column hint and requires every batch to carry exactly
  those columns in that order, with the unhinted read's row count. Sources that
  do not declare the capability skip it; existing subclasses need no changes.
  A connector that declares `ColumnPruning` but yields the hinted columns in
  its declared order rather than the hint's order now fails this fact; a
  subclass that cannot satisfy it for a structural reason can exclude it
  through `ShouldRun`.
- `engine.node_timeout` in `project.yml` (a duration such as `45m`; absent means
  unbounded, as before): the longest one attempt of one node may run. Nothing
  bounded a node until now, so a stalled read, a hung connector process, or a
  stuck `ATTACH` hung the run forever. A node that exceeds the limit is
  cancelled and fails with **PZ0525**, naming the node and the knob. It is not
  retried inside the same run — a cancelled attempt can leave half-built
  staging behind — and `pz retry` reruns it. If the cancelled work ignores
  cancellation for 30 s the node fails with **PZ0526** and the rest of the run
  is cancelled, because abandoned work may still hold the run's DuckDB
  connection. The clock is wall time for the attempt, including time spent
  queued for the run's one DuckDB connection, so size it for the slowest node
  plus what may run ahead of it.

### Fixed

- Cancelling a run now interrupts a statement already running inside DuckDB.
  Ctrl-C (and the new node timeout) used to wait for the statement to finish on
  its own, however long that took.

- A transient failure an out-of-process connector raises mid-stream (a rate
  limit thrown from a partition's read after its first batch, or from a sink's
  write) now reaches the engine with its transience and retry-after intact and
  is retried, instead of surfacing as a permanent "stream truncated" or
  "connection closed" failure. The data plane carries bytes and no diagnosis,
  so PCP gains a side-effect-free `GetStreamFailure` RPC: the host asks a
  still-alive connector why a stream tore, and both SDKs (C# and Rust) record
  the failure before closing the stream so the answer exists by the time the
  host asks. A connector built before this RPC answers UNIMPLEMENTED, which
  the host treats as "no failure known" and falls back to its previous
  diagnosis, so no connector needs rebuilding.
- The shape guards on the Connectors SDK data plane and on the engine's Arrow
  ingest now compare Arrow types structurally: nested child types, decimal
  precision and scale, timestamp unit and timezone, fixed-size widths,
  dictionary and union parameters, and an extension type's storage. A batch
  whose `list<utf8>` arrives where `list<int32>` was declared is refused with
  both shapes spelled out, instead of reaching DuckDB. The comparison is
  strict about timestamp unit and timezone spelling: a connector whose
  declared schema says `+00:00` and whose batches say `UTC` is refused with
  both shapes named.
- An exception no verb anticipated now ends as `error PZ0500: internal error …`
  with exit code 3 and a request to report it, instead of a raw stack trace
  with exit code 1 (which the exit-code contract reserves for node failures).
  `PZ_DEBUG=1` adds the stack trace.
- Two identical checks on one pipeline (`- not_null: [id]` twice) are refused
  as PZ0113 "duplicate check" at load. They compile to one node id, and used to
  crash `pz compile`/`pz run` with an `ArgumentException`.
- Distinct checks that share the conventional node name — two `row_count`
  checks, two `accepted_values` on one column, `not_null: [a_b]` beside
  `not_null: [a, b]` — get distinct names: the first keeps
  `check_<pipeline>_<type>_<columns>`, later ones take `_2`, `_3`, … so
  `--select` addresses exactly one. Node ids are unchanged.
- `pz run`/`retry`/`test` now wind down on SIGTERM and SIGHUP the way they do on
  Ctrl-C, for as long as that takes: nodes are cancelled cooperatively, the
  renderer drains, `run_results.json` gets its terminal status, connectors are
  shut down, and the exit code is 3. Until now a stop signal (the default from
  systemd, Docker, Kubernetes and Airflow) — and Ctrl-C too — cancelled the run
  and then **force-exited the process two seconds later** with 143/130,
  whatever it was doing: a run slower than that to unwind was cut off between a
  sink's commit and its watermark, left `run_results.json` at `running`, and
  orphaned connector processes. The handlers now cover the whole run, setup and
  finalization included. A second signal terminates immediately, so a run that
  will not wind down is never a trap.
- **Wrong rows.** Predicate pushdown no longer filters a source ahead of a join
  that could have null-extended it. `select b.id from b left join
  {{ source(…) }} a on … where a.id is null` pushed `id IS NULL` to the source,
  landed nothing, and the anti-join returned every `b` row. A predicate is now
  pushed only when every join above the source preserves its rows (inner/cross,
  the preserved side of left/right, the left of semi/anti); full, asof and
  positional joins push nothing. A self-join pushes no predicate, a source also
  read from a CTE or subquery pushes nothing at all, and a predicate over a
  select-list alias or beside a derived table stays in DuckDB. Affects
  connectors with `PredicatePushdown`.
- Column pruning no longer drops a column a join still needs. With more than
  one relation in scope, an unqualified column, a `USING`/`NATURAL` join, or a
  qualifier that is not a FROM alias now means "read every column" instead of
  being skipped; `select order_id from {{ source(…) }} o join c using
  (customer_id)` failed to bind `order_id`. A struct path `o.payload.kind`
  keeps `payload`, and CTE bodies no longer widen or defeat the hint. Affects
  connectors with `ColumnPruning`.
- Out-of-process connectors: a connector that writes to stdout no longer hangs
  the run. The host redirected the child's stdout and never read it, so after
  about 64 KB (a `Console.WriteLine` per batch, a Rust `println!`, a chatty
  library) the child blocked in `write` forever with no diagnostic. Stdout is
  now drained and discarded; stderr remains the connector's diagnostic channel.
- Overlapping `pz run`s in one project no longer lose watermark updates. The
  local state files are rewritten whole on every write with nothing guarding
  the read-modify-write, so two runs finishing together kept only one run's
  entries, and the next incremental run re-extracted the other's datasets.
  Writes now happen under an OS-held lock on a sibling `.lock` file (freed by
  the OS if the holder dies). Two runs advancing the *same* dataset now get
  PZ0520 for the later writer — the contract the SQL Server and HTTP state
  backends already keep — instead of the later finisher silently regressing
  the watermark to its older `MAX(cursor)`. Overlapping runs over different
  datasets remain supported.
- Overlapping runs no longer die with `PZ0500 … plan.json … being used by
  another process`: `manifest.json`, `compiled/*.sql`, `plan.json` and
  `schemas.json` are written aside and renamed into place, so every writer
  succeeds and a reader never sees a partial file.
- Watermark and sync-state advancement no longer stops at the first dataset
  whose write fails. With a remote state store, one deadlock or conflict on
  dataset 1 meant datasets 2..N never advanced either, and the next run
  re-extracted all of them. Each dataset is now written on its own; an
  unreachable store (PZ0518) is retried up to three times; and the run's note
  carries **PZ0527** and names every dataset that did not advance and why,
  instead of reporting only the first exception. The exit code is unchanged:
  the sinks committed, so the run still reports its node outcome.
- Typed YAML values are rendered as text the way they were written, on every
  host: `ConnectorConfig.GetString`, the loader, and the HTTP connector's
  `query:`/`headers:` values used the machine's culture and .NET's spelling, so
  an unquoted `archived: true` was sent as `True` and `1.5` as `1,5` on a
  comma-decimal locale.
- **Wrong columns.** Native `append` into SQLite, MySQL, DuckDB, DuckLake,
  MotherDuck, Quack and Iceberg now matches columns by name
  (`insert into … by name`). It was positional, so appending into a table that
  predates pz, or orders its columns differently from the pipeline's select
  list, succeeded whenever the types happened to be compatible — with every
  value in the wrong column. A target column the pipeline does not produce
  keeps its default; a produced column the target lacks is now an error naming
  it, where a same-width positional insert used to accept it.
- Out-of-process connectors: a connector process now ends when the source or
  sink opened on it is disposed, instead of living until the run ends. The
  engine opens a connection once per node, so a project with a hundred entities
  on one external connection accumulated a couple of hundred live child
  processes — each with its gRPC channel and pump, each possibly holding a
  remote connection — over the course of one run. Live children now track the
  nodes in flight (bounded by `engine.threads`).

## [0.6.1] - 2026-09-10

### Changed

- Every published NuGet package now carries the pz icon.

### Fixed

- Out-of-process connectors: a read with a column-pruning hint that is not a
  leading prefix of the declared schema no longer crashes `pz run`. The
  Connectors SDK opened the data-plane stream with the declared (unpruned)
  schema, so pruned batches were decoded by position into the wrong columns
  and handed to DuckDB as garbage. The SDK now opens the stream with the
  declared schema narrowed to the hint, refuses a batch shaped differently
  from the stream, and the engine refuses a batch shaped differently from
  the staging table before DuckDB sees it. Connectors built on
  `Pz.Connectors.Sdk` 0.6.0 or earlier must be rebuilt on the fixed SDK to
  pick up the stream fix; the engine-side guard turns the crash into a node
  failure for connectors that have not been. Until a connector is rebuilt, a
  pruning read that previously happened to produce correct rows (a hint that
  is a leading prefix of the declared schema) now fails the node cleanly as
  well: with this release and an un-rebuilt pruning connector, every pruned
  read fails until that connector ships on the fixed SDK.

## [0.6.0] - 2026-09-08

### Added

- Out-of-process connector telemetry: `HostInfo` carries the run id and OTLP
  endpoint, and PCP injects W3C trace context into the connector handshake, so
  a connector process's own OpenTelemetry spans and meters nest under the
  engine's node span. Both connector SDKs build their OTel providers from
  `HostInfo`, parent `pcp.<Rpc>` server spans and data-plane stream spans on the
  host's traceparent, bound telemetry flush at shutdown to the aggregate flush
  bound, and restore the ambient activity after a root span. The Rust SDK
  additionally exports tracing spans and meters to the host's OTLP endpoint,
  and `pz-connector` exposes a composable tracing layer with span attributes
  aligned to the C# SDK.
- Hosted connector instances are named after the connection (`pz.instance`),
  and the engine opens the run span around every phase, not just execute.

### Fixed

- Rust SDK: a pre-installed subscriber is reported instead of silently
  exporting nothing; a flush that misses its bound says so, and exporter
  warnings surface on stderr; the write-stream span drops noisy code/thread
  attributes.
- Connectors SDK: a header-less RPC now roots its own span and reports an
  unusable endpoint instead of failing silently.

## [0.5.2] - 2026-09-07

### Fixed

- Connectors SDK: `PzPackaging=aot` fails loudly at restore time when the
  restored package never saw `PublishAot` (PZSDK005), instead of silently
  falling back to a CoreCLR-hosted package that PZ0360 then refuses.

## [0.5.1] - 2026-09-07

### Fixed

- Connectors SDK: a project-level `PzPackaging` MSBuild property is honored
  instead of being overridden; `pz connector test`'s config now interpolates
  environment variables.

Everything below ships in the first public release.

### The tool

- `pz` as a self-contained .NET global tool: `init`, `compile`, `plan`, `validate
  [--connect]`, `restore`, `run`, `test`, `retry`, `ls`, `connectors`, `cdc`,
  `clean`, `state`, `schema`, `mcp`. Exit codes 0/1/2/3; `--log-format json`
  emits the documented NDJSON event stream (https://pipelinez.dev/events/).
- dbt-shaped authoring: one `connections.yml` (connections → entities), SQL
  pipelines with `ref()`/`source()`/`sink()` templating, checks as first-class
  nodes, a compiled and inspectable DAG under `.pz/target`.
- DuckDB as the engine: sources stage into a per-run DuckDB database, SQL runs
  in-process, sinks drain out over zero-copy Arrow; two data-plane tiers (native
  scan/copy and the universal Arrow path) chosen per node by the planner.
- Incremental EL: SQL-declared watermarks, bounded backfill windows, commit-gated
  advancement, per-instance retries and circuit breakers, `pz retry` with staged
  data reuse and carried-forward sinks, CDC from Postgres (pgoutput) and SQL
  Server (change tables) with `pz cdc status`/`drop`.
- Correctness guardrails: schema-drift detection (`on_source_drift`), duplicate
  merge-key warnings (PZ0522), lossy integer-inference warnings (PZ0523),
  ambiguous date-order warnings (PZ0524), memory budgeting with disclosed limits.
- Eight first-party connectors: local files (csv/NDJSON/parquet), Postgres,
  SQL Server, MySQL, SQLite, S3-compatible object storage (source and sink,
  with a documented Google Cloud Storage recipe via its S3 interoperability
  mode), Azure Blob Storage, HTTP APIs. Object-store format parity: parquet,
  csv, and NDJSON json on local files, S3, and Azure Blob alike, in both
  directions.
  Third-party connectors ship as ordinary NuGet packages against the versioned
  ABI in `Pz.Connectors.Abstractions`, with `Pz.Connectors.TestKit` as the
  acceptance suite.
- `pz mcp`: a Model Context Protocol server exposing typed introspect/verify/
  author tools (execution gated behind `--allow-run`), with credential and
  path-containment guards for agent-driven use.
- State that can leave the machine: SQL Server- and HTTP-backed state stores for
  ephemeral hosts; local deterministic JSON artifacts otherwise.
