# Changelog

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions are git
tags (`v*`), computed into package versions by MinVer. Breaking changes are listed
first in each release, each with a migration note — see
the [versioning policy](https://pipelinez.dev/versioning/).

## [Unreleased]

### Added

- **`IProjectingNativeScan`**, for connector authors whose native scan can read only the requested columns.

### Fixed

- **DECIMAL(38) values beyond `System.Decimal` are no longer silently corrupted.** Arrow's
  `Decimal128Array.GetValue` cannot hold a value with more than 28 significant digits (or a scale above 28) and
  drops digits without an error, so such values left DuckDB changed. Universal-tier egress, the CSV and NDJSON
  writers (and the NDJSON reader), the SqlServer sink, the Azure/GCS/SFTP partitioned writers, the SFTP
  incremental window filter and check result formatting now carry every digit. The Postgres sink cannot (Npgsql
  writes `numeric` only from `System.Decimal`), so a universal-tier write of such a value now fails, naming the
  column, instead of committing a wrong number; round or cast the column in the pipeline. Connector authors get
  `Decimal128Cells` in `Pz.Connectors.Abstractions.Batches` for exact reads, `ArrowBatchBuilder` accepts a
  `SqlDecimal` for a decimal column, and the TestKit sink suite gains a beyond-range decimal fact on the
  type-matrix hooks.

### Changed

- **A slow source no longer blocks other nodes' DuckDB work.** Channel-path ingest (Postgres, SqlServer, Sftp,
  Azure Blob and GCS reads) held the session gate for the whole stream, so every other node's DuckDB work waited
  for the slowest source. It now takes the gate once to create the table and once per batch. With a Postgres
  source streaming 400k rows over ~9 s next to an independent local 1M-row CSV group-by chain, that chain
  finished at ~9.25 s before and ~0.23 s after (alone: ~0.19 s). A slow sink still blocks other nodes' DuckDB
  work: egress drains and transactions hold the gate for their whole duration.
- **Faster compile and egress.** Compile reuses one DuckDB parse session per command (a wide project with 200
  sources: 2250 to 284 ms). Universal egress concatenates DuckDB chunks by column (1M rows: 246 to 73 ms; mixed
  utf8/decimal/timestamp/date: 576 to 102 ms). `scripts/macro-bench.sh` times the built CLI instead of
  `dotnet run`, `scripts/scale-bench.sh` measures project-size scaling, and `PZ_BENCH_INPROCESS=1` switches the
  benchmarks to BenchmarkDotNet.
- **Egress concatenates straight into pooled native memory.** Universal egress writes each concatenated
  column directly into the pooled allocator instead of staging it on the managed heap. Managed allocation per
  1M-row egress drops from ~38.5 MB to ~1.1 MB (mixed types: ~63.7 to ~1.3 MB), and Gen2 collections from
  10,000 to 6,000 and from 17,000 to ~10,000 per 1k operations (BenchmarkDotNet, in-process).
- **Native scans stage only the columns and rows the pipeline reads; less per-node overhead.** A wide Parquet source
  (2M rows, 50 columns) read by `select id, c1 from … where k = 7` now stages 3 columns and 20,000 rows instead of
  50 columns and 2,000,000 rows (SourceLoad 19.8 s to 63 ms, min of 3). A native SourceLoad's `rows`
  (`run_results.json` and NDJSON) now counts the filtered staged rows, and `pz plan` reports pushdown for native
  scans. Per-node overhead drops too: `run_results.json` is written in the background (except right after a sink
  commit), watermark and sync-state writes are batched for the local state store (the HTTP and SQL Server backends
  still write per key), and the dry compile runs in memory (`scripts/scale-bench.sh`, wide-200, 600 nodes: execute
  2014 to 1963 ms). The drift baseline re-seeds once on the first run after upgrading. Drift detection on native
  scans now covers the columns the pipeline reads, so an added column the pipeline never reads is no longer
  reported, as on the universal tier's pruning connectors.

## [0.11.3] - 2026-10-16

### Fixed

- **Iceberg `storage: azure` on Linux.** The iceberg connector loaded DuckDB's azure extension without choosing a
  transport, so on Linux every HTTPS request to Azure Storage failed with `Problem with the SSL CA cert` (the bug
  0.11.2 fixed for azureblob). It now sets `azure_transport_option_type = 'curl'` on Linux, as azureblob does.
- **Native IO errors keep the detail that explains them.** Redaction masked every quoted literal in a DuckDB error,
  so an Azure CA-bundle failure read as `could not open file: '***' ... Original error message: '***'`. Object/file
  URLs are now kept (userinfo and query string, e.g. a SAS token, masked) and an extension's nested
  `Original error message: '...'` is kept when it ends the line, with URLs, inner quoted literals and credential
  values (`key=value`, `key: value`, JSON pairs, `Bearer`/`Basic`/`SharedKey`) masked. Every other literal is
  still masked, and the echoed statement is still dropped.

## [0.11.2] - 2026-10-15

### Fixed

- **Azure Blob native reads and writes on Linux.** The DuckDB azure extension's default transport could not find
  the system CA bundle (seen on Ubuntu 24.04 and Arch), so every native scan and native copy failed before any
  request left the host, reported as `PZ0501 ... this could mean the credentials used were wrong`. The azureblob
  connector now sets `azure_transport_option_type = 'curl'` on Linux, which uses the system bundle. Managed
  identity (`auth: managed_identity` with `client_id`) now works natively on Azure Container Apps.

## [0.11.1] - 2026-10-14

### Security

- **Patched `System.Security.Cryptography.Pkcs`.** NuGet.Protocol and Microsoft.Data.SqlClient brought in 8.0.1 and
  9.0.4, flagged by CVE-2026-84008 and CVE-2026-70162 (GHSA-846f-f764-76mx, GHSA-2fmv-rv2c-59x7). `Pz.PackageManagement`,
  `Pz.State.SqlServer` and the SQL Server connector now pin 10.0.13.
- **Patched `rustls` in the Rust connector SDK lockfile.** 0.23.45 to 0.23.46, for RUSTSEC-2026-0346, -0347, -0348
  and -0349.

## [0.11.0] - 2026-10-13

### Added

- **Connector stats ABI.** A connector reports what its remote service did through `ConnectorStats.Current`
  (requests, wire bytes, remote time, throttles, one cost unit, up to eight namespaced extras) and declares
  `ConnectorCapabilities.ReportsStats`. Invalid values are dropped, never thrown. The TestKit gains opt-in
  `Reported_stats_are_valid` / `Reported_write_stats_are_valid` facts that skip without the capability.
- **`remote` on `node_completed` and `run_results.json`.** The stats a node's connector reported, omitted when there
  are none.
- **`pz.remote.*` span attributes and metrics** for the same figures.
- **C# connector SDK.** The `ctx.Remote` helper, an HTTP handler that records requests, wire bytes and 429/503 throttles, a
  200-span cap per operation, and redaction of spans from foreign `ActivitySource`s. The HTTP handler also counts
  the bytes of streamed (chunked) request and response bodies as they are sent and read. `RemoteCalls` has a public
  constructor taking an `ActivitySource`, so connector unit tests can build one.
- **Sink stats are snapshotted after the write session is disposed** (C# and Rust SDKs), so numbers a session reports
  while disposing are included in the commit response, as they already were for abort.
- Libraries built on Azure.Core emit their `ActivitySource` only when the AppContext switch
  `Azure.Experimental.EnableActivitySource` is set; set it before creating clients, then list the source in
  `PzConnectorHostOptions.ActivitySources`.
- **Rust connector SDK.** `pz_connector::stats()` and `pz_connector::remote::start`, for sinks.
- **`pz connector test` vector `commit-stats-valid`** for connectors declaring `ReportsStats`. It commits once more
  to the probe target.
- **PCP.** The `GetReadStats` RPC, and `stats` on the write-commit and abort responses.

### Changed

- **Exported spans carry error codes and types only.** A failed node span's status description is its error code
  (for example `PZ0501`), its `exception` event carries `exception.type` and `pz.error.code` with no
  `exception.message`, and the `retry_scheduled` span event drops `reason` for `exception.type` and `pz.error.code`,
  because a connector's message can echo data values. Full messages remain in `run_results.json`, the NDJSON events
  and console output.

### Notes

- Attributes from libraries listed in `ActivitySources` and `Meters` fail closed: only a fixed set of safe keys
  (system, operation, database and collection names, server address and port, status codes) is exported, and every
  other attribute is dropped. A span is dropped, not exported, when it records an exception or any event attribute
  other than `exception.type`, or has a link carrying attributes: events and links cannot be edited, and exception
  messages can carry data values. Its status description is cleared. Children of a dropped span export with a parent
  id pointing at it. A foreign span's name is replaced by its source name plus the operation name, if recorded (at most
  96 characters), a span with any event not named `exception` is dropped, and only the exact `Pz.Connector` source and
  meter are treated as the SDK's own (`Pz.Connector.*` is filtered like any library). An invalid OTLP endpoint is
  reported on stderr by scheme, host and port only.
- A commit or abort that fails reports no stats for that call.
- A partition read abandoned early (cancellation, or a failure elsewhere) reports no stats for that stream.
- Rust SDK stats cover sinks only; the Rust SDK has no source support.
- Requests a hosted connector makes during open, plan and schema calls are not counted.
- The Rust SDK does not redact foreign `tracing` spans.
- Rust `stats()` reaches only code awaited inside the SDK's call, not work moved to `tokio::spawn`.

## [0.10.0] - 2026-10-12

### Added

- **Node facts on every completed node.** `node_completed` and `run_results.json`'s `nodes[]` carry `connector`,
  `connection`, `strategy` (`native_scan`, `native_copy` or `arrow` — the tier that actually moved the data),
  `stages` (`extractMs`, `ingestMs`, `egressMs`, `writeMs`, each stage's own wall time in the last attempt, plus
  `extractStartMs`…`writeStartMs`, when it began counted from the node's start) and
  `attempts`. All optional; a node that carries none writes exactly what it wrote before.
- **Run phase timings.** `run_completed` carries `phases` (`loadMs`, `compileMs`, `validateMs`, `restoreCheckMs`,
  `planMs`, `executeMs`); the terminal `run_results.json` adds `finalizeMs`.
- **Richer OpenTelemetry.** Node spans carry the node facts plus rows, ops, stall timings, provenance, caught-up,
  partition, delivery and CDC counts; failed nodes get an error status with a sanitized one-line description and an
  `exception` event; retries are `retry_scheduled` span events whose reason is sanitized to one line. The `run` span
  is back-dated to the run's first phase, has a `phase.<name>` child per phase, carries status and node counts, ends
  in error when a run fails or stops early (naming the phase: "run ended during <phase>" / "run ended after
  <phase>"), and records `breaker.state_changed` events. Failing `pcp.*` RPC spans in both connector SDKs carry an
  error status, `rpc.grpc.status_code` and `error.type` (the gRPC code name), never the message. Never on a span: SQL
  text, connection config, watermark values, sync tokens or CDC positions.

## [0.9.3] - 2026-10-10

### Added

- **`pz run --until-caught-up [--max-runs N]`.** Repeats the run while any windowed source with a stop (`until`,
  `until: now`, or a SQL ceiling) reports `caughtUp: false`, so a backfill in slices no longer needs a shell loop.
  Each pass is a full run that writes its sinks and advances the watermark before the next starts. The loop stops when
  every such source has caught up (exit 0), when a run fails (that run's exit code), on a stop signal (exit 3), or
  after `--max-runs` passes (default 100; exit 0 — the next invocation continues from the stored watermarks). With no
  windowed source that has a stop, it runs once. A final `note: until-caught-up: …` line gives the pass count and why
  it stopped (stderr under `--log-format json`). `--full-refresh` applies to the first pass only.

## [0.9.2] - 2026-10-09

### Fixed

- **`--full-refresh` resets the watermark on the HTTP and SQL Server state backends.** A full refresh skipped reading the
  stored watermark (and a feed's or CDC source's sync state), so a versioned store saw its advancement as a write by
  another run and refused it: the run printed `PZ0527 … PZ0520` and kept the old value, and the next run went on from
  there instead of from `initial`. The run now reads the entry and ignores it, so its own advancement replaces it. The
  local file store was not affected.

## [0.9.1] - 2026-10-09

### Added

- **OTLP over HTTP.** `pz run`, `pz test` and `pz retry` take `--otel-protocol http/protobuf` with
  `--otel-traces-endpoint` and `--otel-metrics-endpoint` (full URLs, used as-is; either alone is fine), so a run can
  export straight to a backend that only takes OTLP/HTTP, such as Azure Monitor. Bodies are gzipped and span batches
  are capped at 256 so a request stays under 1 MB. Metrics over HTTP are delta, with base-2 exponential histograms.
- **`--otel-headers-file`.** Header lines (`Name=value`) re-read before every export, so the caller can refresh a
  token while the run is going. A missing or malformed file sends without it and prints one `note:`; the content is
  never printed.
- **`pz.run.id` on every metric point**, so one run's metrics can be filtered where resource attributes are dropped.
- Connectors receive the HTTP settings through `HostInfo` (`otel_protocol`, `otel_traces_endpoint`,
  `otel_metrics_endpoint`, `otel_headers_file`); both SDKs export over HTTP. Connectors built on an older SDK export
  nothing under `http/protobuf` (gRPC is unchanged).

### Changed

- **`--otel-*` flags win over `PZ_OTEL_*` variables as a set.** When any `--otel-*` flag is given, every `PZ_OTEL_*`
  variable is ignored, so a stray variable on the host cannot turn an explicit flag set into a usage error. With no
  flag, the variables apply as before.
- pz no longer follows ambient `OTEL_EXPORTER_OTLP_PROTOCOL` or endpoint variables: every exporter option comes from
  its flag or `PZ_OTEL_*` variable. `OTEL_EXPORTER_OTLP_HEADERS` is still honoured.
- The Rust connector SDK adds the OTLP/HTTP transport (reqwest's blocking client with rustls and aws-lc-rs, the
  provider rustls defaults to, so a connector's own rustls code is unaffected), which adds about 7 MB to a release
  connector binary.

## [0.9.0] - 2026-10-09

### Added

- **`pz run`, `pz test` and `pz retry` join a caller's trace.** With telemetry on, a W3C `TRACEPARENT` (and
  `TRACESTATE`) in the environment makes the `run` span a child in that trace, so an orchestrator can link a run
  to its trace before it starts. The caller's sampling decision is honored: an unsampled parent (flags `00`) means
  pz exports no spans for that run, while metrics still flow. An invalid value is ignored with a `note:` line.
- **`caughtUp` on `node_completed` and in `run_results.json`.** A windowed source with `until` (or a SQL-declared
  ceiling) now says whether it has caught up, so a platform or a shell loop can repeat `pz run` until every
  backfill is done instead of reading the `note:` line or comparing watermarks.
- **`until: now`.** A windowed dataset with a date or timestamp cursor can stop at the time the run started (UTC)
  instead of a fixed value, so a scheduled job keeps the source current one window at a time. The last window stops
  just before the run's start: a date cursor loads up to yesterday (UTC), never half of today. The cursor must hold UTC
  values: an empty window moves the watermark to its end, so a cursor in a local time behind UTC would skip rows.

## [0.8.0] - 2026-10-08

### Added

- **`pz cdc status` and `pz cdc drop` take `--state-url` and `--log-format json`.** A platform can run them
  against the state store its runs use and read `cdc_status` / `cdc_dropped` NDJSON events.

### Fixed

- **`pz cdc drop` accepts a schema-qualified entity** (`erp.dbo.orders`). The target splits on its first dot,
  so a SQL Server cdc dataset `pz cdc status` reports can also be dropped.

## [0.7.0] - 2026-09-30

### Breaking

- **Quoted YAML scalars stay strings** (`password: "0123456"` no longer becomes `123456`).
  *Migration:* unquote pz's own numeric/boolean keys (`threads: "4"` is now PZ0120).
- **Unquoted `${VAR}` is typed by its value** (`port: ${PGPORT}` is an integer; `$${` is a literal `${`).
  *Migration:* quote references feeding text options, e.g. `password: "${PGPASSWORD}"`.
- **Unknown `write:`/`sink()` options on builtin sinks are refused (PZ0301).**
  *Migration:* remove or fix the named option; it never had an effect.
- **Pipeline file stems and connection names must be plain identifiers**, and names differing only by
  case collide (PZ0110, PZ0230).
  *Migration:* rename offending files, connections or datasets.
- **A relative path escaping a declared `root:` is refused (PZ0365).**
  *Migration:* use an absolute `path:` or move `root:` up.
- **`pz restore` honours `pz.lock.json`** (exact versions, sha512-verified) instead of re-resolving.
  *Migration:* use `pz restore --update` to pick up `project.yml` changes.
- **SinkWrite node ids include `keys:`/`duplicates:`/`on_delete:`.**
  *Migration:* none; a `pz retry` of a pre-upgrade run re-runs its sinks.
- **`Pz.Connectors.Abstractions` drops `Microsoft.Extensions.Logging.Abstractions`.**
  *Migration:* connectors not built on `Pz.Connectors.Sdk` that use `ILogger` add the reference.

### Added

- `pz runs`: list prior runs (`--json` supported, local or SQL Server state).
- `pz completion bash|zsh|fish|pwsh`.
- `engine.node_timeout`: bound one node attempt; exceeding it fails with PZ0525.
- `connector_log` run event: connector log output is no longer dropped.
- Connector SDK name/version on the PCP handshake, shown by `pz connectors`.
- `IOutputConfigSchema`: sinks can publish a schema for their write options.
- Iceberg `storage_scope:` option.
- `connect_timeout_seconds`/`command_timeout_seconds` for sqlserver, postgres (and sftp connect);
  `state.timeout_seconds` for the HTTP state backend.
- Rust connector SDK builds and runs on Windows.
- Windows: connectors are killed with pz (Job Object); socket directory is owner-only.
- Lock records a per-file sha512 and the RID; tampered installs are PZ0326, a wrong platform PZ0321.
- TestKit: more sink acceptance facts and a `ColumnPruning` fact (existing subclasses unaffected).
- Each release publishes a GitHub Release with a standalone `pz-win-x64.zip` (the winget source).
- Release: changelog-entry check and packaging proofs gate the publish; SHA-pinned actions,
  `global.json`, dependabot, `cargo audit`, build provenance.

### Changed

- Rust SDK moves to tonic 0.14 and opentelemetry 0.32.
- Unquoted decimal connector versions (`version: 1.10`) are refused; quote them.

### Fixed

- **Wrong rows/columns:** predicate pushdown across outer joins, column pruning with joins, and
  positional native `append` (now `by name`) for SQLite, MySQL, DuckDB, DuckLake, MotherDuck,
  Quack and Iceberg.
- Parquet writes (managed path) no longer truncate timestamps to milliseconds.
- External connectors start on Windows; a chatty connector (stdout) no longer hangs the run;
  connector processes end when their node is done.
- Rust-SDK sinks could hang at commit.
- Transient errors mid-stream are retried; connector error codes/hints and exit signals are reported.
- Integer connector options arrive as integers over PCP; unknown capability bits no longer fail the
  handshake; Arrow shape guards compare types structurally.
- Ctrl-C and node timeouts interrupt running DuckDB statements; SIGTERM/SIGHUP shut down cleanly.
- Overlapping runs no longer lose watermarks or fail writing `.pz` artifacts.
- Watermark advancement continues past a failing dataset.
- Compile reports every broken pipeline, accepts a trailing `;`, and handles comments/`WITH RECURSIVE`
  in ephemeral inlining.
- New refusals and warnings: duplicate checks (PZ0113), mixed `watermark()` cursors (PZ0231),
  `run_id` in SQL (PZ0232), bad `schema_policy`, unknown keys in `engine:`/`retry:`/`rate_limit:`,
  `${VAR}` inside entity blocks, conflicting MotherDuck tokens, unpinned SFTP host keys (PZ0364).
- Huge integer/decimal kwargs no longer crash compile.
- Unhandled exceptions are PZ0500 (exit 3); usage errors exit 2; disk-full/permission errors have
  their own codes (PZ0531, PZ0532).
- `pz restore` works offline when the cache is complete and repairs torn installs.
- SQL Server/HTTP state: correct transient classification, no O(N²) artifact writes, no hang on a
  dead event store, safe concurrent schema migration.
- Culture-independent run ids and YAML value rendering.
- Relative `private_key_path` (sftp) and `key_file` (gcs) resolve against the project.
- HTTP connector: `max_response_mb` capped, non-object records refused, redaction no longer throws.
- `pz mcp`: JSONC config files, `file://` docs mirrors, schema for undeclared entities,
  `pz_run` reports its own run.
- DuckDB "could not connect to server" is transient; ducklake accepts an empty sqlite catalog.
- Connectors SDK: pooled allocation on the write path, no hang on a dead reverse channel.
- Rust SDK parity fixes (abort semantics and others).

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
