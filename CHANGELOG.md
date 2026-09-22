# Changelog

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions are git
tags (`v*`), computed into package versions by MinVer. Breaking changes are listed
first in each release, each with a migration note — see
the [versioning policy](https://pipelinez.dev/versioning/).

## [Unreleased]

## [0.7.0] - 2026-09-23

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
