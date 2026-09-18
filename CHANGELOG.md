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
- `pz restore` now honours an existing `pz.lock.json` instead of re-resolving and
  overwriting it: every locked package is restored at exactly its locked version
  and must hash to its locked `sha512`, so a version range in `project.yml` no
  longer floats between restores and a package republished under the same
  version is refused (PZ0327) rather than silently accepted. A requirement the
  lock no longer satisfies — a bumped version, a connector added or removed — is
  PZ0321, and a malformed or older-schema lock is no longer regenerated
  silently. The new `pz restore --update` is the one way to re-resolve against
  the feeds and write a new lock. **Migration:** where a script relied on
  `pz restore` picking up a changed `project.yml` or a newer version within a
  range, run `pz restore --update` there instead.
- The lock records a `sha512` for every installed file (the connector's
  entrypoint binary above all), and `pz run`/`plan`/`validate`/`connectors`
  verify the installed files against it before anything is spawned: a modified,
  truncated or replaced file is PZ0326 with `pz restore` as the next step, which
  reinstalls it from the locked package (the package cache entry is re-verified
  and re-extracted too). A lock written before hashes were kept still pins
  versions and package hashes; its next `pz restore` fills the file hashes in.
- The lock's `rid` is compared with the host's: a `.pz/packages` restored on
  one platform and run on another is PZ0321 naming both, instead of the
  "Exec format error" spawn failure it used to reach.

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
- Additive `Hello.sdk {name, version}` on the PCP wire, and a matching `sdk`
  property in `pz.connector.json` — which SDK built an out-of-process
  connector, and at what version, previously invisible anywhere. Populated by
  both SDKs (C# `Pz.Connectors.Sdk`, Rust `pz-connector`) and shown in
  `pz connectors`, `pz connector test`'s handshake vector, and PZ0356/PZ0357
  messages. `HostInfo.pz_version` (declared but never set) now carries this
  pz build's own version on every handshake. Both fields are additive:
  absent on either side of an older SDK/manifest, never a mismatch. This gap
  made the 0.6.1 pruning incident hard to triage.

### Fixed

- An unhandled exception inside a C# SDK connector handler — a connector
  defect the SDK never anticipated, not an operational failure the connector
  reported on purpose — now reaches the engine as a non-transient connector
  error ("unhandled `<Type>`: `<message>`"), instead of surfacing as PZ0357
  "protocol violation … confirm ABI versions", the wrong diagnosis for a bug
  in the connector rather than a mismatch between it and the host.
- A connector-reported error's `code`/`hint` (always sent empty by the C# SDK,
  and discarded by the host even when a future SDK filled them) now survive
  the round trip: `PzConnectorException` gains additive `Code`/`Hint`
  properties, the C# SDK populates the wire detail from them, and the host
  folds a present hint into the exception's message (every existing consumer
  — run_results.json, the NDJSON stream, a retry_scheduled reason — already
  renders `Message`, not a field nothing reads). The Rust SDK already filled
  both; the two SDKs are consistent now.
- PZ0356 (handshake failed), PZ0358 (connector died mid-operation), and a
  connector-reported error the host maps once the connector's process has
  also exited now name the child's exit code — `exited with code 137 (signal
  SIGKILL)` for an OOM-kill, `exited with code 139 (signal SIGSEGV)` for a
  segfault — instead of leaving PZ0358's own hint ("check the connector's
  exit code") with nothing to check.
- A connector reporting a capability bit or manifest capability name this pz
  build does not define — the sanctioned way an out-of-process connector's SDK
  grows the ABI — no longer fails the handshake with PZ0356 "capabilities
  (98304) do not match". Only bits this build's `ConnectorCapabilities`
  actually defines are compared; an unrecognized name or bit is reported once
  as a warning instead, the same way an out-of-process host already reports a
  declared-but-unimplemented capability.
- Five small HTTP connector defects:
  - `StripUrlQueries` (sync-mode error-body redaction) no longer throws
    `UriFormatException` from inside the error-formatting path when a response
    body embeds "scheme://...?..." text that isn't itself a valid URL (e.g. a
    malformed example/hint in an upstream API's own error payload); such a
    match is now masked wholesale instead of crashing.
  - A sink merge key of `.` or `..` is now refused: substituted into `path`,
    `new Uri(base, relative)`'s dot-segment removal collapses the request onto
    the parent/grandparent path instead of the intended keyed resource.
  - A sink 4xx now carries a short, bounded (160-char) snippet of the
    response body and a hint that fits the status (auth for 401/403, the
    output path for 404, the request body otherwise) instead of no body and
    one hint naming both path and auth unconditionally. The snippet masks the
    connection's secret query params, as read errors already do.
  - The `page` pagination strategy has an opt-in `stop_on_short_page` option:
    when the requested `size` is set and a page's row count falls short of
    it, the crawl ends there. Some APIs clamp an out-of-range page number to
    the last real page instead of serving an empty array, so the crawl never
    ended on its own and ran to the unbounded-page ceiling. Default behaviour
    (no option) is unchanged.
  - `CheckConnectionAsync` now follows a 3xx on `check_path` the way a read
    would (bounded, checked against `allow_hosts`) instead of reporting the
    redirect itself as a failed check.
- `ContractProjector.ProjectRow` (shared by the HTTP and SFTP connectors' `columns:`
  contract mode) now throws a permanent "record is not an object (check 'items')"
  error naming the dataset when a record isn't a JSON object, instead of silently
  projecting a row of all NULLs. An `items` pointer that resolves one level off
  used to land N all-NULL rows on a green node whose watermark never advanced.
- The HTTP connector's `max_response_mb` is capped at 2047 (2048 MiB, once
  converted to bytes, overflows `HttpClient.MaxResponseContentBufferSize`'s own
  int.MaxValue ceiling): the cap is now enforced in both `base_url` connection
  validation and the connection JSON Schema, instead of passing validation and
  crashing the read with a raw `ArgumentOutOfRangeException`. A response that
  actually exceeds the configured cap at read time is now a permanent
  `PzConnectorException` naming `max_response_mb`, instead of a bare runtime
  "configured maximum buffer size" message.
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
- `pz connector test`'s read vectors (schema/batch equality, cancellation,
  ticket handling) now report Skip, not Fail, against a connector declaring
  the new `NativeOnlyRead` capability — the wire signal for a source with no
  universal read path at all (`PlanRead` always refuses), mirroring the
  TestKit's own `SkipIfNativeOnly`. Until now these vectors called `PlanRead`
  unconditionally and reported the refusal as a protocol failure. The C# SDK
  declares the capability for any connector that implements
  `INativeOnlySource`, in the handshake and the manifest alike; an author
  never sets the flag by hand.
- Three more type comparisons are now structural instead of `TypeId`-only, using
  the same shape guard as the SDK data plane and engine Arrow ingest: `pz
  connector test`'s schema/batch-equality vector (a `list<int32>` vs
  `list<utf8>` mismatch used to pass conformance and only fail at run time),
  the TestKit's own `SourceConnectorAcceptanceTests.AssertSchemasMatch`, and
  `ContractTypes.ArrowTypesEqual` (the `columns:` contract drift check behind
  PZ0331) — the last of these already compared decimal precision/scale and
  timestamp unit/timezone by hand; it now shares the general comparison
  instead of falling back to `_ => true` for nested, fixed-size and other
  decimal-width types.
- An integer connector option now survives the wire intact. protobuf's
  `Struct` has only `number` (a double), so `max_connections: 5` used to
  arrive at a process-hosted connector — and, symmetrically, come back to the
  host — as `5.0`; both SDKs (C# and Rust) now normalize an integral value
  within the range a double still represents exactly (`|x| <= 2^53`) to a
  proper integer (`long`/`i64`) before the connector or host ever sees it.
  `ConnectorConfig.GetInt` refuses a genuinely fractional value (`2.5`) with a
  clear error instead of silently rounding it via `Convert.ToInt64`, and the
  Rust SDK's `as_i64()` now succeeds for an integral value instead of always
  returning `None` for a float-backed `serde_json::Number`. An `int[]`/
  `List<int>` option — not `IEnumerable<object?>`, since generic variance
  covers reference types only — used to fall through to `ToValue`'s string
  catch-all and cross as .NET's default collection rendering
  (`System.Collections.Generic.List\`1[System.Int32]`) instead of a list of
  numbers; it now crosses correctly. `pz connector test` gained a
  `numeric-option-fidelity` vector that both SDKs answer on the connector's
  behalf (a connector built on an older SDK reports Skip), so every connector
  proves the contract without its author writing anything; the TestKit gained
  an opt-in `Connection_integer_option_delivered_as_a_double_is_accepted` fact.
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
- A pipeline ending in a trailing `;` (the way most SQL formatters write it) no
  longer fails compile with a raw DuckDB parse error. `DagCompiler` now strips
  exactly one trailing semicolon (plus surrounding whitespace) the way
  custom_sql checks already did; a `;` anywhere else in the SQL — a genuine
  second statement — is left alone and still fails loudly.
- Ephemeral-CTE inlining no longer emits invalid SQL for a consumer pipeline
  that opens with a comment before its `WITH`, a consumer using
  `WITH RECURSIVE`, or an ephemeral pipeline whose body ends in its own
  trailing `;`. Assembly now prefers reading and re-emitting the parsed AST
  (DuckDB's own parser, via the existing `ISqlAstReader` seam) over sniffing
  consumer text for a `with` keyword; a textual splice remains the fallback
  when no AST reader is wired or either side fails to parse. A pipeline that
  consumes an ephemeral pipeline therefore runs (and shows in `pz compile`
  output) as DuckDB's own rendering of its SQL: comments are dropped and
  keywords normalized, and its node id changes once on upgrade.
- `pz compile`/`run`/`validate` now report every pipeline's broken template in
  one compile instead of stopping at the first: rendering used to throw on
  pipeline A and never attempt pipeline B, C, … at all. Four independent
  validation stages (incremental/merge-keys, ref()/source() resolution,
  checks-on-ephemeral, ephemeral-chain) that used to stop at whichever ran
  first now also report together in one throw, and so do the incremental and
  cdc pairing matrices. Aggregated errors are ordered
  by file, then position, for a deterministic report. Stages with a genuine
  dependency on an earlier one's success (sink-output binding, the one-reader
  rule, SQL-declared incremental inference, and later) are unchanged.
- `schema_policy` is now validated against its vocabulary (`fail_on_change`,
  `additive`, `evolve`) on both surfaces — the `sink()` keyword argument and the YAML
  `write:` block — instead of riding any string through to the connector,
  which silently treated an unrecognized one as `fail_on_change`
  (`schema_policy: aditive` used to reach postgres/sqlserver unchallenged). A
  near-miss (`aditive` → `additive`) is suggested. Same pass: `max_concurrency`
  is now refused on `sink()` the same way it already was on `source()` (it
  used to ride through silently as a connector write option), both now under
  the connection-level `max_concurrency:` code (PZ0122) instead of the
  rate-limit one; the `sink()` `rate_limit` refusal's hint now correctly says
  to declare it on the connection, not "on the sink"; and a boolean/integer
  connector option that received a YAML string a plain, lowercase
  `true`/`false` would have typed — `True`, `yes`, `null`, `~`, and similar
  YAML 1.1 lookalikes the loader deliberately leaves as text — now says "write
  true/false in lower case, unquoted" (or, for `null`/`~`, "leave the option
  out") instead of the JSON Schema library's raw
  `Value is "string" but should be "boolean"`.
- An unquoted `${VAR}` is now typed by the value it resolves to, when that loses
  nothing: `port: ${PGPORT}` with `PGPORT=5432` is the integer `5432` instead
  of the string `"5432"` that failed tier-3 validation with "expected integer",
  and `${FLAG}` = `true` is a boolean. Substituted text that would not read
  back the same stays a string — `0123456`, `1.10`, `1e5` — because a bare
  `${VAR}` is a password or an account id as often as a port. A quoted
  reference (`port: "${PGPORT}"`) always stays a string. Applies to
  `connections.yml` connection config, `project.yml`'s `vars:` block, and
  `pz connector test --config`. A literal `${` that must NOT be read as a
  reference is now written `$${` (e.g. `$${NAME}` produces the literal text
  `${NAME}`).
  *Migration:* a text option fed by an unquoted `${VAR}` whose value is all
  digits (or `true`/`false`) is now a number (or boolean) and fails validation
  with "the value was read as a number, but this option is text" — quote the
  reference (`password: "${PGPASSWORD}"`). The value is never echoed.
- `${VAR}` inside an `entities: <e>: read:/write:` block in `connections.yml`
  was always silently left as literal, un-substituted text (unlike the
  connection's own top-level config, where it IS interpolated) — it now
  raises a load-time warning naming the connection and option instead of
  reaching the connector unexpanded with no signal at all. The value itself
  is unchanged; only connection-level config is interpolated.
- Several YAML blocks silently ignored an unknown or mistyped key instead of
  refusing it: `engine:`, `engine.duckdb`, `engine.breaker`, the instance-level
  `rate_limit:`, and the YAML `retry:` surface (which now agrees with the
  `sink()`/`source()` kwarg surface's existing unknown-key refusal, using the
  same code). All go through one shared "unknown key" check that also
  suggests a near-miss (e.g. `thread:` → "did you mean 'threads'"). Also:
  - A sidecar `pipelines/configs/*.yml`'s unknown key is refused the same way
    (`materialisation:` now says "did you mean 'materialization'"); its
    `materialization:` value is validated against `table`/`view`/`ephemeral`
    — dbt's `incremental`, or a near-miss like `ephemral`, are now errors
    instead of silently landing as an unrecognized string nothing downstream
    reads. A scalar `tags: daily` is now the one-element list `[daily]`
    rather than silently dropped to no tags.
  - `connections.yml`'s `entities:` being anything other than a mapping (a
    list, a string) used to silently drop every entity to `{}`; it is now a
    load-time error.
  - A `.yaml` file sitting where pz only ever reads `.yml` — `project.yaml`,
    `connections.yaml`, or a `pipelines/configs/*.yaml` sidecar — is silently
    never loaded; it now raises a load-time warning naming the file.
  - A YAML file (`project.yml`, `connections.yml`, a sidecar) whose document
    root is a list or a bare scalar, or that contains a second `---`-separated
    document, used to silently read as `{}`; both are now load-time errors.
  - `project.yml`'s `pz:` key (documented as an engine-version constraint)
    remains accepted but unenforced — nothing in the loader or engine reads
    it today, and its exact constraint syntax is not established in this
    repository, so guessing a shape to enforce risked being wrong. Left as a
    deliberately open follow-up rather than a guess.
  All errors aggregate (a run reports every mistake in a file at once, not
  just the first) and carry the file and a next step, per this project's
  error-reporting rule.
- `state: {backend: sqlserver, artifacts: true}` no longer costs O(N^2) round
  trips per run: `SqlRunArtifactStore.WriteSnapshot` now upserts only the nodes
  that are new or changed since the store's last successful write for that run,
  instead of every node in the cumulative list `SnapshotRunEvents.NodeCompleted`
  re-sends on every call. A 300-node run used to make roughly 45,000 round
  trips (1+2+...+300 across 300 snapshots); it now makes 300. A node is only
  ever marked written after its snapshot's transaction actually commits, so a
  failed write still retries on the next snapshot.
- `SqlEventSink` (the `state: {backend: sqlserver, events: true}` run-event
  store) no longer lets a dead or unreachable store hang a run's shutdown --
  Ctrl-C included -- for minutes. A one-way circuit breaker stops retrying
  after 3 consecutive flush failures and counts every later batch dropped
  without another connect attempt, instead of each remaining batch paying its
  own fresh connect timeout in turn; `DisposeAsync` additionally bounds its
  total wait at a 30s deadline (driven by the sink's own `TimeProvider`) as a
  backstop against a store that is merely slow rather than outright down. When
  any events were dropped, `pz run` now prints a notice naming the count and
  the store instead of leaving it silent in a SQL column only. Separately, the
  snapshot-write warning that always said "could not write run_results.json"
  now names the store that actually failed -- "the SQL state store" under
  `state.artifacts: true`, instead of misnaming it as the local JSON file.
- `SqlStateSchema.EnsureCurrent` (the SQL Server state store's first-use/
  migration path) is now safe against two processes racing the same schema:
  a store that is behind is migrated under an exclusive, transaction-owned
  `sp_getapplock`, with the version read again under the lock, instead of on
  a version read before the transaction even began. A store that is already
  current takes no lock and opens no transaction. A caller that loses the race and
  wakes to find the schema already migrated now does nothing, instead of
  racing a second migration attempt (previously an occasional raw `2714`
  surfaced as PZ0519 with the wrong "check DDL rights" advice, or a duplicate
  `schema_version` row, since that table had no key). A lock that cannot be
  acquired within 30s is the new PZ0528 with a "retry" next step. Schema
  version 3 adds a PRIMARY KEY to `schema_version`, first reducing it to one
  row (the race left two rows at the same version); migrates automatically,
  same as version 2.
- A pipeline's file stem and a connection's name are validated at load time
  as legal unquoted identifiers (`[A-Za-z_][A-Za-z0-9_]*`) instead of being
  interpolated raw into `staging.<name>`/`src_<connection>__<entity>` and
  left to fail as a raw DuckDB parse error: `01_load.sql`, `daily-orders.sql`
  and a connection named `my-warehouse` are now PZ0136 at load, each naming
  the file, the offending name, and a concrete rename (`01_load` ->
  `load_01`, `daily-orders` -> `daily_orders`). A connection name containing
  `__` is refused for the same reason (PZ0136): it is the literal separator
  `src_<connection>__<entity>` splices in, so e.g. `erp__mart` could collide
  with connection `erp` reading a `mart__<entity>` dataset. Two duplicate
  checks are now case-insensitive, matching DuckDB's own unquoted-identifier
  folding: two pipeline files (PZ0110) or two datasets of one connection
  (PZ0110) differing only by case now collide at compile time instead of
  silently sharing one staging relation on Linux (where both files coexist on
  disk). The dataset check also spans connections now: `ERP.orders` and
  `erp.orders`, or `a._b` and `a_.b`, stage to one relation and are PZ0110
  naming both. A pipeline named exactly like a referenced SourceLoad's staging
  relation (`src_<connection>__<entity>`) is the new PZ0230: both would
  target the same `staging.<name>` table. The MCP `pz_write_pipeline`/
  `pz_remove_pipeline` tools enforce the identical rule before ever writing a
  file, off the same shared predicate (`Pz.Core.Model.PzIdentifier`), so an
  agent-authored name cannot pass authoring only to fail at the next load.
  DuckDB's reserved words themselves are unaffected -- `staging.order` parses
  fine schema-qualified, so nothing here refuses on reservedness, only shape.
  **Migration:** a project with a pipeline file or connection name outside
  `[A-Za-z_][A-Za-z0-9_]*`, a connection name containing `__`, or file/dataset
  names that were previously distinguishable only by letter case must rename
  the offending file/connection/dataset; every template and sample under
  `templates/`/`samples/` already conforms and needs no change.
- Template/compile errors now carry a next step instead of `next_step: null`
  on the MCP surface (or a bare code on the CLI): a `source()`/`sink()` call
  split across more than one line -- documented as a single-line-only call,
  but previously left to fail as several raw Scriban parser messages -- is
  now named as exactly that, with a one-line hint, instead of surfacing
  "Expecting an expression for argument function calls instead of this
  token." verbatim; every other unrecognized `{{ }}` expression (PZ0104)
  carries a hint naming the five reachable functions/constants. An unknown
  `env()` reference (PZ0103) now names the variable to set in its hint, not
  just its message. An unknown `var()` reference suggests a near miss among
  the project's declared `vars:`, or points at declaring one. `ref()`,
  `source()`, and `sink()` calls naming an unknown pipeline or connection
  (PZ0201) now suggest a one-edit-or-case near miss the same way
  `schema_policy`/materialization typos already did; the sink form's message
  said "no sink named" for what is actually an unknown *connection* -- it now
  matches the source form's wording. A dependency cycle (PZ0202) now names a
  file (one of the pipelines in the cycle) instead of `file: null`, plus a
  hint. A malformed date-templated path (PZ0218) now carries a hint. Read-side
  kwargs one edit from a pz-owned `source()` option (`sync`/`retry`/
  `columns`/`partition_column`/`partitions`) now draw the same near-miss
  warning the write side already had -- `SourceFunction`'s near-miss check
  existed but was never wired in, so e.g. `source(..., synk: {...})` rode
  through as a silent connector option.
- `pz restore` now repairs a torn or stale `.pz/packages/<id>/<version>`
  instead of trusting that it merely exists. The copy into it is no longer
  done in place: it materializes into a temp sibling directory and one atomic
  `Directory.Move` into place, so a crash mid-copy never leaves a reader
  observing a half-written install, and a sibling a crash left behind is swept
  on the next restore instead of accumulating. Separately, a lock written
  before per-file hashes were kept could trust a directory's content just
  because a file existed at the expected path, without ever checking that the
  file itself was actually there — a partial install missing that file passed
  as "no drift". Existence is now always checked, hash or no hash; only the
  byte-for-byte comparison still needs a recorded hash to run.
- `pz restore` no longer touches the network when it doesn't need to, and no
  longer escapes as a raw stack trace when it fails for a reason outside its
  own coded checks. A lock honoured for exactly this host, with every locked
  package already content-verified in the local package cache, now succeeds
  offline -- the common case once a project has restored once, and every
  restore in CI or an air-gapped environment after that. An unreachable feed,
  one that returns an HTTP 401/403, or a local disk failure while writing
  under `.pz` is now PZ0328 (feed) or PZ0329 (disk) naming the feed
  (credentials and query stripped) or the path and the cause, instead of an
  uncoded exception rendered as an "internal error" bug report. Only a
  failure of the feed client, an HTTP request, a socket or the local disk is
  mapped; anything else is still a fatal error with its stack trace. The feed
  resolver also no longer reads a downloaded `.nupkg` into memory whole to
  hash it; the hash is streamed.
  *Not addressed*: feed credentials (a bearer token or basic auth for a
  private feed) remain out of scope -- `--feeds`/`PZ_FEEDS` still take only a
  URL or a local path.

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
