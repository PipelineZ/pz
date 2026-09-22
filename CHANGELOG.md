# Changelog

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions are git
tags (`v*`), computed into package versions by MinVer. Breaking changes are listed
first in each release, each with a migration note — see
the [versioning policy](https://pipelinez.dev/versioning/).

## [Unreleased]

### Changed

- **The Rust SDK (`rust/pz-connector`) now builds on tonic 0.14 (prost codegen moved to the
  `tonic-prost`/`tonic-prost-build` crates) and opentelemetry/opentelemetry_sdk/opentelemetry-otlp
  0.32.** A Rust connector crate that pins its own older tonic or opentelemetry line alongside
  `pz-connector` would get a second, duplicate copy of both in its `Cargo.lock`.
- **SinkWrite `NodeId` now includes `keys:`/`duplicates:`/`on_delete:`.** These change what a commit
  under that id MEANS (the merge match condition, at-least-once consent, delete routing), so a sink
  whose write semantics changed since a failed run no longer reuses/carries-forward the earlier
  commit under `pz retry` — it re-runs instead. `retry:` (attempts/backoff) stays out of the id: it
  governs how this run attempts the write, not what got committed.
  *Migration:* none required, but every `SinkWrite` node id changes once on upgrade — a `pz retry`
  issued against a run from before the upgrade re-runs its sinks instead of reusing them.
- `IcebergAzureRestTests`' `finally` block no longer lets a failed cleanup `drop table` replace a
  pending assertion failure from the test body — a cleanup exception thrown from `finally` supersedes
  whatever is already propagating, so a real Azure REST catalog failure would have surfaced as an
  unrelated drop-table error. The drop is now caught and reported to stderr, test-only.
- `DataPlaneListener`'s write-stream pump is split into a testable `internal static` overload the
  same way its read-stream pump already was, so a test can drive it against an in-memory stream with
  no live socket. Behavior is unchanged; this is what let the new write-path coverage below be
  written at all.
- **Test coverage: `Pz.Connectors.Sdk`'s write path and multi-open behavior**, gaps a September 2026
  code audit named. `FakeConnectors.cs` gains a sink counterpart to its existing source fakes
  (`FakeSinkConnector`/`FakeSink`/`FakeWriteSession`), and a new `WriteRoundTripTests.cs` drives
  BeginWrite -> the data-plane pump -> CommitWrite/AbortWrite the way `HonestMappingTests` already
  drives the read side. Two more facts (one per direction) pin that `PcpConnectorService` opens a
  connector's source/sink at most once per process and reuses it across ops -- the "connector process
  spawned once per open" contract, not previously exercised for a SECOND op sharing the first one's
  already-open connector.
- **Test coverage: `scripts/verify-sdk-package.sh` now asserts PZSDK001-004**, not only PZSDK005 --
  no staged binary at all, a wanted RID never published, the packing machine's own RID missing from
  what was staged, and an invalid `PzPackaging` -- plus the `pipelinez-connector` nuspec discovery
  tag every packed connector must carry. None of the four new checks needs a Native AOT compile
  (`PzGenerateManifest` only runs the staged binary after they have all already passed), so they run
  even where the AOT toolchain itself is unavailable.
- **Supply-chain polish**: a `global.json` pins the .NET SDK (`10.0.400`, `rollForward: latestFeature`
  -- CI's `actions/setup-dotnet@... # v6` with `10.0.x` always installs a feature band at or above
  this one, which `latestFeature` accepts; it would refuse an OLDER one, so this is not a floor a
  future CI image can silently drop below) so a new SDK feature band cannot change analyzer behavior
  under `TreatWarningsAsErrors` and break `main` with no code change of this repo's own. Every
  third-party GitHub Action in `.github/workflows/*.yml` is now pinned by full commit SHA with its
  version as a trailing comment (`uses: owner/repo@<sha> # v7`); Dependabot's `github-actions`
  ecosystem (`.github/dependabot.yml`) already understands that form and will keep opening PRs that
  update both the SHA and the comment together. `release.yml`'s `release` job now attests build
  provenance (`actions/attest-build-provenance`, Sigstore-signed SLSA) for every `.nupkg` it
  publishes -- library packages, the `pz` pointer, and every `pz.<rid>`/`pz.any` sub-package -- scoped
  to that job alone (`id-token: write`, `attestations: write`); nothing else about the job changed. A
  separate SBOM is not added here: every option needs tooling this repo does not already carry
  (a CycloneDX/SPDX generator step, or `dotnet list package` post-processing), which is out of scope
  for a minimal supply-chain pass.
- **`Pz.Connectors.Abstractions` no longer references `Microsoft.Extensions.Logging.Abstractions`.**
  The reference allowlist is Apache.Arrow only (now enforced by `AbiSurfaceTests`, matching this
  repo's own architecture docs); nothing in Abstractions' own source used the logging package, and
  every SDK-hosted connector already gets it transitively through `Pz.Connectors.Sdk`'s
  `FrameworkReference` to `Microsoft.AspNetCore.App`.
  *Migration:* a connector project that used `ILogger`/logging types via this transitive reference
  without also referencing `Pz.Connectors.Sdk` (or another package that itself brings in
  `Microsoft.Extensions.Logging.Abstractions`) must now add that `PackageReference` explicitly.
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
- **Unknown `write:`/`sink()` options on a builtin sink are refused.** They used to reach the
  connector unchecked and were silently ignored; every builtin sink now publishes a schema for its
  own write options and `pz validate` (tier 3) reports an option it does not read as PZ0301, with
  the accepted options and a near-miss suggestion.
  *Migration:* remove or correct the option the error names -- it never had an effect. Options the
  engine owns (`strategy`, `keys`, `duplicates`, `on_delete`, `schema_policy`, `retry`) and
  `partition_by` are unaffected, and so is any connector that publishes no such schema.
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
- **A relative `path:`/entity-derived location that escapes a connection's `root:` is now refused
  (PZ0365)** instead of silently reading or writing outside it. Only a DECLARED `root:` is a boundary:
  a `localfiles` connection without one resolves relative paths against the project directory exactly
  as before, so a data folder beside the project (`path: ../shared/x.csv`) keeps working.
  *Migration:* a connection that declares `root:` and reaches outside it with `..` — give that
  `path:` as an absolute path, or move `root:` up to a directory that contains both. `localfiles` checks this with real
  filesystem containment (`Path.GetFullPath`, so a `..` segment lands where it actually lands,
  compared against the root with a trailing separator to avoid mistaking a same-prefix sibling
  directory for "inside"); an absolute `path:` is unaffected, unchanged from before. `s3` and `gcs`
  refuse a `..` segment in a `path:` option the same way, since their keys are opaque
  slash-delimited strings with no real filesystem resolution to check containment against, and `..`
  can never be a legitimate authored key component anyway (pz's entity-name grammar already forbids
  one everywhere else). `azureblob` is unaffected: it has no connection-level `root:` for a `path:`
  to escape -- container and path are both always author-declared directly on the dataset/output.
- **The iceberg connector accepts an optional `storage_scope:` connection option**, which becomes
  the `SCOPE` of the storage secret it creates. Without it, a catalog connection's storage secret
  (S3 keys, the AWS credential chain, or an Azure auth method) is unscoped, since a catalog's data
  location is not knowable up front -- and two catalog connections that both configure explicit
  storage credentials therefore both create unscoped secrets, which DuckDB matches
  non-deterministically among several of the same type. `storage_scope:` is validated as a
  URL-shaped prefix (`s3://...`, `abfss://...`, `az://...`) and never logged (setup statements are
  secrets). A `files` catalog's root already implies its own scope; `storage_scope:` overrides it
  the same way. No automated cross-connection warning is added: detecting "two connections with
  different storage credentials" without comparing the credential values themselves (which would
  leak them) is not a reliable signal, so this is documented instead.
- **The `columns:` contract's fixed v0 type matrix (int, bigint, double, decimal, varchar, boolean,
  date, timestamp) is now one shared implementation** (`Pz.Connectors.Toolkit.Formats.ColumnTypeCatalog`)
  instead of four byte-identical copies (`localfiles`' `TypeNameMap`, `s3`'s `S3TypeNameMap`, `gcs`'s
  `GcsTypeNameMap`, `azureblob`'s `AzureTypeNameMap`). No behavior change -- the four copies agreed on
  every mapping and every error message already. `FormatReadRequest` no longer carries a
  `DuckDbTypeName` delegate: the format catalog calls the shared catalog directly instead of a
  per-connector strategy that always ended up passing the identical function.

### Added

- **Process-hosted connectors are harder to orphan.** On Windows, a spawned connector is now assigned
  to a Job Object with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`: if the pz process itself dies ungracefully
  (crash, `taskkill /f`) before its own shutdown ladder can run, the OS kills the connector too, the
  moment the job handle's last reference goes with it. `pz` also sweeps stale `pz-<pid>-*` temp socket
  roots left behind by an earlier, killed pz process (never a live process's, own pid included) each
  time it mints a new one. The out-of-process conformance suite (`pz connector test`) gained an
  `exits-on-connection-loss` vector: a connector must exit on its own within a bounded window of its
  control connection closing without a Shutdown RPC, not only when the host explicitly asks.
  *Not covered*: Linux/macOS have no host-side equivalent of the Windows Job Object reachable from
  .NET's `Process.Start` (the kernel primitive, `prctl(PR_SET_PDEATHSIG, ...)`, must run in the child
  before it execs); a hand-rolled (non-SDK) connector on those platforms still relies on the ordinary
  shutdown ladder alone.
- `.github/dependabot.yml`: weekly, grouped dependency updates for nuget (repo-root `directory`,
  which reaches every csproj by expanding `Pz.slnx`), github-actions, and the Rust workspace
  (`rust/`). Packages that must move together (`Grpc.*`/`Google.Protobuf`, `Apache.Arrow`,
  `JsonSchema.Net`) are grouped explicitly so a bump lands in one PR across every project that pins
  them; major-version bumps are never grouped, so they always arrive as their own PR. No auto-merge.
  The Rust workspace's `tonic`/`prost`/`opentelemetry` crates are one group for every bump size:
  they are all 0.x and only compile as a matched set, so one PR per crate could never go green.
- `ci.yml`'s `rust` job runs `cargo audit` (a version-pinned install, cached via `rust-cache`'s
  `~/.cargo/bin`) after `cargo test`, so a RUSTSEC advisory against a workspace dependency fails CI
  even between dependency bumps.
- CI keeps what `--blame-hang` writes: the `build-test` and `rust` jobs upload `TestResults/` (hang
  dumps and test sequence files) when they fail, and `scripts/rust-conformance.sh` runs its
  `Category=RustPcp` facts under `--blame-hang` with a five-minute ceiling. A rare hang used to be a
  silent job timeout with nothing left to read afterwards.
- Contributors: the `Category=RustPcp` facts run with nothing else in their assembly beside them
  (their telemetry rides on the connector's wall-clock-bounded shutdown flush, which a busy machine
  can starve), and `CONTRIBUTING.md` says how to run them and what `-m:1` is for.
- `release.yml` gates the publish on three checks that previously ran only in PR CI:
  `scripts/check-changelog-entry.sh` (new) fails the release before anything is packed unless
  `CHANGELOG.md` has a `## [X.Y.Z]` heading for the tag (a pre-release tag like `v0.7.0-rc.1` is
  checked against its base version's heading, `## [0.7.0]`), and a new `verify-release` job runs
  `scripts/verify-tool-install.sh`, `verify-aot.sh`, and `verify-sdk-package.sh` against the tagged
  commit before `release` is allowed to push to nuget.org.
- `pz runs`: lists prior runs, newest first, over `IRunArtifactStore` (works
  under `state: {backend: sqlserver}` too, not just local files) — run id,
  status, started/finished time, duration, succeeded/failed/skipped node
  counts, and a reused/carried_forward provenance summary. `--json` prints one
  byte-stable JSON object per run (LF-terminated, invariant-culture numbers,
  UTC ISO-8601 timestamps); `--limit N` caps it to the N most recent runs.
  `run_results.json` gains an additive `finishedAt` field (stamped only on the
  terminal snapshot, absent while a run is still "running"), and both backends'
  `PriorRun`/`PriorNode` now round-trip `startedAt`/`finishedAt` and a node's
  `provenance` for readers.
- `pz completion bash|zsh|fish|pwsh`: prints a shell completion script to
  stdout. The script carries no list of its own: it asks the installed `pz`
  (`pz "[suggest:<position>]" "<line>"`), so verbs, sub-verbs and options all
  complete and an upgrade never leaves a stale script behind; where pz has
  nothing to offer (an option's value is usually a path) the shell's own file
  completion takes over. No network, no file writes; an unrecognized shell
  name is a config error (PZ0535).
- A sink can now publish `IOutputConfigSchema`, an optional JSON Schema for its own
  `write:`/`sink()` options -- the write-side twin of `DatasetConfigSchema`. Before this, a sink's
  output options reached the connector completely unchecked (`ConnectorConfigValidator`'s own
  comment said so): `tablok: true` on a sqlserver output did nothing, silently. `pz validate`
  (tier 3) now schema-checks a sink output's connector-owned options -- against `entities: <e>:
  write:` and `sink()` kwargs alike, since both resolve to the same effective `OutputDef` -- when
  the sink offers the capability, with the same `additionalProperties: false` unknown-option
  message the connection/dataset schemas already give, now also carrying a near-miss "did you
  mean" hint (the edit-distance matcher moved to a shared `Pz.Core.Validation.NearMiss`, with
  `ScriptKwargs.NearMiss` kept as a thin forwarder for its existing call sites). Every builtin sink
  adopts it. `partition_by` is set aside before the check: whether a connector can honour it stays
  the planner's capability question (PZ0314), whatever the sink's schema lists. A sink that does
  not implement the interface is validated exactly as before --
  additive-only, a new capability interface rather than a new `ISinkConnector` member. Forwarded
  over PCP too: `Hello.output_config_schema` (`pz_connector.proto` field 7, additive), populated by
  both SDKs (C# `Pz.Connectors.Sdk`, Rust `pz-connector`) and sanity-checked as valid JSON by
  `pz connector test`'s handshake vector; `pz_connector_reference` (MCP) surfaces it alongside the
  read-side schemas.
- Connectors TestKit: `SinkConnectorAcceptanceTests` gains six facts the fixed 50-row `id
  Int64, name String` fixture never exercised -- a zero-batch commit persists nothing; a large
  batch (20,000 rows across several `WriteBatchAsync` calls) round-trips; an already-cancelled
  token handed to `WriteBatchAsync` is honored (`OperationCanceledException`, not a silent
  success); a null in a nullable column survives a commit; the full v0 type matrix
  (decimal128/timestamp-µs/date32/bool, opt-in via new `TypeMatrixOutput`/
  `ReadTypeMatrixCommittedAsync` hooks, mirroring `MergeOutput`'s null-hook precedent) round-trips;
  and a sink offering `IOutputConfigSchema` offers valid JSON Schema that refuses an unknown key
  (self-detecting -- a no-op for a sink that has not implemented the capability). Every existing
  subclass compiles unchanged, but the new facts run against it: a sink that ignores an
  already-cancelled token now fails the cancellation fact (three of pz's own did). Honour the
  token, or exclude a fact a destination genuinely cannot satisfy through `ShouldRun`; a
  destination that throttles writes can lower the large batch with `LargeBatchRows`. Writing the
  cancellation fact against real connectors found three that silently ignored an already-cancelled
  token (localfiles, azureblob, and the TestKit's own in-memory reference sink), now fixed to check
  it before starting a write, and one that left its Npgsql transaction already disposed by a
  cancelled `BeginBinaryImportAsync` (postgres), whose abort/dispose path now tolerates that rather
  than throwing `ObjectDisposedException` out of `AbortAsync`/`DisposeAsync`. The existing
  `Abort_discards_everything` fact now asserts zero visible rows rather than an empty collection:
  `SmallOutput` is shared with every fact in the suite, and a replace-mode destination that
  truncates-then-stages may legitimately still exist as a zero-row table once another fact has
  committed to it first.
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
- A connector's own log output is no longer dropped on `pz run`/`pz retry`: an
  additive `connector_log` run event (`level`, `connection`, `message`) carries
  a process-hosted connector's `ILogger` output (the C# SDK already queued it
  over the PCP reverse channel; the host wired `logSink: null` and nothing read
  it) and an in-process connector's notice about its connection (e.g. sftp's
  unpinned host key). Every line is in the event stream (`--log-format json`,
  the SQL event store); warn and above is also said once per connection and
  distinct text as a `note:` line, which is what `pz_run` returns too. A
  process-hosted message, with the message of any exception logged beside it,
  passes through the engine's redaction helper first. `pz validate`, `plan`,
  `cdc`, `connectors` and `mcp` still drop connector logs.
  A connector notice's `note:` line now leads with the connection's name
  instead of a host, and the sftp unpinned-host-key notice no longer contains
  the host: a run event must not carry connection config.
  Also fixes two `Pz.Connectors.Sdk` defects this surfaced: a structured log
  property rendered through the connector process's current culture instead
  of invariantly, and an exception logged alongside a message carried only its
  type name onto the wire, never its own message.
- A test walking every `.cs` file under `src/` and `connectors/` for a `"PZ####"` string literal
  outside `PzErrorCode.cs`, enforcing that the catalog stays the one source of truth for a code's
  value: a literal in a project that can reference `Pz.Core` (directly or transitively) must instead
  read `PzErrorCode.SomeName`, and a literal in a project that architecturally cannot (`Pz.PackageManagement`
  and the connector projects, per CLAUDE.md's layering table) must still match a real catalog value.
  `src/Pz.Engine/State/StateEdit.cs`'s three literals (`"PZ0513"`/`"PZ0514"`/`"PZ0515"`) now reference
  `PzErrorCode` instead — the one occurrence the scan found in a project that could already reach the
  catalog and simply never had. Message text is unchanged.

### Fixed

- **`Pz.Connectors.TestKit`'s `StubHttpServer` could fail to start under parallel tests.** It probed
  a free port and then bound it, and anything else could take the port in between ("Address already
  in use"). A failed bind now moves on to a freshly probed port, up to ten times.
- **A pipeline that renders `run_id`/`run_started_at` into its SQL now gets a compile-time warning
  (PZ0232),** once per pipeline. Both constants change every run, so embedding either one in rendered
  SQL changes that Pipeline's NodeId every run too, and `pz retry` (which matches nodes by id against
  a prior run) can never treat two runs of that pipeline as the same node -- it always re-runs it.
  The warning is advisory only; NodeId computation is unchanged.
- **`watermark()` comparisons that name different cursor columns for one dataset are now refused
  (PZ0231)**, instead of silently taking whichever comparison the SQL AST reader returned first --
  e.g. `updated_at > {{ watermark(s, e) }} and created_at < {{ watermark(s, e) }}` used to synthesize
  an incremental cursor off only one of the two columns. Two comparisons that agree on the SAME
  column (a lower bound plus a recognized ceiling, PZ0351) are unaffected.
- **A `source()`/`sink()` kwarg (or a YAML read/write option) with a huge integer literal or a
  decimal literal (Scriban's `BigInteger`/`decimal`) no longer crashes the compile.** `CanonicalJson`,
  which every node's content-addressed id hashes options through, threw an uncaught
  `NotSupportedException` for either type -- unlike a render-time mistake, nothing downstream of
  rendering catches it, so it surfaced as a raw crash instead of a coded error. `CanonicalJson` now
  supports `BigInteger`, `decimal`, and `DateTime` losslessly (arbitrary-precision digits, the decimal
  value, and an ISO-8601 string, respectively); whatever value type still has no lossless canonical
  form is refused as PZ0137, naming the option and the file, never the value. That includes a
  non-finite number (`ratio: 1.0 / 0.0` evaluates to infinity, which JSON cannot represent), and the
  tier-3 schema check accepts the decimal and big-integer values the compiler now hashes, so a kwarg
  that compiles also validates.
- **The C# connector SDK's write-path data plane allocated every incoming batch on the managed
  heap.** `DataPlaneListener`'s `ArrowStreamReader` was constructed with no allocator, unlike the
  host's own read path (`PooledNativeAllocator.Shared`) -- a large enough batch landed on the LOH
  instead of the off-heap native pool the rest of the write path assumes. Batches now come from the
  same pool; ownership is unchanged (a batch is disposed exactly once, immediately after
  `WriteBatchAsync` returns, on every exit including a throw).
- **A write to a Rust-SDK sink could hang forever at commit.** A small write fits in the kernel's
  socket buffer, so the engine can finish the whole data stream and send `CommitWrite` before the
  connector process has accepted the data connection. The Rust SDK's `CommitWrite` revoked the
  session's data-plane ticket on arrival, so that late connection was turned away unread and the
  commit waited for a drain nothing could signal any more — an idle connector, a run that never
  ends (no `engine.node_timeout` by default). It needed a busy machine to show, which is why it
  surfaced as intermittent CI hangs. The ticket now lives as long as the session: burned by the
  data connection the commit waits for, or revoked by `AbortWrite`. Connectors built on the Rust
  SDK pick the fix up by rebuilding against it; the C# SDK never had the early revoke.
- `pz mcp` minors:
  - `pz mcp init` refused an existing `.vscode/mcp.json` (and similar) that legally carries comments or
    trailing commas (JSONC) as "not valid JSON" (PZ0605). It now recognizes JSONC via a tolerant
    fallback parse, and -- since merging the `pz` entry in and serializing back through
    `System.Text.Json` would silently delete every comment -- refuses to rewrite that file with a new,
    distinct PZ0611 whose next step pastes in the exact entry to add by hand, rather than either
    silently dropping the user's comments or refusing with the same code (and message) as genuinely
    broken JSON.
  - `pz_write_pipeline`'s self-verify-failure envelope dropped `result` even though the write had
    already applied, leaving a caller with `applied:true` but no way to know which file pz actually
    wrote. It now carries `result` on both the success and failure envelope, matching the connection/
    entity authoring tools' existing convention. The pipeline/checks-sidecar files are now also written
    via the same atomic temp+rename helper (`Pz.Core.Artifacts.AtomicFile`) other artifact writers use,
    instead of `File.WriteAllText` in place.
  - `PathGuard`'s project-containment check compared resolved paths with `StringComparison.Ordinal`
    unconditionally, which could false-positive "escapes the project directory" (PZ0606) for a
    legitimate path whose resolved casing merely differed from `projectDir`'s on a case-insensitive
    filesystem. The comparison is now `OrdinalIgnoreCase` on Windows/macOS (whose default filesystems
    are case-insensitive) and stays `Ordinal` on Linux, where a same-named-differently-cased sibling
    directory is a real, different directory the guard must keep telling apart.
- The SQL Server state backend stamped `updated_at`/`finished_at` from the ambient wall clock
  (`DateTime.UtcNow`) instead of the injected `TimeProvider` in `SqlKeyedStateStore`/
  `SqlRunArtifactStore` — untestable, and inconsistent with `LocalRunArtifactStore`/`RunResultsWriter`,
  which already thread one through. `RunCommand`'s own retention sweep had the same gap
  (`DateTimeOffset.UtcNow` passed to `RunSweeper.Sweep` where every sibling call in that method already
  uses `TimeProvider.System`). Both stores also assigned a key/cursor/name value past the length its
  `sp_executesql` parameter declares (`@key NVARCHAR(512)`, `@watermark_cursor NVARCHAR(256)`, etc.)
  silently truncated by SQL Server — no warning, no error — so a state key or watermark longer than
  the column allows would be stored (and later looked up) truncated instead of failing loudly. Every
  such parameter is now length-checked client-side and refused with PZ0536, naming the field kind and
  the limit, never the value. A run-artifact field over its limit costs that run's resume/retry
  record (the run itself goes on, with the existing "could not write" warning); the hint names the
  way out — rename what the author controls, or `state.artifacts: false` to keep run artifacts local
  while watermarks and events stay on SQL Server.
- `RunResultsWriter` (`run_results.json`) and `KeyedJsonStateStore` (`.pz/state/*.json`) each hand-rolled
  their own write-aside-and-rename instead of using `Pz.Core.Artifacts.AtomicFile`, the shared helper
  `PlanWriter`/`SchemaCacheWriter`/`ManifestWriter` already published to. Both now route through it, so
  there is exactly one temp+rename implementation instead of three independently-maintained copies.
  Byte output is unchanged (proven by the existing byte-stable/golden tests); a new test pins
  `SchemaCacheWriter`'s overlapping-writer behavior the way `PlanWriterTests` already pinned
  `PlanWriter`'s.
- **`DuckTransientErrors` (the native-tier closed classifier gating retry for a connector's native
  scan/COPY SQL) missed DuckDB's actual wording for a closed-port connection failure.** A real
  httpfs HEAD against nothing listening reports `IO Error: Could not connect to server error for
  HTTP HEAD to '<url>'` -- distinct from the `could not establish connection` phrase already
  classified transient, so this shape fell through to permanent (no retry) despite being exactly
  the kind of transient network condition the classifier exists to catch. Found by provoking a real
  DuckDB httpfs failure (a closed local port) instead of only testing hand-authored fixture
  strings; `could not connect to server` is now a recognized transient phrase. The classifier
  itself, the HTTP 429/500/502/503/504 adjacency matching, "connection reset", "timed out" and the
  false-positive guards (a Parser/Binder/Catalog error's echoed SQL/URL text never reaches the
  classifier) were already in place and already wired into every native seam (setup statements,
  native scan, native COPY) -- CONN-8's audit finding did not reproduce against current `main`.
  Both native scan (read) and native COPY (write) failures are retried on the same classification:
  the write path's existing temp-write-then-atomic-move finalization and explicit rollback-on-failure
  already make a failed native COPY attempt safe to retry (it never leaves a partially-visible
  output, and any open transaction is rolled back before the exception propagates), so there was no
  case for restricting retry to reads only.
- **The managed (Parquet.Net) parquet write path silently truncated timestamps to millisecond
  precision** in the LocalFiles sink, and the AzureBlob and Gcs universal write tiers
  (`ParquetSinkWriteSession`/`AzureBlobFormat`/`GcsFormat`). Each built its timestamp column with
  `DateTimeFormat.DateAndTime` plus `unit: DateTimeTimeUnit.Micros`, but Parquet.Net's
  `DateTimeDataField` constructor hardcodes `Unit = Millis` for `DateAndTime` regardless of the
  `unit:` argument — it only honors a requested unit for the `*Micros`/`*Nanos` format constants.
  Every value's sub-millisecond component (down to DuckDB's own microsecond resolution) was
  dropped on write with no error. Fixed by requesting `DateTimeFormat.DateAndTimeMicros` instead.
  The native COPY path (DuckDB's own `COPY ... TO parquet`) was never affected.

- **`Pz.Connectors.Sdk` hardening sweep** (parked minors from the SDK's final review):
  - A `HostOperationGate`-gated operation whose PCP reverse channel resets (or never attaches at
    all) no longer hangs forever trying to send its best-effort `GateComplete`/log/budget message.
    `HostChannelPeer` fails a send issued after the channel is gone instead of buffering it for a
    reattach that (the host opens `HostChannel` exactly once per process) is never coming, and the
    SDK also closes the peer once the process starts stopping, bounding the "never attaches at
    all" case the same way.
  - `PcpConnectorService`'s per-op `CancellationTokenSource`s and planned reads were never disposed
    or cleared, and the `WebApplication` serving PCP was never disposed either, so neither ran even
    at process exit. The service now releases them from `Dispose`, called once the DI container
    disposes it as part of disposing the now-disposed `WebApplication`.
  - `Configure`'s "already configured" check and its write to `_config` were two separate,
    unsynchronized field accesses: two concurrent `Configure` calls could both observe "not
    configured yet" and both proceed. The check and the write are now one critical section.
  - Reopening a partition (a retry, or a host reading it again) reused the previous attempt's
    `SyncStateCapture`, so `GetReadState` could answer with an already-completed token before the
    new attempt had even started draining. A fresh capture is minted per open, same as the sibling
    failure capture already did.
  - `Handshake` never checked the host's declared protocol major against the connector's own (the
    Rust SDK already does); a mismatch is now refused there too, with both majors named.
  - `--pz-socket ""` (or an all-whitespace path) was accepted and only failed later, confusingly, in
    Kestrel; it is now a usage error naming `--pz-socket` directly.
  - A relative `PzNativeStaging` (a project-file or `-p:` override; the props file's own default was
    already absolute) resolved against two different roots: `PzStageNative`'s `Copy`/`RemoveDir`
    against the project directory, `_PzFindStagedRids`'s raw `System.IO.Directory` call against the
    invoking process's own working directory. `Pz.Connectors.Sdk.targets` now anchors it to an
    absolute path once, before either target reads it.
  - `TraceContextServerInterceptor.Begin` parsed the `traceparent`/`tracestate` metadata on every RPC
    even with nothing exporting (`StartActivity` was already a no-op, but the parse ahead of it was
    not); it now short-circuits on `ActivitySource.HasListeners()` first. `ConnectorTelemetry`'s
    `InstanceId` (written once from Configure, read from every later RPC's own thread) is now backed
    by a volatile field.
- `PZ_DOCS_URL=file://…` (the documented air-gapped route for the `pz_docs_*`
  tools) now actually works: `DocsCatalog` reads a `file:` mirror straight off
  disk instead of handing it to `HttpClient`, which threw `NotSupportedException`
  and surfaced as PZ0609 "this is a pz defect". A missing/unreadable mirror
  now reports PZ0607, the same coded failure an unreachable http mirror
  already gives. Every fetch, over either transport, is capped at 25 MB --
  an oversized `llms.txt`/`llms-full.txt` is a coded refusal (new PZ0610)
  rather than an unbounded read or a silent truncation, and a response that
  declares no length is refused while it is still arriving, not after it has
  all been buffered.
- `pz_entity_schema` now works for an entity not yet declared under a
  connection's `entities:` block -- an entity is just a name in that place,
  matching the natural authoring order (look at the table, then write the
  pipeline). Its new optional `read` argument supplies whatever options the
  connector's schema discovery needs beyond the bare name (e.g.
  `format: parquet`, since localfiles otherwise defaults an undeclared
  entity to csv, which requires a `columns:` contract this call deliberately
  doesn't have); for an entity that IS declared its own read applies, and
  the result carries a `note` saying the passed options went unused. A
  contract-bearing entity whose live schema has grown
  beyond the declared contract now says so additively (`differs_from_contract`
  plus `extra_columns`) instead of silently reporting only the stale
  contract. Every `PzError.File` this tool and the connection/entity/pipeline
  authoring tools emit is project-relative now (e.g. `connections.yml`,
  `pipelines/<name>.sql`), never the machine's absolute temp/project path.
- `pz_run`/`pz_retry` now report the run they just executed instead of
  whatever run happens to read back as "latest" — a stale/foreign run could
  win that race (another run's artifacts sorting newer by the time the MCP
  envelope was built). `McpRunOutcome` gains an additive `RunId`, populated
  the instant the run begins, and `pz_run`/`pz_retry`'s result envelope reads
  that run by id instead of re-reading whatever `ReadLatest()` returns
  afterward -- on every state backend: `pz_run_results(run_id)` likewise
  finds the run by id on a SQL Server state store now, where it used to
  return the latest run with a note saying it could not look one up.
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
- The SQL Server connector and state store no longer forward
  `SqlException.IsTransient` unchanged: Microsoft.Data.SqlClient's own signal
  covers Azure SQL's connection-resiliency reconnect cases only, so a deadlock
  victim (error 1205) or a client-side command timeout (error -2) both come
  back `IsTransient == false` -- verified against a live SQL Server container
  -- and every raise site treated them as permanent instead of retrying. A
  shared `MsTransient` classifier (linked into both assemblies, mirroring
  `AzureTransient`) now also recognizes the deadlock/timeout numbers plus the
  documented pre-login-transport and Azure SQL throttling/failover numbers
  (233, 64, 10053, 10054, 10060, 40613, 40197, 40501, 49918, 49919, 49920).
- The SQL Server and HTTP state backends now split "never reached the store"
  from "reached it, the request itself failed": every `SqlException` and every
  HTTP 429/5xx used to land on PZ0518 ("cannot reach"), which the engine never
  retries, even once a connection or an HTTP response had already come back --
  and the message rendered only the exception's .NET type name, so a 18456
  login failure, a 229 permission error and a 1205 deadlock were
  indistinguishable. A query or request that fails after a successful
  connect/response is now the new PZ0529, naming the SQL error number or HTTP
  status (never the connection string or a bearer token); a SQL error number
  or an HTTP 429/502/503/504 that `MsTransient`/a small closed status set
  classifies transient is retried a bounded number of times first (delay
  through `TimeProvider`, honouring a server's `Retry-After` up to a 30s cap)
  before either succeeding or reporting PZ0529 (which then says how many
  attempts were made and that the failure is usually temporary). A retry can
  never write twice, and it does not mistake its own write for another
  run's: when a SQL write applied and only its acknowledgement was lost, the
  retry finds its own payload at the version it was writing and succeeds
  instead of reporting PZ0520; over HTTP a versioned `PUT` is not replayed
  after a 502/504, where the outcome is unknown, only after a 429/503. The
  wait between attempts ends early when the run is cancelled.
- `Pz.State.Http` (`backend: http`) is no longer stuck on a fixed 100s
  `HttpClient` timeout with no way to cancel it mid-request: a new
  `state.timeout_seconds` (or `PZ_STATE_TIMEOUT_SECONDS`) bounds every
  request, and a run's own cancellation now aborts an in-flight state
  request instead of only the 100s timeout being able to end it -- the
  cancellation propagates uncaught, the same as anywhere else in a run,
  never wrapped into a config-error exit. `Set`/`Remove` now accept `200`
  as success (some servers answer with a body instead of `201`/`204`) and
  `412` as the same version conflict as `409` (the RFC-native rejection
  for a failed `If-Match`); a weak `ETag` (`W/"3"`, common once a reverse
  proxy's gzip layer sits in front of the state server) is now accepted
  for its version instead of reading as absent and downgrading the next
  write to insert-if-absent -- which used to report a spurious PZ0520 on
  every single run behind such a proxy. A `state.url` of `http://` with a
  bearer token configured now warns (new PZ0530, never blocks a run): the
  token would otherwise travel in cleartext with no signal at all. A
  loopback URL is exempt. A request that outlasts the timeout says so
  ("timed out after 30s") and names `state.timeout_seconds` as the knob.
- The sqlserver and postgres connectors accept `connect_timeout_seconds`
  and `command_timeout_seconds` on their connection config, applied to
  every connection/command that does not already set its own (absent ->
  each driver's own default, 15s connect / 30s command, unchanged
  behaviour). A SQL Server command timeout already classifies transient
  via the earlier `MsTransient` fix; a Postgres one already did (Npgsql's
  own `IsTransient` reports true for a command timeout, unlike SqlClient's).
  The mysql connector still refuses both keys: there is no driver here to
  apply them to (DuckDB's own `mysql` extension is the entire data plane)
  and its ATTACH/secret syntax accepts no timeout parameter at all --
  accepting the option and silently doing nothing with it would be exactly
  the deployment-knob-ignored failure this project's error philosophy
  forbids.
- `pz run`/`pz retry`/`pz test`/`pz connector test` (and the `pz mcp` tools
  that share the same execute path) no longer forward a raw OS exception as
  the generic PZ0500 for three diagnosable local I/O failures: permission
  denied writing under the project directory or `.pz` (new PZ0531), the
  filesystem out of space (new PZ0532), and, on Windows, a file another
  process has open without sharing it (new PZ0533). Each names the path and
  a next step; anything else still stays PZ0500 with the underlying
  exception's own message. Classified by exception type/HResult only, never
  by message text.
- A usage error -- an unrecognized command or option (`pz bogus`), or a
  missing required argument -- now exits 2 (the config-error code), not 1
  (which the exit-code contract reserves for "one or more nodes failed"):
  System.CommandLine's own parse-error handling hardcodes exit 1, so a CI
  caller could not tell a mistyped invocation from a run that actually
  executed nodes and failed some of them. `--help`/`--version` are
  unaffected. `ExitCodes` now documents every code, including what a
  cancelled run returns today (unchanged): fatal (3) unless a node had
  already failed before the cancellation was observed, in which case it
  stays node-failures (1).
- sftp's `private_key_path` and gcs's `key_file` now anchor against the
  project directory the same way `localfiles`' `root` and `sqlite`'s `path`
  already do: a relative value joins `base_dir` (the CLI-injected project
  directory) instead of wherever `pz` happened to be invoked from, so
  `pz run --project ../x` no longer breaks a relative credential-file path.
  An absolute path, a `~`-prefixed home-directory shorthand, or a
  URL-shaped value passes through untouched. The shared resolver
  (`Pz.Connectors.Toolkit.ProjectRelativePath`) is available to any other
  first-party connector with the same shape of option.
- The sftp connector's connect (both `pz validate --connect`'s probe and
  every source/sink open) now honours cancellation and a new
  `connect_timeout_seconds` connection option (integer, 1-3600; absent ->
  SSH.NET's own 30s default, unchanged behaviour). `CheckConnectionAsync`
  used to ignore its `CancellationToken` entirely and the underlying
  connect ran SSH.NET's synchronous `Connect()`, so a hung/firewalled host
  could not be cancelled and had no way to bound the wait. A cancelled
  connect now surfaces as a plain `OperationCanceledException`, never
  wrapped into a connector error.
- The sftp connector no longer accepts an unpinned host key silently. With no
  `host_key_fingerprint` declared, `pz validate` now warns naming the
  option (a connector's own `ValidationResult.Warnings` is now collected
  and rendered as a non-blocking PZ0364, a new generic code any connector
  can use the same way -- `ValidationResult.Warnings` is an init-only
  member, so the record's one-argument constructor that compiled connectors
  bind to is untouched, and it crosses PCP as the additive
  `ValidationResultMsg.warnings` field, which the C# SDK fills from the
  connector's result), `pz run` says the same as a run notice, once per
  run however many entities are read through the connection (a new additive
  `INoticeAware` connector interface, wired the same way
  `IOperationGateAware` already is; not yet forwarded over PCP, so it
  reaches in-process connectors only), and `pz validate --connect` prints the fingerprint the server
  presented in the exact `SHA256:<base64>` form the option accepts, so
  pinning is copy-paste. The default (accept any host key when unpinned)
  is unchanged. That fingerprint rides a successful connection check's
  message, which `pz validate --connect` used to discard: it now prints as
  a `note:` line for every connector, so the "not checked: … has no offline
  probe" and "reachable over tcp; credentials are verified at run time"
  messages several connectors already returned are finally visible.
- A forced-universal (`engine.force_universal`) xlsx write to the azure
  connector reported the native-COPY-only refusal ("xlsx write is
  localfiles-only ... DuckDB's excel writer aborts the whole process")
  instead of the universal-tier one ("format 'xlsx' is native-only ...
  azureblob has no native tier here"), because `AzureSink.BeginWriteAsync`
  resolved the final blob location -- which folds in the native-COPY
  check -- before reaching its own universal-tier check. The native-COPY
  check now runs only where it belongs, inside `TryGetNativeCopy`. gcs
  and s3 do not share this bug: gcs's universal path never ran the
  native-COPY check to begin with, and s3 has no universal write path at
  all (`BeginWriteAsync` always refuses outright).
- `pz validate --connect` no longer refuses a ducklake `catalog: sqlite`
  connection whose catalog file exists but is empty: verified directly
  against the sqlite/ducklake DuckDB extensions, that backend initializes
  a zero-byte existing file as a fresh catalog on first attach, the same
  way `pz run` already treats it -- the connect check was refusing it as
  "not a SQLite database file", disagreeing with the run it is supposed
  to predict. `catalog: duckdb` (and the plain `duckdb` connector) is
  unaffected and deliberately unchanged: DuckDB's own native format does
  the opposite -- `attach if not exists` refuses a zero-byte EXISTING
  file outright -- so both already agreed there.
- Two motherduck connections declaring different tokens in one project
  now draw a warning from `pz validate` (PZ0311, the code the run-time
  failure uses) instead of being discovered only at run time: DuckDB accepts
  `set motherduck_token` only before the first attach in a session, so a
  run that touches both fails at the second one. A warning and not a
  refusal, because the project is sound when each run selects only one of
  them. The check compares the two connections' resolved tokens
  (after `${VAR}` interpolation) and names both connections without ever
  printing either token.
- `pz run`'s minted run id (and therefore `.pz/runs/<id>`, the NDJSON `runId`
  field, and every artifact keyed by it) and `run_results.json`'s `startedAt`
  no longer pick up the process's current culture. Both were formatted with a
  custom `yyyy-MM-dd`/`yyyyMMdd`-style pattern and no explicit
  `CultureInfo.InvariantCulture`, so on a machine whose locale uses a
  non-Gregorian calendar (Thai Buddhist, for example) the year rendered
  543 years off and `RunRetention.TryParseRunTimestamp`'s age math silently
  went wrong. `--log-format json`'s `at` field (`JsonRenderer`) had the same
  bug and is fixed the same way.
- Rust connector SDK (`pz-connector`) parity fixes against the C# SDK:
  - A sink's `Sink::abort_semantics()` (new, defaulted to `DiscardsAll` --
    additive, an existing sink keeps compiling unchanged) now crosses into
    `WriteSessionTicket.abort_semantics` verbatim. Every out-of-process Rust
    sink used to report `DiscardsAll` regardless of what it actually wrapped,
    which the delivery-guarantee matrix takes at face value.
  - `CheckConnection` now answers a connector's failed `check()` as
    `ConnectionCheckMsg { ok: false, message }` instead of a gRPC status --
    mirroring the C# SDK, which never lets `CheckConnectionAsync`'s
    `ConnectionCheck(false, ...)` escape as an `RpcException`. `message` is
    exactly what the connector's own `check()` reported, verbatim.
  - A second `Configure` RPC on the same process is now refused
    (`FAILED_PRECONDITION`, "connector is already configured") instead of
    silently re-pointing the connector at a different config, mirroring the
    C# SDK's guard.
  - A new `--pz-manifest` mode prints the connector's `pz.connector.json`
    manifest (the same JSON shape and byte-stable ordering as the C# SDK's
    `ManifestWriter`, generated from the same `ConnectorDecl` `Handshake`
    answers from) to stdout and exits, so a hand-written manifest can no
    longer drift from what the connector actually declares at the
    handshake. Unlike the C# SDK's `--pz-manifest --out <file>` (which also
    takes `--entrypoint`/`--project-directory-anchor`), this crate has no
    RID-based packaging pipeline yet, so it always prints to stdout with an
    empty `entrypoints` map; a packaging step fills that in itself.
  - Connector logging now reaches the host: every `tracing::info!`/`warn!`/
    `error!` call forwards as a `LogEvent` over the reverse channel, the same
    way the C# SDK's `HostLoggerProvider` forwards an injected `ILogger`, so
    the host's `connector_log` run event now carries something for a Rust
    sink too. A connector with no `tracing` subscriber of its own gets this
    automatically (`serve_sink` composes it into a subscriber it installs at
    process start, before either socket binds, so a log from inside
    `Configure` is not lost either); one that installs its own subscriber
    composes the new `pz_connector::log_layer()` into it, the same shape it
    already composes `pz_connector::layer()` into for OTel spans. Levels use
    the same ordinals as `Microsoft.Extensions.Logging.LogLevel`. Nothing in
    the bridge ever reads a connector's `Config`, so a configured value can
    only reach a log line if a connector author puts it there explicitly --
    the same posture the C# SDK's own doc states.
    The reverse channel's other half -- an operation gate a Rust connector
    could use to rate-limit its own outbound calls through the host -- is
    left unwired: it would need a new public trait surface for "a host
    service this connector consumes," which this SDK does not have in v1
    (the wire protocol already defines `GateAcquire`/`GateGrant`/
    `GateComplete`/`GateBudget`; only this crate's trait surface does not
    expose them to connector authors yet).
  - A sink can now report validation warnings: `SinkConnector::validate_warnings()`
    (new, defaulted to empty -- additive) crosses into
    `ValidationResultMsg.warnings`, the write side of the parity `Validate`
    already had (errors only) against the C# SDK's `ValidationResult`.

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
