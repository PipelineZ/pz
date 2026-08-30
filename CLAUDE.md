# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

PipelineZ (`pz`) is a lightweight, developer-first batch data pipeline engine for SQL-based
ETL/ELT, powered by DuckDB, that can run anywhere without requiring a data platform. A project
is one
`connections.yml` (places with credentials) + SQL files (transformations, and the reads and writes
they perform); `pz` compiles them into a dependency-ordered DAG and executes it in-process, moving
data through zero-copy Arrow via a versioned connector ABI. Pre-release (v0.x).

**The authoring surface.** There are no `sources/`/`sinks/` directories — one `connections.yml`,
where a **connection** is a place with credentials, an **entity** is a thing in that place named the
way that place names it (no `schema:`/`table:` — PZ0348), and the **direction** is the function you
call. `outputs:` is retired (PZ0347); leftovers are PZ0346.

- **Two surfaces, one declaration.** Every read and write option may be declared under
  `entities: <e>: read:`/`write:` **or** as a `source()`/`sink()` keyword argument — never both
  (**PZ0341**), never merged. There is no effective-config assembly; that is what keeps two surfaces
  from becoming a precedence problem. Kwarg names equal YAML keys at every nesting level, so moving
  an option is cut-and-paste.
- **`source()`/`sink()` are `IScriptCustomFunction`s** (`Pz.Core/Templating/`), not imported
  delegates — Scriban otherwise binds an unrecognized named argument into the next free *positional*
  slot. Named arguments are readable only off `((ScriptFunctionCall)callerContext).Arguments`. A call
  cannot span lines.
- **A source dataset is read by exactly one pipeline (PZ0349)**, which is what lets a pipeline's own
  SQL be the whole story for its reads.
- **SQL-declared incrementals need no `columns:` contract**:
  `where cursor > {{ watermark(s, e) }}` is the declaration, typed from the stored watermark. The
  bounded-window trio (`initial`/`max_window`/`until`) still needs a contract — bounds must be
  computable before the first extraction (PZ0213) — and ceilings are recognized (PZ0351).
- **`ReadHints`**: projection and predicate pushdown extracted from pipeline SQL by DuckDB's own
  parser and passed to capable connectors; part of the SourceLoad content hash.
- **`root:`** on `localfiles`/`s3` says where the place is; `path:` is optional (a read with none is
  `<root>/<entity>.<format>`, a write `<root>/<entity>/`).
- Samples and templates are points on that spectrum: `mssql-mart` declares its whole read in SQL,
  `templates/http` in YAML, `templates/sample` one of each.

## Build & test

Requires the .NET 10 SDK. Docker is optional — suites needing Postgres/MinIO (Testcontainers) use
`Xunit.SkippableFact` and SKIP cleanly without it (`tests/Pz.TestSupport/DockerFacts.cs`).

```bash
dotnet build Pz.slnx -c Release            # zero warnings required (TreatWarningsAsErrors)
dotnet test Pz.slnx -c Release --no-build  # zero failures required (skips OK without docker)

# Single project / single test
dotnet test tests/Pz.Core.Tests -c Release
dotnet test tests/Pz.Engine.Tests -c Release --filter "FullyQualifiedName~Watermark"

# Packaging end-to-end proof (pack → tool install → pz init → pz run, offline; installs the
# host-RID Native AOT sub-package). Also a PR CI gate (ci.yml's pack-and-verify job); still worth
# running locally after touching src/Pz.Cli, templates/, or any packable .csproj.
scripts/verify-tool-install.sh

# Native AOT runtime proof (publish native image → init/run/restore/PZ0360/PCP-spawn/MCP).
# Also a PR CI gate (ci.yml's verify-aot job). Linux only.
scripts/verify-aot.sh
```

`PZ_TESTS_OFFLINE=1` skips network-dependent tests. Benchmarks live in `tests/Pz.Benchmarks`
(BenchmarkDotNet) plus `scripts/macro-bench.sh`.

No direct pushes to `main` — land changes through a PR; CI (`.github/workflows/ci.yml`) must be
green. Two jobs: `build-test`, an ubuntu+windows matrix where both legs build but only ubuntu runs
`dotnet test` (with `PZ_TESTS_OFFLINE=1` and `--blame-hang-timeout 10m`) — windows is build-only,
because the docker suites can't pull Linux images there — and `pack-and-verify` (ubuntu), which runs
`scripts/verify-tool-install.sh` so the install path a stranger's first five commands depend on
cannot silently rot. `release.yml` stays tag-triggered.

## Architecture

**The documentation lives at `https://pipelinez.dev`, not in this repository** — concepts (the
authoritative design description, kept drift-free against the code), how-to guides, the CLI and
`project.yml` reference, the NDJSON event contract at `/events/`, and memory/benchmarks at
`/performance/`. It is maintained in the `pz-site` repository. A change here that makes one of those
pages wrong needs a matching PR there.

Two files stay in this repo because the build and the test suite read them off disk:
`docs/events.md` (pinned by `EventsDocReflectionTests`, which asserts every `RunEvent` property is
documented there) and `docs/reference/authoring-for-agents.md` (embedded into the binary). Edit those
here — they are the source of truth, and the site's copies are generated from them. See
`docs/README.md`.

**Hub-and-spoke with DuckDB as the hub.** Sources land data into a disk-backed DuckDB staging DB
(`.pz/runs/<id>/staging.duckdb`), SQL pipelines transform inside DuckDB, sinks drain results out.
DuckDB is the buffer manager — the .NET side only ever holds in-flight Arrow batches.

**Layering (strictly downward):**

| Project | Responsibility |
|---|---|
| `src/Pz.Cli` | verbs, console rendering, exit codes (0 ok, 1 node failures, 2 config/validation, 3 fatal) |
| `src/Pz.Core` | project model, YAML/SQL parsing, Scriban templating, DAG compilation, validation |
| `src/Pz.Engine` | dispatcher, node executors, retries, run artifacts, watermark state (`Pz.Engine/State`) |
| `src/Pz.DuckDb` | DuckDB interop (Arrow ingest/export, query, EXPLAIN) behind an interface |
| `src/Pz.PackageManagement` | in-proc NuGet resolution, `pz.lock.json`, the out-of-process connector host (PCP) |
| `src/Pz.Connectors.Abstractions` | **the connector ABI — the contract of the ecosystem**; may reference Apache.Arrow only |
| `src/Pz.Connectors.TestKit` | acceptance/contract test suite every connector runs against |
| `src/Pz.Diagnostics` | typed events, ActivitySource, meters; console/NDJSON renderers over one event stream |
| `src/Pz.State.Http` | pluggable state backend: `IKeyedStateStore` over a server's run-scoped HTTP state endpoints (ETag/`If-Match` CAS), keyed state only — referenced directly by `Pz.Cli` |
| `src/Pz.State.SqlServer` | pluggable state backend: `IKeyedStateStore`/`IRunArtifactStore` over SQL Server, schema creation/migration, batched event persistence — referenced directly by `Pz.Cli`, not loaded as a connector |
| `src/Pz.Mcp` | the `pz mcp` verb's server: 22 typed tools (introspect/verify/author/docs always registered, `pz_run`/`pz_retry`/`pz_run_results` only under `--allow-run`) — referenced directly by `Pz.Cli`, not loaded as a connector |
| `connectors/` | first-party connectors: LocalFiles, Postgres, S3, SqlServer, AzureBlob, Http, MySql, Sqlite |

`templates/` holds real, in-place-runnable projects that are simultaneously the browsable examples
and `pz init`'s only source, bound to `TemplateCatalog` by set-equality tests.

> `src/Pz.Mcp/Pz.Mcp.csproj` embeds `docs/reference/authoring-for-agents.md` so `pz mcp init` can copy
> it onto disk with no network and no source tree. It is the only documentation file still in this
> repository; moving or renaming it breaks the build. Every other page is fetched from the site by
> `pz_docs_list`/`pz_docs_search`/`pz_docs_get`.

**Load-bearing decisions** (don't undo these casually — see `https://pipelinez.dev/concepts/architecture-overview/`'s
"Decision log" for the full log):

- **Two-tier data plane, chosen per edge by the planner**: native scan/copy (connector hands DuckDB a
  SQL fragment; data never enters .NET) preferred over the universal Arrow `RecordBatch` stream path.
- **Arrow `RecordBatch` is the one canonical in-memory format** — zero-copy across the DuckDB C Data
  Interface, pooled off-heap native buffers (Arrow's managed default would put every batch on the LOH,
  so final buffers come from a power-of-two pool over `NativeMemory` and are recycled on dispose).
  Batch handed to `WriteBatchAsync` is engine-owned until the call returns; ownership bugs are the
  worst bugs here, and the TestKit enforces the lifetime protocol.
- **External (non-builtin) connectors are hosted out of process only (PZ0360).** A restored package
  must declare `runtime: "process"` (PCP); `"dotnet"` or a missing manifest is refused at registry
  construction — the process host is the trust and crash boundary for third-party code, and builtins
  are the only in-process connectors. The collectible-ALC host is deleted, and the CLI ships
  **Native AOT**: hybrid RID-specific tool packaging (`pz.<rid>` AOT sub-packages for
  linux-x64/linux-arm64/win-x64/osx-arm64 + a CoreCLR `pz.any` fallback, pointer package `pz`).
  First-party code stays at zero trim/AOT warnings (analyzers error); the six third-party
  assemblies whose internals warn are runtime-proven by `scripts/verify-aot.sh` (a CI gate), which
  drives init/run/restore/PZ0360/PCP-spawn/MCP against the native image.
- **DAG edges come from `ref()`/`source()`/`sink()` template calls at render time** (sandboxed
  Scriban, whitelisted functions only), never from parsing SQL. DuckDB still validates rendered SQL
  via EXPLAIN/PREPARE (validation tier 4). One narrow exception covers *derivation*, not edges:
  where a recognized form is total-or-error, DuckDB's own parser (`json_serialize_sql`, behind
  `ISqlAstReader`) may read it — that is how `watermark()` comparisons and `ReadHints` are extracted.
  Hand-rolled or regex SQL parsing remains forbidden.
- **One serialized DuckDB connection per run** gated by a `SemaphoreSlim` — a correctness fix (one
  `DuckDBConnection` is not safe for concurrent statement execution; concurrent dispatch raced on
  native pending-query state), and measured ~free: concurrent/sequential ratio 0.94–1.04, and
  connection-per-operation measured the same, so removing the gate buys nothing. Concurrency comes
  from the topological dispatcher + bounded channels, not parallel connections.
- **Execution is always staged materialize-then-drain**, never statement-scoped DML — even for the
  inline `INSERT INTO {{ sink(...) }}` form, which the compiler strips.
- **Every command runs the same 8 phases** (`load → restore-check → compile → validate → plan →
  execute → finalize → report`); `compile`/`plan` stop early. Node kinds: SourceLoad, Pipeline, Check,
  SinkWrite. Nodes have stable content-addressed IDs — that's what makes `pz retry` and incremental
  watermarks coherent.
- **Watermarks**: `.pz/state/watermarks.json` via `Pz.Engine/State/WatermarkStore`; connectors AND
  `cursor > $wm` into extraction; new watermark = post-land `MAX(cursor)` against local staging,
  persisted only after every downstream SinkWrite commits (carried-forward sinks count, but only when
  their SourceLoads landed as `reused` — the advancement-time provenance gate in
  `WatermarkAdvancement`).
- **Retry reuse + delivery guarantees**: `pz retry` copies succeeded SourceLoads' staged tables from
  the failed run (per-node ATTACH alias; any guard failure falls back to re-extraction with a note)
  and seeds prior-committed sinks as carried-forward results; `run_results.json`/NDJSON carry additive
  `provenance`/`watermark` fields (omit-when-absent). The guarantee matrix is a stability contract
  (`https://pipelinez.dev/concepts/delivery-guarantees/`): merge/replace effectively-once, append at-least-once —
  incremental→append fails compile (PZ0214) without `write: { duplicates: accept }` on the output.

## Binding conventions

- xunit with **plain `Assert.*`** — no FluentAssertions or similar.
- **Conventional commits, one commit per green build/test cycle.**
- **Error philosophy**: every user-facing error carries a `PZ####` code and names the file/node,
  cause, and a next step. Validation reports **all** errors (aggregate, never fail-one-at-a-time). No
  silent failures. Exception taxonomy: `PzConfigException`, `PzValidationException`,
  `PzConnectorException` (`IsTransient`/`RetryAfter` drive engine retries — connectors never retry
  internally), `ConnectorHostException`, `RestoreException`.
- **Determinism**: byte-stable writers (LF line endings, final newline, explicit ordering) for every
  `.pz` artifact. Golden-file tests (sample projects, snapshot-compared compile output) are the
  determinism regression net — golden changes must be sanctioned per-case and explained line-by-line
  in review. No `DateTime.Now`-style nondeterminism; time goes through injectable `TimeProvider`.
- **ABI changes are additive-only**: no `ISourceConnector2`; growth via new capability interfaces +
  `ConnectorCapabilities` flags. The Abstractions reference allowlist (Apache.Arrow only) is fixed.
  TestKit additions must not break existing subclasses (virtual-with-default).
- **Secret/PII hygiene**: no connection config or SQL text in planner Reason strings, `plan.json`, or
  events; `SetupStatements` (CREATE SECRET etc.) are never logged unredacted. Generated SQL uses typed
  literals and `Quote` (injection-safety pattern).
- **Tests**: docker suites must SKIP (not fail) without docker; no wall-clock-sleep tests — use
  gate-based determinism.
- **`docs/events.md` is a stability contract**: event fields are append-only; renaming/removing a
  field or event name is breaking.
- **Terminology: "dispatch"/"dispatcher", never "scheduler".** Within-run dependency-ordered dispatch
  is `RunOrchestrator` in `Pz.Engine.Dispatch` — "scheduler" reads as cron/Airflow to data engineers,
  which pz deliberately is not. Reserve "scheduler"/"scheduling" for genuinely external triggering
  (Windows Task Scheduler, Airflow, run spacing). Two deliberate exceptions stay: the
  `RetryScheduled`/`retry_scheduled` event (an events.md contract, and a retry genuinely *is*
  scheduled) and "no orchestration/scheduling" in the v1 non-goals.
- **Comments state the constraint, not its provenance.** Write the rule a reader needs — why an
  ordering is load-bearing, who owns an Arrow batch, which quoting rule a dialect requires — never a
  citation to a document outside this repository.
- Versioning is MinVer from git tags (`v*` prefix); releases are tag-triggered and publish via NuGet
  trusted publishing (see `CONTRIBUTING.md`).
