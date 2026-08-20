# Run events (`--log-format json` NDJSON contract)

`pz run`/`pz test --log-format json` write one JSON object per line (NDJSON) to stdout — one object
per run event, in the order the engine published them. Every object shares three envelope fields:

| Field | Type | Description |
|---|---|---|
| `event` | string | The event's snake_case name (record name minus the trailing `Event`, e.g. `NodeCompletedEvent` → `node_completed`). |
| `at` | string | UTC timestamp, `yyyy-MM-ddTHH:mm:ss.fffZ` (millisecond precision, `Z` suffix). Stamped by an injectable `TimeProvider` (production always `TimeProvider.System`). |
| `runId` | string | The run identity shared with `run_results.json`'s `runId` field. |

Every other field below is specific to that `event` value. **Stability promise: fields are
append-only; renaming or removing a field, or an `event` value, is a breaking change.** New fields may
be added to any event without notice; consumers must ignore fields they don't recognize.

Ordering guarantee: for any single node, events are observed as `node_started` → `node_progress`* →
[`retry_scheduled`*] → [`lossy_integer_inference_detected`] → [`ambiguous_date_inference_detected`] → [`source_drift_detected`] → [`merge_key_duplicates_detected`] →
`node_completed`, because the engine publishes them
from one logical flow per node onto a single-reader channel. Interleaving across concurrently-running
nodes reflects real concurrency and carries no ordering guarantee relative to each other. Exactly one
`run_started` opens the stream and exactly one `run_completed` closes the node stream; a
`retention_swept` line may follow it as the stream's final event (see below).

## `run_started`

Fired once, before any node begins.

| Field | Type | Description |
|---|---|---|
| `projectName` | string | The project's `name:` from `project.yml`. |
| `nodeCount` | number | Size of the effective node set for this run (after `--select` expansion). A `pz retry` run may additionally emit `node_completed` events for carried-forward nodes that lie outside this set, so the count of `node_completed` events can exceed `nodeCount`. |

## `node_started`

Fired once per node, when the executor begins running it.

| Field | Type | Description |
|---|---|---|
| `nodeId` | string | Stable content-hash node id (matches `run_results.json`'s `nodes[].id`). |
| `kind` | string | `SourceLoad` \| `Pipeline` \| `Check` \| `SinkWrite`. |
| `name` | string | The node's declared name. |

## `node_progress`

Fired zero or more times per node while it is running (batch-cadence, not every row).

| Field | Type | Description |
|---|---|---|
| `nodeId` | string | Node id. |
| `name` | string | Node name. |
| `rows` | number | Rows moved so far. |
| `bytes` | number | Approximate bytes moved so far (derived from Arrow batch buffer sizes). |
| `batches` | number | Batches moved so far. |

Deliberately has no `rate` field: rows/sec is presentation a renderer can derive from successive
events' `at` timestamps — not a value worth baking into the schema contract.

## `retry_scheduled`

Fired once per retry attempt scheduled for a node.

| Field | Type | Description |
|---|---|---|
| `nodeId` | string | Node id. |
| `name` | string | Node name. |
| `attempt` | number | The attempt number about to run (1-based, counting the retry itself). |
| `maxAttempts` | number | The retry policy's configured attempt ceiling. |
| `delayMs` | number | Delay before the retry, in milliseconds. |
| `reason` | string | Short human-readable reason for the retry (e.g. the transient error's message). Sanitized the same way a native-engine error is elsewhere (see `NativeStatementRedactor.SanitizeEngineMessage`) before publication, so a connector that echoes a raw DuckDB error into its exception message can never leak a `LINE <n>: ...` statement-echo (and any secret literal it carries) into this event. |

### Retry safety

The engine retry loop (`KindDispatchingExecutor`) only retries a
`PzConnectorException` whose `IsTransient` is true, up to the configured `RetryPolicy.MaxAttempts` — and,
in practice, only `SourceLoad` and `SinkWrite` nodes ever take that path:

- **SourceLoad is safe to retry.** Ingest is all-or-nothing: `DuckSession.IngestArrowAsync` drops the
  destination staging table on any failure (including cancellation) before the exception propagates, so
  a retried attempt always starts from a clean table — it can never append on top of a partially-ingested
  one from an earlier attempt.
- **SinkWrite is safe to retry.** `SinkWriteExecutor` opens a fresh write session per attempt (no session
  state survives across attempts), aborts that session on any write-phase failure before the exception
  propagates (`CommitAsync` is only ever attempted after every batch has been written successfully, and is
  never followed by an Abort), and connectors that write durable files commit via temp-write-then-atomic-move
  — the same pattern `RunResultsWriter` uses for `run_results.json`. A failed attempt therefore never
  leaves a partially-visible output for the next attempt to build on.
- **Pipeline (and Check) nodes are never retried in practice.** Their failures come from plain DuckDB SQL
  exceptions, not `PzConnectorException` — the retry loop's `when (ex.IsTransient ...)` guard never
  matches them, so they fall straight through to the same `PZ0501`-wrapping path a permanent failure does.

## `source_drift_detected`

Fired by a SourceLoad node's drift gate whenever the observed schema
differs from the accepted baseline, under **both** `warn` and `fail` policy — always before that node's
own `node_completed`. `observed` carries the dataset's full new schema (not just the changed columns)
plus `hintsHash`, so an event consumer can implement "accept the new schema" purely from this one event without
a separate schema-fetch round trip. A project whose SourceLoad has no schema baseline configured, or
whose observed schema matches the baseline exactly, never publishes it at all.

Column names and types are fine to log here; connection config is never included, in any field.

| Field | Type | Description |
|---|---|---|
| `nodeId` | string | Node id of the SourceLoad node that detected the drift. |
| `connection` | string | The connection name the dataset was read from. |
| `entity` | string | The entity name within that connection. |
| `policy` | string | The drift policy in effect: `warn` \| `fail`. |
| `changes` | array | One entry per column-level difference between the baseline and the observed schema — see below. |
| `observed` | array | The dataset's full observed schema (every column, not just the changed ones) — see below. |
| `hintsHash` | string | Stable digest of the effective read shape (`SchemaDriftDiffer.HashHints`) the observed schema was read under — the same value a subsequent accepted baseline would be keyed by. |

Each entry in `changes`:

| Field | Type | Description |
|---|---|---|
| `kind` | string | `added` \| `removed` \| `retyped`. |
| `column` | string | The column name. |
| `from` | string \| null | The baseline's type for this column. `null` for `kind: added`. |
| `to` | string \| null | The observed type for this column. `null` for `kind: removed`. |

Each entry in `observed`:

| Field | Type | Description |
|---|---|---|
| `name` | string | Column name. |
| `type` | string | Column type, as reported by DuckDB's `DESCRIBE` of the staged table (the schema truth is the actual landed data, never a connector's own schema introspection). |

## `merge_key_duplicates_detected`

Fired by a SinkWrite node when a `strategy: merge` output's staged
input holds more than one row for at least one merge-key group — always after that node's
`node_started` and before its `node_completed`. The in-batch collapse keeps one connector-determined
survivor per key (physical staging order, NOT cursor order — the sink ABI's documented Absorb
contract), so a stale row can win over a newer one while the watermark still advances past both; this
event makes that collapse loud. It is a warning, never a failure: event-log-shaped inputs legitimately
carry duplicate keys, and the fix when order matters is a deterministic dedup (e.g. max-cursor per key)
in the pipeline. A merge output with unique staged keys never publishes it at all.

Key column names and counts only — never row values, and never connection config.

| Field | Type | Description |
|---|---|---|
| `nodeId` | string | Node id of the SinkWrite node whose staged input holds the duplicates. |
| `output` | string | The output's name. |
| `keys` | array | The declared merge `keys:` column names, in declaration order. |
| `duplicateGroups` | number | How many distinct key groups hold more than one staged row. |
| `extraRows` | number | Total staged rows beyond one-per-key — exactly the number of rows the merge collapse will discard. |

## `lossy_integer_inference_detected`

Fired by a SourceLoad node whose connector declared
the read's schema was DuckDB-inferred (a contract-less csv/json `auto_detect` scan) and whose staged
table holds at least one DOUBLE column whose non-null values are all finite whole numbers with at
least one beyond 2^53 — the shape auto-detect produces from a >int64 integer column, where digits may
already have been silently lost (`12345678901234567890` lands as `1.2345678901234567e+19`). Always
after that node's `node_started` and before its `node_completed`. It is a warning, never a failure:
genuinely floating-point data can look integral. The remedy when the column IS an integer key is a
`columns:` contract (`bigint`/`ubigint`/`hugeint`), which loads such values losslessly and fails
loudly on overflow. Columns whose magnitude exceeds the HUGEINT range never fire it — no declarable
integer type could hold them, so they are scientific-notation floats, not corrupted keys. Database
sources never fire it at all: their DOUBLE columns were already doubles at the source.

Column names only — never row values, and never connection config.

| Field | Type | Description |
|---|---|---|
| `nodeId` | string | Node id of the SourceLoad node whose staged table holds the suspect column(s). |
| `connection` | string | The connection's name. |
| `entity` | string | The entity's name. |
| `columns` | array | The suspect DOUBLE column names, in staged-schema order. |

## `ambiguous_date_inference_detected`

Fired by a SourceLoad node whose schema-inferred csv
read committed to a day-first/month-first date or timestamp format (e.g. `%d/%m/%Y`) while no staged
value's day exceeds 12 — every value was ambiguous, so the sniffer's field-order pick was a guess and
a month-first (US) source is misread on every row. The two signals are combined deliberately: the
sniffed format alone cannot distinguish "forced by a day->12 value" from "assumed" (both report the
same format string), and the staged data alone cannot distinguish an ambiguous `01/02/2024` from an
unambiguous ISO `2024-02-01`. Always after that node's `node_started` and before its
`node_completed`. It is a warning, never a failure: the data may genuinely be day-first. The escape
hatch when it is not: normalize the source to ISO 8601, or declare the column `varchar` in a
`columns:` contract and parse it explicitly in SQL. Fired once per format family (date, timestamp)
with all suspect columns of that family collected.

Column names and the picked format only — never row values, and never connection config.

| Field | Type | Description |
|---|---|---|
| `nodeId` | string | Node id of the SourceLoad node whose staged table holds the suspect column(s). |
| `connection` | string | The connection's name. |
| `entity` | string | The entity's name. |
| `columns` | array | The suspect DATE/TIMESTAMP column names, in staged-schema order. |
| `format` | string | The strftime format the sniffer committed to (e.g. `%d/%m/%Y`). |

## `node_completed`

Fired exactly once per node in the effective set, whatever its outcome (success, failure, or skip). A
`pz retry` run additionally emits one for each carried-forward node (a sink committed by the prior run
and recorded into this retry's results) even though such nodes lie outside the effective set counted by
`run_started.nodeCount` — so a retry can emit more `node_completed` events than that announced count.

| Field | Type | Description |
|---|---|---|
| `nodeId` | string | Node id. |
| `kind` | string | `SourceLoad` \| `Pipeline` \| `Check` \| `SinkWrite`. |
| `name` | string | Node name. |
| `status` | string | `success` \| `failed` \| `skipped`. |
| `rows` | number | Rows moved by this node. |
| `durationMs` | number | Wall-clock duration of this node, in milliseconds. |
| `errorCode` | string \| null | `PZ####` error code, present only when `status` is `failed`. |
| `errorMessage` | string \| null | Human-readable error message, present only when `status` is `failed`. |
| `timings` | object \| null | See below. `null` for `Pipeline`/`Check` nodes (no channel to measure) and for any node whose execution bypassed the channel-instrumented path (the native-scan/native-copy tiers). Present for `SourceLoad`/`SinkWrite` nodes that went through their Arrow channel. |
| `provenance` | string | Optional (append-only addition). `"reused"` — a retried SourceLoad satisfied from the failed run's staging; `"carried_forward"` — a sink committed by the prior run, recorded into this retry's results. Absent for normally-executed nodes. |
| `ops` | object | Optional (append-only addition). See below. Absent when the node's connector isn't gate-aware, the node ran through a native tier (no .NET operation gate), or the node failed. |
| `partitions` | object \| null | Partition-mode extraction stats `{total, completed, reused, resumed}`; omitted for non-partition-mode nodes and for failed nodes. Counts only — never partition identifiers. |
| `delivery` | object \| null | `{abortSemantics, rowsVisible, resumedRows}` — honest-abort / delivery-resume stats. Omitted for nodes without delivery semantics: only present on a failed sink node whose connector declares non-DiscardsAll abort semantics, or a successful sink node that resumed past a delivery checkpoint. |
| `cdc` | object \| null | `{inserts, updates, deletes, position}` — raw per-op change counts from the last-event-per-key collapse, never net counts. Omitted for every non-cdc dataset; present only on a successful cdc-shaped SourceLoad. |

A failed `Check` node's `errorMessage` records up to 5 offending row values verbatim (mirrored in
`run_results.json`) — do not enable checks on columns holding sensitive/PII values if that isn't
acceptable for your project (a redaction opt-out is planned).

`timings` (when present):

| Field | Type | Description |
|---|---|---|
| `producerStallMs` | number | Milliseconds spent stalled on the side of this node's channel that generates batches: for `SourceLoad`, the partition reader blocked pushing into the channel (channel full ⇒ ingest is the bottleneck); for `SinkWrite`, the egress reader blocked waiting on DuckDB to produce the next batch (staging query is the bottleneck). |
| `consumerStallMs` | number | Milliseconds spent stalled on the side of this node's channel that consumes batches: for `SourceLoad`, the ingest drain blocked waiting for the next batch (channel empty ⇒ the source is the bottleneck); for `SinkWrite`, the write call blocked on the sink (the sink is the bottleneck). |

Measured with `Stopwatch.GetTimestamp()` deltas around the one batch-level await each side already
performs (never per-row) — see `StallAccumulator` (Pz.Engine). When one side's stall covers at least 60%
of `durationMs`, `pz run`'s text-mode renderers (`ConsoleRenderer`/`LiveTreeRenderer`) print an extra
`hint: <name>: <label> — ...` line right after the node's own line (`source-bound`/`ingest-bound` for
`SourceLoad`, `staging-bound`/`sink-bound` for `SinkWrite`). The hint text is presentation only — every
consumer can recompute it from `timings` and `durationMs` — and is never itself an NDJSON field.

`ops` (when present):

| Field | Type | Description |
|---|---|---|
| `executed` | number | Count of universal-tier operation attempts the connector's operation gate executed for this node (pacing + retry loop). |
| `retried` | number | Count of those attempts that were retried by the gate after a transient, idempotent failure. |
| `throttleWaitMs` | number | Total milliseconds the gate spent waiting on pacing (bucket/budget hints) for this node — never retry backoff. |

Present only for a `SourceLoad`/`SinkWrite` node whose connector is gate-aware and ran through the
universal tier and succeeded — same `run_results.json` `ops` object, just camelCase
field names instead of snake_case.

`partitions` (when present):

| Field | Type | Description |
|---|---|---|
| `total` | number | Total partitions this SourceLoad's plan enumerated. |
| `completed` | number | Partitions the plan finished extracting this run. |
| `reused` | number | Partitions satisfied from a prior run's staged data rather than re-extracted. |
| `resumed` | number | Partitions that resumed a previously in-flight (interrupted) extraction rather than starting over. |

Present only for a partition-mode SourceLoad — same `run_results.json` `partitions`
object, field names unchanged (already single-word, no snake_case/camelCase divergence).

`delivery` (when present):

| Field | Type | Description |
|---|---|---|
| `abortSemantics` | string | The sink's declared abort semantics: `discards_all` \| `best_effort` \| `none`. |
| `rowsVisible` | number | Rows already visible at the destination when the sink aborted, or rows visible after a resumed delivery. |
| `resumedRows` | number | Rows a checkpoint resume skipped re-delivering (`0` on a failure with no prior resume). |

Present only on a failed sink node whose connector declares non-DiscardsAll abort semantics, or a
successful sink node that resumed past a delivery checkpoint — same `run_results.json`
`delivery` object, just camelCase field names instead of snake_case.

`cdc` (when present):

| Field | Type | Description |
|---|---|---|
| `inserts` | number | Raw count of insert-op change rows landed in `<staging>__changes` before the collapse. |
| `updates` | number | Raw count of update-op change rows landed before the collapse. |
| `deletes` | number | Raw count of delete-op change rows landed before the collapse. |
| `position` | string \| null | The opaque candidate token (Postgres LSN / SQL Server CDC log position) this window's cdc partition emitted — the same value `run_results.json`'s `cdc.position` records. `null` when the connector emitted none this window. |

Present only for a successful cdc-shaped (`mode: cdc`) SourceLoad — same `run_results.json` `cdc`
object, just camelCase field names instead of snake_case.

## `run_completed`

Fired exactly once, after every node in the effective set has reached a terminal status.

| Field | Type | Description |
|---|---|---|
| `status` | string | `success` \| `completed_with_failures` \| `fatal`. |
| `succeeded` | number | Count of nodes with `status: success`. |
| `failed` | number | Count of nodes with `status: failed`. |
| `skipped` | number | Count of nodes with `status: skipped`. |
| `durationMs` | number | Wall-clock duration of the whole run, in milliseconds. |

## `breaker_state_changed`

Fired whenever a source/sink instance's circuit breaker (`engine.breaker:`) transitions
state. Unlike every other event above, this one is **not** part of any single node's `node_started` →
... → `node_completed` sequence — a breaker is shared by every node of one instance, so this can fire
before, between, or independent of any one node's own lifecycle. A project with no `engine.breaker:`
configured never publishes it at all.

| Field | Type | Description |
|---|---|---|
| `instance` | string | The source/sink instance's key: `conn:<name>` -- the CONNECTION, not the direction, so a database pz both reads and writes trips one breaker (the same instance-scoped granularity as `max_concurrency`). |
| `oldState` | string | `closed` \| `open` \| `half_open`. |
| `newState` | string | `closed` \| `open` \| `half_open`. |
| `trigger` | string | Short human-readable reason for the transition, e.g. `5 consecutive transient failures`, `cool-down elapsed`, `probe succeeded`, `probe failed`. |
| `coolDownMs` | number | The cool-down duration, in milliseconds, for a transition INTO `open` — the greater of `engine.breaker.cool_down` and any `RetryAfter` floor reported by the failure that tripped it. `0` for every other transition (`open` → `half_open`, `half_open` → `closed`), which has no fresh wait to report. |

## `retention_swept`

Emitted once at the end of a run when automatic retention (`retention:` in `project.yml`) deleted at
least one `staging.duckdb` or stale `.pz/tmp` workdir. Arrives **after** `run_completed` — it is the
last event of the stream. Absent entirely when `retention: off`, and when the sweep did nothing at
all — nothing swept, no stale workdirs, no failures. Present even when the sweep freed nothing, as
long as at least one deletion failed.

| Field | Type | Description |
|---|---|---|
| `runsSwept` | number | Run directories whose `staging.duckdb` was deleted. |
| `bytesFreed` | number | Total bytes reclaimed, including stale `.pz/tmp` workdirs. |
| `failures` | number | Directories that could not be deleted; the sweep continued past each. |

Counts only. Run-directory names and filesystem paths are deliberately absent.

When `state.artifacts` resolves to a SQL Server store,
`runsSwept` counts whole runs deleted from `pz.runs`/`run_nodes`/`run_events` instead of `staging.duckdb`
files, and those runs contribute nothing to `bytesFreed` (a row count is not a byte count) — only stale
`.pz/tmp` bytes do. Field meanings for the local backend are unchanged.

## Persisting this stream (`state.events: true`)

**Stdout NDJSON remains the contractual surface described by this whole document, unconditionally.**
With `state.events: true` (see [Move state off the local disk](https://pipelinez.dev/how-to/remote-state/)), the same
event stream is *also* persisted into `pz.run_events` — one row per event, ordered by an explicit
per-run `seq` column rather than insert order, since a batched writer cannot rely on the latter and
`at`'s millisecond precision has real ties. This is an additional consumer of the existing stream, not
a second contract: every row's `event`/payload fields mirror this document's NDJSON shape exactly. The
persisted stream is lossy under sustained overload by design — a bounded in-memory buffer drops events
rather than ever stalling the engine or the console — and a truncated tail is reported as `events_dropped`
on the run's header row (`pz.runs.events_dropped`), never silently. That header row is why
`state.events: true` requires `state.artifacts: true` (`PZ0124`): only `artifacts` writes it, and
without it the drop count would have nowhere to land and the run's `run_events` rows would never be
retention candidates. NDJSON to stdout is never affected
by any of this: it is a separate renderer and always sees every event.

## Example

```json
{"event":"run_started","at":"2026-07-04T10:00:00.000Z","runId":"20260704T100000000Z-ab12","projectName":"hello_pz","nodeCount":3}
{"event":"node_started","at":"2026-07-04T10:00:00.010Z","runId":"20260704T100000000Z-ab12","nodeId":"a1b2","kind":"SourceLoad","name":"src_crm__customers"}
{"event":"node_completed","at":"2026-07-04T10:00:00.041Z","runId":"20260704T100000000Z-ab12","nodeId":"a1b2","kind":"SourceLoad","name":"src_crm__customers","status":"success","rows":3,"durationMs":31,"errorCode":null,"errorMessage":null,"timings":null}
{"event":"run_completed","at":"2026-07-04T10:00:00.120Z","runId":"20260704T100000000Z-ab12","status":"success","succeeded":3,"failed":0,"skipped":0,"durationMs":120}
```
