# Authoring pz projects as an agent

A quick-reference map of the pz authoring surface for an agent working through the `pz` MCP
server's tools. Everything here is **convenience, not ground truth**: it exists to help you write
correct YAML/SQL on the first try and to know which tool to reach for next. If anything here ever
disagrees with what a tool call actually returns — a compile error, a `next_step` hint, a
`pz_project_overview` result — the tool is right and this page is stale. A client that cannot read
MCP resources at all loses nothing it needs: every rule, error code, and next step below is also
what the tools themselves report.

## Project anatomy

A pz project is a directory of YAML and SQL, no registration lists:

```
my-project/
├── project.yml          # identity, connector requirements, engine config, vars
├── connections.yml       # every place pz talks to, and each place's entities
├── pipelines/
│   ├── stg_orders.sql
│   ├── orders_enriched.sql
│   └── configs/
│       └── orders_enriched.yml   # optional sidecar: materialization, tags, checks
└── .pz/                  # generated, gitignored — compiled SQL, run history, watermarks
```

- **`project.yml`** — project name/version, the `connectors:` list (NuGet package + version each
  connector ships as), `vars:` (referenced from SQL via `var('name')`), and `engine:` tuning.
  Minimal example:

  ```yaml
  name: pz_new_project
  version: 0.1.0

  connectors:
    - package: Pz.Connector.LocalFiles
      version: 0.1.0

  vars:
    min_amount: 10
  ```

- **`connections.yml`** — one top-level YAML key per **connection** (a place with credentials);
  there is no `connections:` wrapper and no nested `connection:` key. Connector options sit
  directly under the connection's name. A connection may declare `entities:` for the things
  (tables, files, endpoints) that live there:

  ```yaml
  crm:
    connector: localfiles
    entities:
      orders:
        read:
          path: data/orders.csv
          format: csv
          columns:
            id: bigint
            customer_id: bigint
            amount: double
            status: varchar

  lake:
    connector: localfiles
    root: out   # a source() write lands under out/<entity>/ unless overridden per entity
  ```

  An entity does not have to be declared in YAML at all — see [Two surfaces, one
  declaration](#two-surfaces-one-declaration-pz0341) below. `sources:`/`sinks:` directories and a
  top-level `outputs:` block are retired (`PZ0346`/`PZ0347`); everything is one `connections.yml`.

- **`pipelines/*.sql`** — one pipeline per file, named after the file. Extract is
  `{{ source(...) }}`/`{{ ref(...) }}` in the `FROM`; transform is the `SELECT`; load is an
  optional leading `INSERT INTO {{ sink(...) }}`. A pipeline with no `INSERT INTO` is an
  intermediate — it stays valid as long as another pipeline `ref()`s it.

- **`pipelines/configs/<name>.yml`** — optional sidecar matched by pipeline name, for
  `materialization` (`table`/`view`/`ephemeral`), `tags`, and data-quality `checks`:

  ```yaml
  # pipelines/configs/orders_enriched.yml
  pipeline: orders_enriched
  materialization: table
  tags: [daily, crm]
  checks:
    - not_null: [id, email]
    - unique: [id]
  ```

  Other check types: `row_count: { min, max }`, `freshness: { column, max_age }`,
  `accepted_values: { column, values }`, `custom_sql: { name, sql }`. Checks are observational —
  a failing check fails the run but never blocks the pipeline's own sink write.

## Two surfaces, one declaration (PZ0341)

Every read or write option can be declared **either** under `connections.yml`'s
`entities: <e>: read:`/`write:`, **or** as a keyword argument on the `source()`/`sink()` call that
uses it — **never both**. There is no merge of the two; a reader of one file sees the whole story
for that entity-side. Kwarg names equal YAML keys at every nesting level, so moving an option
between the two surfaces is cut-and-paste. Declaring the same entity-side in both places is
`PZ0341`.

Read, declared at the call site:

```sql
join {{ source('crm', 'customers', path: 'data/customers.csv', format: 'csv') }} as c
```

Read, declared in YAML instead (`connections.yml`):

```yaml
crm:
  entities:
    customers:
      read:
        path: data/customers.csv
        format: csv
```

Write, declared at the call site:

```sql
INSERT INTO {{ sink('lake', 'orders_curated', format: 'parquet', strategy: 'replace') }}
```

Write, declared in YAML instead:

```yaml
lake:
  entities:
    orders_curated:
      write:
        format: parquet
        strategy: replace
```

An entity that appears in neither `connections.yml` nor as a bare name is not an error — a
`source()`/`sink()` call that names it *is* the declaration. Only an unknown **connection** is an
error (`PZ0201`).

## The four template functions

Pipeline SQL is Scriban in sandboxed mode with a small whitelisted function set — no file I/O, no
network, no wall clock:

| Call | Does |
|---|---|
| `source('<connection>', '<entity>', **kwargs)` | Reads an entity, resolves to a staging table name, declares a DAG edge. `**kwargs` are read options per the two-surfaces rule above. |
| `ref('<pipeline>')` | Reads another pipeline's output, resolves to its staging table, declares a DAG edge. Takes no other arguments. |
| `sink('<connection>', '<entity>', **kwargs)` | Must be the pipeline's **leading** `INSERT INTO` marker (whitespace/comments before it are fine). Records the load binding. `**kwargs` are write options per the two-surfaces rule above — `strategy`, `keys`, `duplicates`, `on_delete`, `schema_policy`, `retry` are pz's own names; anything else passes through to the connector unchecked. **The call must fit on one line** — a `sink()`/`source()` split across lines is a template parse error (`PZ0104`). |
| `watermark('<connection>', '<entity>')` | Renders the entity's stored incremental cursor value for a comparison in `WHERE`. Declares the entity incremental **in SQL** — see below. Creates no DAG edge (the paired `source()` call already does). |

A pipeline can feed more than one sink by passing a list of markers instead of one:
`INSERT INTO [{{ sink('a', 'x') }}, {{ sink('b', 'y') }}]` — the query runs once, drains to every
listed output.

## One reader per source (PZ0349)

A source entity is read by exactly one pipeline. Two different pipelines calling
`source('crm', 'orders')` is a compile error (`PZ0349`) — to share a dataset, `ref()` the pipeline
that reads it, not `source()` it a second time. This is also why a SQL-declared incremental's
`WHERE` clause is the whole story for that dataset: there is no second reader that could disagree
about the cursor.

## Incremental reads: watermark()

Declare a source entity incremental by comparing its cursor column against
`{{ watermark('<connection>', '<entity>') }}` in the pipeline's `WHERE` clause — no `columns:`
contract or `sync:` block required for a plain floor:

```sql
INSERT INTO {{ sink('mart', 'mart.orders_current', strategy: 'merge', keys: ['order_id']) }}
select
    order_id,
    customer_id,
    amount,
    status,
    updated_at
from {{ source('erp', 'dbo.orders', partition_column: 'order_id', partitions: 4, retry: { max_attempts: 3 }) }}
where updated_at > {{ watermark('erp', 'dbo.orders') }}
```

The recognized shape is an ordered comparison — `<cursor column>` followed by `>`, `>=`, `<`, or
`<=`, then an expression containing exactly one `watermark()` call and no column references
(flipped spellings, expression-then-operator-then-column, mean the same and are also accepted).
`>`/`>=` are floors (the resume point); `<`/`<=` are ceilings. Anything else — `=`/`!=`, a function
on the cursor side, a column on the value side, a bare `watermark()` outside a comparison — is
`PZ0224`.

**Bounded windows still need a `columns:` contract.** A plain floor (as above) needs no contract at
all — the cursor's type is discovered from the stored watermark or, on the first run, from what the
bound expression evaluates to. But the `initial`/`max_window`/`until` trio (written in SQL as a
`coalesce(...)` floor plus a `least(...)` ceiling, or declared in YAML `sync:`) must be able to
compute its bounds **before the first extraction** — the cursor has to be typed up front, so the
entity needs a declared `columns:` contract (`PZ0213`). This is the one gap the 2026-08-12
schema-inference simplification did not close: a contract-less csv/json dataset with a bounded
window still needs a hand-written `columns:` map for the cursor column.

Declare incrementality **either** in YAML (`sync: { mode: incremental }`) **or** in SQL
(`watermark()`), never both for the same entity — `PZ0225`.

## Write modes and the delivery-guarantee consent rule (PZ0214)

`sink()`'s `strategy` is `replace` (default), `append`, or `merge` (needs `keys: [...]`). An
incremental source feeding an `append` sink is **at-least-once** by construction (a re-run or
retry can re-deliver a slice) — pz refuses that pairing at compile time unless you say so
explicitly with `duplicates: 'accept'`:

```sql
-- Incremental extraction paired with an append sink is at-least-once ... pz refuses this
-- pairing at compile time (PZ0214) unless you consent -- which a delta log deliberately does.
INSERT INTO {{ sink('lake', 'issues_log', format: 'parquet', path: 'out/issues/', strategy: 'append', duplicates: 'accept') }}
select id, number, title, state, updated_at
from {{ source('github', 'issues') }}
```

An incremental source feeding `replace` is refused outright (`PZ0335`, no consent escape — a
partial extraction would silently truncate the target). `merge` is effectively-once regardless.

## Out-of-process connectors (PCP)

Every external (non-builtin) connector package hosts its connector as a separate OS process —
in-process loading is reserved for builtins — declared in the package's `pz.connector.json`:

- `runtime: "process"` — required for external packages; a package declaring `"dotnet"` (or
  shipping no manifest, which means the same) is refused with `PZ0360`.
- `entrypoints` — a RID → package-relative binary path map, e.g.
  `{"linux-x64": "runtimes/linux-x64/native/pz-mysink", "win-x64": "runtimes/win-x64/native/pz-mysink.exe"}`.
  Resolved with `RuntimeIdentifierGraph` fallback (a package shipping only `linux-x64` is still
  reachable from `linux-musl-x64`), and rejected if a path would resolve outside the package
  directory.

This is packaging-time detail an agent authoring `connections.yml`/pipelines never touches
directly — the connector's `connector:` name in `connections.yml` and its `ConnectionConfigSchema`/
`DatasetConfigSchema` (from `pz_connector_reference`) look identical either way.

**`allow_unsigned_extensions:`** — a connection-level key, alongside `connector`/`entities`/
`max_concurrency`/`rate_limit`/`retry`, default `false`. A native scan/copy that would load an
unsigned packaged DuckDB extension is refused at plan time (`PZ0359`) unless the connection sets
`allow_unsigned_extensions: true`.

**PCP error codes:**

| Code | Meaning |
|---|---|
| `PZ0354` | No usable entrypoint for this host: unknown `runtime`, no `entrypoints` (or none reachable for this RID), an entrypoint path that is missing or resolves outside the package directory, or a `runtime: "process"` manifest with no `name`. |
| `PZ0355` | The connector executable failed to spawn (exec error, missing/non-executable binary, or no room for a socket path under the temp root). |
| `PZ0356` | Handshake with the spawned connector failed: timeout waiting for `Hello`, malformed `Hello`, or a capability/name mismatch against the manifest. |
| `PZ0357` | Protocol violation during data-plane operations (bad/reused write ticket, malformed Arrow IPC stream). |
| `PZ0358` | The connector process died unexpectedly mid-operation. |
| `PZ0359` | An unsigned packaged DuckDB extension was refused for a native scan/copy; set `allow_unsigned_extensions: true` on the connection to allow it. |
| `PZ0360` | An external connector package declares runtime `"dotnet"` (or ships no manifest) — external connectors are hosted out of process only. Use a `runtime: "process"` (PCP) package or a builtin. |

**`pz connector test <entrypoint-or-package-dir> [--config file.yml]`** — runs black-box PCP
protocol conformance checks against one out-of-process connector, independent of any pz project.
The target is a package directory containing `pz.connector.json` or a bare entrypoint binary;
`--config` names the connection to configure and the `read:`/`write:` dataset(s) to probe (a
`connection:` block plus optional `read: { dataset: ... }` and/or `write: { output: ..., mode: ...,
schema_policy: ... }`). Every applicable vector runs regardless of earlier failures, printed as one
`PASS`/`FAIL`/`SKIP <vector>[: detail]` line each. Exit codes: `0` every applicable vector passed,
`1` one or more vectors failed, `2` a config/usage problem (bad target, malformed manifest or
`--config`) meant no vector could even be attempted.

## Recommended tool loop

1. **Reference** — read this guide and the relevant `docs/concepts/*.md`/`docs/how-to/*.md`
   resource (`pz://docs/...`), or call `pz_connector_reference` for the connector's exact
   connection/dataset option schemas and `pz_project_overview` for what already exists in this
   project (connections, entities, pipelines, the compiled DAG).
2. **Author** — write or edit `connections.yml`/`pipelines/*.sql` by hand, or use the authoring
   tools (`pz_add_connection`, `pz_add_entity`, `pz_write_pipeline`, and their `update`/`remove`
   counterparts). Every authoring tool self-verifies after applying and reports the resulting
   errors, so a bad edit is visible immediately, not just on the next `pz_compile`.
3. **Validate** — `pz_compile` (does the DAG build?), then `pz_validate` (config/SQL sanity;
   pass `connect: true` to also probe live connections and fetch real schemas), then `pz_plan`
   (which per-node strategy the engine would use, and why) — cheapest checks first, each one a
   pure read with no side effects (no `manifest.json`/`plan.json`/`schemas.json` written).
4. **Run** (only if the server was started with execution allowed) — `pz_run` to execute a named
   flow (or `all: true` for the whole project), `pz_run_results` to inspect a prior run's node
   outcomes, `pz_retry` to re-run the last failed run reusing staged data where safe. Execution
   tools are absent from the tool listing entirely when the server was not started with
   `--allow-run` — there is nothing to call, not a runtime refusal.

Every tool's error response carries a `code` (`PZ####`), a `message`, and a `next_step` — trust
that over any generic advice above when they disagree.
