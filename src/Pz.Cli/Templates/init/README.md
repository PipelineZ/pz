# {{PROJECT_NAME}}

Scaffolded by `pz init`. This is a runnable PipelineZ project with TWO independent flows over
builtin-connector `localfiles` data:

- **orders** — `stg_orders` (staging/filter) → `orders_enriched` (join to customers, with
  `not_null`/`unique` checks) and `order_totals` (aggregation in `INSERT INTO` form), draining to
  `out/orders_curated/` and `out/order_totals/`
- **products** — `product_catalog`, draining to `out/product_catalog/`

Run one flow (a flow is the named node plus everything upstream and downstream of it), fully
offline:

    pz run orders_enriched
    pz run product_catalog

Because this project has more than one independent flow, bare `pz run` refuses with `PZ0215`;
run everything explicitly:

    pz run --all

Then explore:

- `pz plan` — see the compiled DAG and per-node execution plan without running it
- `pz validate` — check config shape, SQL, and (with `--connect`) live connectivity
- `pz test` — run only the data-quality checks (`not_null`, `unique`, `row_count`, `freshness`, `accepted_values`, `custom_sql`)
- `pz retry` — resume a failed run from where it left off

Project layout:

- `project.yml` — name, version, declared connectors, vars, engine settings
- `connections.yml` — every place pz talks to, and optionally the entities in each
- `pipelines/*.sql` (+ `pipelines/configs/*.yml` sidecars) — one file per transformation
- `data/` — the sample CSVs this template ships with; swap in your own and adjust the reads

Reads and writes have two interchangeable spellings, and this template ships one of each so you
can pick per entity:

- in YAML, under `entities: <name>: read:` in `connections.yml` (`customers`, `orders`)
- at the call site, as keyword arguments on the `source()` that reads it (`products`, in
  `pipelines/product_catalog.sql`) or the `sink()` that writes it (every write here)

Declaring the same entity-side in both places is an error (`PZ0341`) rather than a precedence
puzzle: there is no merging, so whichever file you open tells you the whole story.

See `https://pipelinez.dev/quickstart/` in the PipelineZ repository for a full walkthrough of every verb.
