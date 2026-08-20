# hello-pz — the smallest complete project

Three CSV files in, a staged filter, a join, an aggregation, two files out. No docker, no
database, no network:

```bash
pz run --project samples/hello-pz --all
```

Outputs land under `samples/hello-pz/out/` — `orders_curated/` as parquet, `order_totals/` as
CSV. The `lake` connection sets `root: out` and neither `sink()` names a path, so each write goes
to a directory named after its entity.

## What it demonstrates

- **Both authoring surfaces, side by side.** `crm.orders` declares its read in
  `connections.yml` under `entities: orders: read:`; `crm.customers` declares its read at the
  `source()` call in `pipelines/orders_enriched.sql`. Both compile to the same node — the choice
  is about where you want to read it, not what it means. Declaring one entity-side in both
  places is `PZ0341`, not a precedence rule: nothing is merged.
- **Writes at the call site.** `sink('lake', 'orders_curated', format: 'parquet', strategy:
  'replace')` is the whole write declaration. There is no `outputs:` block anywhere.
- **`ref()` between pipelines.** `stg_orders` is an ephemeral staging step both downstream
  pipelines read.
- **Inline checks.** `pipelines/configs/orders_enriched.yml` attaches `not_null` and `unique`
  checks that run as their own nodes.

For a project whose reads are declared entirely in SQL — including its incremental — see
`samples/mssql-mart/`. For one that keeps everything in YAML, see `samples/http-api/`.
