# pz_new_project

An incremental PipelineZ project: each run extracts only what changed since the last one.

Run it twice:

    pz run --all
    pz run --all

The first run lands all five orders. The second lands none -- the stored watermark advanced past
every row's `updated_at`, so the pipeline is bounded and nothing new lands. Whether that bound also
reaches extraction (so the connector itself reads less) depends on the connector: local CSV has no
way to push a predicate into a file scan, so the second run still reads all five rows before the
pipeline's `where` clause discards them. See `pz init <name> --template sqlserver` for a connector
that pushes the bound into the query it sends. Add a row to `data/orders.csv` with a later
`updated_at` and run again to see just that row land.

## How the incremental is declared

`pipelines/orders_log.sql` says `where updated_at > {{ watermark('raw', 'orders') }}`. That comparison
IS the declaration -- there is no `sync:` block to keep in step with it, and the comparison's type comes
from the stored watermark. `connections.yml` declares only what CSV cannot describe about itself: the
column types.

## Delivery guarantee

Incremental read + append sink is **at-least-once**. pz rejects that pairing at compile time
(`PZ0214`) unless you consent, which `duplicates: 'accept'` does here -- correct for a delta log,
where a replayed run re-delivering a slice is harmless and you deduplicate downstream.

Effectively-once needs a sink that can merge on a key, which local files cannot do. See
`pz init <name> --template sqlserver` for that.

## Where state lives

`.pz/state/watermarks.json`. `pz run --full-refresh` ignores it for one run. Delete it to start over.

Full documentation: https://pipelinez.dev
