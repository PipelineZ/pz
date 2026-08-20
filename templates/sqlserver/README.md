# pz_new_project

A SQL Server to SQL Server project: incremental extraction, a keyed merge, data-quality checks, and
optional remote state.

## Before your first run

This template needs a live SQL Server -- it is the one starting point that is not runnable as
scaffolded. Set the credentials it reads:

    export ERP_DB_HOST=... ERP_DB_NAME=... ERP_DB_USER=... ERP_DB_PASSWORD=...
    export MART_DB_HOST=... MART_DB_NAME=... MART_DB_USER=... MART_DB_PASSWORD=...

Then check the shape before moving any data:

    pz validate           # config and SQL
    pz validate --connect # also probes connectivity and the columns contract
    pz plan               # the compiled DAG, still without running
    pz run --all

## Delivery guarantee

Incremental read + merge sink is **effectively-once**: the merge is keyed on `order_id`, so a
replayed run converges rather than duplicating. The watermark advances only after every downstream
write commits, so an interrupted run re-reads rather than skipping.

## Data-quality checks

`pipelines/configs/orders_current.yml` uses all five check kinds. `pz test` runs only the checks;
`pz run` runs them inline and fails the node when one does.

## Remote state

`.pz/state/watermarks.json` is local by default, which means the host holding it is load-bearing.
Uncomment the `ops:` connection in `connections.yml` and the `state:` block in `project.yml` to keep
watermarks, run results, and the event stream in SQL Server instead -- then `.pz` is disposable and
any machine can run the next run.

Full documentation: https://pipelinez.dev
