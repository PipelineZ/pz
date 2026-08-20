# 04 — query-mode + procedure-mode entities, named flows

Two independent flows in one project, both with their reads declared in `connections.yml`
(the YAML surface) and their writes at the `sink()` call sites:

- **`order_totals_out`** — a `query:` entity (join + group by, runs verbatim, never receives
  pushdown) into a replace sink. Its key, `order_totals`, is just a label: no object of that
  name exists in the database.
- **`recent_orders_out`** — a `procedure:` entity (`dbo.orders_since`) whose
  `min_id: "$watermark"` parameter binds the stored cursor value, making the proc itself the
  incremental pushdown; merge sink on `order_id`.

Because the project has 2+ independent flows, bare `pz run` refuses with PZ0215 — that's part
of the scenario:

```console
$ pz run --project 04-query-and-procedure                     # PZ0215: name a flow or pass --all
$ pz run order_totals_out --project 04-query-and-procedure    # just the query flow
$ pz run recent_orders_out --project 04-query-and-procedure   # just the proc flow (first run: all 120)
$ pz run recent_orders_out --project 04-query-and-procedure   # proc gets @min_id=120 -> 0 rows
$ pz run --all --project 04-query-and-procedure               # everything
```

Insert a new order, then watch the proc slice just it:

```console
$ docker exec pz-mssql-tour /opt/mssql-tools18/bin/sqlcmd -C -S localhost \
    -U sa -P "$PZ_MSSQL_PASSWORD" -d pz \
    -Q "insert into dbo.orders values (121, 5, 42.00, 'pending', sysutcdatetime())"
$ pz run recent_orders_out --project 04-query-and-procedure   # extracts exactly 1 row
```

Things to try:
- Move the whole `recent_orders` read to `source()` kwargs (`procedure: 'dbo.orders_since'`,
  `parameters: { min_id: '$watermark' }`, …) — kwarg names equal YAML keys at every level, so
  it is cut-and-paste. Leave any of it behind in `entities:` and get PZ0341.
- Add `partition_column`/`partitions` to `recent_orders` — rejected: procs can't do
  partitioned reads.
- Point a `watermark()` call at `recent_orders` while its `sync:` block is still in YAML →
  PZ0225.
- `pz validate --connect` — note the "no columns: contract" line for `erp.order_totals`: a
  query entity has no catalog object to discover a schema from.
