# 02 — SQL-declared incremental into a merge sink

The effectively-once path, declared entirely in `pipelines/orders_current.sql`:

```sql
from {{ source('erp', 'dbo.orders', retry: { max_attempts: 3 }) }}
where updated_at > {{ watermark('erp', 'dbo.orders') }}
```

`connections.yml` has **no `entities:` block at all** — that `where` is the incremental
declaration. The cursor is read off the comparison and typed from the stored
watermark, so no `columns:` contract is needed either. Exercises watermark persistence
(`.pz/state/watermarks.json`), delta extraction (`updated_at > $wm` pushed into the SELECT),
and the sink's #temp-staging MERGE.

```console
$ pz run --project 02-incremental-merge     # first run: full table (120 rows), watermark stored
$ pz run --project 02-incremental-merge     # second run: 0 rows moved (no new data)
$ cat 02-incremental-merge/.pz/state/watermarks.json
```

Simulate a source update, then watch a 1-row delta merge in:

```console
$ docker exec pz-mssql-tour /opt/mssql-tools18/bin/sqlcmd -C -S localhost \
    -U sa -P "$PZ_MSSQL_PASSWORD" -d pz \
    -Q "update dbo.orders set status = 'delivered', updated_at = sysutcdatetime() where order_id = 7"
$ pz run --project 02-incremental-merge     # extracts 1 row, merges it (row count stays 120)
```

Things to try:
- `pz run --full-refresh --project 02-incremental-merge` — ignores the stored watermark for
  one run; merge keeps the target duplicate-free anyway.
- Change the sink call to `strategy: 'append'` (drop `keys:`) — compile fails with PZ0214
  (incremental → append needs `duplicates: 'accept'`); scenario 03 shows the consented version.
- Add `sync: { mode: incremental, cursor: updated_at }` for `dbo.orders` in `connections.yml`
  while keeping the `watermark()` call → PZ0225 (either/or, never both).
- Add a lookback: `where updated_at >= {{ watermark(...) }} - interval 2 hour` — an inclusive
  bound is at-least-once by construction; merge absorbs the re-read.
- `rm -rf 02-incremental-merge/.pz` — forget all state, start over.
