# mssql-mart — SQL Server → SQL Server mart template

Copy this directory as the starting point for a production mart: an incremental `sqlserver`
source read in partitioned ranges, one transformation, and a merge sink (effectively-once
delivery).

The whole project is two files. `connections.yml` holds credentials and nothing else — no
entities, no columns, no sync block. Everything about *what* to read is written where it is
read, in `pipelines/orders_mart.sql`:

```sql
from {{ source('erp', 'dbo.orders', partition_column: 'order_id', partitions: 4, retry: { max_attempts: 3 }) }}
where updated_at > {{ watermark('erp', 'dbo.orders') }}
```

That `where` is the incremental declaration — pz reads the cursor column off the
comparison, so there is no `sync:` block to keep in sync with it. `columns:` is omitted because
SQL Server has a catalog: `pz validate` discovers the real schema and writes it to
`.pz/target/schemas.json`. Declare `columns:` only when you want the contract enforced (reads
prune to exactly its columns, and `--connect` fails on drift instead of tolerating it).

Placeholders: set `ERP_DB_HOST`, `ERP_DB_NAME`, `MART_DB_HOST`, `MART_DB_NAME` in the
environment (see [secure connection config](https://pipelinez.dev/how-to/secure-connection-config/)).
Both connections use managed identity — create the DB users from the VM's identity
(`CREATE USER [vm-identity-name] FROM EXTERNAL PROVIDER`) with `db_datareader` on the
source and `db_datareader, db_datawriter, db_ddladmin` on the mart. On the mart database,
also create the target schema once: `CREATE SCHEMA mart;` (the sink creates tables, never schemas).

## Production checklist (first real mart)

1. **Adapt**: rename the connections, the entity, and the SELECT list to the real tables. Pick
   the real cursor column (must be monotonic, indexed) for the `where` comparison and a
   numeric/temporal `partition_column`.
2. **Validate offline**: `pz compile` then `pz validate` — zero errors.
3. **Validate online**: `pz validate --connect` from the VM — proves identity auth,
   connectivity, and that the discovered schema covers what the SQL selects
   (see [handle schema drift](https://pipelinez.dev/how-to/handle-schema-drift/)).
4. **First run**: `pz run --all` — the initial extract is the FULL table (no watermark yet).
   If that backlog is too big for one run, bound each run to a slice by widening the `where` you
   already have — the window lives in the same SQL as the rest of the read, so nothing moves:

   ```sql
   where updated_at >  coalesce({{ watermark('erp', 'dbo.orders') }}, TIMESTAMP '2024-01-01')
     and updated_at <= coalesce({{ watermark('erp', 'dbo.orders') }}, TIMESTAMP '2024-01-01') + interval 30 day
   ```

   The `coalesce` fallback is where the first run starts; the `+ interval 30 day` ceiling is how
   much each run takes. To stop at a fixed point, fold that constant into the ceiling with
   `least(<the expression above>, TIMESTAMP '2026-01-01')` — a *standalone* `and updated_at <= …`
   is an ordinary filter, not a stop (see [declaring the window in
   SQL](https://pipelinez.dev/concepts/project-structure/#declaring-the-window-in-sql)). Drop the ceiling
   line once the watermark has caught up.

   The YAML equivalent (`sync: { mode: incremental, cursor: updated_at, initial: …, max_window: … }`
   under `entities: dbo.orders: read:`) is the other route, and the one to take if you want the
   window shared as a property of the entity. It costs more here: because the two surfaces are
   either/or per entity-side (`PZ0341`), the *whole* read has to move into `connections.yml` — the
   kwargs and the `where` line together — plus a `columns:` contract, since a YAML window must be
   computable before the first extraction and there is no discovered schema to type the cursor from
   yet (`PZ0213`). See [backfill in slices](https://pipelinez.dev/how-to/backfill-in-slices/).

   Either way, confirm `.pz/state/watermarks.json` now has the cursor.
5. **Second run**: `pz run --all` again — should extract only the delta (watch
   `rows` in the run summary drop).
6. **Schedule it**: follow [run scheduled on Windows](https://pipelinez.dev/how-to/run-scheduled-on-windows/)
   (Task Scheduler + run-pz.ps1 + Azure Monitor alerts).
7. **Prove failure handling**: force a runtime failure (point the source at a nonexistent
   table), confirm the Azure Monitor alert fires, fix, `pz retry` — staged data reuse should
   skip re-extracting succeeded sources.
8. **Prove drift handling**: add a column on the source; confirm `--connect` stays green, then
   add a `columns:` contract if you want a rename to fail loudly instead.

Scale tuning knobs, in the order to reach for them: `partitions` (up to 16), `engine.threads`,
the YAML `max_window` from step 4, `retry`/`max_concurrency`/pacing (see
[throttle a source](https://pipelinez.dev/how-to/throttle-a-source/)).
