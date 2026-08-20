# mysql-native — MySQL → MySQL through the native-only connector

Copy this directory as the starting point for moving data between MySQL databases with the
[`mysql` connector](../../connectors/Pz.Connector.MySql/README.md) — the connector whose entire
data plane is DuckDB's own `mysql` extension (no .NET MySQL driver anywhere).

Like [`mssql-mart`](../mssql-mart/), the whole project is credentials plus SQL: `connections.yml`
holds the two connections and nothing else, and everything about *what* to read is written where
it is read, in the pipelines. The two pipelines are the connector's two delivery shapes:

- **`orders_log.sql`** — an incremental read (`where updated_at > {{ watermark(…) }}` *is* the
  declaration; the predicate is pushed into MySQL, so each run extracts only the delta)
  feeding an **append** output. `mysql` has no merge — the DuckDB mysql catalog has no upsert —
  so incremental delivery is at-least-once, and `duplicates: 'accept'` is the explicit consent
  the compiler requires for that pairing (PZ0214). The result is a change log, not a
  current-state table; dedup downstream on `(order_id, updated_at)` if you need one.
- **`products_snapshot.sql`** — a full read feeding a **replace** output: the mart table is
  rebuilt from scratch each run. Effectively-once from the pipeline's perspective, with one
  caveat inherited from MySQL itself: the swap is drop-and-recreate (MySQL DDL commits
  implicitly), so a reader querying exactly during the copy can see the table absent.

If you need a merged current-state table (true effectively-once incremental), use the
`postgres` or `sqlserver` connector as the sink instead — merge is exactly the capability this
connector trades away for the native-only experiment.

Placeholders: set `SHOP_DB_HOST`/`SHOP_DB_NAME`/`SHOP_DB_USER`/`SHOP_DB_PASSWORD` and the four
`MART_DB_*` equivalents in the environment (see
[secure connection config](https://pipelinez.dev/how-to/secure-connection-config/)). Credentials travel
as a DuckDB secret, never inside the attach path, and are never logged.

## Running it

1. `pz compile` then `pz validate` — offline, zero errors expected. (`pz validate --connect`
   probes reachability via the MySQL greeting handshake; it cannot verify credentials or fetch
   schemas without a `columns:` contract — both are checked for real at run time.)
2. `pz run --all` — two flows, so bare `pz run` is PZ0215 by design; name one (`pz run
   orders_log`) to run just that flow. The first run needs network access once for DuckDB's
   `install mysql`, does a full extract of `orders` (no watermark yet), and creates both mart
   tables (`append` auto-creates on first write).
3. `pz run --all` again — the orders extract now ships only rows past the stored watermark
   (watch `rows` drop in the run summary), and the snapshot rebuilds.
4. Break it on purpose: set a wrong `SHOP_DB_PASSWORD` and re-run — the node fails with a
   redacted PZ0311 (the password appears nowhere in the message or events), which is the
   regression the connector's e2e suite pins.
