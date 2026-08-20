# 01 — full refresh into a replace sink

Baseline scenario: table-mode extraction (no watermark) into `strategy: 'replace'`.
Exercises the sink's transactional TRUNCATE-then-bulk-load path — every run is
idempotent; row count in `mart.customers_dim` is always 20.

**Surface split:** the read is declared in `connections.yml` (`entities: dbo.customers: read:`
with a `columns:` contract); the write is declared at the `sink()` call site. Two independent
declarations of two different entity *sides* — legal, and moving either to the other surface is
cut-and-paste (declaring the same side twice is PZ0341).

```console
$ pz run --project 01-full-refresh-replace
$ pz run --project 01-full-refresh-replace   # same result — replace is idempotent
```

Verify:

```console
$ docker exec pz-mssql-tour /opt/mssql-tools18/bin/sqlcmd -C -S localhost \
    -U sa -P "$PZ_MSSQL_PASSWORD" -d pz \
    -Q "select count(*) from mart.customers_dim"
```

Things to try:
- `pz plan --project 01-full-refresh-replace` — see the per-node strategy.
- Drop a declared column from `columns:` in `connections.yml` — reads prune to the
  contract, so the mart table loses that column on the next run.
- `pz validate --connect --project 01-full-refresh-replace` — tier-5 probe; then rename a
  column in `columns:` to something bogus and watch schema-drift detection fail it.
- Move `columns:` out of `connections.yml` and pass it as a `source()` kwarg instead — same
  behaviour. Declare it in *both* places and get PZ0341.
- `mkdir 01-full-refresh-replace/sources` → PZ0346 (the retired directory layout).
