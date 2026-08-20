# 05 — data-quality checks, failure, and `pz retry`

Five check types on one pipeline (`pipelines/configs/orders_checked.yml`): `not_null`,
`unique`, `row_count`, `accepted_values`, `custom_sql`. Checks and the sink write are both
children of the pipeline — a failing check fails the RUN (exit 1) but does NOT block the sink
write; bad rows still land. The `erp` connection carries a connection-level `retry:` block
(transient-error policy: 3 attempts, 1s base delay) alongside the entity's `columns:` contract.

```console
$ pz run --project 05-checks-and-retry     # all checks pass, mart.orders_checked loaded
$ pz test --project 05-checks-and-retry    # checks only (plus required ancestors)
```

Break it, watch it fail, fix it, `pz retry`:

```console
# 1. Poison the source: a status outside the accepted list AND a negative amount.
$ docker exec pz-mssql-tour /opt/mssql-tools18/bin/sqlcmd -C -S localhost \
    -U sa -P "$PZ_MSSQL_PASSWORD" -d pz \
    -Q "insert into dbo.orders values (9999, 1, -5.00, 'bogus', sysutcdatetime())"

# 2. Fails: accepted_values reports 'bogus', no_negative_amounts returns 1 row. Exit 1.
#    NOTE: the console prints only `FAIL <check>` with no reason — the PZ0510 message with
#    the sample values is in run_results.json, or use `--log-format json`.
$ pz run --project 05-checks-and-retry

# 3. Fix the data.
$ docker exec pz-mssql-tour /opt/mssql-tools18/bin/sqlcmd -C -S localhost \
    -U sa -P "$PZ_MSSQL_PASSWORD" -d pz \
    -Q "delete from dbo.orders where order_id = 9999"

# 4. `pz retry` FAILS AGAIN — by design: it reuses the failed run's STAGED data (watch the
#    "reusing staged data" / "carrying forward 1 committed sink write" notes), so the checks
#    re-evaluate the same poisoned 122 rows. Retry is for transient failures, not data fixes.
$ pz retry --project 05-checks-and-retry

# 5. A data fix needs a fresh extraction: plain `pz run` goes green (8/8), and the replace
#    sink drops the poison row from mart.orders_checked.
$ pz run --project 05-checks-and-retry
```

Things to try:
- `pz run --log-format json --project 05-checks-and-retry` after poisoning — check events
  carry up to 5 sample offending values (`sample_values: false` on a check opts out). This is
  currently the only way to see the failure reason without opening `run_results.json`.
- Point `dbo.orders` at a nonexistent table → non-transient connector error; note retry does
  NOT burn attempts on it.
- Move `retry:` from the connection down to a `source()` kwarg — same policy, narrower scope.
