# 06 — remote state: `.pz` is disposable

The pipeline is 02's, on purpose — SQL-declared incremental into a merge sink. What changed is
where `pz` remembers: a `state:` block in `project.yml` moves watermarks, run results, and (opted
in) the run-event stream into SQL Server, under a `pzstate` schema `pz` creates itself:

```yaml
state:
  backend: sqlserver
  connection: ops      # a connections.yml entry, connector: sqlserver
  schema: pzstate
  events: true         # the chatty one -- off by default
```

This is the ephemeral-host story ([Move state off the local disk](https://pipelinez.dev/how-to/remote-state/)):
a container dies between scheduled runs, and the next run — on a machine that has never seen this
project — still extracts only the delta. `staging.duckdb` stays local either way; it is the run's
buffer, not state.

```console
$ pz run --project 06-remote-state          # first run: 120 rows, watermark stored in SQL Server
note: state backend: sqlserver (from project.yml)
$ ls 06-remote-state/.pz/state 2>/dev/null  # nothing -- no watermarks.json is ever written
$ pz state show --project 06-remote-state   # reads the remote store; prints the backend header
```

Now kill the "host" and prove nothing was lost:

```console
$ rm -rf 06-remote-state/.pz                # the ephemeral host dies
$ docker exec pz-mssql-tour /opt/mssql-tools18/bin/sqlcmd -C -S localhost \
    -U sa -P "$PZ_MSSQL_PASSWORD" -d pz \
    -Q "update dbo.orders set status = 'delivered', updated_at = sysutcdatetime() where order_id = 7"
$ pz run --project 06-remote-state          # extracts exactly 1 row -- the watermark survived
```

Peek at `pz`'s own tables — the keyed state, one row per run, one per node, and (because
`events: true`) the persisted event stream, `seq`-ordered with any truncation reported on the
run's header row (`events_dropped`), never silently:

```console
$ docker exec pz-mssql-tour /opt/mssql-tools18/bin/sqlcmd -C -S localhost \
    -U sa -P "$PZ_MSSQL_PASSWORD" -d pz \
    -Q "select scope, state_key, payload from pzstate.state;
        select run_id, status, events_dropped from pzstate.runs;
        select top 5 run_id, seq, event from pzstate.run_events order by run_id desc, seq"
```

Things to try:
- `pz state rollback 'erp.dbo.orders' --to-run <id> --project 06-remote-state` — the run-by-run
  history now comes from `pzstate.runs`/`run_nodes`, not local files.
- Set `artifacts: false` while keeping `events: true` → PZ0124: without the runs header row the
  drop count has nowhere to land and `run_events` would never be swept.
- Add `schema: pzstate` under `backend: local` → PZ0124 (backend-specific key, refused not ignored).
- Point `connection:` at a name that is not in `connections.yml` → PZ0125, at validation time.
- Delete the `state:` block and export `PZ_STATE_BACKEND=sqlserver` plus
  `PZ_STATE_CONNECTION_STRING="Server=localhost,14333;Database=pz;User Id=sa;Password=$PZ_MSSQL_PASSWORD;TrustServerCertificate=true"`
  — same behavior, and the note line now says `(from PZ_STATE_BACKEND)`: ambient config is
  printed, never invisible. An explicit `project.yml` key always beats the environment.
- `pz clean --keep-last 1 --project 06-remote-state` after a few runs — under a remote backend a
  swept run is deleted whole (its rows *and* its local staging directory), `--purge` or not.
- Break the pipeline (e.g. select a bogus column), run, fix it, then `pz retry` — retry finds the
  failed run in `pzstate.runs`, even after another `rm -rf .pz`.

Reset for this project is different from the others: `rm -rf 06-remote-state/.pz` deletes nothing
that matters. `./infra/up.sh` reseeds everything including the `pzstate` tables, or
`pz state clear 'erp.dbo.orders' --project 06-remote-state` forgets just the watermark.
