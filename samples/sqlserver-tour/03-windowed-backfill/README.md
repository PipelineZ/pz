# 03 — SQL-declared bounded window, partitioned reads, append + duplicates: accept, retention

Backfill safety against 2000 seeded rows in `dbo.events`. The whole window lives in the
pipeline's own `WHERE` — there is no `sync:` block anywhere:

```sql
from {{ source('erp', 'dbo.events', partition_column: 'id', partitions: 4, retry: { max_attempts: 3 }) }}
where id >  coalesce({{ watermark('erp', 'dbo.events') }}, cast(0 as bigint))
  and id <= least(coalesce({{ watermark('erp', 'dbo.events') }}, cast(0 as bigint)) + 500, cast(2000 as bigint))
```

- the `coalesce` fallback is **`initial`** (0) — where the first run starts,
- `+ 500` is **`max_window`** — how much each run takes,
- the `least(..., 2000)` arm is **`until`** — where the backfill stops.

> [!IMPORTANT]
> The `until` constant has to sit **inside** the expression holding the `watermark()` call.
> Only a comparison whose value side contains that call is folded into an extraction bound, so a
> standalone `and id <= 2000` would be an ordinary filter: extraction would stay bounded by
> `max_window` alone and the watermark would step a further 500 past 2000 on every empty run.
> `least(...)` is what makes the ceiling load-bearing — see [declaring the window in
> SQL](https://pipelinez.dev/concepts/project-structure/#declaring-the-window-in-sql).

The sink is `strategy: 'append'` with `duplicates: 'accept'` — the explicit consent that makes
the incremental→append combination compile at all (PZ0214 without it), because append is
at-least-once and a retried slice can land twice.

`project.yml` also sets `retention: { keep_last: 2 }`, so from the third run on each run prints a
`cleaned N staging database(s)` line and `.pz/runs/*/staging.duckdb` never exceeds 2 — while
every `run_results.json` is kept, so `pz retry` and run history are unaffected.

```console
$ pz run --project 03-windowed-backfill    # rows 1..500
$ pz run --project 03-windowed-backfill    # rows 501..1000
$ pz run --project 03-windowed-backfill    # rows 1001..1500  (+ first retention sweep)
$ pz run --project 03-windowed-backfill    # rows 1501..2000 — watermark reaches the ceiling
$ pz run --project 03-windowed-backfill    # "note: ... is caught up", 0 rows, exit 0
$ ls 03-windowed-backfill/.pz/runs/*/staging.duckdb | wc -l   # 2
```

Confirm the watermark pinned at the ceiling rather than running past it:

```console
$ cat 03-windowed-backfill/.pz/state/watermarks.json     # value: "2000", and stays there
```

Things to try:
- Drop `duplicates: 'accept'` from the `sink()` call → PZ0214.
- Drop the floor and keep only the ceiling → PZ0351: a ceiling alone never resumes — the first
  run would advance straight to it and every later run would extract nothing.
- Replace `cast(0 as bigint)` with a bare `0` → PZ0505 at run time: the bound types the cursor
  `int` while `id` lands as `BIGINT`, and the two must match.
- Rewrite the ceiling as a standalone `and id <= 2000` and watch the watermark climb past it
  (2000 → 2500 → 3000) once the data runs out — the failure mode the callout above describes.
- `pz plan --project 03-windowed-backfill` — the partitioned read strategy, per node.
- Set `retention: off` in `project.yml` and watch staging databases accumulate again.
- Kill a run midway (Ctrl-C), then run again — append is at-least-once; count the duplicates in
  `mart.events_log` to see why merge is the default advice.
