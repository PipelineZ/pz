# sqlserver-tour — six runnable projects against one local SQL Server

Where [`mssql-mart`](../mssql-mart/) is a template to copy, this is a tour to *run*. Six small
projects share one seeded SQL Server container; each isolates a different path through the
engine, and each README ends with ways to break it on purpose and see which `PZ####` fires.

| # | Project | What it isolates |
|---|---|---|
| 01 | [`01-full-refresh-replace`](01-full-refresh-replace/) | YAML read + call-site write, `columns:` contract, replace (TRUNCATE path), idempotency |
| 02 | [`02-incremental-merge`](02-incremental-merge/) | SQL-declared incremental (`watermark()`, no `entities:` at all), merge sink, `--full-refresh` |
| 03 | [`03-windowed-backfill`](03-windowed-backfill/) | SQL-declared window, partitioned reads, append + `duplicates: 'accept'`, `retention:` |
| 04 | [`04-query-and-procedure`](04-query-and-procedure/) | `query:` entity, `procedure:` entity with `$watermark`, named flows / PZ0215 |
| 05 | [`05-checks-and-retry`](05-checks-and-retry/) | five check types, a failing run, `pz test`, `pz retry`'s staged-data reuse |
| 06 | [`06-remote-state`](06-remote-state/) | `state:` block — watermarks/run results/events in SQL Server, `.pz` disposable, `PZ_STATE_*` defaults |

Between them they cover **both authoring surfaces in both directions** — read options in
`connections.yml` (01, 04, 05) and at the `source()` call (02, 03); write options always at the
`sink()` call. Nothing here uses `sources/`, `sinks/`, or `outputs:`; those are retired (PZ0346).

## Setup (once, ~2 min)

Needs docker. The container is local-only and disposable — a fixed SA password in `infra/env.sh`
is fine here and nowhere else; real deployments use managed identity (see
[secure connection config](https://pipelinez.dev/how-to/secure-connection-config/) and `mssql-mart`).

```console
$ cd samples/sqlserver-tour
$ ./infra/up.sh          # mssql 2022 on port 14333 + a deterministic seed
$ source infra/env.sh    # the PZ_MSSQL_* vars every connections.yml interpolates
```

Define `pz` once per shell — either the installed tool, or from a repo checkout:

```console
$ pz() { dotnet run --project "$(git rev-parse --show-toplevel)/src/Pz.Cli" -c Release -- "$@"; }
```

For the repeated runs scenarios 03 and 04 ask for, call the built binary instead and skip the
`dotnet run` overhead:

```console
$ pz() { "$(git rev-parse --show-toplevel)/src/Pz.Cli/bin/Release/net10.0/Pz.Cli" "$@"; }
```

Then run any project from this directory:

```console
$ pz run --project 01-full-refresh-replace
```

## Suggested order

01 and 02 first — they establish the two delivery strategies (replace is idempotent, merge is
effectively-once) and the two places a read can be declared. 03 builds the window on top of 02's
watermark. 04 and 05 are independent and can be taken in either order. 06 is 02's pipeline again
with `pz`'s own state moved into the same server — take it last, once local state feels familiar.

## Reset

- One project's watermarks and run history: `rm -rf <project>/.pz` — except 06, whose state
  deliberately lives in the server (`pz state clear` it, or reseed)
- All data — sources, mart targets, and 06's `pzstate` tables alike: `./infra/up.sh`
  (idempotent reseed)
- Everything: `./infra/down.sh`

Seeded data, deterministic and free of randomness: `dbo.customers` (20 rows), `dbo.orders`
(120 rows), `dbo.events` (2000 rows), the procedure `dbo.orders_since`, and an empty `mart`
schema the sinks write into. Scenarios 02, 04 and 05 mutate `dbo.orders` by design — re-run
`./infra/up.sh` to get back to 120 rows.

## Where the sinks write

Every project writes into the same `mart` schema of the same database it reads from. That keeps
the tour to one container; it is not a shape to copy. A real deployment separates source and
mart — that is what `mssql-mart`'s two connections show.
