# pz_new_project

Scaffolded by `pz init --template http`. This is a runnable PipelineZ project over the builtin
`http` connector: pull issues from GitHub's REST API — pagination, a typed column contract,
watermark-incremental extraction — and append each run's delta as a parquet file.

This template needs internet access (it reads the public GitHub API) but no credentials and no
database — `pz run --all` works as-is, rate-limited to ~60 requests/hour.

## What it demonstrates

- **Pagination** — GitHub's RFC 8288 `Link` header, followed automatically
  (`pagination: { strategy: link_header }`).
- **Typed contract** — `columns:` lands exactly five typed columns at extraction time; every
  other JSON key is ignored, and the schema is known offline (no API probing at validate time).
- **Incremental extraction** — `sync: { mode: incremental, cursor: updated_at }` plus
  `since: "{{ watermark }}"` in the query. The first run omits `since` entirely; every later
  run sends the stored watermark and pulls only what changed.
- **Bounded crawls** — `max_pages: 5` caps one run at 500 issues, so this template is polite to
  the API and fast to try. Delete the line to backfill the full history.
- **The YAML read surface, in full.** Every option above lives under `entities: issues: read:`
  in `connections.yml`, and this template keeps it there deliberately. A `source()` call cannot
  span lines, so a read this large would become one unreadable line; and an http incremental
  declared purely in SQL has an unguarded first run (nothing bounds the very first crawl), which
  is exactly what `max_pages` is here to prevent.
- **The delivery-guarantee consent rule** — incremental + an append sink is at-least-once
  (GitHub's `since` is "at or after", so the boundary row re-lands next run). pz refuses that
  pairing at compile time (PZ0214) unless the output says `write: { duplicates: accept }` — which a
  delta log deliberately does. See
  [delivery guarantees](https://pipelinez.dev/concepts/delivery-guarantees/).

## Run it

    pz validate   # offline: schema is declared, not probed
    pz run --all  # first run: up to 5 pages, newest first
    pz run --all  # second run: only issues updated since run 1

Each run appends one parquet file under `out/issues/`. The watermark lives in
`.pz/state/watermarks.json` and only advances after the sink commits — kill a run mid-flight and
the next one re-claims the same slice instead of skipping it. `--full-refresh` ignores the stored
watermark for one run.

Unauthenticated, GitHub allows roughly 60 requests/hour per IP — plenty for the capped demo
(a run is at most 5 requests). For heavier use, export a token and uncomment the `auth:` line in
`connections.yml`:

    export GITHUB_TOKEN=ghp_...

The line ships commented out because `${VAR}` interpolation fails fast (PZ0103) when the
variable is unset — the project must load with zero setup.

## Reading the delta log

Duplicates across run boundaries are expected (that's the at-least-once consent above). The
current-state view is one query away in any DuckDB:

    select * from read_parquet('out/issues/*.parquet')
    qualify row_number() over (partition by id order by updated_at desc) = 1;

## Going further

- Point it at your own repo: change `path:` in `connections.yml`; the mechanism is
  identical for any `Link`-header API. Page-number and cursor-token pagination, API-key and
  basic auth, raw-payload landing, JSON-pointer record extraction, sync state for delta-link
  APIs, and writing *to* an HTTP API (append POST / keyed merge PUT) are all covered in
  [Extract from an HTTP API](https://pipelinez.dev/how-to/extract-from-http-api/).
- Rate limiting and request pacing (`Retry-After`, operation gating) are covered in
  [Throttle a source](https://pipelinez.dev/how-to/throttle-a-source/).
- `pz init <name> --template sqlserver` is the equivalent template for a SQL Server to SQL Server
  mart.
