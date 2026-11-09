# Changelog

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions are git
tags (`v*`), computed into package versions by MinVer. Breaking changes are listed
first in each release, each with a migration note — see
the [versioning policy](https://pipelinez.dev/versioning/).

## [Unreleased]

### Added

- **http: watermark formats, epoch timestamps and nested contract columns.** Four dataset `read:` options.
  - `watermark_format` sets how a timestamp cursor renders in `{{ watermark }}` and `{{ window_upper }}`: `iso8601`
    (the default, unchanged), `epoch_s`, `epoch_ms`, `date`, or a custom format of the letters `y M d H m s f` (for
    example `yyyy-MM-dd HH:mm:ss`; quoted text may not hold `\` or `"`). The lower bound is floored to the format's
    unit and the upper bound is rounded up to the next one, so a window is never under-covered. An epoch value that is
    a whole JSON body value is sent as a number. A `date` cursor takes only `date` or a date-only custom format;
    numeric and text cursors refuse the option. A unit of a day or longer (`date`, `yyyy-MM`, `yyyy`, ...) re-reads up
    to that whole unit on every run or window; into an append sink those rows land again, which `pz validate` reports
    as a PZ0364 warning for an incremental http dataset whose cursor is not a date. Rounding past 9999-12-31 is a permanent error.
  - `watermark_time_zone` (an IANA id, default UTC) makes `date` and custom formats render, floor and round up in
    local wall time. Across a DST change a bound that falls in a skipped or repeated hour moves to the safe side in
    3-hour steps (earlier for the lower bound, later for the upper), so the window only grows; a transition that no
    step within 48 hours clears is a permanent error, never a narrower window. It is refused with
    `iso8601` and the epoch formats, with a format that writes `Z` or `UTC`, and on date cursors.
  - `epoch_unit: s | ms` reads JSON numbers in `timestamp` contract columns, raw `cursor_type: timestamp` cursors
    and `for_each` item fields as epoch time; strings are still parsed as text. Values that look like the other
    unit are refused: under `ms`, anything before 1973-03-03 (below 1e11) except exactly `0`, which is the epoch
    itself; under `s`, anything after 3000-01-01.
    Without the option, an epoch number fails with a message naming `epoch_unit`.
  - A contract column's long form takes `pointer`, a JSON pointer into the record:
    `status: { type: varchar, pointer: /fields/status/name }`. It works for JSON pages and JSON files, with `redact`,
    and on the incremental cursor column; changing a pointer changes the SourceLoad node id. It is refused on a
    column filled from a `for_each` field and with csv/tsv/xml files. On any other connector it is PZ0301, both in
    `pz validate` and when `pz run` opens the source (a connector opts in by listing `column_pointers` in its dataset
    schema; an external connector with no schema is refused). pz writes that `column_pointers` option itself:
    writing it by hand, in `read:` or as a `source()` argument, is PZ0101.
- **http: sign in with a refresh token.** `auth.type: oauth2_refresh_token` takes `token_url`, `client_id`,
  `refresh_token`, and optionally `client_secret` (omit it for a public client), `client_auth` (`body` or `basic`;
  `basic` needs a secret), `scope`, `params` and `rotation`. It exchanges the refresh token for access tokens with
  `grant_type=refresh_token` and otherwise behaves like `oauth2_client_credentials`: the same token cache, 401
  handling, `token_url` rules and `proxy`/`tls`. pz cannot store a new refresh token, so `rotation` says what happens
  when the token response returns a different `refresh_token`:
  - `refuse` (the default): the read fails with the permanent error `PZ0366` and the new access token is not used,
    because the provider may already have revoked the configured token.
  - `ignore`: the new access token is used, the configured `refresh_token` stays in use for later refreshes, and a
    notice says the provider rotated it. Use it only for providers that keep old refresh tokens valid, such as
    Microsoft Entra and Google. If the provider does revoke the old token, the next refresh fails with `invalid_grant`,
    which is permanent.

  An `invalid_grant` answer says to authorize again and update `auth.refresh_token`. A `PZ0366` refusal, or an
  `invalid_grant`, `invalid_client` or `unauthorized_client` answer, is remembered for the rest of the run (the rest of the process): later reads, writes and nodes
  using the same token fail at once without sending it again, so a provider's reuse detection never sees a replay. Any
  `refresh_token` or `client_secret` that the provider echoes in its error text is replaced with `***`.
- **http: sign in with a login request.** `auth.type: session` sends `login` (`path` relative to `base_url`, or
  `url`; `method` `post` or `get`; a literal `body` sent as JSON, as a form with `body_format: form`, or as a string with
  `content_type`; and `headers`), reads the token at `token_pointer` in the JSON response, and sends it on every API
  request as set by `inject`: `bearer` (the default), `{ header: <name>, prefix: <text> }`, or `{ query: <name> }`.
  - A query token is redacted from messages and is never stored in a checkpoint; a resumed read adds the current one.
  - A header or bearer token must be printable ASCII.
  - Set exactly one of `ttl` (`1s` to `30d`, for example `30m`) or `expires_in_pointer` (seconds in the login
    response).
  - The login must go to `base_url`'s origin or an `allow_hosts` host, and it uses the connection's `proxy` and `tls`.
    It never follows a redirect. Connection `headers:` are not sent on the login.
  - A form field that is empty (null, for example an unset variable) is a validation error.
  - A login answered `401` or `403` is a permanent error. pz makes one refused login per connection per run: later
    reads, writes and nodes in the same run (the same process) fail at once without logging in again. Scheduled
    runs still try again, once each.
  - `408`, `429` and `5xx` on the login are transient.
  - A `401` from the API on a session that worked before is transient, and the engine's retry logs in again.
    `reauth_status` (any 4xx except `407` and `429`) lists more statuses that mean the session expired, for example
    `[419, 440]`. `401` always counts: a list adds to it and never replaces it. Any other status, `403` included, is
    not treated as an expired session unless it is listed.
- **http: status rules and a configurable Retry-After header.** `on_status` on a dataset is a list of rules
  `{status, body_contains, action, retry_after}` that classify non-2xx responses before the built-in rules (408/429/5xx
  transient, the rest permanent). `status` is a code from 400 to 599, `4xx`/`5xx`, or a list of those; `body_contains` is
  a plain substring matched without regard to case (a file download matches the first 64 KiB of the error body). The
  first matching rule wins. `action: transient` makes the response retryable, for example GitHub's `403` secondary rate
  limit; its wait is the response's `Retry-After`, else the connection's `retry_after` header, else the rule's
  `retry_after` (a duration up to `600s`), else the engine's backoff. A transient rule that matches an export's start
  request fails the read instead, because a retry would send the start again and could create a second export job.
  `action: permanent` fails the read with the rule named. `action: skip` ends the read with a notice saying 0 rows
  landed, and only on the read's first request (the first page, a `for_each` item's first page, or a direct file
  download); a skip rule that matches a later request, or the first request of a resumed attempt, fails the read.
  Skipped `for_each` items are counted in the read's end notice together with `on_missing` skips. A skip rule without
  `body_contains` must name exact codes and none of 401, 403, 408 or 429. `skip` is refused with `export` and on
  datasets that are or could be incremental: a raw `cursor`, a `{{ watermark }}`/`{{ window_upper }}` reference, an
  `incremental:` block, or a `columns:` contract (its columns can be the cursor of a SQL-declared incremental, which the
  connector cannot see on a first run or under `--full-refresh`). At run time a skip rule that matches a read the engine sent a watermark (a cursor, value or upper bound) fails the read instead of skipping. One case stays open: on the first run, or under `--full-refresh`, of a raw-mode SQL-declared incremental with an inclusive SQL ceiling, no watermark is sent, so a skip there lands 0 rows and the engine advances the watermark to the ceiling. **A skipped read lands an empty table: an output in
  `replace` mode fed only by it is emptied for that run.** The dataset's rules cover its own (item) requests;
  `for_each.on_status` covers the list request. Built-in cases still come first: a proxy `407` (which a rule cannot
  name), a `401` handled by an OAuth2 or Azure AD token, and a `410` on a replayed delta link; rules then come before
  `for_each.on_missing`. The connection key `retry_after: {header, kind}` (`kind`: `seconds`, `ms` or `epoch`) supplies
  the wait when a `429` or `503`, or a response a rule classifies as transient, has no usable `Retry-After`. It is capped
  at 600 seconds (a stated wait above a day adds a note to check `kind`), also applies to sink requests, and moves the
  shared pause of a concurrent read. A value that is not a number, or a wait that is already over (zero, or an epoch in
  the past), is ignored with a note, and the next source of a wait applies. Without these keys, behaviour is unchanged.
- **http: `stop_at_watermark` for newest-first feeds.** A dataset with `cursor_order: desc` and
  `stop_at_watermark: true` stops crawling at the first page that holds a record at or below the stored watermark;
  those records are dropped in the connector, the newer ones on that page land, and a notice gives the page and the
  dropped count. Records with a null cursor land. Without a watermark (first run, `--full-refresh`) the feed is read
  to the end. The feed must really be newest-first: a cursor value greater than the one before it fails the read,
  on every run. The option is refused with `for_each`, `file`, `delta_pointer`, `max_pages` and
  `pagination.concurrency` above 1, so the crawl always ends at the watermark or at the natural end of the feed.
  Checkpoints still resume the crawl page by page.
- **http: `for_each.values` and `for_each.dedupe`.** `for_each.values` is a literal list of maps used instead of a list
  request, for "the same endpoint per region / account / metric": each map's keys are the item fields (every entry
  must have the same keys; values are strings, numbers, booleans or null), and the item request, escaping, `pz_item`,
  `on_missing`, `concurrency` and checkpoints work as with a list request. The list-request keys (`path`, `query`,
  `fields`, `pagination`, `max_items`, ...) are refused next to it. `for_each.dedupe: true` skips a list record whose
  fields tuple an earlier record of the same read already had (first occurrence wins; fields compare as JSON values,
  so `"1"` and `1` differ), and reports the count and the first duplicate in a notice. Duplicates do not count toward
  `max_items`. Dedupe is per attempt: a retry resumed mid-list may read again an item first seen before its resume
  page.
- **http: cursor tokens in headers.** `strategy: cursor` can read its next-page token from a response header with
  `header: <name>` instead of `pointer:` (exactly one of the two), and send it in a request header with
  `location: header`, where `param` is the request header name. Both combine with each other, with `location: body`
  on POST reads, and with the `for_each` list request. A missing or empty header ends the crawl; a header sent more
  than once is a permanent error. A token sent in a header must be visible ASCII with no whitespace at either end,
  otherwise the read fails without showing it. The page header counts in the repeated-request guard and is kept
  across redirects. Its name cannot be one pz or the HTTP stack owns (`Authorization`, `Proxy-Authorization`,
  `Cookie`, `Host`, `Connection`, `Transfer-Encoding`, `Expect`, `TE`, `Upgrade`), a header .NET treats as a content
  header (`Content-*`, `Expires`, `Last-Modified`, `Allow`), or one the connection's `headers:` or `api_key` auth also
  set; the last is refused when the read is planned, before any request. Checkpoints of a header-paged read use a
  new `v4:` token that stores the URL, the body and the page header only, never connection headers or credentials;
  every other read's tokens are unchanged. An unknown `location` on a GET read is still ignored, as before, now with
  a notice; `pz validate` reports it through the dataset schema.
- **http: proxy, client certificates and a private CA.** Two optional connection keys. `proxy` is `none` or a map
  `{url, user, password, bypass}`; `url` must be an `http://` URL (the TLS connection to the target still runs end to
  end through the proxy), it supports basic auth only, and credentials go in `user`/`password` (a `url` with userinfo is
  refused). `tls.ca_file` adds trust anchors from a PEM file; the anchor may be an intermediate CA. The system roots
  stay trusted, and expiry and hostname checks always apply. There is no switch that disables validation.
  `tls.client_certificate` is PKCS#12 (`.pfx`/`.p12`) or PEM plus `key_path`. It is presented to `base_url`'s origin and
  to the `allow_hosts` hosts the connection's own client reaches (for example a next-page link), the same way connection
  headers travel; it is not presented by the bare client (credential-free file downloads from other hosts) or to an
  OAuth2 token endpoint on another origin. Both settings apply to dataset, sink, check and OAuth2 token requests. For
  `auth.type: azure_ad`, `service_principal` uses them, `managed_identity` ignores them, and `credential_chain` combined
  with `proxy` or `tls` is refused. Failures are explicit: a TLS trust failure is permanent and names `tls.ca_file`; a
  proxy `407` is permanent and names `proxy.user`/`proxy.password`; a dropped connection while a client certificate is
  configured is a transient error with a `tls.client_certificate` hint.
- **http: parallel fetching.** Two opt-in keys, each an integer 1-16 (default 1, which behaves exactly as before).
  `pagination.concurrency` reads up to N pages at once; it needs `strategy: page`, applies only to datasets without
  `for_each`, and cannot be combined with `delta_pointer`. Pages still land in order. `for_each.concurrency` runs up
  to N items of one list page at once, with a barrier before the next list page; rows from different items
  interleave. Checkpoints are taken only at list-page barriers, so a retry re-fetches the current list page's items.
  A list page whose items yield no rows gets no checkpoint of its own. When concurrency is above 1, a `429` or `503`
  response with `Retry-After` pauses every request of that read until that time, capped at 600 seconds (with
  concurrency 1 there is no shared pause). Set `rate_limit:` on the connection when using concurrency. File items
  use one scratch directory per worker, so disk use can reach N times the size of one download.
- **http: read with POST.** `method: post` on a dataset sends a request body, so APIs that take their query in the
  body work: Elasticsearch/OpenSearch `_search`, GraphQL, and report endpoints. `body` is a map (sent as JSON, or as a
  form with `body_format: form`) or a string. A string body has its bindings expanded and is sent with `content_type`
  (default `application/json`; set e.g. `application/graphql`); GraphQL APIs usually want the map form
  `{ query, variables }`. String values in the body expand the same engine bindings as `query:`. A binding that is
  null on the first run becomes JSON `null` in the body; use `initial:` to avoid it. `pagination.location: body`
  writes the page number, offset or cursor into the body instead of the URL (`param` and `size_param` are JSON
  pointers, for example `/from` and `/size`; `size` with `location: body` requires `size_param`), and a crawl
  resumes from a checkpoint at the right page. A GraphQL error returned with HTTP 200 fails the read when
  `error_pointer` points at the response's `errors`. A `307` or `308` redirect keeps POST and the body; `301`, `302`
  and `303` switch to GET without a body. A POST read cannot be combined with `delta_pointer`. Elasticsearch
  `search_after` is not supported, so `from`/`size` paging cannot page past Elasticsearch's default 10,000-hit window
  (the request fails). Point a `read:` only at endpoints with no side effects: a retry or a resume sends the request
  again.
- **http: two more pagination strategies.** `next_url` follows a next-page link in the response body
  (`pointer: /@odata.nextLink` for Microsoft Graph/OData, `/links/next`, `/next`); a relative link resolves against the
  current request, and every link is still checked against the connection's allowed hosts. `offset` pages with
  `offset`/`limit` (rename with `param`/`size_param`, start with `start`) and advances by the rows each page actually
  returned, so an API that serves fewer than `size` rows cannot make the crawl skip any. Both work with checkpointed
  resume; `offset` also takes `stop_on_short_page`.
- **http: sign in with OAuth2.** Two new `auth` types. `azure_ad` signs in to Microsoft Entra ID (Azure AD) as a service
  principal (`client_secret` or `certificate_path`), a managed identity, or the default Azure credential chain
  (`credential_chain`, which includes `az login`). Its `scope` must be the API's `<resource>/.default`
  (for example `https://graph.microsoft.com/.default`); `pz validate` warns about any other form, a malformed
  `tenant_id` is refused at validation, and a scope Entra refuses fails with a hint naming it. `certificate_path` is
  resolved against the project directory and also accepts `~/` for the home directory. The generic
  `oauth2_client_credentials` works with any token endpoint (`token_url`, `client_id`, `client_secret`, optional
  `scope`). Both cache the access token and refresh it up to five minutes before
  it expires (half the lifetime for short-lived tokens). `timeout_seconds` bounds each sign-in request (the Azure CLI
  step of `credential_chain` has its own timeout), and retries are left to the engine. A `401` on a token the API had
  never accepted fails permanently with a hint to check `scope`; a `401` on a token it had accepted drops the token and
  is retried by the engine, which signs in again. A sign-in failure is reported without the secret or token.
  `pz validate` warns when `certificate_path` points inside the project directory, where it is easily committed.
  `http` now joins the connectors anchored on the project directory.
- **http: read files, zip archives and export jobs.** A dataset with a `file:` block reads the response as a file
  instead of a JSON page: `format` is `csv`, `tsv`, `json` (`layout: lines` for NDJSON, or `document` with `items`) or
  `xml` (`records: /a/b/c`, DTDs prohibited). `archive: auto` (default) sniffs a zip or gzip from the first bytes; a
  zip needs `members`, a glob such as `sessions_*.csv`, so one daily zip becomes several tables (several datasets can
  read different members of the same archive, each with its own format and columns). `if_missing: empty` reads zero
  rows when no member matches, and emits a notice naming the dataset and glob. The default `error` lists the entries.
  `file_column` adds the member path as a column; raw mode (no `columns:`) lands an exact envelope with `payload`,
  `pz_file`, `pz_row` and `pz_fetched_at` (plus the cursor column when the dataset declares one). CSV takes
  `delimiter` (TSV fixes it to tab and rejects it); CSV and TSV take `header` and `skip_lines`; `encoding`
  (`utf-8`, `utf-16`, `iso-8859-1`) applies to every format.
  Files stream through a temp file that is always deleted; `max_download_mb` on the connection (default 4096) caps the
  download, and decompressed bytes are capped at 20 times that, so a zip bomb fails instead of filling the disk. A
  `200` response with an error-page content type (an HTML login page, or a JSON error for a non-JSON format) is refused
  instead of landed as rows. An `export:` block covers asynchronous exports: the dataset's request (often a `method:
  post`) starts the job, `status_path` is polled until `status_pointer` reaches a `ready` value (or fails on a `failed`
  one, quoting `message_pointer`), then the file is downloaded from `download_pointer` or `download_path`. Polling defaults to every 10 seconds
  (`poll_interval_seconds`) and gives up after 30 minutes (`timeout_minutes`). The start
  request is never retried by the gate, so a 503 cannot create a duplicate job; a timeout is transient and the engine
  retries with a new job. A pre-signed download URL on another host must be in `allow_hosts` and is fetched without
  credentials. `{job_id}` and the engine bindings expand inside `status_path` and `download_path`. File datasets take
  no checkpoint, `pagination`, `delta_pointer` or `max_pages`. In file mode, a redirect or download to another host
  carries no credentials and no connection headers, and its query is hidden in messages.
- **http: dated URLs.** `path` now expands bindings like `query` and `body`, each value escaped as one path segment
  (a null binding in `path` is an error that names the fix). The new `{{ run_date }}` binding is the day of the run
  shifted by whole days, configured by `run_date: { offset_days: -1, timezone: Europe/Bucharest, format: yyyy-MM-dd }`
  (defaults 0, UTC, `yyyy-MM-dd`), and read once per partition so start, polls and download agree across midnight. For
  a file with a date column, a windowed dataset catches up a missed day per run: `sync: { mode: incremental, cursor:
  day, initial: "2026-10-01", max_window: 1d }` with `path: /exports/daily/{{ window_upper }}.zip`. The window's upper
  bound can reach today, whose file may not be published yet: that download fails (404) and nothing is lost, the
  watermark stays and a later run fetches the day. Use `until:` or schedule the run after the vendor publishes.
- **http: one request per list record (`for_each`).** For APIs that expose child data only per parent
  (`/orders/{id}/lines`), `for_each:` describes a list request, and the dataset's own `path`, `query`, `body`,
  `pagination`, `items` and `file:` describe the item request sent once per list record. The list request takes the
  usual request keys (`path`, `query`, `method`, `body`, `body_format`, `content_type`, `items`, `pagination`,
  `error_pointer`, `max_pages`), so it can page, POST and use `{{ watermark }}`, `{{ window_upper }}` and
  `{{ run_date }}` (`run_date:` stays at dataset level and is shared). `fields` maps a name to a JSON pointer into
  each list record, and `{{ item.<name> }}` uses it in the item request's `path`, `query` and `body`. In a string
  `body:` whose content type is JSON (`application/json`, the default, or any `+json`), an item reference is refused
  at validation, since a value spliced into JSON text could change its structure: write the body as a map, where the
  reference is a typed JSON value. Each item crawl pages to its last page before the next item starts, and in file
  mode the item request is a download, so "list the exports, then download each one" works. A value from the API is
  escaped by position (a path segment, a query value, a typed JSON value, or form-encoded text), is never expanded
  again, and a path segment that item values turn into `.`, `..` or nothing is refused. Numbers are sent with their
  JSON text, and a referenced field that is missing, null, an object or an array fails the read, naming the list page,
  the record and the field.
  In contract mode a `columns:` entry named like a field is filled from the item. In raw mode a `pz_item` column (a
  JSON object of every declared field, a field missing from the record as null) follows the envelope columns, and the
  cursor may be an item field in either mode. `for_each.on_missing: skip` moves on after a 404 or 410 on an item's
  first request (a 404 on a later page still fails) and reports the count in a notice at the end of the read; the
  default `error` fails. `for_each.max_items` stops after that many list records, with a notice, and does not fetch a
  further list page once it is reached. An item-level `max_pages` that stops items before their last page is reported
  in one notice at the end of the read. Like `for_each.max_items`, that notice counts per attempt. On an incremental dataset (a declared watermark, a raw `cursor:`, or a
  `{{ watermark }}`/`{{ window_upper }}` reference in either request) `for_each.max_items`, `for_each.max_pages` and
  the item-level `max_pages` are refused at validation, because an item cut short would be skipped for good once the
  watermark passes it; the 50,000-page ceiling stays an error everywhere. A read resumes from a checkpoint at the item
  and page it reached. The list page is fetched again, and if the record the checkpoint was taken at is no longer
  where it was (the list shifted), the list page is read again from its first record rather than mislabelling items or
  skipping records at the old index: duplicates are possible, and a merge sink removes them. Skips are carried in the
  checkpoint, so they are reported across retries; `max_items` counts per attempt. A file-mode item checkpoints only
  between items. `for_each` cannot be combined with `delta_pointer` or `export:`. Datasets without `for_each` are
  unchanged.

### Changed

- **A JSON number in a contract `timestamp` column has a clearer error.** It used to be "value is not a valid
  timestamp". The http connector now says it is a JSON number and names `epoch_unit`; sftp's JSON reader says it
  "does not read epoch numbers as timestamps".
- **JSON pointers follow RFC 6901 array indexes.** In http's `items`, `cursor_pointer`, pagination and `for_each`
  pointers, an array index with a leading zero, a sign or blanks (`/data/01`, `/data/+1`) no longer selects an
  element; it resolves to nothing, as the RFC says. `/data/0` and `/data/1` are unchanged.
- **http: nanosecond watermarks are cut, not rounded.** A stored value with more than 7 fractional digits used to be
  rounded to the 100 ns tick, which could carry into the next second and skip that second's rows; the extra digits
  are now dropped.

- **http: responses are requested compressed.** Reads, writes and the connection check send
  `Accept-Encoding: gzip, deflate, br` and unpack what comes back; JSON pages typically shrink to 10–20% on the wire
  (measured: a 1 MB page 54% faster, a GitHub issues page 35% faster). `max_response_mb` caps the unpacked size.
  Servers that ignore the header send plain responses as before. No new option.

### Fixed

- **http: an `allow_hosts` entry no longer receives credentials over plain http.** A next-page link or redirect to
  `http://<allowed host>` from an `https://` `base_url` sent the connection's token unencrypted; it is now refused.
  An `http://` `base_url` may still reach allowed hosts over http. `pz validate` now warns when an `auth:` block or an
  `Authorization` header is sent to an `http://` `base_url` (loopback excepted).

### Security

- **Build with .NET SDK 10.0.403 (runtime 10.0.14).** `global.json` now requires it, so the Native AOT compiler resolves
  to `Microsoft.DotNet.ILCompiler` 10.0.14, which fixes the Brotli decoder out-of-bounds write (CVE-2026-111497,
  GHSA-p3jp-9v83-76w9); with 10.0.400 the NuGet audit failed the build. The Rust workspace takes `pulldown-cmark`
  0.13.6 (RUSTSEC-2026-0433, quadratic parsing; a build-time dependency).

## [0.17.0] - 2026-11-01

### Added

- **Mark sensitive columns with `redact: true`.** On a source `columns:` entry, with the long form
  `email: { type: varchar, redact: true }` (in `connections.yml` or at the `source()` call site), and in a pipeline
  sidecar's new `columns:` list (`- name: contact` / `redact: true`), which only marks and enforces nothing. Marks do
  not follow a column through a pipeline; mark it again on the step. `pz preview` shows a marked column's values as
  `•••`. Adding or removing a mark changes no node id. `pz validate` warns (PZ0403) when a sidecar marks a column
  the pipeline does not output.
- **`pz preview <node>` shows a sample of a source or pipeline.** `--run <id>` / `--run-dir <path>` read a finished
  run's `staging.duckdb` read-only; the run directory alone suffices, because `pz run`/`pz retry` now record each
  node's relation and redaction marks in it (runs from an older pz report PZ0537). `--limit` 1-500 (default 50);
  `--json` prints one JSON object. Columns marked `redact: true` show `•••`; long values are cut at 200 characters.
  A view is previewable from a run; a sink, a check or an ephemeral pipeline is not (PZ0538). New codes: PZ0537
  (nothing to preview from this run), PZ0538 (not previewable), PZ0539 (invalid options). `--fresh` compiles the
  current project and runs the node and its ancestors in a throwaway in-memory DuckDB, reading at most `--slice` rows
  per source (default 1000, 1-100000), so joins and totals may look thin; it never runs a sink or a check and never
  reads or writes state (`--state-url`/`PZ_STATE_URL` are ignored), and a cdc dataset is read as a snapshot. An
  ephemeral pipeline can be previewed fresh. Connectors now receive the slice as `ReadHints.Limit`; pz stops reading
  after it either way.

## [0.16.2] - 2026-10-30

### Fixed

- **A failed node's next step is shown.** Several node failures carried their remedy in a field no output reads:
  the console, `run_results.json`, the NDJSON stream and MCP show only a node error's code and message. So
  "run 'pz schema accept'" on schema drift, "run 'pz restore'", the circuit breaker's "retry with 'pz retry' after
  the cool-down", the node timeout's `engine.node_timeout`, the windowed watermark type mismatch's
  `--full-refresh`, and the sink merge and upsert guidance never reached anyone. pz now appends that next step to
  the message as a `pz:` line, like its other failure guidance.

## [0.16.1] - 2026-10-30

### Fixed

- **A watermark whose cursor type does not fit the column fails with the fix, not a bare conversion error.** A
  watermark set with the wrong `pz state set --type` (say `date` on an integer `id`) failed the next plain
  incremental run with `PZ0501` and DuckDB's `Could not convert string '***' to INT64`, naming neither the watermark
  nor the remedy. pz now checks the stored type against a `columns:` contract before reading, and against the
  column's landed type before trimming staging, and fails with `PZ0505` naming the key, both types and the commands
  that fix it (`pz state clear`, then `pz state set` with the column's type). `int` and `bigint` read each other's
  values and pass. When a database connector applies the watermark inside the read and the read fails, and the
  watermark was set by hand, the `PZ0501` message now ends with a note naming it.

## [0.16.0] - 2026-10-29

### Breaking

- **An incremental read from a file source passes only rows after the watermark.** A plain incremental dataset
  (`sync: {mode: incremental, cursor: …}` with no `max_window`) relied on the connector to apply `cursor >
  watermark`, which is optional, and the file and object-store connectors (`localfiles`, `s3`, `azureblob`, `gcs`,
  `sftp`) do not. Every run therefore sent the whole source downstream: an `append` output received every row again
  on each run, and pipelines over it recomputed over all rows. pz now removes the rows at or below the stored
  watermark from staging after landing, on every path, as it already did inside a `max_window` window. Database
  connectors already filtered, and SQL-declared incrementals (`watermark()` in a pipeline) filter in their own SQL,
  so neither changes. A file-sourced incremental run now moves only the new rows: reported row counts drop, an
  `append` output stops receiving the backlog, and an aggregate over the source covers the new rows only.
  *Migration:* nothing to edit for the usual intent (load what is new). A pipeline that relied on seeing the whole
  file every run, such as a running total recomputed from scratch, should read the file without `sync:`, or run
  with `--full-refresh` whenever it needs the full source.

## [0.15.0] - 2026-10-29

### Added

- **`pz state set` creates a missing watermark.** It used to edit existing entries only, so a fresh clone could not
  be given a starting point: the first run always extracted in full. Pass the cursor column and its type with the
  value: `pz state set erp.orders --value 2026-07-01 --cursor updated_at --type timestamp`. Without both, a missing
  key is still refused (PZ0513), now naming the two flags. On an existing entry `--cursor` and `--type` are optional
  and must match what is stored (PZ0516 otherwise); a set never changes them. This is how a platform run's window can
  be replayed on a laptop.

### Fixed

- **The linux-x64 pz starts on Ubuntu 24.04, Debian 12 and older again.** 0.13.0 and 0.14.0 were packed on
  `ubuntu-latest` after GitHub moved it to Ubuntu 26.04, so their Native AOT binary needed glibc 2.43 and
  failed with ``version `GLIBC_2.43' not found`` everywhere older (most container base images, Azure
  Container Apps, CI runners). Linux packages now build on a pinned `ubuntu-24.04`, and the release fails
  before anything is pushed if any Linux binary needs a glibc newer than 2.39
  (`scripts/check-glibc-baseline.sh`). linux-arm64 was already pinned and was not affected.

## [0.14.0] - 2026-10-28

### Added

- **Opt-in ambient cloud identity for external connectors.** pz starts every connector process with a
  cleared environment, so a connector using a cloud SDK's managed identity, workload identity, IRSA, ECS task
  role or GCP federation could not find it (only the VM metadata service and credential files under `HOME`
  worked). A connector now declares the clouds it needs with `<PzAmbientIdentity>azure;aws</PzAmbientIdentity>`
  (groups `aws`, `azure`, `gcp`); the SDK writes `"ambientIdentity"` into `pz.connector.json`, and pz forwards
  that cloud's identity variables (such as `IDENTITY_ENDPOINT` and `IDENTITY_HEADER` on Azure Container Apps),
  plus its region and project settings, to that package only. `azure` also forwards a service principal
  configured through the environment (`AZURE_CLIENT_ID`, `AZURE_TENANT_ID` with `AZURE_CLIENT_SECRET` or
  `AZURE_CLIENT_CERTIFICATE_PATH`), so that setup works for the connector as it does for builtin ones. Other
  variables holding a static secret, such as `AWS_SECRET_ACCESS_KEY`, are never forwarded; a
  `GOOGLE_APPLICATION_CREDENTIALS` that names a service-account key file is, so a `gcp` package can use that key. `pz restore` prints
  `receives host identity: <groups>` under every package that declares any. A manifest without the field is
  written byte-for-byte as before and keeps the same environment; an unknown group fails the pack
  (`PZSDK006`), is ignored with one warning when pz loads a hand-written manifest, and an older pz ignores the
  field. The Rust SDK does not write the field yet: add `"ambientIdentity"` to its manifest by hand.

## [0.13.0] - 2026-10-27

### Added

- **`--log-sql` on `pz run` and `pz retry`** publishes a `node_sql` event with the exact statement each
  pipeline node and `custom_sql` check hands DuckDB, after templating and watermark substitution, once per
  attempt and before it runs. Capped at 65,536 characters (`truncated` and `chars` say when it was cut). Off by
  default: `env()` can inline a secret into SQL, so a consumer that turns it on must mask what it shows. It needs
  `--log-format json` (otherwise the run is refused with exit 2) and is written to stdout NDJSON only: never
  persisted to `pz.run_events` under `state.events: true`, and not counted as a dropped event there.

## [0.12.0] - 2026-10-26

### Added

- **`IProjectingNativeScan`**, for connector authors whose native scan can read only the requested columns.
- **`extensions:` in `project.yml`** lists DuckDB extensions, core or community (such as `jev`), that pipeline
  SQL calls. pz installs and loads them the way DuckDB does, before any SQL runs, in every session that runs or
  validates the project; an extension already in DuckDB's extensions folder is used without a download, which is
  how an offline machine works. A failure is reported for every listed extension as **PZ0402**, and an unknown
  function in a pipeline now hints at listing its extension. `run_results.json` records each loaded extension
  and the build DuckDB reports for it.

### Fixed

- **A whole-row reference is no longer pushed down as a column.** `jev(t, …)` passes the row `t`, which source
  pushdown read as a column named `t`; the read now fetches every column.

- **DECIMAL(38) values beyond `System.Decimal` are no longer silently corrupted.** Arrow's
  `Decimal128Array.GetValue` cannot hold a value with more than 28 significant digits (or a scale above 28) and
  drops digits without an error, so such values left DuckDB changed. Universal-tier egress, the CSV and NDJSON
  writers (and the NDJSON reader), the SqlServer sink, the Azure/GCS/SFTP partitioned writers, the SFTP
  incremental window filter and check result formatting now carry every digit. The Postgres sink cannot (Npgsql
  writes `numeric` only from `System.Decimal`), so a universal-tier write of such a value now fails, naming the
  column, instead of committing a wrong number; round or cast the column in the pipeline. Connector authors get
  `Decimal128Cells` in `Pz.Connectors.Abstractions.Batches` for exact reads, `ArrowBatchBuilder` accepts a
  `SqlDecimal` for a decimal column, and the TestKit sink suite gains a beyond-range decimal fact on the
  type-matrix hooks.

### Changed

- DuckDB calls wait for the session's connection asynchronously instead of blocking a thread-pool thread:
  callers queued behind a held connection no longer each occupy a pool thread (64 queued callers previously
  grew the pool by about 60 threads). Wall time is unchanged within noise on a 2-CPU run (32 callers x 50
  1,000-row appends).
- **A slow source no longer blocks other nodes' DuckDB work.** Channel-path ingest (Postgres, SqlServer, Sftp,
  Azure Blob and GCS reads, and LocalFiles under `force_universal`) held the session gate for the whole stream, so
  every other node's DuckDB work waited for the slowest source. It now holds the gate while waiting for a batch
  only while no other operation is waiting for it: the rows appended so far are committed and the gate handed
  over as soon as one is (and after at most 122,880 rows per hold). With a Postgres source streaming 400k rows
  over ~9 s next to an independent local 1M-row CSV group-by chain, that chain finished at ~9.25 s before (when
  the slow source starts first) and ~0.23 s after (alone: ~0.19 s). A slow sink still blocks other nodes' DuckDB
  work: a transaction holds the gate throughout, and an egress drain until all but its last buffered batches are
  read out. An uncontended ingest still commits once per 122,880 rows, so tiny batches cost what they did before:
  10,000 one-row batches into a file-backed staging database on an NVMe disk take ~53–60 ms either way, read
  directly or through the bounded channel a source load feeds.
- **Faster compile and egress.** Compile reuses one DuckDB parse session per command (a wide project with 200
  sources: 2250 to 284 ms). Universal egress concatenates DuckDB chunks by column (1M rows: 246 to 73 ms; mixed
  utf8/decimal/timestamp/date: 576 to 102 ms). `scripts/macro-bench.sh` times the built CLI instead of
  `dotnet run`, `scripts/scale-bench.sh` measures project-size scaling, and `PZ_BENCH_INPROCESS=1` switches the
  benchmarks to BenchmarkDotNet.
- **Egress concatenates straight into pooled native memory.** Universal egress writes each concatenated
  column directly into the pooled allocator instead of staging it on the managed heap. Managed allocation per
  1M-row egress drops from ~38.5 MB to ~1.1 MB (mixed types: ~63.7 to ~1.3 MB), and Gen2 collections from
  10,000 to 6,000 and from 17,000 to ~10,000 per 1k operations (BenchmarkDotNet, in-process).
- **Native scans stage only the columns and rows the pipeline reads.** A wide Parquet source
  (2M rows, 50 columns) read by `select id, c1 from … where k = 7` now stages 3 columns and 20,000 rows instead of
  50 columns and 2,000,000 rows (SourceLoad 19.8 s to 63 ms, min of 3). A native SourceLoad's `rows`
  (`run_results.json` and NDJSON) now counts the filtered staged rows, and `pz plan` reports pushdown for native
  scans. The dry compile runs in an in-memory database. The drift baseline re-seeds once on the first run after
  upgrading. Drift detection on native scans now covers the columns the pipeline reads, so an added column the
  pipeline never reads is no longer reported, as on the universal tier's pruning connectors.

## [0.11.3] - 2026-10-16

### Fixed

- **Iceberg `storage: azure` on Linux.** The iceberg connector loaded DuckDB's azure extension without choosing a
  transport, so on Linux every HTTPS request to Azure Storage failed with `Problem with the SSL CA cert` (the bug
  0.11.2 fixed for azureblob). It now sets `azure_transport_option_type = 'curl'` on Linux, as azureblob does.
- **Native IO errors keep the detail that explains them.** Redaction masked every quoted literal in a DuckDB error,
  so an Azure CA-bundle failure read as `could not open file: '***' ... Original error message: '***'`. Object/file
  URLs are now kept (userinfo and query string, e.g. a SAS token, masked) and an extension's nested
  `Original error message: '...'` is kept when it ends the line, with URLs, inner quoted literals and credential
  values (`key=value`, `key: value`, JSON pairs, `Bearer`/`Basic`/`SharedKey`) masked. Every other literal is
  still masked, and the echoed statement is still dropped.

## [0.11.2] - 2026-10-15

### Fixed

- **Azure Blob native reads and writes on Linux.** The DuckDB azure extension's default transport could not find
  the system CA bundle (seen on Ubuntu 24.04 and Arch), so every native scan and native copy failed before any
  request left the host, reported as `PZ0501 ... this could mean the credentials used were wrong`. The azureblob
  connector now sets `azure_transport_option_type = 'curl'` on Linux, which uses the system bundle. Managed
  identity (`auth: managed_identity` with `client_id`) now works natively on Azure Container Apps.

## [0.11.1] - 2026-10-14

### Security

- **Patched `System.Security.Cryptography.Pkcs`.** NuGet.Protocol and Microsoft.Data.SqlClient brought in 8.0.1 and
  9.0.4, flagged by CVE-2026-84008 and CVE-2026-70162 (GHSA-846f-f764-76mx, GHSA-2fmv-rv2c-59x7). `Pz.PackageManagement`,
  `Pz.State.SqlServer` and the SQL Server connector now pin 10.0.13.
- **Patched `rustls` in the Rust connector SDK lockfile.** 0.23.45 to 0.23.46, for RUSTSEC-2026-0346, -0347, -0348
  and -0349.

## [0.11.0] - 2026-10-13

### Added

- **Connector stats ABI.** A connector reports what its remote service did through `ConnectorStats.Current`
  (requests, wire bytes, remote time, throttles, one cost unit, up to eight namespaced extras) and declares
  `ConnectorCapabilities.ReportsStats`. Invalid values are dropped, never thrown. The TestKit gains opt-in
  `Reported_stats_are_valid` / `Reported_write_stats_are_valid` facts that skip without the capability.
- **`remote` on `node_completed` and `run_results.json`.** The stats a node's connector reported, omitted when there
  are none.
- **`pz.remote.*` span attributes and metrics** for the same figures.
- **C# connector SDK.** The `ctx.Remote` helper, an HTTP handler that records requests, wire bytes and 429/503 throttles, a
  200-span cap per operation, and redaction of spans from foreign `ActivitySource`s. The HTTP handler also counts
  the bytes of streamed (chunked) request and response bodies as they are sent and read. `RemoteCalls` has a public
  constructor taking an `ActivitySource`, so connector unit tests can build one.
- **Sink stats are snapshotted after the write session is disposed** (C# and Rust SDKs), so numbers a session reports
  while disposing are included in the commit response, as they already were for abort.
- Libraries built on Azure.Core emit their `ActivitySource` only when the AppContext switch
  `Azure.Experimental.EnableActivitySource` is set; set it before creating clients, then list the source in
  `PzConnectorHostOptions.ActivitySources`.
- **Rust connector SDK.** `pz_connector::stats()` and `pz_connector::remote::start`, for sinks.
- **`pz connector test` vector `commit-stats-valid`** for connectors declaring `ReportsStats`. It commits once more
  to the probe target.
- **PCP.** The `GetReadStats` RPC, and `stats` on the write-commit and abort responses.

### Changed

- **Exported spans carry error codes and types only.** A failed node span's status description is its error code
  (for example `PZ0501`), its `exception` event carries `exception.type` and `pz.error.code` with no
  `exception.message`, and the `retry_scheduled` span event drops `reason` for `exception.type` and `pz.error.code`,
  because a connector's message can echo data values. Full messages remain in `run_results.json`, the NDJSON events
  and console output.

### Notes

- Attributes from libraries listed in `ActivitySources` and `Meters` fail closed: only a fixed set of safe keys
  (system, operation, database and collection names, server address and port, status codes) is exported, and every
  other attribute is dropped. A span is dropped, not exported, when it records an exception or any event attribute
  other than `exception.type`, or has a link carrying attributes: events and links cannot be edited, and exception
  messages can carry data values. Its status description is cleared. Children of a dropped span export with a parent
  id pointing at it. A foreign span's name is replaced by its source name plus the operation name, if recorded (at most
  96 characters), a span with any event not named `exception` is dropped, and only the exact `Pz.Connector` source and
  meter are treated as the SDK's own (`Pz.Connector.*` is filtered like any library). An invalid OTLP endpoint is
  reported on stderr by scheme, host and port only.
- A commit or abort that fails reports no stats for that call.
- A partition read abandoned early (cancellation, or a failure elsewhere) reports no stats for that stream.
- Rust SDK stats cover sinks only; the Rust SDK has no source support.
- Requests a hosted connector makes during open, plan and schema calls are not counted.
- The Rust SDK does not redact foreign `tracing` spans.
- Rust `stats()` reaches only code awaited inside the SDK's call, not work moved to `tokio::spawn`.

## [0.10.0] - 2026-10-12

### Added

- **Node facts on every completed node.** `node_completed` and `run_results.json`'s `nodes[]` carry `connector`,
  `connection`, `strategy` (`native_scan`, `native_copy` or `arrow` — the tier that actually moved the data),
  `stages` (`extractMs`, `ingestMs`, `egressMs`, `writeMs`, each stage's own wall time in the last attempt, plus
  `extractStartMs`…`writeStartMs`, when it began counted from the node's start) and
  `attempts`. All optional; a node that carries none writes exactly what it wrote before.
- **Run phase timings.** `run_completed` carries `phases` (`loadMs`, `compileMs`, `validateMs`, `restoreCheckMs`,
  `planMs`, `executeMs`); the terminal `run_results.json` adds `finalizeMs`.
- **Richer OpenTelemetry.** Node spans carry the node facts plus rows, ops, stall timings, provenance, caught-up,
  partition, delivery and CDC counts; failed nodes get an error status with a sanitized one-line description and an
  `exception` event; retries are `retry_scheduled` span events whose reason is sanitized to one line. The `run` span
  is back-dated to the run's first phase, has a `phase.<name>` child per phase, carries status and node counts, ends
  in error when a run fails or stops early (naming the phase: "run ended during <phase>" / "run ended after
  <phase>"), and records `breaker.state_changed` events. Failing `pcp.*` RPC spans in both connector SDKs carry an
  error status, `rpc.grpc.status_code` and `error.type` (the gRPC code name), never the message. Never on a span: SQL
  text, connection config, watermark values, sync tokens or CDC positions.

## [0.9.3] - 2026-10-10

### Added

- **`pz run --until-caught-up [--max-runs N]`.** Repeats the run while any windowed source with a stop (`until`,
  `until: now`, or a SQL ceiling) reports `caughtUp: false`, so a backfill in slices no longer needs a shell loop.
  Each pass is a full run that writes its sinks and advances the watermark before the next starts. The loop stops when
  every such source has caught up (exit 0), when a run fails (that run's exit code), on a stop signal (exit 3), or
  after `--max-runs` passes (default 100; exit 0 — the next invocation continues from the stored watermarks). With no
  windowed source that has a stop, it runs once. A final `note: until-caught-up: …` line gives the pass count and why
  it stopped (stderr under `--log-format json`). `--full-refresh` applies to the first pass only.

## [0.9.2] - 2026-10-09

### Fixed

- **`--full-refresh` resets the watermark on the HTTP and SQL Server state backends.** A full refresh skipped reading the
  stored watermark (and a feed's or CDC source's sync state), so a versioned store saw its advancement as a write by
  another run and refused it: the run printed `PZ0527 … PZ0520` and kept the old value, and the next run went on from
  there instead of from `initial`. The run now reads the entry and ignores it, so its own advancement replaces it. The
  local file store was not affected.

## [0.9.1] - 2026-10-09

### Added

- **OTLP over HTTP.** `pz run`, `pz test` and `pz retry` take `--otel-protocol http/protobuf` with
  `--otel-traces-endpoint` and `--otel-metrics-endpoint` (full URLs, used as-is; either alone is fine), so a run can
  export straight to a backend that only takes OTLP/HTTP, such as Azure Monitor. Bodies are gzipped and span batches
  are capped at 256 so a request stays under 1 MB. Metrics over HTTP are delta, with base-2 exponential histograms.
- **`--otel-headers-file`.** Header lines (`Name=value`) re-read before every export, so the caller can refresh a
  token while the run is going. A missing or malformed file sends without it and prints one `note:`; the content is
  never printed.
- **`pz.run.id` on every metric point**, so one run's metrics can be filtered where resource attributes are dropped.
- Connectors receive the HTTP settings through `HostInfo` (`otel_protocol`, `otel_traces_endpoint`,
  `otel_metrics_endpoint`, `otel_headers_file`); both SDKs export over HTTP. Connectors built on an older SDK export
  nothing under `http/protobuf` (gRPC is unchanged).

### Changed

- **`--otel-*` flags win over `PZ_OTEL_*` variables as a set.** When any `--otel-*` flag is given, every `PZ_OTEL_*`
  variable is ignored, so a stray variable on the host cannot turn an explicit flag set into a usage error. With no
  flag, the variables apply as before.
- pz no longer follows ambient `OTEL_EXPORTER_OTLP_PROTOCOL` or endpoint variables: every exporter option comes from
  its flag or `PZ_OTEL_*` variable. `OTEL_EXPORTER_OTLP_HEADERS` is still honoured.
- The Rust connector SDK adds the OTLP/HTTP transport (reqwest's blocking client with rustls and aws-lc-rs, the
  provider rustls defaults to, so a connector's own rustls code is unaffected), which adds about 7 MB to a release
  connector binary.

## [0.9.0] - 2026-10-09

### Added

- **`pz run`, `pz test` and `pz retry` join a caller's trace.** With telemetry on, a W3C `TRACEPARENT` (and
  `TRACESTATE`) in the environment makes the `run` span a child in that trace, so an orchestrator can link a run
  to its trace before it starts. The caller's sampling decision is honored: an unsampled parent (flags `00`) means
  pz exports no spans for that run, while metrics still flow. An invalid value is ignored with a `note:` line.
- **`caughtUp` on `node_completed` and in `run_results.json`.** A windowed source with `until` (or a SQL-declared
  ceiling) now says whether it has caught up, so a platform or a shell loop can repeat `pz run` until every
  backfill is done instead of reading the `note:` line or comparing watermarks.
- **`until: now`.** A windowed dataset with a date or timestamp cursor can stop at the time the run started (UTC)
  instead of a fixed value, so a scheduled job keeps the source current one window at a time. The last window stops
  just before the run's start: a date cursor loads up to yesterday (UTC), never half of today. The cursor must hold UTC
  values: an empty window moves the watermark to its end, so a cursor in a local time behind UTC would skip rows.

## [0.8.0] - 2026-10-08

### Added

- **`pz cdc status` and `pz cdc drop` take `--state-url` and `--log-format json`.** A platform can run them
  against the state store its runs use and read `cdc_status` / `cdc_dropped` NDJSON events.

### Fixed

- **`pz cdc drop` accepts a schema-qualified entity** (`erp.dbo.orders`). The target splits on its first dot,
  so a SQL Server cdc dataset `pz cdc status` reports can also be dropped.

## [0.7.0] - 2026-09-30

### Breaking

- **Quoted YAML scalars stay strings** (`password: "0123456"` no longer becomes `123456`).
  *Migration:* unquote pz's own numeric/boolean keys (`threads: "4"` is now PZ0120).
- **Unquoted `${VAR}` is typed by its value** (`port: ${PGPORT}` is an integer; `$${` is a literal `${`).
  *Migration:* quote references feeding text options, e.g. `password: "${PGPASSWORD}"`.
- **Unknown `write:`/`sink()` options on builtin sinks are refused (PZ0301).**
  *Migration:* remove or fix the named option; it never had an effect.
- **Pipeline file stems and connection names must be plain identifiers**, and names differing only by
  case collide (PZ0110, PZ0230).
  *Migration:* rename offending files, connections or datasets.
- **A relative path escaping a declared `root:` is refused (PZ0365).**
  *Migration:* use an absolute `path:` or move `root:` up.
- **`pz restore` honours `pz.lock.json`** (exact versions, sha512-verified) instead of re-resolving.
  *Migration:* use `pz restore --update` to pick up `project.yml` changes.
- **SinkWrite node ids include `keys:`/`duplicates:`/`on_delete:`.**
  *Migration:* none; a `pz retry` of a pre-upgrade run re-runs its sinks.
- **`Pz.Connectors.Abstractions` drops `Microsoft.Extensions.Logging.Abstractions`.**
  *Migration:* connectors not built on `Pz.Connectors.Sdk` that use `ILogger` add the reference.

### Added

- `pz runs`: list prior runs (`--json` supported, local or SQL Server state).
- `pz completion bash|zsh|fish|pwsh`.
- `engine.node_timeout`: bound one node attempt; exceeding it fails with PZ0525.
- `connector_log` run event: connector log output is no longer dropped.
- Connector SDK name/version on the PCP handshake, shown by `pz connectors`.
- `IOutputConfigSchema`: sinks can publish a schema for their write options.
- Iceberg `storage_scope:` option.
- `connect_timeout_seconds`/`command_timeout_seconds` for sqlserver, postgres (and sftp connect);
  `state.timeout_seconds` for the HTTP state backend.
- Rust connector SDK builds and runs on Windows.
- Windows: connectors are killed with pz (Job Object); socket directory is owner-only.
- Lock records a per-file sha512 and the RID; tampered installs are PZ0326, a wrong platform PZ0321.
- TestKit: more sink acceptance facts and a `ColumnPruning` fact (existing subclasses unaffected).
- Each release publishes a GitHub Release with a standalone `pz-win-x64.zip` (the winget source).
- Release: changelog-entry check and packaging proofs gate the publish; SHA-pinned actions,
  `global.json`, dependabot, `cargo audit`, build provenance.

### Changed

- Rust SDK moves to tonic 0.14 and opentelemetry 0.32.
- Unquoted decimal connector versions (`version: 1.10`) are refused; quote them.

### Fixed

- **Wrong rows/columns:** predicate pushdown across outer joins, column pruning with joins, and
  positional native `append` (now `by name`) for SQLite, MySQL, DuckDB, DuckLake, MotherDuck,
  Quack and Iceberg.
- Parquet writes (managed path) no longer truncate timestamps to milliseconds.
- External connectors start on Windows; a chatty connector (stdout) no longer hangs the run;
  connector processes end when their node is done.
- Rust-SDK sinks could hang at commit.
- Transient errors mid-stream are retried; connector error codes/hints and exit signals are reported.
- Integer connector options arrive as integers over PCP; unknown capability bits no longer fail the
  handshake; Arrow shape guards compare types structurally.
- Ctrl-C and node timeouts interrupt running DuckDB statements; SIGTERM/SIGHUP shut down cleanly.
- Overlapping runs no longer lose watermarks or fail writing `.pz` artifacts.
- Watermark advancement continues past a failing dataset.
- Compile reports every broken pipeline, accepts a trailing `;`, and handles comments/`WITH RECURSIVE`
  in ephemeral inlining.
- New refusals and warnings: duplicate checks (PZ0113), mixed `watermark()` cursors (PZ0231),
  `run_id` in SQL (PZ0232), bad `schema_policy`, unknown keys in `engine:`/`retry:`/`rate_limit:`,
  `${VAR}` inside entity blocks, conflicting MotherDuck tokens, unpinned SFTP host keys (PZ0364).
- Huge integer/decimal kwargs no longer crash compile.
- Unhandled exceptions are PZ0500 (exit 3); usage errors exit 2; disk-full/permission errors have
  their own codes (PZ0531, PZ0532).
- `pz restore` works offline when the cache is complete and repairs torn installs.
- SQL Server/HTTP state: correct transient classification, no O(N²) artifact writes, no hang on a
  dead event store, safe concurrent schema migration.
- Culture-independent run ids and YAML value rendering.
- Relative `private_key_path` (sftp) and `key_file` (gcs) resolve against the project.
- HTTP connector: `max_response_mb` capped, non-object records refused, redaction no longer throws.
- `pz mcp`: JSONC config files, `file://` docs mirrors, schema for undeclared entities,
  `pz_run` reports its own run.
- DuckDB "could not connect to server" is transient; ducklake accepts an empty sqlite catalog.
- Connectors SDK: pooled allocation on the write path, no hang on a dead reverse channel.
- Rust SDK parity fixes (abort semantics and others).

## [0.6.1] - 2026-09-10

### Changed

- Every published NuGet package now carries the pz icon.

### Fixed

- Out-of-process connectors: a read with a column-pruning hint that is not a
  leading prefix of the declared schema no longer crashes `pz run`. The
  Connectors SDK opened the data-plane stream with the declared (unpruned)
  schema, so pruned batches were decoded by position into the wrong columns
  and handed to DuckDB as garbage. The SDK now opens the stream with the
  declared schema narrowed to the hint, refuses a batch shaped differently
  from the stream, and the engine refuses a batch shaped differently from
  the staging table before DuckDB sees it. Connectors built on
  `Pz.Connectors.Sdk` 0.6.0 or earlier must be rebuilt on the fixed SDK to
  pick up the stream fix; the engine-side guard turns the crash into a node
  failure for connectors that have not been. Until a connector is rebuilt, a
  pruning read that previously happened to produce correct rows (a hint that
  is a leading prefix of the declared schema) now fails the node cleanly as
  well: with this release and an un-rebuilt pruning connector, every pruned
  read fails until that connector ships on the fixed SDK.

## [0.6.0] - 2026-09-08

### Added

- Out-of-process connector telemetry: `HostInfo` carries the run id and OTLP
  endpoint, and PCP injects W3C trace context into the connector handshake, so
  a connector process's own OpenTelemetry spans and meters nest under the
  engine's node span. Both connector SDKs build their OTel providers from
  `HostInfo`, parent `pcp.<Rpc>` server spans and data-plane stream spans on the
  host's traceparent, bound telemetry flush at shutdown to the aggregate flush
  bound, and restore the ambient activity after a root span. The Rust SDK
  additionally exports tracing spans and meters to the host's OTLP endpoint,
  and `pz-connector` exposes a composable tracing layer with span attributes
  aligned to the C# SDK.
- Hosted connector instances are named after the connection (`pz.instance`),
  and the engine opens the run span around every phase, not just execute.

### Fixed

- Rust SDK: a pre-installed subscriber is reported instead of silently
  exporting nothing; a flush that misses its bound says so, and exporter
  warnings surface on stderr; the write-stream span drops noisy code/thread
  attributes.
- Connectors SDK: a header-less RPC now roots its own span and reports an
  unusable endpoint instead of failing silently.

## [0.5.2] - 2026-09-07

### Fixed

- Connectors SDK: `PzPackaging=aot` fails loudly at restore time when the
  restored package never saw `PublishAot` (PZSDK005), instead of silently
  falling back to a CoreCLR-hosted package that PZ0360 then refuses.

## [0.5.1] - 2026-09-07

### Fixed

- Connectors SDK: a project-level `PzPackaging` MSBuild property is honored
  instead of being overridden; `pz connector test`'s config now interpolates
  environment variables.

Everything below ships in the first public release.

### The tool

- `pz` as a self-contained .NET global tool: `init`, `compile`, `plan`, `validate
  [--connect]`, `restore`, `run`, `test`, `retry`, `ls`, `connectors`, `cdc`,
  `clean`, `state`, `schema`, `mcp`. Exit codes 0/1/2/3; `--log-format json`
  emits the documented NDJSON event stream (https://pipelinez.dev/events/).
- dbt-shaped authoring: one `connections.yml` (connections → entities), SQL
  pipelines with `ref()`/`source()`/`sink()` templating, checks as first-class
  nodes, a compiled and inspectable DAG under `.pz/target`.
- DuckDB as the engine: sources stage into a per-run DuckDB database, SQL runs
  in-process, sinks drain out over zero-copy Arrow; two data-plane tiers (native
  scan/copy and the universal Arrow path) chosen per node by the planner.
- Incremental EL: SQL-declared watermarks, bounded backfill windows, commit-gated
  advancement, per-instance retries and circuit breakers, `pz retry` with staged
  data reuse and carried-forward sinks, CDC from Postgres (pgoutput) and SQL
  Server (change tables) with `pz cdc status`/`drop`.
- Correctness guardrails: schema-drift detection (`on_source_drift`), duplicate
  merge-key warnings (PZ0522), lossy integer-inference warnings (PZ0523),
  ambiguous date-order warnings (PZ0524), memory budgeting with disclosed limits.
- Eight first-party connectors: local files (csv/NDJSON/parquet), Postgres,
  SQL Server, MySQL, SQLite, S3-compatible object storage (source and sink,
  with a documented Google Cloud Storage recipe via its S3 interoperability
  mode), Azure Blob Storage, HTTP APIs. Object-store format parity: parquet,
  csv, and NDJSON json on local files, S3, and Azure Blob alike, in both
  directions.
  Third-party connectors ship as ordinary NuGet packages against the versioned
  ABI in `Pz.Connectors.Abstractions`, with `Pz.Connectors.TestKit` as the
  acceptance suite.
- `pz mcp`: a Model Context Protocol server exposing typed introspect/verify/
  author tools (execution gated behind `--allow-run`), with credential and
  path-containment guards for agent-driven use.
- State that can leave the machine: SQL Server- and HTTP-backed state stores for
  ephemeral hosts; local deterministic JSON artifacts otherwise.
