# pz-connector-deltalake

A Delta Lake **sink** for [PipelineZ](https://pipelinez.dev) (`pz`), built on
[delta-rs](https://github.com/delta-io/delta-rs) and served over the `pz-connector` Rust SDK's
out-of-process connector protocol (PCP). Connector name on the wire: `deltalake-rs`.

Read this before you point it at real data.

## 1. What actually works, measured, not projected

Everything below was exercised by `cargo test -p pz-connector-deltalake` and
`scripts/rust-conformance-deltalake.sh` against **local filesystem tables only** (a `tempfile`
temp directory).

**`s3://`/`az://`/`gs://` roots cannot work in this build, plainly, not just "unverified".** This
crate's `Cargo.toml` enables only delta-rs's `datafusion` feature -- not `s3`/`azure`/`gcs` -- so no
object-store backend for those schemes is compiled in. `validate`/`check`/`open` all refuse such a
`root:` up front with a clear error and a `hint` naming the missing cargo feature, rather than
letting the write fail deep inside delta-rs with an opaque "Cannot infer storage location from: ..."
the first time anything actually tries to reach the store. Adding real S3/Azure support means
enabling those features on the `deltalake` dependency, which roughly doubles the build's dependency
footprint (`aws-sdk-*`/Azure SDK crates) -- out of scope here, since this task's own test requirement
is local temp-dir tables only.

| write mode | status |
|---|---|
| `append` | tested (two-batch append, row/schema check on reopen) |
| `replace` | tested (full overwrite) |
| `merge` | tested (`when_matched_update`/`when_not_matched_insert` on `keys:`) |
| `partition_by` | tested (partition column recorded in the table's own metadata) |
| abort | tested (leaves the table version unchanged, every mode) |

Platform: built and tested on `linux-x64` only in this repository, same as every other toolchain-gated
script here.

## 2. The one Delta table per output

`root:` is the connection -- where the place is. Each `output`/entity gets its own Delta table at
`<root>/<output>/`, the same "no `path:` override means `<root>/<entity>/`" convention `localfiles`
uses. There is no single table spanning multiple outputs.

The `connection:` shape this connector's `Configure` RPC accepts is:

```yaml
root: /abs/path/to/lake            # an s3://, az:// or gs:// root is refused in this build -- see §1
storage_options:                   # optional, passed verbatim to delta-rs's object_store backend
  AWS_REGION: us-east-1
```

(How a `connections.yml` entry points pz at this binary in the first place is the out-of-process
connector packaging/registration mechanism, which lives elsewhere in this repository -- not
something this connector defines. `scripts/rust-conformance-deltalake.sh` shows the config shape
above driving the binary directly, via `pz connector test`.)

```sql
insert into {{ sink('lake', 'orders', mode: 'merge', keys: ['id']) }}
select id, region, amount from {{ source(...) }}
```

writes to `<root>/orders/`.

## 3. Modes

- **`append`** streams straight into delta-rs's own `RecordBatchWriter` -- batches flow to parquet
  as they arrive, `commit` is one `flush_and_commit` carrying `spec.attempt` (when the host sent one)
  as the commit's `userMetadata` (`{"pzNode":...,"pzRun":...,"pzOrdinal":...}`). This is the
  memory-safest mode: nothing here ever holds more than the batch currently in flight.
- **`replace`** and **`merge`** cannot use that writer (both need delta-rs APIs gated behind the
  `datafusion` feature, whose inputs are not "hand it one batch at a time" shaped). Instead every
  batch is staged to a local parquet file (never inside the table's own storage) as it arrives, and
  only at `commit` does delta-rs read it back:
  - `replace` re-reads every staged file into one `Vec<RecordBatch>` -- `DeltaTable::write(...)
    .with_save_mode(Overwrite)` has no lazy-scan input path in this delta-rs version, so commit-time
    memory here is genuinely O(dataset). A table too large to overwrite in memory needs `merge`
    instead. A replace that stages zero batches (a pipeline that legitimately produced no rows this
    run) still truncates the table via a real overwrite commit -- one zero-row batch carrying the
    declared schema, since delta-rs treats a literally empty `Vec<RecordBatch>` as "no data source
    supplied" (an error) regardless of write mode.
  - `merge` opens the staged directory as a DataFusion `DataFrame` (lazy scan), so its commit-time
    memory stays bounded regardless of how much was staged.

  See `src/sink.rs`'s `DeltaWriteSession` doc comment for the full reasoning.
- **`partition_by`** (an output option, any mode) becomes the table's `partition_by` at creation
  time. Against an already-existing table, a *declared* `partition_by:` that disagrees with the
  table's own partitioning is refused with a clear error before any write is attempted (this
  connector's own check, in `open_or_create_table`) -- Delta partitioning is fixed at creation, and
  delta-rs has no "repartition an existing table" operation. Omitting `partition_by:` on a write to
  an already-partitioned table is not an error: the table's own partitioning is respected either way.
- **abort** never touches delta-rs's commit path in any mode -- the table's version (and, for
  `replace`/`merge`, its storage too) is exactly as it was before the write session began.

## 4. Secret hygiene

delta-rs/object_store error strings can embed the table's own URI complete with a presigned query
string, or a bare `AWS_SECRET_ACCESS_KEY=...`-shaped credential. `root:` itself can too, if someone
puts a query string or a credential-shaped value directly in it. Every error this connector reports
goes through `src/redact.rs` first: query strings are stripped to `?<redacted>`, and any
`key=value`/`key: value` pair whose key looks credential-shaped has its value blanked. See that
module's tests, including one proving a presigned-URL query string is redacted, and `sink.rs`'s
`root_with_secrets_never_leaks_from_validate_or_open` test, which proves neither `validate()`'s
`Vec<String>` nor `open()`'s `PzError` leaks a secret embedded directly in `root:`. Every plain
`PzError` this crate constructs goes through one `pz_error()` helper (or, for a `ConnectionSettings`
parse failure specifically, `ConfigError::into_pz_error`/`into_redacted_string`) so redaction can't be
forgotten at a new call site.

## 5. Building and running the conformance suite

```bash
cargo build --bin pz-deltalake --manifest-path rust/pz-connector-deltalake/Cargo.toml
scripts/rust-conformance-deltalake.sh   # builds + runs `pz connector test` against it
```

`scripts/rust-conformance-deltalake.sh` probes `mode: replace` (not `merge`): the conformance CLI's
`--config` shape has no way to populate `OutputSpec.Keys`, and this connector correctly refuses a
keyless merge. `replace` exercises the same commit/abort/premature-commit/transient-error/
control-plane-size protocol paths merge would; merge's own correctness is covered by
`cargo test`'s `merge_updates_matching_keys_and_inserts_new_ones`, which drives the `WriteSession`
trait directly.
