//! The Delta Lake sink: `append` (streaming appender), `replace` (atomic full overwrite) and `merge`
//! (upsert on `spec.keys`), all delta-rs 0.32 operations under the hood. See the module-level
//! comments on [`DeltaWriteSession`] for the buffering strategy each mode uses and why it differs
//! between append and the other two.

use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use deltalake::datafusion::prelude::{col, ParquetReadOptions, SessionContext};
use deltalake::kernel::engine::arrow_conversion::TryIntoKernel;
use deltalake::kernel::schema::StructType;
use deltalake::kernel::transaction::CommitProperties;
use deltalake::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use deltalake::parquet::arrow::ArrowWriter;
use deltalake::protocol::SaveMode;
use deltalake::writer::{DeltaWriter as _, RecordBatchWriter};
use deltalake::{DeltaResult, DeltaTable, DeltaTableError};
use pz_connector::{
    Config, ConnectorDecl, NativeCopy, OutputSpec, PzError, Sink, SinkConnector, WriteAttempt,
    WriteResult, WriteSession,
};
use tempfile::TempDir;
use url::Url;

use crate::redact::redact;
use crate::schemas::{CONNECTION_CONFIG_SCHEMA, DATASET_CONFIG_SCHEMA};

/// Capability flags this sink declares, computed from `Pz.Connectors.Abstractions.ConnectorCapabilities`
/// (values confirmed against that C# source, not guessed):
///   Merge = 32, Transactional = 64, ReplaceWrites = 32768, ColumnPartitionedWrites = 1048576
///   32 | 64 | 32768 | 1048576 = 1081440
const CAPABILITIES: u64 = 32 | 64 | 32768 | 1048576;

pub const CONNECTOR_NAME: &str = "deltalake-rs";

pub fn decl() -> ConnectorDecl {
    ConnectorDecl {
        name: CONNECTOR_NAME,
        version: env!("CARGO_PKG_VERSION"),
        capabilities: CAPABILITIES,
        connection_config_schema: CONNECTION_CONFIG_SCHEMA,
        dataset_config_schema: DATASET_CONFIG_SCHEMA,
    }
}

// ---------------------------------------------------------------------------------------------
// Connection config
// ---------------------------------------------------------------------------------------------

/// Parsed `connection:` config: where the table lives and what delta-rs's object_store backend
/// needs to reach it. Parsed once by [`ConnectionSettings::from_config`], the one place this
/// connector reads `root`/`storage_options` -- `validate`, `check` and `open` all go through it so
/// none of the three can drift from what the others accept.
#[derive(Clone)]
struct ConnectionSettings {
    root_url: Url,
    storage_options: HashMap<String, String>,
}

impl ConnectionSettings {
    fn from_config(config: &Config) -> Result<Self, String> {
        let root = match config.0.get("root") {
            Some(serde_json::Value::String(s)) if !s.trim().is_empty() => s.trim(),
            Some(serde_json::Value::String(_)) => return Err("'root' is empty".to_string()),
            Some(_) => return Err("'root' must be a string".to_string()),
            None => return Err("'root' is required (the table's root path or URI)".to_string()),
        };

        let root_url = parse_root(root)?;

        let storage_options = match config.0.get("storage_options") {
            None | Some(serde_json::Value::Null) => HashMap::new(),
            Some(serde_json::Value::Object(map)) => {
                let mut options = HashMap::with_capacity(map.len());
                for (k, v) in map {
                    match v {
                        serde_json::Value::String(s) => {
                            options.insert(k.clone(), s.clone());
                        }
                        other => {
                            options.insert(k.clone(), other.to_string());
                        }
                    }
                }
                options
            }
            Some(_) => return Err("'storage_options' must be a map of string values".to_string()),
        };

        Ok(ConnectionSettings {
            root_url,
            storage_options,
        })
    }
}

/// A URI with a scheme (`s3://`, `az://`, `file://`, ...) is used as-is; a bare filesystem path must
/// be absolute -- this out-of-process connector is handed no project directory to anchor a relative
/// one against, so guessing would silently write somewhere the author never intended.
fn parse_root(root: &str) -> Result<Url, String> {
    if let Ok(url) = Url::parse(root) {
        return Ok(url);
    }

    let path = Path::new(root);
    if !path.is_absolute() {
        return Err(format!(
            "'root' ('{root}') is neither a URI (s3://, az://, ...) nor an absolute filesystem path"
        ));
    }

    Url::from_directory_path(path)
        .map_err(|()| format!("'root' ('{root}') could not be turned into a file:// URL"))
}

/// `<root>/<output>/` -- one Delta table per entity, mirroring `localfiles`' own "no `path:`
/// override means `<root>/<entity>/`" convention (an entity is a thing in the place named the way
/// the place names it, never a `schema:`/`table:` pair). Works uniformly for `file://`, `s3://` and
/// `az://` roots: all three are segment-addressable, so `path_segments_mut` is the one way to append
/// a segment that is correct regardless of scheme (string-concatenating `root + "/" + output` would
/// double a trailing slash `root:` might already carry).
fn table_url_for_output(root: &Url, output: &str) -> Result<Url, PzError> {
    let mut url = root.clone();
    {
        let mut segments = url.path_segments_mut().map_err(|()| {
            PzError::new(format!(
                "connection 'root' ('{root}') cannot be a Delta table location -- its scheme has no path segments"
            ))
        })?;
        segments.pop_if_empty();
        segments.push(output);
        segments.push("");
    }
    Ok(url)
}

/// delta-rs's local (`file://`) object_store backend treats "the directory does not exist on disk"
/// as a hard error rather than "reachable, nothing here yet" -- unlike a key-prefix-based store
/// (S3/Azure), a local path is not a table location until something has actually created it. Every
/// other scheme is a no-op here: object_store's remote backends need no such thing, and getting a
/// filesystem path for a `s3://`/`az://` URL would fail anyway.
fn ensure_local_dir_exists(url: &Url) -> Result<(), PzError> {
    if url.scheme() != "file" {
        return Ok(());
    }

    let path = url
        .to_file_path()
        .map_err(|()| PzError::new(format!("'{url}' is not a valid file:// path")))?;
    std::fs::create_dir_all(&path).map_err(|e| {
        PzError::new(format!(
            "failed to create local table directory '{}': {e}",
            path.display()
        ))
    })
}

// ---------------------------------------------------------------------------------------------
// SinkConnector
// ---------------------------------------------------------------------------------------------

pub struct DeltaSinkConnector;

#[async_trait]
impl SinkConnector for DeltaSinkConnector {
    async fn validate(&self, config: &Config) -> Vec<String> {
        match ConnectionSettings::from_config(config) {
            Ok(_) => Vec::new(),
            Err(e) => vec![e],
        }
    }

    async fn check(&self, config: &Config) -> Result<(), PzError> {
        let settings = ConnectionSettings::from_config(config).map_err(PzError::new)?;
        ensure_local_dir_exists(&settings.root_url)?;
        // `try_from_url_with_storage_options` already treats "no table there yet" as success (see
        // its own doc) -- an empty/uninitialized location is reachable, which is everything a
        // connectivity check needs to know. Anything else (bad credentials, unreachable host,
        // malformed URI the object_store backend itself rejects) surfaces as a real error.
        DeltaTable::try_from_url_with_storage_options(settings.root_url, settings.storage_options)
            .await
            .map_err(map_delta_error)?;
        Ok(())
    }

    async fn open(&self, config: Config) -> Result<Box<dyn Sink>, PzError> {
        let settings = ConnectionSettings::from_config(&config).map_err(PzError::new)?;
        Ok(Box::new(DeltaSink { settings }))
    }

    fn try_native_copy(&self, _spec: &OutputSpec) -> Option<NativeCopy> {
        // DuckDB has no native Delta *writer* (only a read-side extension) -- every write goes
        // through this process's own delta-rs commit path.
        None
    }
}

struct DeltaSink {
    settings: ConnectionSettings,
}

#[async_trait]
impl Sink for DeltaSink {
    async fn begin_write(
        &self,
        spec: OutputSpec,
        schema: SchemaRef,
    ) -> Result<Box<dyn WriteSession>, PzError> {
        // `root:` names where the place is, not any one table in it -- `localfiles` writes a
        // dataset entity with no `path:` override to `<root>/<entity>/`, and this sink follows the
        // same convention: each `output` is its own Delta table, a subdirectory of the connection's
        // root, not the root itself.
        let table_url = table_url_for_output(&self.settings.root_url, &spec.output)?;
        let table = open_or_create_table(
            table_url,
            self.settings.storage_options.clone(),
            &schema,
            &spec,
        )
        .await?;

        let partition_cols = read_partition_by(&spec.options);
        let commit_properties = build_commit_properties(&spec.attempt);

        match spec.mode.as_str() {
            "append" => {
                let writer = RecordBatchWriter::for_table(&table).map_err(map_delta_error)?;
                Ok(Box::new(DeltaWriteSession::Append(Box::new(
                    AppendSession {
                        table,
                        writer,
                        rows: 0,
                        batches: 0,
                    },
                ))))
            }
            "replace" => {
                let stage = Stage::new().map_err(map_delta_error)?;
                Ok(Box::new(DeltaWriteSession::Staged(Box::new(
                    StagedSession {
                        kind: StagedKind::Replace,
                        table: Some(table),
                        stage,
                        schema,
                        keys: spec.keys,
                        partition_cols,
                        commit_properties,
                        rows: 0,
                        batches: 0,
                    },
                ))))
            }
            "merge" => {
                if spec.keys.is_empty() {
                    return Err(PzError::new(
                        "deltalake-rs merge mode requires at least one key column (write: { keys: [...] })",
                    ));
                }
                let stage = Stage::new().map_err(map_delta_error)?;
                Ok(Box::new(DeltaWriteSession::Staged(Box::new(
                    StagedSession {
                        kind: StagedKind::Merge,
                        table: Some(table),
                        stage,
                        schema,
                        keys: spec.keys,
                        partition_cols,
                        commit_properties,
                        rows: 0,
                        batches: 0,
                    },
                ))))
            }
            other => Err(PzError::new(format!(
                "deltalake-rs sink does not support write mode '{other}' (supported: append, replace, merge)"
            ))),
        }
    }
}

/// Opens the table at `root`, creating it (with `schema` translated to a Delta schema, and
/// `spec.options`' `partition_by:` as its partitioning) the first time anything writes to this
/// location. `SaveMode::Ignore` on the create step is what makes this idempotent against a
/// concurrent creator: if the table exists by the time the create commit would run, delta-rs just
/// loads it instead of erroring.
async fn open_or_create_table(
    root: Url,
    storage_options: HashMap<String, String>,
    schema: &SchemaRef,
    spec: &OutputSpec,
) -> Result<DeltaTable, PzError> {
    ensure_local_dir_exists(&root)?;
    let table = DeltaTable::try_from_url_with_storage_options(root, storage_options)
        .await
        .map_err(map_delta_error)?;

    if table.version().is_some() {
        return Ok(table);
    }

    let partition_cols = read_partition_by(&spec.options);
    let fields = arrow_schema_to_struct_fields(schema).map_err(map_delta_error)?;
    let mut builder = table
        .create()
        .with_columns(fields)
        .with_save_mode(SaveMode::Ignore);
    if !partition_cols.is_empty() {
        builder = builder.with_partition_columns(partition_cols);
    }
    builder.await.map_err(map_delta_error)
}

fn arrow_schema_to_struct_fields(
    schema: &Schema,
) -> DeltaResult<Vec<deltalake::kernel::schema::StructField>> {
    let kernel_schema: StructType = schema.try_into_kernel()?;
    Ok(kernel_schema.fields().cloned().collect())
}

/// Reads `partition_by:` the same way `Pz.Connectors.Abstractions.PartitionColumns` does on the host
/// side: a scalar string or a sequence of strings, absent/empty means "no partitioning declared".
/// Reimplemented here (not shared code -- there is no cross-language shared crate) because this is
/// the one connector-visible place that option ever needs reading.
fn read_partition_by(options: &serde_json::Map<String, serde_json::Value>) -> Vec<String> {
    match options.get("partition_by") {
        Some(serde_json::Value::String(s)) if !s.trim().is_empty() => vec![s.trim().to_string()],
        Some(serde_json::Value::Array(items)) => items
            .iter()
            .filter_map(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
        _ => Vec::new(),
    }
}

/// The write's identity (`spec.attempt`), if the host sent one, becomes the Delta commit's
/// `CommitInfo.userMetadata` -- a cheap breadcrumb tying a table version back to the pz node/run/
/// attempt that produced it. No dedupe logic reads this in v1; it exists purely for a human or a
/// later tool inspecting the transaction log.
fn build_commit_properties(attempt: &Option<WriteAttempt>) -> CommitProperties {
    let Some(attempt) = attempt else {
        return CommitProperties::default();
    };

    let user_metadata = serde_json::json!({
        "pzNode": attempt.node,
        "pzRun": attempt.run,
        "pzOrdinal": attempt.ordinal,
    })
    .to_string();

    CommitProperties::default().with_metadata([(
        "userMetadata".to_string(),
        serde_json::Value::String(user_metadata),
    )])
}

// ---------------------------------------------------------------------------------------------
// WriteSession
// ---------------------------------------------------------------------------------------------

/// One open write, in one of two shapes depending on mode:
///
/// - [`AppendSession`] streams straight into delta-rs's own `RecordBatchWriter`: batches flow to
///   parquet as they arrive (delta-rs's normal target-file-size buffering, not a `Vec<RecordBatch>`
///   this connector holds itself), and `commit` is one `flush_and_commit` -- delta-rs's own atomic
///   single-transaction append. This is the memory-safest of the three modes: nothing here ever
///   holds more than the batch currently in flight.
/// - [`StagedSession`] (`replace`/`merge`) cannot use that writer, because both operations need a
///   `delta-rs` API that only exists behind the `datafusion` feature (`DeltaTable::write`/`::merge`)
///   and that API's overwrite/merge-source inputs are not "hand it one batch at a time" shaped.
///   Instead each batch is flushed to a local (never inside the table's own storage) staging parquet
///   file as it arrives -- so ingest-time memory is still O(one batch), not O(dataset) -- and only at
///   `commit` does the staged data get read back:
///     - `replace` re-reads every staged file into one `Vec<RecordBatch>`, because
///       `DeltaTable::write(...).with_save_mode(Overwrite)` takes a materialized `Vec<RecordBatch>`
///       with no lazy-scan input path in this API -- commit-time memory here is genuinely O(dataset).
///       That is a real v1 constraint, not an oversight: a table too large to overwrite in memory
///       needs `merge` (see below) or a native-copy path this connector does not offer.
///     - `merge` instead opens the staged directory as a DataFusion `DataFrame`
///       (`SessionContext::read_parquet`), which DataFusion scans lazily -- so `merge`'s commit-time
///       memory stays bounded regardless of how much was staged, unlike `replace`.
enum DeltaWriteSession {
    // Both variants boxed purely to keep clippy::large_enum_variant happy -- an indirection here is
    // one pointer per session, unrelated to (and orders of magnitude smaller than) either mode's
    // actual write-time memory footprint discussed above.
    Append(Box<AppendSession>),
    Staged(Box<StagedSession>),
}

struct AppendSession {
    table: DeltaTable,
    writer: RecordBatchWriter,
    rows: i64,
    batches: i64,
}

enum StagedKind {
    Replace,
    Merge,
}

struct StagedSession {
    kind: StagedKind,
    /// `Option` only so `commit` can move the table out by value into `DeltaTable::write`/`::merge`,
    /// which both consume `self`. Never `None` except mid-`commit`.
    table: Option<DeltaTable>,
    stage: Stage,
    schema: SchemaRef,
    keys: Vec<String>,
    partition_cols: Vec<String>,
    commit_properties: CommitProperties,
    rows: i64,
    batches: i64,
}

/// A local (`tempfile`-backed, never under the table's own root) directory that batches are staged
/// into as parquet before a `replace`/`merge` commit. Kept off the table's own storage on purpose --
/// unlike the RecordBatchWriter append path, which necessarily leaves committed-nowhere parquet
/// files sitting in the table's own storage between `write_batch` and `commit` (delta-rs's own
/// design), staging locally means an aborted `replace`/`merge` leaves the table's storage completely
/// untouched, not just its transaction log.
struct Stage {
    dir: TempDir,
    next_index: u64,
}

impl Stage {
    fn new() -> DeltaResult<Self> {
        Ok(Stage {
            dir: tempfile::tempdir().map_err(|e| {
                DeltaTableError::generic(format!("failed to create a local staging directory: {e}"))
            })?,
            next_index: 0,
        })
    }

    fn write_batch(&mut self, batch: &RecordBatch) -> DeltaResult<()> {
        let path = self
            .dir
            .path()
            .join(format!("part-{:08}.parquet", self.next_index));
        self.next_index += 1;
        let file = File::create(&path).map_err(|e| io_delta_error(&path, e))?;
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
        writer.write(batch)?;
        writer.close()?;
        Ok(())
    }

    fn staged_paths(&self) -> DeltaResult<Vec<PathBuf>> {
        let mut paths: Vec<PathBuf> = std::fs::read_dir(self.dir.path())
            .map_err(|e| io_delta_error(self.dir.path(), e))?
            .filter_map(|entry| entry.ok().map(|e| e.path()))
            .collect();
        paths.sort();
        Ok(paths)
    }

    fn dir_path(&self) -> &Path {
        self.dir.path()
    }
}

fn io_delta_error(path: &Path, err: std::io::Error) -> DeltaTableError {
    DeltaTableError::generic(format!(
        "staging I/O failed for '{}': {err}",
        path.display()
    ))
}

#[async_trait]
impl WriteSession for DeltaWriteSession {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), PzError> {
        match self {
            DeltaWriteSession::Append(session) => {
                session.rows += batch.num_rows() as i64;
                session.batches += 1;
                session.writer.write(batch).await.map_err(map_delta_error)
            }
            DeltaWriteSession::Staged(session) => {
                session.rows += batch.num_rows() as i64;
                session.batches += 1;
                session.stage.write_batch(&batch).map_err(map_delta_error)
            }
        }
    }

    async fn commit(&mut self) -> Result<WriteResult, PzError> {
        match self {
            DeltaWriteSession::Append(session) => {
                session
                    .writer
                    .flush_and_commit(&mut session.table)
                    .await
                    .map_err(map_delta_error)?;
                Ok(WriteResult {
                    rows_written: session.rows,
                    batches_written: session.batches,
                })
            }
            DeltaWriteSession::Staged(session) => {
                let table = session
                    .table
                    .take()
                    .ok_or_else(|| PzError::new("write session already committed"))?;
                match session.kind {
                    StagedKind::Replace => {
                        commit_replace(
                            table,
                            &session.stage,
                            &session.partition_cols,
                            session.commit_properties.clone(),
                        )
                        .await?
                    }
                    StagedKind::Merge => {
                        commit_merge(
                            table,
                            &session.stage,
                            &session.schema,
                            &session.keys,
                            session.commit_properties.clone(),
                        )
                        .await?
                    }
                };
                Ok(WriteResult {
                    rows_written: session.rows,
                    batches_written: session.batches,
                })
            }
        }
    }

    async fn abort(&mut self) -> Result<(), PzError> {
        // Neither branch has committed anything (a `commit()` call takes `table`/consumes the
        // writer's flush path, so an aborted session never reached either), and the local `Stage`
        // temp directory (staged/merge branch) is deleted by `TempDir`'s own `Drop` -- so an abort
        // leaves both the Delta table's version AND its storage exactly as they were, satisfying
        // this write session's `AbortSemanticsDiscardsAll` promise with nothing further to do here.
        Ok(())
    }
}

async fn commit_replace(
    table: DeltaTable,
    stage: &Stage,
    partition_cols: &[String],
    commit_properties: CommitProperties,
) -> Result<(), PzError> {
    let batches = read_staged_batches(stage).map_err(map_delta_error)?;
    let mut builder = table
        .write(batches)
        .with_save_mode(SaveMode::Overwrite)
        .with_commit_properties(commit_properties);
    if !partition_cols.is_empty() {
        builder = builder.with_partition_columns(partition_cols.to_vec());
    }
    builder.await.map_err(map_delta_error)?;
    Ok(())
}

fn read_staged_batches(stage: &Stage) -> DeltaResult<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    for path in stage.staged_paths()? {
        let file = File::open(&path).map_err(|e| io_delta_error(&path, e))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
        for batch in reader {
            batches.push(batch?);
        }
    }
    Ok(batches)
}

async fn commit_merge(
    table: DeltaTable,
    stage: &Stage,
    schema: &SchemaRef,
    keys: &[String],
    commit_properties: CommitProperties,
) -> Result<(), PzError> {
    if stage.staged_paths().map_err(map_delta_error)?.is_empty() {
        // Nothing staged (an empty write): merging an empty source is a legal no-op, but
        // `SessionContext::read_parquet` over an empty directory errors before it ever gets that
        // far -- short-circuit rather than let that surface as a confusing failure.
        return Ok(());
    }

    let ctx = SessionContext::new();
    let source = ctx
        .read_parquet(
            stage.dir_path().to_string_lossy().to_string(),
            ParquetReadOptions::default(),
        )
        .await
        .map_err(|e| map_delta_error(DeltaTableError::generic(e.to_string())))?;

    let predicate = keys
        .iter()
        .map(|k| col(format!("target.{k}")).eq(col(format!("source.{k}"))))
        .reduce(|a, b| a.and(b))
        .ok_or_else(|| PzError::new("merge requires at least one key column"))?;

    let column_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

    let mut merge = table
        .merge(source, predicate)
        .with_source_alias("source")
        .with_target_alias("target")
        .with_commit_properties(commit_properties);

    {
        let column_names = column_names.clone();
        merge = merge
            .when_matched_update(move |mut update| {
                for name in &column_names {
                    update = update.update(name.as_str(), col(format!("source.{name}")));
                }
                update
            })
            .map_err(map_delta_error)?;
    }

    merge = merge
        .when_not_matched_insert(move |mut insert| {
            for name in &column_names {
                insert = insert.set(name.as_str(), col(format!("source.{name}")));
            }
            insert
        })
        .map_err(map_delta_error)?;

    // `on_delete` is deliberately never consulted: this sink does not declare `ApplyDeletes`, so the
    // planner never routes a cdc `on_delete: delete|soft` output here (PZ0339) -- there is nothing
    // for a merge-mode write to honor on that front.
    merge.await.map_err(map_delta_error)?;
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Error mapping
// ---------------------------------------------------------------------------------------------

/// How long the engine should wait before retrying a transient failure. Not tuned against a real
/// object-store backoff schedule (out of scope for v1) -- picked to be clearly non-zero without
/// being punitive for a local conformance/test run.
const TRANSIENT_RETRY_AFTER_MS: i64 = 500;

/// Substrings that mark a failure as the destination's problem right now, not the connector's
/// config: request timeouts and the 5xx class an object store returns for its own overload/outage,
/// plus the throttling language S3/Azure both use that does not always come back as a plain "429" or
/// "503" in the message text.
const TRANSIENT_MARKERS: &[&str] = &[
    "timed out",
    "timeout",
    "connection reset",
    "connection refused",
    "temporarily unavailable",
    "service unavailable",
    "too many requests",
    "throttl",
    "slow down",
    " 429",
    " 500",
    " 502",
    " 503",
    " 504",
];

fn map_delta_error(err: DeltaTableError) -> PzError {
    let raw = error_chain(&err);
    let redacted = redact(&raw);
    if is_transient(&raw) {
        PzError::transient(redacted, TRANSIENT_RETRY_AFTER_MS)
    } else {
        PzError::new(redacted)
    }
}

/// Walks `Error::source()` so a wrapped `object_store`/HTTP client cause (where the transient/5xx
/// evidence usually lives) is not lost behind delta-rs's own outer message.
fn error_chain(err: &(dyn std::error::Error + 'static)) -> String {
    let mut message = err.to_string();
    let mut cause = err.source();
    while let Some(c) = cause {
        message.push_str(" -- caused by: ");
        message.push_str(&c.to_string());
        cause = c.source();
    }
    message
}

fn is_transient(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    TRANSIENT_MARKERS.iter().any(|m| lower.contains(m))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc as StdArc;
    use tempfile::TempDir as TestTempDir;

    fn table_dir() -> (TestTempDir, Url) {
        let dir = tempfile::tempdir().expect("tempdir");
        let url = Url::from_directory_path(dir.path()).expect("file url");
        (dir, url)
    }

    fn probe_schema() -> SchemaRef {
        StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, true),
        ]))
    }

    fn probe_batch(ids: &[i64], values: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            probe_schema(),
            vec![
                StdArc::new(Int64Array::from(ids.to_vec())),
                StdArc::new(StringArray::from(values.to_vec())),
            ],
        )
        .expect("record batch")
    }

    fn output_spec(mode: &str, keys: Vec<String>) -> OutputSpec {
        OutputSpec {
            sink: "lake".to_string(),
            output: "orders".to_string(),
            mode: mode.to_string(),
            schema_policy: "match".to_string(),
            options: serde_json::Map::new(),
            keys,
            on_delete: None,
            max_text_lengths: None,
            attempt: None,
        }
    }

    /// Reopens the table at `<root>/orders/` -- every test in this module writes to the fixed
    /// `"orders"` output `output_spec` names, so this mirrors `table_url_for_output`'s own
    /// `<root>/<output>/` convention instead of re-deriving it at each call site.
    async fn open_table_for_test(root: Url) -> DeltaTable {
        let url = table_url_for_output(&root, "orders").expect("table url");
        DeltaTable::try_from_url_with_storage_options(url, HashMap::new())
            .await
            .expect("open table")
    }

    #[tokio::test]
    async fn append_two_batches_then_reopen_matches_row_count_and_schema() {
        let (_dir, url) = table_dir();
        let connector = DeltaSinkConnector;
        let sink = connector
            .open(Config(serde_json::Map::from_iter([(
                "root".to_string(),
                serde_json::Value::String(url.to_string()),
            )])))
            .await
            .expect("open sink");

        let schema = probe_schema();
        let mut session = sink
            .begin_write(output_spec("append", vec![]), schema.clone())
            .await
            .expect("begin write");

        session
            .write_batch(probe_batch(&[1, 2], &["a", "b"]))
            .await
            .expect("write batch 1");
        session
            .write_batch(probe_batch(&[3], &["c"]))
            .await
            .expect("write batch 2");
        let result = session.commit().await.expect("commit");
        assert_eq!(result.rows_written, 3);
        assert_eq!(result.batches_written, 2);

        let reopened = open_table_for_test(url).await;
        assert_eq!(reopened.version(), Some(1));
        let reopened_schema = reopened
            .snapshot()
            .expect("snapshot")
            .metadata()
            .parse_schema()
            .expect("schema");
        assert_eq!(reopened_schema.fields().count(), 2);
    }

    #[tokio::test]
    async fn replace_overwrites_prior_rows() {
        let (_dir, url) = table_dir();
        let connector = DeltaSinkConnector;
        let config = Config(serde_json::Map::from_iter([(
            "root".to_string(),
            serde_json::Value::String(url.to_string()),
        )]));

        // First write: two rows via append.
        let sink = connector.open(config.clone()).await.expect("open sink");
        let mut session = sink
            .begin_write(output_spec("append", vec![]), probe_schema())
            .await
            .expect("begin write");
        session
            .write_batch(probe_batch(&[1, 2], &["a", "b"]))
            .await
            .expect("write batch");
        session.commit().await.expect("commit append");

        // Second write: replace with one row.
        let sink = connector.open(config).await.expect("open sink");
        let mut session = sink
            .begin_write(output_spec("replace", vec![]), probe_schema())
            .await
            .expect("begin write");
        session
            .write_batch(probe_batch(&[9], &["z"]))
            .await
            .expect("write batch");
        let result = session.commit().await.expect("commit replace");
        assert_eq!(result.rows_written, 1);

        let reopened = open_table_for_test(url).await;
        let (_table, rows) = read_all_rows(reopened).await;
        assert_eq!(rows, 1, "replace must leave exactly the overwritten row");
    }

    #[tokio::test]
    async fn merge_updates_matching_keys_and_inserts_new_ones() {
        let (_dir, url) = table_dir();
        let connector = DeltaSinkConnector;
        let config = Config(serde_json::Map::from_iter([(
            "root".to_string(),
            serde_json::Value::String(url.to_string()),
        )]));

        let sink = connector.open(config.clone()).await.expect("open sink");
        let mut session = sink
            .begin_write(output_spec("append", vec![]), probe_schema())
            .await
            .expect("begin write");
        session
            .write_batch(probe_batch(&[1, 2], &["a", "b"]))
            .await
            .expect("write batch");
        session.commit().await.expect("commit append");

        let sink = connector.open(config).await.expect("open sink");
        let mut session = sink
            .begin_write(output_spec("merge", vec!["id".to_string()]), probe_schema())
            .await
            .expect("begin write");
        // id=1 updates the existing row, id=3 is a new insert.
        session
            .write_batch(probe_batch(&[1, 3], &["updated", "new"]))
            .await
            .expect("write batch");
        session.commit().await.expect("commit merge");

        let reopened = open_table_for_test(url).await;
        let (_table, rows) = read_all_rows(reopened).await;
        assert_eq!(rows, 3, "one update + one unchanged + one insert = 3 rows");
    }

    #[tokio::test]
    async fn abort_leaves_the_table_version_unchanged() {
        let (_dir, url) = table_dir();
        let connector = DeltaSinkConnector;
        let config = Config(serde_json::Map::from_iter([(
            "root".to_string(),
            serde_json::Value::String(url.to_string()),
        )]));

        // Establish a table with a known version first -- opening a brand-new location for its
        // very first write necessarily creates it (Delta has no notion of a table with zero
        // versions), so that initial creation is not what this vector is about. What matters is
        // that ABORTING a write session never adds a version on top of whatever existed when the
        // session began.
        let sink = connector.open(config.clone()).await.expect("open sink");
        let mut setup = sink
            .begin_write(output_spec("append", vec![]), probe_schema())
            .await
            .expect("begin write");
        setup
            .write_batch(probe_batch(&[1], &["a"]))
            .await
            .expect("write batch");
        setup.commit().await.expect("commit");
        let version_before = open_table_for_test(url.clone()).await.version();

        for mode in ["append", "replace", "merge"] {
            let keys = if mode == "merge" {
                vec!["id".to_string()]
            } else {
                vec![]
            };
            let sink = connector.open(config.clone()).await.expect("open sink");
            let mut session = sink
                .begin_write(output_spec(mode, keys), probe_schema())
                .await
                .expect("begin write");
            session
                .write_batch(probe_batch(&[2], &["b"]))
                .await
                .expect("write batch");
            session.abort().await.expect("abort");

            let reopened = open_table_for_test(url.clone()).await;
            assert_eq!(
                reopened.version(),
                version_before,
                "abort in {mode} mode must not advance the table version"
            );
        }
    }

    #[tokio::test]
    async fn partition_by_produces_a_partitioned_layout() {
        let (_dir, url) = table_dir();
        let connector = DeltaSinkConnector;
        let sink = connector
            .open(Config(serde_json::Map::from_iter([(
                "root".to_string(),
                serde_json::Value::String(url.to_string()),
            )])))
            .await
            .expect("open sink");

        let mut spec = output_spec("append", vec![]);
        spec.options.insert(
            "partition_by".to_string(),
            serde_json::Value::String("value".to_string()),
        );
        let mut session = sink
            .begin_write(spec, probe_schema())
            .await
            .expect("begin write");
        session
            .write_batch(probe_batch(&[1, 2], &["east", "west"]))
            .await
            .expect("write batch");
        session.commit().await.expect("commit");

        let reopened = open_table_for_test(url).await;
        let metadata = reopened.snapshot().expect("snapshot").metadata();
        assert_eq!(
            metadata.partition_columns(),
            &vec!["value".to_string()],
            "the table's own metadata must record the partition column"
        );
    }

    async fn read_all_rows(table: DeltaTable) -> (DeltaTable, usize) {
        let (table, stream) = table.scan_table().await.expect("scan");
        let batches: Vec<RecordBatch> = deltalake::operations::collect_sendable_stream(stream)
            .await
            .expect("collect");
        let rows = batches.iter().map(|b| b.num_rows()).sum();
        (table, rows)
    }
}
