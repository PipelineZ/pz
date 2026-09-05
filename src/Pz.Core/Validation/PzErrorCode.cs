namespace Pz.Core.Validation;

public static class PzErrorCode
{
    // 01xx load, 02xx semantic, 030x connector host/registry, 031x native path, 032x restore/lock,
    // 033x connectivity, 04xx SQL, 05xx runtime, 051x data checks, 06xx MCP server — the
    // authoring/verification tools exposed over the Model Context Protocol.
    public const string YamlShape = "PZ0101";
    // --vars: raw JSON text failed to parse, or parsed to something other than a JSON object.
    public const string VarsInvalid = "PZ0102";
    public const string UndeclaredEnvVar = "PZ0103";
    public const string TemplateError = "PZ0104";
    public const string DuplicateName = "PZ0110";
    public const string SidecarUnknownPipeline = "PZ0111";
    // The sink-output `input:` field was removed — a pipeline declares its load inline as
    // `INSERT INTO {{ sink('<sink>', '<output>') }}`. A leftover `input:` is rejected at load time so
    // the migration is explicit, not silently ignored.
    public const string RemovedInputField = "PZ0112";
    /// <summary>A sidecar check definition is invalid: unknown type, missing/malformed per-type
    /// options, or unknown option keys.</summary>
    public const string InvalidCheck = "PZ0113";
    public const string InvalidEngineConfig = "PZ0120";
    // A source/sink file's `retry:` block is malformed (shape, bounds, or an unparseable duration).
    // One code for the whole block, mirroring InvalidEngineConfig's granularity.
    public const string RetryConfigInvalid = "PZ0121";
    // A source/sink file's top-level `max_concurrency:` is malformed (not an integer, or < 1). One
    // code for the whole check, mirroring RetryConfigInvalid's granularity.
    public const string ConcurrencyConfigInvalid = "PZ0122";
    // project.yml's top-level `retention:` is malformed -- a scalar outside the off-set
    // (off/false/no), or a map whose keep_last is missing, non-integer, or < 1. One code for the
    // whole block, mirroring InvalidEngineConfig's granularity. keep_last: 0 is deliberately an error
    // and not "sweep everything": automatic retention must never be able to delete the staging DB of
    // the run that just finished (`pz clean --keep-last 0` remains the explicit human act).
    public const string RetentionConfigInvalid = "PZ0123";
    /// <summary>project.yml's `state:` block is malformed, names an unknown backend, or sets
    /// backend-specific keys under <c>backend: local</c> — refused rather than ignored, per the
    /// fail-loudly rule.</summary>
    public const string StateBackendConfigInvalid = "PZ0124";

    /// <summary><c>state.connection</c> names a connection absent from connections.yml or one whose
    /// connector is not sqlserver; or a non-local backend resolved with no credentials from either the
    /// named connection or PZ_STATE_CONNECTION_STRING.</summary>
    public const string StateConnectionInvalid = "PZ0125";
    // project.yml's top-level `on_source_drift:` is not one of ignore/warn/fail. Mirrors
    // RetentionConfigInvalid/InvalidEngineConfig's granularity -- one code for the whole key. The
    // loader still resolves the key to DriftPolicy.Ignore on this path (the aggregate-errors
    // convention: this error never blocks the rest of project.yml from being parsed).
    public const string DriftPolicyInvalid = "PZ0126";
    // `pz schema accept`: a named `<connection>.<entity>` argument does not resolve to a SourceLoad in
    // the latest run carrying a recorded `observed_schema` -- either the name matches no contract-less
    // SourceLoad in the current project's dag at all, or it does but the latest run's node carries no
    // `observed_schema` (the dataset is under `on_source_drift: ignore`, is a `columns:` contract
    // dataset the gate never DESCRIBEs, or -- the known warn+retry-reuse edge -- the latest run reused
    // a previously-staged SourceLoad and therefore carries `Observed = null` for it even though drift
    // was reported on an earlier run). Reuses DriftPolicyInvalid's PZ012x family rather than a new range.
    public const string SchemaAcceptTargetInvalid = "PZ0127";
    public const string InitTargetNotEmpty = "PZ0130"; // `pz init`: target directory exists and is not empty
    public const string InitTemplateUnknown = "PZ0131";   // `pz init --template`: no such template id
    // `pz init`: neither a name nor --list-templates was given, or both were. Two invocation shapes
    // are valid and anything else is this error, so the code covers the whole shape rather than just
    // the missing-name half.
    public const string InitInvocationInvalid = "PZ0132";
    public const string UnresolvedRef = "PZ0201";
    public const string Cycle = "PZ0202";
    // PZ0203 (was SinkInputMissing: a YAML `input:` that matched no pipeline/source dataset) is
    // retired -- YAML `input:` binding was removed entirely (inline `INSERT INTO {{ sink(...) }}` is
    // the sole load path), so the code is unreachable. The number is not reallocated.
    public const string EphemeralChain = "PZ0204";
    public const string ChecksOnEphemeral = "PZ0205";
    // Inline sink binding: an output is bound by exactly one pipeline's sink() call.
    public const string SinkBindingConflict = "PZ0206"; // multiple pipelines claim one output (ERROR)
    // Retired (was: an output declared in a sink's `outputs:` that no pipeline's `INSERT INTO
    // {{ sink(...) }}` targets, a non-blocking orphan-output WARNING). An output now exists precisely
    // because a sink() call site declared it, so it cannot be declared without a writer. The number is
    // not reallocated.
    public const string SinkOutputUnbound = "PZ0207";
    public const string InvalidSinkCall = "PZ0208";     // malformed sink() call (see DagCompiler)
    // PZ0210 is already taken by SelectorNoMatch below -- skipped, not renumbered.
    public const string MergeRequiresKeys = "PZ0209";   // `mode: merge` output declares no keys:
    public const string SelectorNoMatch = "PZ0210";
    public const string KeysWithoutMerge = "PZ0211";    // non-merge output declares keys:
    public const string CursorInvalid = "PZ0212";       // incremental cursor missing from, or mistyped in, a declared columns: contract
    // A dataset's bounded-window config (max_window/initial/until) violates a semantic rule — missing
    // initial, undeclared cursor type, form/type mismatch, until <= initial, or query-mode. One code
    // for the family, mirroring CursorInvalid's granularity.
    public const string WindowConfigInvalid = "PZ0213";
    // An incremental dataset structurally feeds a mode: append sink output that has not declared
    // `accept_duplicates: true` — append delivery is at-least-once (duplicate windows: `pz run` after
    // a partial failure, a failed watermark persist, a partial --select, a reuse-fallback retry), so
    // the combination requires recorded consent.
    public const string IncrementalAppendUnacknowledged = "PZ0214";
    // Bare `pz run` on a project whose DAG has 2+ disjoint connected components ("independent flows")
    // — running everything must be said out loud (`--all`, a flow name, or --select) once a project
    // holds more than one flow.
    public const string MultiFlowNeedsSelection = "PZ0215";
    // Positional flow names, --select, and --all are mutually exclusive selection mechanisms on
    // `pz run`/`pz plan`.
    public const string SelectionConflict = "PZ0216";
    // DagCompiler validation of date-templated dataset paths: `path` has date tokens
    // ({yyyy}/{MM}/{dd}/{HH}/{mm}) but the dataset has no `incremental.cursor`, or the declared cursor
    // type is not date/timestamp -- there is nothing to substitute the tokens from at extraction time.
    public const string TemplatedPathCursorInvalid = "PZ0217";
    // The path's token sequence itself is malformed (unknown token, or not a contiguous coarse->fine
    // run) -- delegates to PathTemplate.Validate, re-raised here as an aggregated compile error naming
    // the source file/dataset.
    public const string TemplatedPathTokensInvalid = "PZ0218";
    // DagCompiler validation of a partitioned sink output's DECLARATION: `partition_by:` must name a
    // column or a list of columns, and must agree with `path:`. Calendar tokens in the path are pz's own
    // layout rule -- one timestamp column rendered into a folder -- so tokens with no partition_by have
    // no column to substitute from, and tokens with several columns have no way to choose. partition_by
    // WITHOUT tokens is not refused here: a format that records its own partitioning (Delta, Iceberg,
    // Hive-layout parquet) is correct as written, and whether the connector can honour it is PZ0314's
    // capability question, which needs the connector instance the compiler does not have. Whether a
    // named column EXISTS is likewise not checked here -- OutputDef carries no declared column set at
    // compile time, so that is deferred to the runtime write session; PZ0220 is reserved for it and
    // intentionally unclaimed here.
    public const string PartitionedOutputConfigInvalid = "PZ0219";
    // A date-templated `path` whose dataset does not declare a bounded window (`initial:` +
    // `max_window:`, mirroring PZ0213's notion of "bounded") -- cover pruning needs both watermark
    // bounds stamped on every run, including the first, which only a bounded window guarantees. Only
    // fires once the cursor is confirmed date/timestamp (see TemplatedPathCursorInvalid) -- one root
    // cause, one error.
    public const string TemplatedPathWindowRequired = "PZ0221";
    // A source dataset's `files_per_partition` option (coalesces N consecutive matched blobs into one
    // partition, amortizing per-file overhead for many-tiny-files Azure datasets) must be a positive
    // integer when present -- a non-integer value or one <= 0 is rejected here so the connector never
    // has to guess a fallback at run time.
    public const string FilesPerPartitionInvalid = "PZ0222";
    // A non-ephemeral pipeline that neither loads to a sink (no INSERT INTO) nor is consumed by any
    // ref() -- computes a result nothing uses. A WARNING, not an error: a legitimate work-in-progress
    // state (inspecting intermediate data). See PzWarning.
    public const string DeadLeafPipeline = "PZ0223";
    // Watermark expressions in pipeline SQL. A pipeline's `watermark(source, dataset)`
    // comparison could not be recognized, or resolved to a table that doesn't trace to the claimed
    // dataset (e.g. a ref()'d pipeline table), or the cursor type could not be determined from the
    // source's declared columns contract, or another structural violation. The error message names the
    // pipeline file/reason; next step is to check the shape against the one recognized pattern
    // (<cursor column> > or >= <expression containing watermark()>).
    public const string UnrecognizedWatermarkExpression = "PZ0224";
    // WatermarkInference's per-dataset fold: the dataset has BOTH a YAML `incremental:`
    // block AND a SQL watermark() declaration ("either/or"; a windowed YAML incremental, i.e.
    // `max_window` non-null, always also has a non-null `incremental:`, so it hits this case too --
    // its own more specific "windowed backfill is YAML-only" message takes precedence, still just one
    // PZ0225). Fires only after UnrecognizedWatermarkExpression passes (PZ0224 doesn't block further
    // analysis). Recognized comparisons disagreeing on the cursor column ACROSS pipelines is not
    // represented here: PZ0349 refuses a source read by more than one pipeline.
    public const string ConflictingIncrementalDeclaration = "PZ0225";
    // PZ0226 (was InconsistentIncrementalConsumers) is retired: the condition needed two readers of
    // one dataset, which PZ0349 now refuses outright. The number is not reallocated.

    // A pipeline's watermark() call names a source/dataset (e.g. watermark('crm','orders')),
    // and that dataset DECLARES a columns: contract, but the cursor column does not appear in it, or its
    // declared type falls outside the allowed set (int/bigint/decimal/date/timestamp). A contract prunes
    // reads to exactly its columns, so a cursor outside it would never be extracted. A dataset with NO
    // contract is legal -- its cursor type comes from the stored watermark at run time. Fires after
    // PZ0224 and PZ0225 pass (both check structural correctness ahead of the contract check).
    public const string WatermarkCursorUndeclared = "PZ0227";
    // A source dataset is read by exactly one pipeline. Sharing is expressed by reading it once and
    // ref()-ing that pipeline, so the shared relation is a file the author wrote and can open. With one
    // reader, what a pipeline's SQL asks for IS what pz extracts, which is what makes ReadHints
    // projection/predicate pushdown decidable without cross-file inference. Raised in DagCompiler
    // stage 6, BEFORE watermark inference, so PZ0225's cross-pipeline arm is unreachable rather than
    // merely redundant. Message lists every reading file sorted ordinally and shows the staging pipeline
    // to add. Counts pipelines, not references -- a self-join is one reader.
    public const string SourceReadByMultiplePipelines = "PZ0349";
    // A watermark() comparison declares an UPPER bound on the cursor with no lower bound anywhere for
    // that dataset. A ceiling alone is a filter, not an increment -- the first run would advance the
    // watermark straight to it and every later run would extract nothing. A cursor filter with no
    // resume semantics belongs in the WHERE without a watermark() call, where it rides ReadHints as an
    // ordinary predicate.
    public const string WatermarkCeilingWithoutFloor = "PZ0351";
    // Retired (was: a sink output's `mode:` outside append/replace/merge). Write strategy is a sink()
    // keyword argument now, and SinkFunction refuses one outside replace/append/merge at the call site
    // (PZ0334), so no OutputDef can reach DagCompiler carrying an unknown mode. Whether the target
    // connector supports a KNOWN mode is still the planner's PZ0324. The number is not reallocated.
    public const string WriteModeUnknown = "PZ0228";
    // `incremental:` + `cursor_order: desc` + `max_pages` -- a truncated descending crawl would advance
    // the MAX(cursor) watermark past unfetched rows, and no safe resume exists (advancing skips them;
    // not advancing re-fetches the same head forever). Refused at compile when declared; HttpPartition's
    // runtime guard covers the undeclared case.
    public const string DescendingCursorTruncatable = "PZ0229";
    public const string ConnectorConfigInvalid = "PZ0301";
    public const string ConnectorPackageMissing = "PZ0304";
    public const string ConnectorNotInstalled = "PZ0305";
    public const string ProtocolMismatch = "PZ0306";
    public const string NoConnectorEntryPoint = "PZ0307";
    public const string NativeSetupFailed = "PZ0311";
    public const string NativePathRequired = "PZ0312";
    // A dataset declares max_window but its connector does not declare
    // ConnectorCapabilities.BoundedWindow — raised by the planner (never mid-run), because a connector
    // that ignores the bound silently extracts everything the window exists to prevent.
    public const string WindowCapabilityMissing = "PZ0313";
    // A dataset's `path` has date tokens, or a sink output declares `partition_by`, but the connector
    // does not declare the capability that would honour it -- raised by the planner (never mid-run),
    // mirroring WindowCapabilityMissing (PZ0313): DagCompiler's PZ0217/0218/0219/0221 already validated
    // the DECLARATION connector-agnostically, but only the planner has the connector instance and its
    // declared Capabilities, so this refusal must live here, not in DagCompiler. Which capability is
    // required follows from `path:`, because that is what says who owns the layout: calendar tokens mean
    // pz renders it (PathTemplating), no tokens mean the destination records its own partitioning
    // (ColumnPartitionedWrites).
    public const string TemplatingCapabilityMissing = "PZ0314";
    // Two resume mechanisms declared for one dataset. Guards two conflicts the unified `sync:` block
    // does not rule out:
    //   1. WatermarkInference (see WatermarkInference.cs ~184): a SQL `watermark()` declaration targeting a
    //      dataset that also declares `sync: {mode: auto}` -- ordered cursor (SQL) vs. opaque token (YAML).
    //   2. ExecutionPlanner's per-node guard (feed-natural conflict): a cursor-incremental dataset (YAML
    //      `sync: {mode: incremental}` or SQL watermark()) on a connector that manages its OWN change feed
    //      for it (INaturalReadShapeSource resolves Feed, e.g. http `delta_pointer`) -- the ordered cursor
    //      and the connector's opaque token both claim to resume the read.
    public const string SyncStateConflict = "PZ0315";
    // A `sync:` dataset on a connector that declares ConnectorCapabilities.PartitionedRead -- raised by
    // the planner (never mid-run, mirroring PZ0313/PZ0314), because a single opaque continuation token
    // cannot reconcile across N independent partition reads.
    public const string SyncPartitionedReadConflict = "PZ0316";
    // An instance declares rate_limit: but its connector does not declare
    // ConnectorCapabilities.GatedOperations -- raised by the planner (never mid-run, mirroring
    // PZ0313/PZ0314), because a connector that ignores the gate silently never paces.
    public const string PacingUnsupported = "PZ0317";
    // rate_limit: block is malformed, out of bounds, or declared at dataset/output level (it is
    // instance-level only).
    public const string RateLimitConfigInvalid = "PZ0318";
    // A connector declared StablePartitionIds but a planned partition lacks IIdentifiedPartition / has
    // an empty or duplicate id (run-time, returned NodeResult), or declared CheckpointableReads without
    // StablePartitionIds (planner-time, aggregated).
    public const string PartitionIdentityInvalid = "PZ0319";
    public const string RestoreFailed = "PZ0320";
    public const string LockDrift = "PZ0321";
    public const string LockMissing = "PZ0322";
    // NuGetResolver.ParseRequirementRange's floating-version-range rejection.
    public const string FloatingVersionRejected = "PZ0323";

    // A sink output's KNOWN mode (PZ0228 already killed unknown strings at compile time) is not
    // supported by the target connector -- merge without the Merge capability, or replace without
    // ReplaceWrites. Raised by the planner (never mid-run, mirroring PZ0313/PZ0314/PZ0317) rather than
    // from BeginWriteAsync at run time.
    public const string WriteModeUnsupported = "PZ0324";
    // Two resolved packages provide the same lib/ or native/ file name, and both must be flattened into
    // one connector package's directory for its ALC to probe them. Raised by PackageMaterializer rather
    // than resolved by picking a winner: the flattening is a plain copy, so a "winner" is whichever
    // package enumeration reached last.
    public const string PackageAssetCollision = "PZ0325";
    // NOTE: Pz.PackageManagement cannot reference Pz.Core (see RestoreException's doc comment), so every
    // PZ032x code above is ALSO duplicated as a bare string literal over there (NuGetResolver.cs,
    // RestoreException.cs, DriftChecker.cs comments). This registry is the source of truth for the
    // *value*, but nothing enforces the two staying in sync -- that drift is how PZ0322 came to be used
    // for two unrelated errors. Changing a PZ032x value here means grepping PackageManagement for the
    // old literal too.
    public const string ConnectionCheckFailed = "PZ0330";
    public const string SchemaDrift = "PZ0331";
    // A dataset declares the old top-level `incremental:` block, which the unified
    // `sync: { mode: incremental }` block replaced. The message carries the exact rewrite (the old
    // cursor value, interpolated when parseable) so the migration is copy-pasteable, not just "removed".
    public const string RetiredReadSurface = "PZ0332";
    // A sink output declares the old `mode:`/`keys:`/`accept_duplicates:` surface directly instead of
    // the unified `write:` block.
    public const string RetiredWriteSurface = "PZ0333";
    // A dataset's `sync:` block is malformed -- not a mapping, missing/unknown `mode`,
    // `mode: incremental` missing `cursor`, `mode: cdc` missing/malformed cdc-specific sub-keys, or an
    // unknown sub-key under the resolved mode. One code for the whole block, mirroring
    // RateLimitConfigInvalid's granularity.
    public const string SyncModeInvalid = "PZ0334";
    // A resolved read shape paired with a `write:` strategy the compile-time pairing matrix refuses.
    // Currently: an explicitly declared `sync: {mode: incremental}` dataset feeding
    // `write.strategy: replace` (DagCompiler stage 10b) -- discards previously loaded rows every run,
    // defeating the point of an ordered-cursor read.
    public const string IncompatiblePair = "PZ0335";
    // A cdc-fed `write.strategy: merge` output has not declared `write.on_delete` -- deletes on the
    // source table need an explicit routing choice (delete/soft/ignore); there is no safe default.
    // Raised by DagCompiler.
    public const string CdcConsentMissing = "PZ0336";
    // `write.on_delete` declared on an output that is not cdc-fed (no upstream `sync: {mode: cdc}`
    // dataset reaches it), or `on_delete: delete`/`soft` where the delete keys cannot be routed (a
    // multi-source upstream pipeline). Raised by DagCompiler.
    public const string CdcDeleteRouteInvalid = "PZ0337";
    // A `sync: {mode: cdc}` dataset's source connector does not declare
    // ConnectorCapabilities.ChangeCapture -- mirrors WindowCapabilityMissing/TemplatingCapabilityMissing
    // (PZ0313/PZ0314): the loader/DagCompiler validated the declaration syntax connector-agnostically,
    // but only the planner has the connector instance and its declared Capabilities. Two raise sites:
    // ExecutionPlanner (plan-time capability refusal, before any node runs) and, reusing the same code,
    // SourceLoadExecutor.CollapseCdcAsync at runtime (the landed _pz_op/_pz_lsn change-row contract, or
    // the connector's IChangeCapturePartition key-column reporting, is violated -- a landing-contract
    // failure the planner cannot see ahead of time).
    public const string ChangeCaptureUnsupported = "PZ0338";
    // An `on_delete: delete`/`soft` output's sink connector does not declare
    // ConnectorCapabilities.ApplyDeletes (`on_delete: ignore` needs no capability). Raised by
    // ExecutionPlanner, mirroring ChangeCaptureUnsupported/PZ0338.
    public const string DeleteApplyUnsupported = "PZ0339";
    // At drain time, a cdc-fed merge output's declared merge `keys:` are missing from, or null in some
    // row of, the deletes relation -- there is nothing to route the delete by. Raised by
    // SinkWriteExecutor as a NodeResult failure, before the upsert drain starts.
    public const string CdcDeleteKeysUnavailable = "PZ0340";

    // The sink `outputs:` block is retired -- every write option is a sink() keyword argument now, so
    // the option and the pipeline that writes it are visible in one file rather than two. Loud and
    // PZ-coded, following the PZ0332/PZ0333 precedent; the hint reconstructs the exact call the retired
    // block was equivalent to.
    public const string RetiredOutputsBlock = "PZ0347";

    // An entity name that cannot name anything -- empty, an empty dotted segment, or embedded
    // whitespace. Structural only, and deliberately connector-agnostic: see Pz.Core.Model.EntityName.
    public const string EntityNameInvalid = "PZ0344";

    // `schema:`/`table:` are retired on BOTH sides -- the entity name carries its own qualification, so
    // the dataset key and sink()'s argument 2 are the single place an object is named. Loud and
    // PZ-coded, following the PZ0332/PZ0333/PZ0347 precedent.
    public const string RetiredEntityQualifier = "PZ0348";

    // An entity-side's options are declared in connections.yml AND at the call site. There is
    // deliberately no precedence rule to fall back on -- the whole point of two surfaces is that a
    // reader of one file sees the whole story for that entity-side.
    public const string WriteSurfaceSplit = "PZ0341";

    // PZ0343 is retired before use: it would have refused two pipelines passing call-site write options
    // for one entity, but PZ0206 (SinkBindingConflict) already refuses two pipelines claiming one output
    // at all, whatever options each passes, so the condition cannot occur. The number is not reallocated.

    // A connector's ConnectionConfigSchema declares a property pz owns at connection level
    // (connector/entities/max_concurrency/rate_limit/retry). It could never receive that key, so the
    // connector is refused rather than silently starved of it.
    public const string ReservedConnectionKey = "PZ0345";

    // sources/ or sinks/ still present. One error per file, whose hint reconstructs that file as the
    // connections.yml block it becomes -- this is the first error such a project hits, so it carries the
    // whole migration for that file.
    public const string RetiredConnectionDirectory = "PZ0346";

    // project.yml declared `feeds:`, which was removed -- feeds are host configuration
    // (PZ_FEEDS / --feeds), not pipeline authoring. Loud and PZ-coded, following the PZ0346/PZ0348
    // retired-surface precedent.
    public const string FeedsRemoved = "PZ0352";

    /// <summary>A connector refused to build a native fragment because the config it was handed cannot
    /// produce one. Two shapes, one code, both directions: a declared <c>columns:</c> contract inconsistent
    /// with the file read (the localfiles csv positional-binding guard in <c>CsvSource.TryGetNativeScan</c>:
    /// <c>read_csv(..., columns = {...})</c> binds the contract to the file BY POSITION, so a declared order
    /// that disagrees with the actual header would silently load each column's values under a different
    /// name), and an entity or file the connector cannot address at all (<c>DuckDbSql.SplitEntity</c>'s
    /// three-part name; a duckdb read of a database file that does not exist). The connector reports it as a
    /// <c>PzConnectorException</c> out of <c>TryGetNativeScan</c>/<c>TryGetNativeCopy</c>; the planner
    /// catches it on both sides and attaches this code so it surfaces as an aggregated, exit-2 config error
    /// rather than an unexpected engine failure. Raised only for a node the run will execute: on a node
    /// outside the run's selection (plus ancestors) the refusal is recorded in the plan's reason and the
    /// run proceeds, so a same-project flow that writes the file can run before the flow that reads it.</summary>
    public const string NativePathContractMismatch = "PZ0353";

    /// <summary>A process-hosted connector's <c>pz.connector.json</c> (or the host's own check of it)
    /// leaves this host with no usable entrypoint to spawn: an unrecognized <c>runtime</c> value, a
    /// <c>runtime: "process"</c> manifest with no <c>entrypoints</c> or none for the current RID (even
    /// after the RuntimeIdentifierGraph fallback walk), an entrypoint path the manifest names but that
    /// does not exist on disk, a manifest whose declared <c>runtime</c> disagrees with the out-of-process
    /// host that is trying to load it, or a <c>runtime: "process"</c> manifest with no <c>name</c>.
    /// Raised both by Pz.PackageManagement's <c>ManifestReader</c> (as a hardcoded literal — that
    /// assembly must not reference Pz.Core) and by
    /// <c>ProcessConnectorHost</c> itself; pinned by HostErrorCodeTests.</summary>
    public const string ProcessEntrypointMissing = "PZ0354";

    /// <summary>Spawn of a process-hosted connector's executable failed at launch time (exec error,
    /// OSError, process did not start). Raised by the host spawn path; next step is to check the
    /// executable exists, is readable, and matches the target platform. Also raised earlier, before any
    /// spawn is attempted, when the temp directory a runless verb would host a control socket under
    /// (<c>ProcessSocketRoot</c>'s fallback) is itself too deep to leave room for the socket path -- the
    /// same "nothing a user could act on" failure a bind error from inside the child would otherwise be,
    /// but caught at the one point that can still name the cause (point TMPDIR/TEMP at a shorter
    /// directory).</summary>
    public const string ConnectorSpawnFailed = "PZ0355";

    /// <summary>Handshake between host and a process-hosted connector failed: timeout waiting for the
    /// Hello message, malformed Hello body (not JSON or wrong schema), or mismatch between manifest
    /// (<c>ConnectorManifest</c>) and Hello (<c>ConnectorInfo</c>) — capability flag disagreement, or name
    /// disagreement. Raised by the host handshake phase; next step depends on which part failed (check
    /// logs, connector readiness, and manifest/compiled capability consistency).</summary>
    public const string ConnectorHandshakeFailed = "PZ0356";

    /// <summary>Protocol violation from a process-hosted connector during data-plane operations: bad or
    /// reused data-plane ticket issued by the planner, or malformed Arrow IPC stream from the connector's
    /// WriteBatchAsync reply. Raised at data-plane read/write time; next step is to check connector logs
    /// and confirm connector and host ABI versions are compatible.</summary>
    public const string ProtocolViolation = "PZ0357";

    /// <summary>A process-hosted connector's executable died unexpectedly during an operation that was in
    /// flight. Raised by the host when an I/O or serialization failure reveals the process exited; next
    /// step is to check the connector's exit code and stderr logs, and confirm connector stability under
    /// the dataset being processed.</summary>
    public const string ConnectorDiedMidOperation = "PZ0358";

    /// <summary>Planner refused the native-scan tier for a dataset because a packaged DuckDB extension
    /// (loaded into DuckDB for native scans) is unsigned and the source connection does not declare
    /// <c>allow_unsigned_extensions: true</c>. Unsigned extensions are inherently risky (no signature
    /// verification); the explicit allow gates them. Raised by ExecutionPlanner; next step is to sign the
    /// extension, or add <c>allow_unsigned_extensions: true</c> to the source connection's YAML
    /// config.</summary>
    public const string UnsignedExtensionRefused = "PZ0359";

    /// <summary>An external (non-builtin) connector package declares runtime <c>"dotnet"</c> (or ships
    /// no manifest, which means the same) — external connectors are hosted out of process only. In-proc
    /// loading would run third-party code with the engine's full privileges (every connection's
    /// credentials, the state store, the staging DB); the process host is the trust and crash boundary.
    /// Raised when the connector registry is built, aggregated over every offending package; next step
    /// is to use a connector published as a <c>runtime: "process"</c> (PCP) package, or a builtin.</summary>
    public const string ExternalConnectorNotOutOfProcess = "PZ0360";

    /// <summary>A file-place connector (localfiles, s3, gcs, azureblob, sftp) was asked for a
    /// <c>format:</c> it does not support in that direction or on that tier: an unknown format name, a
    /// read-only format (avro) on a sink, a native-only format (xlsx, json <c>layout: array</c>) on a
    /// managed reader/writer such as sftp, or a multi-file read of a single-workbook format. The message
    /// names the dataset/output and the supported set; next step is to pick a supported format or a
    /// connector with a native tier.</summary>
    public const string FileFormatUnsupported = "PZ0361";

    /// <summary>A format-scoped option (<c>delimiter</c>, <c>layout</c>, <c>sheet</c>, <c>header</c>) is
    /// invalid: declared on an entity of a format that does not admit it, or carrying a bad value (a
    /// multi-character delimiter, an unknown <c>layout</c>). Next step is to remove the option or fix its
    /// value; the message names the option, the format and the entity.</summary>
    public const string FileFormatOptionInvalid = "PZ0362";

    public const string SqlDryCompile = "PZ0401";
    public const string UnexpectedEngineFailure = "PZ0500";
    public const string NodeFailed = "PZ0501";
    public const string NoPriorRun = "PZ0502"; // `pz retry` found no readable prior run to resume from
    public const string PriorRunIncomplete = "PZ0503"; // `pz retry`'s prior run snapshot is still "running" (crashed mid-run)
    public const string PriorRunFatal = "PZ0504"; // `pz retry`'s prior run ended in an orchestrator-level "fatal"
    // A source dataset's incremental cursor column landed with a DuckDB type outside
    // int/bigint/decimal/date/timestamp -- discovered only at extraction time (no declared columns:
    // contract to catch it at compile time via PZ0212).
    public const string UnsupportedCursorType = "PZ0505";
    // A node's owning source/sink instance CircuitBreaker is Open (or stayed Open through the executor
    // gate's bounded MaxOpenWaits give-up) -- the connector was never invoked for this attempt.
    public const string BreakerOpen = "PZ0506";
    // `pz cdc drop` requires exactly one <source>.<dataset> target argument -- missing, extra, or
    // malformed (not exactly one '.').
    public const string CdcTargetInvalid = "PZ0507";
    // `pz cdc drop`'s target does not resolve to a source/dataset the project declares, or resolves to
    // one that is not a `sync: { mode: cdc }` dataset.
    public const string CdcTargetNotFound = "PZ0508";
    public const string CheckFailed = "PZ0510";
    // `pz clean`: --keep-last and --older-than are the two ways to pick which runs are candidates, and
    // exactly one may be given. There is deliberately no "keep N and anything newer than D"
    // combination -- one selector, one mental model.
    public const string CleanSelectorConflict = "PZ0511";
    // `pz clean`: a selector argument is unusable -- --older-than unparseable or non-positive, or
    // --keep-last negative. One code covers both because the user's next step is identical (fix the
    // argument), which is the test the error philosophy applies.
    public const string CleanSelectorInvalid = "PZ0512";

    // `pz state`: the key names no stored watermark, so there is nothing to show or change -- and the
    // next run already extracts that dataset in full. Names `pz state show` as the way to list what
    // exists, and `pz cdc drop` when the key has sync state instead.
    public const string StateKeyNotFound = "PZ0513";
    // `pz state rollback --to-run` names a run that recorded no usable watermark for this key --
    // artifacts purged, the run never touched the dataset, it tracked a different cursor column, or the
    // key resolved ambiguously against the recorded node names.
    public const string StateRollbackTargetInvalid = "PZ0514";
    // The requested value cannot be used for this operation -- unparseable for the stored cursor type, a
    // rollback that would move the watermark FORWARD, or a stored type pz has no arithmetic for. Three
    // causes with three different next steps, deliberately collapsed into one code (a conscious
    // exception to PZ0512's stricter identical-next-step grouping); the message always names the next
    // step for the case that fired.
    public const string StateValueInvalid = "PZ0515";
    // The invocation is unusable -- the subcommand's required flag (--to-run, --value) is missing, no
    // key was given, or stdout is not a TTY and --yes was not passed.
    public const string StateArgumentInvalid = "PZ0516";
    // A run holds its RunDirLock, so its watermark advancement would silently overwrite this edit
    // (KeyedJsonStateStore.Set is a read-modify-write with no compare-and-swap). Names the live run id.
    public const string StateRunInFlight = "PZ0517";
    /// <summary>The configured state backend could not be reached, or authentication failed. Names the
    /// server and database, never the credential.</summary>
    public const string StateStoreUnavailable = "PZ0518";

    /// <summary>The store's schema_version is NEWER than this build understands (a newer pz elsewhere
    /// may depend on columns this one does not know), or a forward migration failed partway.</summary>
    public const string StateSchemaVersionMismatch = "PZ0519";

    /// <summary>A keyed-state write lost its optimistic-concurrency check — another run advanced the
    /// same dataset concurrently.</summary>
    public const string StateConcurrencyConflict = "PZ0520";

    /// <summary>A <c>strategy: merge</c> output's staged input has NULL values in one or more declared
    /// merge <c>keys:</c>. A merge cannot match a NULL key (ON CONFLICT / MERGE ... ON never join on
    /// NULL), so such rows collapse within the batch (only one survives the key-dedup) AND re-insert on
    /// every run — silent data loss plus unbounded duplication, violating the merge = effectively-once
    /// delivery guarantee. Raised by SinkWriteExecutor as a NodeResult failure, before any session opens,
    /// mirroring the CDC-deletes <see cref="CdcDeleteKeysUnavailable"/> (PZ0340) guard.</summary>
    public const string MergeKeyNull = "PZ0521";

    /// <summary>A <c>strategy: merge</c> output's staged input holds duplicate merge-key groups, which
    /// collapse to one connector-determined survivor (physical staging order, not cursor order — the
    /// sink ABI's documented Absorb contract). A WARNING code, never a failure: event-log-shaped inputs
    /// legitimately carry duplicate keys; the fix when order matters is a deterministic dedup in the
    /// pipeline. Carried by the <c>merge_key_duplicates_detected</c> run event's MCP-envelope warning
    /// projection.</summary>
    public const string MergeKeyDuplicates = "PZ0522";

    /// <summary>A contract-less csv/json read's DuckDB-auto-detected DOUBLE column holds only whole
    /// numbers with at least one beyond 2^53 — the shape auto-detect produces from a &gt;int64 integer
    /// column, where digits may already have been silently lost. A WARNING code, never a failure:
    /// genuinely floating-point data can look integral. The remedy is a <c>columns:</c> contract
    /// (<c>bigint</c>/<c>ubigint</c>/<c>hugeint</c>), which loads such values losslessly and fails loudly
    /// on overflow. Carried by the <c>lossy_integer_inference_detected</c> run event's MCP-envelope
    /// warning projection.</summary>
    public const string LossyIntegerInference = "PZ0523";

    /// <summary>A contract-less csv read's sniffed date/timestamp format is a day-first/month-first
    /// family and no value's day exceeds 12 — every value was ambiguous, so the field-order pick was a
    /// guess and a month-first source is misread on every row. A WARNING code, never a failure: the data
    /// may genuinely be day-first. Remedy: normalize the source to ISO 8601, or declare the column
    /// <c>varchar</c> and parse it explicitly in SQL. Carried by the
    /// <c>ambiguous_date_inference_detected</c> run event's MCP-envelope warning projection.</summary>
    public const string AmbiguousDateInference = "PZ0524";

    /// <summary>An authoring tool's connection-config value looks like a literal credential (a
    /// password/token/key typed directly into YAML) rather than an env var reference (`${VAR}`) --
    /// refused rather than written, so a generated connections.yml never carries a secret in
    /// plaintext.</summary>
    public const string McpLiteralCredential = "PZ0601";

    /// <summary>A YamlSurgeon mutation's target is inconsistent with the requested operation --
    /// InsertMappingEntry's key already exists, or ReplaceMappingEntry's/RemoveMappingEntry's key does
    /// not.</summary>
    public const string McpMutationTarget = "PZ0602";

    /// <summary>An MCP <c>pz_init</c>-style scaffolding tool's target directory exists and is not empty
    /// -- mirrors the CLI's <see cref="InitTargetNotEmpty"/> (PZ0130) for the MCP surface.</summary>
    public const string McpInitDirNotEmpty = "PZ0603";

    /// <summary>An MCP tool that would run the project (or otherwise needs the run lock) found another
    /// run already holding it -- refused rather than contending, since an agent-driven run has no
    /// interactive operator to arbitrate a conflict.</summary>
    public const string McpRunLockHeld = "PZ0604";

    /// <summary><c>pz mcp init</c>'s client-setup surface is invalid -- either an existing client config
    /// file (`.vscode/mcp.json`, `.mcp.json`, `~/.copilot/mcp-config.json`, `opencode.json`) fails to
    /// parse as JSON, so pz refuses to overwrite it and leaves it byte-untouched, or the invocation
    /// itself named no client and no `--all`, or named a client outside
    /// `vscode`/`claude-code`/`copilot-cli`/`opencode` -- explicit over implicit, the same posture as
    /// <see cref="MultiFlowNeedsSelection"/> (PZ0215).</summary>
    public const string McpClientConfigInvalid = "PZ0605";

    /// <summary>Under `pz mcp`, a localfiles <c>path:</c>/<c>root:</c>/<c>base_dir:</c> resolving outside
    /// the project directory — refused uniformly (verify, execute, introspect, and proposed authoring
    /// blocks), matching the posture PZ0602 takes for <c>../</c> in mutation targets. The plain CLI stays
    /// paths-are-trusted; only the agent surface refuses.</summary>
    public const string McpPathEscapesProject = "PZ0606";

    /// <summary>Under `pz mcp`, the documentation tools could not reach the documentation site. The
    /// docs are published rather than shipped inside the tool, so these three tools — and only these
    /// three — need network access; every other tool keeps working offline. Reported as a real error
    /// naming the URL, never as an empty result, so a wrong or unreachable <c>PZ_DOCS_URL</c> mirror
    /// is diagnosable instead of looking like documentation that simply has no match.</summary>
    public const string McpDocsUnavailable = "PZ0607";

    /// <summary>Under `pz mcp`, a documentation request the catalog cannot answer as asked: a slug no
    /// published page carries, or an empty search query. Deliberately distinct from PZ0607 — "the site
    /// is unreachable" and "you asked for a page that does not exist" need different fixes, and
    /// collapsing them would send a caller checking its network over a typo.</summary>
    public const string McpDocsRequestInvalid = "PZ0608";

    /// <summary>Under `pz mcp`, a tool handler failed with an exception no handler-level catch
    /// classified. Never a diagnosis — it is the backstop that keeps the "no silent failures" rule
    /// true across the MCP boundary: without it the SDK answers its own "An error occurred invoking
    /// '&lt;tool&gt;'." with the exception text discarded, leaving an agent nothing to act on and no
    /// server-side trace either. A PZ0609 in the wild means a real handler is missing a typed catch;
    /// the exception text it carries is what identifies which.</summary>
    public const string McpToolFailed = "PZ0609";
}
