namespace Pz.Connectors.Abstractions;

/// <summary>Identity a connector reports about itself. <paramref name="ProtocolMajor"/> must equal
/// <see cref="ProtocolVersion.Major"/> for the host to load it.</summary>
public sealed record ConnectorInfo(string Name, string Version, int ProtocolMajor);

/// <summary>An opaque bag of configuration values (from YAML), already env-interpolated by the host.</summary>
public sealed record ConnectorConfig(IReadOnlyDictionary<string, object?> Values)
{
    public static readonly ConnectorConfig Empty = new(new Dictionary<string, object?>());
    public string? GetString(string key) => Values.TryGetValue(key, out var v) ? v?.ToString() : null;
    public long? GetInt(string key) => Values.TryGetValue(key, out var v) && v is not null
        ? Convert.ToInt64(v, System.Globalization.CultureInfo.InvariantCulture) : null;
    public bool GetBool(string key, bool defaultValue = false) => Values.TryGetValue(key, out var v) && v is not null
        ? Convert.ToBoolean(v, System.Globalization.CultureInfo.InvariantCulture) : defaultValue;
}

/// <summary>Names one dataset of one source plus its dataset-level options.</summary>
public sealed record DatasetSpec(string Source, string Dataset, IReadOnlyDictionary<string, object?> Options)
{
    /// <summary>When set alongside <see cref="WatermarkValue"/>, connectors MAY apply
    /// <c>cursor &gt; value</c> during extraction; ignoring it is always correct — merge dedups.
    /// A connector MUST gate the predicate on <see cref="WatermarkValue"/> being non-null too, not on
    /// this field alone: <c>SpecBuilder</c> stamps this field with the declared cursor NAME on every
    /// incremental dataset's spec — including a first run / <c>--full-refresh</c>, and the planner's
    /// planning-time probe — while <see cref="WatermarkValue"/> stays null until a watermark is
    /// actually known; a cursor-set/value-null spec is equivalent to no watermark at all.</summary>
    public string? WatermarkCursor { get; init; }

    /// <summary>The cursor value paired with <see cref="WatermarkCursor"/>, in the same canonical string
    /// form <c>Pz.Engine.State.Watermark</c> stores it in (int/bigint/decimal as plain digits, date as
    /// <c>yyyy-MM-dd</c>, timestamp as <c>yyyy-MM-ddTHH:mm:ss.ffffff</c>).</summary>
    public string? WatermarkValue { get; init; }

    /// <summary>When set (only ever alongside
    /// <see cref="WatermarkCursor"/>/<see cref="WatermarkValue"/>), a <see cref="ConnectorCapabilities.BoundedWindow"/>-
    /// declaring source MUST apply <c>cursor &lt;= value</c> during extraction (same canonical string
    /// form as <see cref="WatermarkValue"/>). Additive and execution-only, exactly like its two siblings.
    /// The engine additionally caps watermark advancement at this bound, so an over-extracting connector
    /// can never advance the cursor past the window.</summary>
    public string? WatermarkUpperBound { get; init; }

    /// <summary>When true (only ever alongside WatermarkCursor/WatermarkValue), the lower
    /// bound is inclusive — a ConnectorCapabilities.InclusiveWatermarkBound source applies
    /// <c>cursor >= value</c> instead of <c>cursor > value</c>. The engine never sets this for a connector
    /// without the capability (it pushes no bound instead — safe over-extraction; the pipeline
    /// predicate cuts), so ignoring it can never under-extract.</summary>
    public bool WatermarkLowerInclusive { get; init; }

    /// <summary>The opaque sync-state token the engine stored after the previous successful run of a
    /// `sync:` dataset (Pz.Engine SyncStateStore), replayed verbatim so the connector can resume the
    /// change feed. Null on the first run or under `--full-refresh`. The engine never inspects it.</summary>
    public string? PriorSyncState { get; init; }

    /// <summary>True when this read must land the change-row contract
    /// (dataset declared `sync: {mode: cdc}`). The prior log position arrives via
    /// <see cref="PriorSyncState"/> (null = first run or --full-refresh = snapshot).</summary>
    public bool ChangeCapture { get; init; }

    /// <summary>Optional `sync.slot` override (Postgres slot name). Null = connector default.</summary>
    public string? ChangeCaptureSlot { get; init; }
}

/// <summary>Names one output of one sink plus write disposition and options.</summary>
public sealed record OutputSpec(string Sink, string Output, string Mode, string SchemaPolicy,
    IReadOnlyDictionary<string, object?> Options)
{
    /// <summary>Merge key columns for a <c>mode: merge</c> output, empty for every other mode.
    /// Additive -- defaults to empty so every <c>new OutputSpec(...)</c> call site keeps constructing
    /// exactly as before (mirrors <see cref="DatasetSpec.WatermarkCursor"/>'s additive-property
    /// precedent).</summary>
    public IReadOnlyList<string> Keys { get; init; } = [];

    /// <summary>`write.on_delete` ("delete"|"soft"|"ignore") for a cdc-fed
    /// merge output; null otherwise. Only sessions implementing IDeleteApplyingWriteSession ever see
    /// delete/soft (planner-guarded, PZ0339).</summary>
    public string? OnDelete { get; init; }

    /// <summary>Per-column maximum text length (Unicode code points, as DuckDB's length())
    /// observed in the staged relation, for Arrow String columns only.
    /// Null when not computed (sink lacks <see cref="ConnectorCapabilities.TextLengthStats"/>, or no
    /// string columns). A column absent from the map had no non-null value. Additive — mirrors the
    /// <see cref="Keys"/> precedent.</summary>
    public IReadOnlyDictionary<string, long>? MaxTextLengths { get; init; }
}

/// <summary>Pushdown hints. Connectors ignore hints they did not declare capabilities for; the engine
/// re-applies unpushed filters in DuckDB, so honoring hints is an optimization, never a correctness duty.</summary>
public sealed record ReadHints(IReadOnlyList<string>? Columns = null, string? PredicateSql = null, long? Limit = null)
{
    public static readonly ReadHints None = new();
}

/// <summary>Batch shaping for the universal read path.</summary>
public sealed record BatchOptions(int TargetBatchBytes = 32 * 1024 * 1024, int MaxRowsPerBatch = 122_880)
{
    public static readonly BatchOptions Default = new();
}

/// <summary>A DuckDB-native scan: a SQL fragment usable as a FROM source, plus statements
/// (secrets, extension loads) the engine must execute on the DuckDB session first.</summary>
public sealed record NativeScan(string SqlFragment, IReadOnlyList<string> SetupStatements)
{
    /// <summary>Short, user-facing description of the mechanism (e.g. "read_csv") — surfaces only in
    /// planner Reason strings, never in setup statements or SQL sent to DuckDB.</summary>
    public string? Mechanism { get; init; }

    /// <summary>Additive (same discipline as <c>OutputSpec.MaxTextLengths</c>): true when the scan
    /// lets DuckDB invent the schema
    /// (contract-less csv/json <c>auto_detect</c>) rather than reading one the source itself declares
    /// (a database's catalog, parquet metadata, a <c>columns:</c> contract). The engine runs its
    /// integer-inference lint only on inferred schemas — a database DOUBLE holding big integral
    /// values was already a double at the source, so warning there would be a false positive.</summary>
    public bool SchemaInferred { get; init; }

    /// <summary>Additive: a FROM-usable fragment
    /// exposing DuckDB's own sniffer verdict for this scan's file — <c>sniff_csv('&lt;path&gt;')</c> —
    /// so the engine can ask which <c>DateFormat</c>/<c>TimestampFormat</c> the read committed to and
    /// warn when the pick was an unverifiable day/month guess. Null when there is nothing to sniff
    /// (declared contract, parquet/json, multi-file window covers); only meaningful alongside
    /// <see cref="SchemaInferred"/>. Runs on the engine's DuckDB session AFTER the scan's
    /// SetupStatements, so extensions/secrets the scan needed are already in place.</summary>
    public string? SniffFragment { get; init; }
}

/// <summary>A DuckDB-native copy: a complete COPY statement, plus setup statements to run first.</summary>
public sealed record NativeCopy(string CopySql, IReadOnlyList<string> SetupStatements)
{
    /// <summary>Short, user-facing description of the mechanism (e.g. "COPY ... (FORMAT parquet)") —
    /// surfaces only in planner Reason strings, never in setup statements or SQL sent to DuckDB.</summary>
    public string? Mechanism { get; init; }

    /// <summary>Filesystem finalizations the engine applies after the COPY succeeds. Empty for
    /// object-store copies, whose per-object PUT is already atomic.</summary>
    public IReadOnlyList<FileMove> Finalizations { get; init; } = [];
}

/// <summary>A filesystem finalization the engine applies after a successful native COPY: same-directory
/// atomic move from the temp path the COPY wrote to the final path. Object-store copies leave
/// Finalizations empty (per-object PUT is already atomic).</summary>
public sealed record FileMove(string TempPath, string FinalPath);

/// <summary>Offline config validation outcome. Empty <see cref="Errors"/> means valid.</summary>
public sealed record ValidationResult(IReadOnlyList<string> Errors)
{
    public static readonly ValidationResult Success = new([]);
    public static ValidationResult Failed(params string[] errors) => new(errors);
    public bool IsValid => Errors.Count == 0;
}

/// <summary>Online connectivity probe outcome.</summary>
public sealed record ConnectionCheck(bool Ok, string? Message = null);

/// <summary>A dataset's schema as the source will actually produce it.</summary>
public sealed record DatasetSchema(Apache.Arrow.Schema Schema);

/// <summary>What a committed write session accomplished.</summary>
public sealed record WriteResult(long RowsWritten, long BatchesWritten);
