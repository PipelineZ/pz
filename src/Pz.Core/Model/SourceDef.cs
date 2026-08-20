namespace Pz.Core.Model;

public sealed record DatasetDef(string Name, IReadOnlyDictionary<string, object?> Options,
    IReadOnlyDictionary<string, string>? Columns, SyncModeDef? SyncMode = null, RetryDef? Retry = null);

/// <summary>The unified read-mode declaration (<c>sync: { mode: incremental | cdc | auto }</c> in
/// sources/*.yml). Null on DatasetDef ⇔ the block is omitted ⇔ <c>mode: auto</c> (the connector's
/// natural read). <see cref="Incremental"/> carries the cursor/window payload and is non-null exactly
/// when <see cref="Mode"/> is <see cref="SyncMode.Incremental"/>. Declaring two read modes is
/// unrepresentable in this shape. <see cref="Slot"/> is cdc-only (non-null only when
/// <see cref="Mode"/> is <see cref="SyncMode.Cdc"/>) — an optional Postgres replication slot-name
/// override (default slot naming is connector-derived).</summary>
public enum SyncMode { Auto, Incremental, Cdc }
public sealed record SyncModeDef(SyncMode Mode, IncrementalDef? Incremental, string? Slot = null);

/// <summary>One <c>watermark()</c> comparison a pipeline's SQL declared on a dataset's cursor,
/// as recognized by <see cref="Dag.ISqlAstReader"/> and folded by <c>WatermarkInference</c>.
/// <paramref name="ValueExprSql"/> still contains the quoted sentinel; the engine substitutes the stored
/// value (or the literal <c>NULL</c> on a first run) and evaluates it in DuckDB.
/// <paramref name="IsUpper"/> marks a ceiling — how <c>max_window</c> and <c>until</c> are spelled in
/// SQL. Defaulted, so a construction site that omits it means "floor". A ceiling is load-bearing for
/// advancement in a way a floor is not: floors reduce to the LOOSEST candidate, ceilings to the
/// TIGHTEST.</summary>
public sealed record SqlWatermarkBound(string Pipeline, bool Inclusive, string ValueExprSql, string Sentinel,
    bool IsUpper = false);

/// <summary>Dataset-level incremental extraction config (<c>incremental: { cursor: &lt;col&gt; }</c> in
/// sources/*.yml); a <c>watermark()</c> comparison in pipeline SQL is the equivalent SQL-declared
/// route. DagCompiler validates the
/// cursor at compile time (PZ0212) against the dataset's <c>columns:</c> contract, when one is declared --
/// allowed cursor types are <c>int</c>/<c>bigint</c>/<c>decimal</c>/<c>date</c>/<c>timestamp</c>. The optional
/// bounded-window trio (MaxWindow/Initial/Until) is RAW strings here — the loader only enforces scalar
/// shape (PZ0101); every semantic rule is DagCompiler's job (PZ0213). <c>DeclaredInSql</c>
/// flags whether this incremental was declared via watermark() expressions in pipeline SQL (synthesized by
/// WatermarkInference, PZ0224-0227). <c>SqlBounds</c> records the comparison-bound traces when <c>DeclaredInSql</c>
/// is true; each bound's <c>ValueExprSql</c> still contains the quoted watermark() sentinel for pipeline
/// uniqueness tracing (rewritten out at execute time by PipelineExecutor)</summary>
public sealed record IncrementalDef(string Cursor, string? MaxWindow = null, string? Initial = null, string? Until = null,
    bool DeclaredInSql = false, IReadOnlyList<SqlWatermarkBound>? SqlBounds = null);
