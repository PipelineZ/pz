namespace Pz.Core.Dag;

/// <summary>Parser seam for watermark inference. Pz.Core must not reference Pz.DuckDb, so
/// DagCompiler consumes this interface; Pz.DuckDb implements it over DuckDB's own parser
/// (json_serialize_sql) — never hand-rolled SQL text matching.</summary>
public interface ISqlAstReader
{
    /// <summary>Parses <paramref name="sql"/> and, for each sentinel, locates the comparison
    /// containing it. Returns recognized comparisons, shape violations (never guesses), and the
    /// full statement rewritten with the NULL-guard `(<expr> IS NULL OR <col> <op> <expr>)`
    /// around every recognized comparison (DuckDB-normalized text).</summary>
    WatermarkAnalysis Analyze(string sql, IReadOnlyList<string> sentinels);

    /// <summary>Reads <paramref name="sql"/> and reports what may be pushed to the connector backing
    /// <paramref name="baseTable"/> (the staging relation name, e.g. "src_crm__orders").
    /// <paramref name="cursorColumn"/>, when non-null, is excluded from predicates: comparisons on the
    /// cursor route to DatasetSpec's watermark bounds, which are load-bearing rather than best-effort.</summary>
    ReadHintPlan ExtractReadHints(string sql, string baseTable, string? cursorColumn);

    /// <summary>Prepends <paramref name="ctes"/> as the leading common table expressions of
    /// <paramref name="consumerSql"/>'s own WITH clause -- creating one if it has none -- by reading and
    /// re-emitting the parsed AST (never sniffing consumer text for a "with" keyword), so it is correct
    /// whether the consumer's own body opens with a comment before WITH or is itself WITH RECURSIVE
    /// (DuckDB decides to emit RECURSIVE for the merged clause from the AST alone; this method never
    /// inspects that). What DagCompiler.BuildInlinedSql needs to fold an ephemeral pipeline's SELECT
    /// into a consumer. Returns null -- the default implementation, and also what an override returns
    /// when <paramref name="consumerSql"/> or a CTE body fails to parse -- when the AST route cannot be
    /// taken; the caller falls back to a textual splice, which still lets downstream tier-4
    /// EXPLAIN/PREPARE report genuinely malformed SQL with full context.</summary>
    string? PrependCtes(string consumerSql, IReadOnlyList<(string Alias, string Sql)> ctes) => null;
}

/// <summary>What a single reading pipeline lets pz ask the connector for. Null members mean "push
/// nothing for this half" — always safe, it degrades to extracting everything. Column names and
/// predicate text are unqualified: the connector builds its
/// own <c>select … from "schema"."table"</c> with no alias in scope, so a qualifier that survived here
/// would name a relation the connector's SQL never declares.</summary>
public sealed record ReadHintPlan(IReadOnlyList<string>? Columns, string? PredicateSql)
{
    public static readonly ReadHintPlan None = new(null, null);
}

/// <summary>ColumnTable is the resolved FROM-clause base-table name (e.g. "src_crm__orders") for
/// the cursor column's qualifier — or the sole base table when the column is unqualified.
/// <paramref name="Inclusive"/> means the boundary value itself is included, in whichever direction
/// this bound runs: `&gt;=` and `&lt;=` are inclusive, `&gt;` and `&lt;` are not.
/// <paramref name="IsUpper"/> distinguishes a ceiling — how `max_window` and `until` are spelled in
/// SQL — from a floor. Defaulted, so a construction site that omits it means "floor".
/// ValueExprSql still contains the quoted sentinel.</summary>
public sealed record WatermarkComparison(
    string Sentinel, string Column, string ColumnTable, bool Inclusive, string ValueExprSql,
    bool IsUpper = false);

public sealed record WatermarkShapeViolation(string Sentinel, string Reason);

public sealed record WatermarkAnalysis(
    IReadOnlyList<WatermarkComparison> Comparisons,
    IReadOnlyList<WatermarkShapeViolation> Violations,
    string RewrittenSql);
