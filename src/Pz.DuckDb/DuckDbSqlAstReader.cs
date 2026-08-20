using System.Text.Json.Nodes;
using Pz.Core.Dag;

namespace Pz.DuckDb;

/// <summary>Reads watermark comparisons out of rendered pipeline SQL using DuckDB's own
/// parser (json_serialize_sql), and rewrites each recognized comparison with the NULL-guard via
/// AST mutation + json_deserialize_sql. Total-or-error: anything outside the blessed shape is a
/// WatermarkShapeViolation, never a guess. A fresh in-memory session per call — parse-only, no
/// catalog, deterministic.</summary>
public sealed class DuckDbSqlAstReader : ISqlAstReader
{
    // The "unknown location" marker DuckDB itself emits for synthesized nodes (UINT64 max / invalid
    // index). Reused for the guard nodes we inject so json_deserialize_sql accepts them.
    private const ulong UnknownLocation = 18446744073709551615UL;

    public WatermarkAnalysis Analyze(string sql, IReadOnlyList<string> sentinels)
    {
        using var duck = DuckDbSync.OpenInMemory();
        var serialized = duck.Scalar($"select json_serialize_sql('{Escape(sql)}')");
        var root = JsonNode.Parse(serialized)!;
        if (root["error"]?.GetValue<bool>() == true)
        {
            // Unparseable SQL is tier-4's problem (EXPLAIN reports it with full context); here we
            // just can't analyze — report every sentinel as unanalyzable so the caller errors loudly.
            return new WatermarkAnalysis([],
                [.. sentinels.Select(s => new WatermarkShapeViolation(s, "SQL could not be parsed for watermark analysis"))], sql);
        }

        var statement = root["statements"]![0]!["node"]!;
        // Scope-aware alias resolution: each SELECT node — the outer statement AND
        // every cte_map entry's query — is its own scope with its own from_table. A comparison inside
        // a CTE body (as DagCompiler.BuildInlinedSql assembles ephemeral pipelines) must resolve its
        // cursor qualifier against THAT CTE's FROM clause, not the outer statement's (whose from_table
        // is merely the __pz_cte__ reference). Falls back to an empty scope for any comparison the walk
        // somehow missed, so resolution fails loudly rather than against the wrong tables.
        var scopeByComparison = MapComparisonScopes(statement);
        var emptyScope = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        var comparisons = new List<WatermarkComparison>();
        var violations = new List<WatermarkShapeViolation>();
        foreach (var sentinel in sentinels.Distinct(StringComparer.Ordinal))
        {
            var matchedComparisons = FindComparisonsContaining(statement, sentinel);

            // Occurrence-level total-or-error: capture, BEFORE any mutation, whether every
            // occurrence of this sentinel's CONSTANT in the whole statement is contained within one
            // of the comparison nodes we matched above. A sentinel can appear both inside a
            // perfectly valid comparison AND loose elsewhere (e.g. the select list) — the recognized
            // comparison must not let that second, loose occurrence silently ride along.
            var coveredConstants = new HashSet<JsonObject>(ReferenceEqualityComparer.Instance);
            foreach (var cmp in matchedComparisons)
            {
                foreach (var constant in SentinelConstants(cmp, sentinel))
                {
                    coveredConstants.Add(constant);
                }
            }

            var hasLooseOccurrence = SentinelConstants(statement, sentinel).Any(c => !coveredConstants.Contains(c));

            foreach (var node in matchedComparisons)
            {
                var scope = scopeByComparison.TryGetValue(node, out var s) ? s : emptyScope;
                Classify(node, sentinel, scope, duck, comparisons, violations);
            }

            if (!comparisons.Any(c => c.Sentinel == sentinel) && !violations.Any(v => v.Sentinel == sentinel))
            {
                violations.Add(new WatermarkShapeViolation(sentinel,
                    "watermark() must appear inside a comparison of the form <column> > / >= / < / <= <expression>"));
            }
            else if (hasLooseOccurrence && !violations.Any(v => v.Sentinel == sentinel))
            {
                violations.Add(new WatermarkShapeViolation(sentinel,
                    "watermark() must appear inside a comparison of the form <column> > / >= / < / <= <expression> — found an additional occurrence outside any recognized comparison"));
            }
        }

        var rewritten = violations.Count == 0 && comparisons.Count > 0
            ? Deserialize(duck, root) // root was mutated in place by Classify's guard injection
            : sql;
        return new WatermarkAnalysis(comparisons, violations, rewritten);
    }

    /// <summary>Both halves fail toward "push nothing", so an unrecognized shape costs speed and
    /// never correctness. The two unsafe directions are pruning a column the SQL still references
    /// (the staged table would lack it) and splitting a disjunction (rows silently dropped) — the
    /// STAR/subquery rules guard the first, the CONJUNCTION_AND check the second.</summary>
    public ReadHintPlan ExtractReadHints(string sql, string baseTable, string? cursorColumn)
    {
        using var duck = DuckDbSync.OpenInMemory();
        var serialized = duck.Scalar($"select json_serialize_sql('{Escape(sql)}')");
        var root = JsonNode.Parse(serialized)!;
        if (root["error"]?.GetValue<bool>() == true)
        {
            return ReadHintPlan.None; // unparseable is tier-4's problem; here we simply push nothing
        }

        var statement = root["statements"]![0]!["node"]!;
        var aliases = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        CollectBaseTables(statement["from_table"], aliases);

        // Only the OUTER scope's FROM is consulted. A source reached through a CTE body or a subquery
        // resolves nothing here and pushes nothing — conservative by construction, and the shape
        // DagCompiler produces for a plain reader always names the table in the outer FROM.
        var targetAliases = new HashSet<string>(
            aliases.Where(kv => string.Equals(kv.Value, baseTable, StringComparison.Ordinal)).Select(kv => kv.Key),
            StringComparer.OrdinalIgnoreCase);
        if (targetAliases.Count == 0)
        {
            return ReadHintPlan.None;
        }

        var soleTable = aliases.Count == 1;
        return new ReadHintPlan(
            ExtractColumns(statement, targetAliases, soleTable),
            ExtractPredicate(duck, statement["where_clause"], targetAliases, soleTable, cursorColumn));
    }

    /// <summary>The columns the target table must supply, or null to read every column. Null wins on
    /// any doubt: a star over the target, or any subquery at all (whose own scope makes an unqualified
    /// reference belong to some other table, so collecting it would have pz ask the source for a column
    /// it has not got).</summary>
    private static IReadOnlyList<string>? ExtractColumns(
        JsonNode statement, HashSet<string> targetAliases, bool soleTable)
    {
        var columns = new SortedSet<string>(StringComparer.Ordinal);
        foreach (var node in OuterScopeDescendants(statement).OfType<JsonObject>())
        {
            switch (ClassOf(node))
            {
                case "SUBQUERY":
                    return null;
                case "STAR":
                    var relation = node["relation_name"]?.GetValue<string>() ?? string.Empty;
                    if (relation.Length == 0 || targetAliases.Contains(relation))
                    {
                        return null;
                    }

                    break;
                case "COLUMN_REF":
                    if (ResolveTargetColumn(node, targetAliases, soleTable) is { } column)
                    {
                        columns.Add(column);
                    }

                    break;
            }
        }

        return columns.Count > 0 ? [.. columns] : null;
    }

    /// <summary>The bare column name when this COLUMN_REF resolves to the target table, else null.
    /// Unqualified references resolve only when the target is the query's sole base table; with a join
    /// in scope an unqualified name could belong to either side.</summary>
    private static string? ResolveTargetColumn(JsonObject columnRef, HashSet<string> targetAliases, bool soleTable)
    {
        if (columnRef["column_names"] is not JsonArray names || names.Count == 0)
        {
            return null;
        }

        var column = names[^1]!.GetValue<string>();
        if (names.Count == 1)
        {
            return soleTable ? column : null;
        }

        return targetAliases.Contains(names[^2]!.GetValue<string>()) ? column : null;
    }

    /// <summary>Every descendant of <paramref name="statement"/> except the insides of subqueries,
    /// which are their own name scope.</summary>
    private static IEnumerable<JsonNode> OuterScopeDescendants(JsonNode? node)
    {
        if (node is null)
        {
            yield break;
        }

        yield return node;
        if (ClassOf(node) == "SUBQUERY")
        {
            yield break; // yielded so the caller can see it, but its contents belong to another scope
        }

        var children = node switch
        {
            JsonObject obj => obj.Select(kv => kv.Value),
            JsonArray arr => arr.AsEnumerable(),
            _ => [],
        };
        foreach (var child in children)
        {
            foreach (var descendant in OuterScopeDescendants(child))
            {
                yield return descendant;
            }
        }
    }

    /// <summary>The AND-conjuncts of the WHERE clause that are safe to hand the connector, joined back
    /// with " AND " and stripped of table qualifiers (the connector's own SELECT declares no alias).
    /// Null when nothing survives.</summary>
    private static string? ExtractPredicate(DuckDbSync duck, JsonNode? whereClause,
        HashSet<string> targetAliases, bool soleTable, string? cursorColumn)
    {
        if (whereClause is null)
        {
            return null;
        }

        var kept = new List<string>();
        foreach (var conjunct in Conjuncts(whereClause))
        {
            if (IsPushable(duck, conjunct, targetAliases, soleTable, cursorColumn))
            {
                kept.Add(RegenerateExprSql(duck, StripQualifiers(conjunct.DeepClone())));
            }
        }

        return kept.Count > 0 ? string.Join(" AND ", kept) : null;
    }

    /// <summary>Flattens nested ANDs into independent terms. A node is split ONLY on
    /// CONJUNCTION_AND: <c>OR</c> shares class CONJUNCTION and differs only in type, and splitting it
    /// would push one arm alone and silently drop every row the other arm alone matched.</summary>
    private static IEnumerable<JsonNode> Conjuncts(JsonNode node)
    {
        if (ClassOf(node) == "CONJUNCTION" && TypeOf(node) == "CONJUNCTION_AND"
            && node["children"] is JsonArray children)
        {
            foreach (var child in children)
            {
                foreach (var conjunct in Conjuncts(child!))
                {
                    yield return conjunct;
                }
            }

            yield break;
        }

        yield return node;
    }

    private static bool IsPushable(DuckDbSync duck, JsonNode conjunct,
        HashSet<string> targetAliases, bool soleTable, string? cursorColumn)
    {
        // The sentinel is substituted per-run by PipelineExecutor, long after compile — pushing a
        // conjunct carrying it would send the placeholder text itself to the source. Watermark bounds
        // reach the connector through DatasetSpec instead, which refuses rather than degrades.
        if (conjunct.ToJsonString().Contains("__pz_watermark__", StringComparison.Ordinal))
        {
            return false;
        }

        foreach (var node in Descendants(conjunct).OfType<JsonObject>())
        {
            if (ClassOf(node) is "SUBQUERY" or "STAR")
            {
                return false;
            }

            if (ClassOf(node) != "COLUMN_REF")
            {
                continue;
            }

            var column = ResolveTargetColumn(node, targetAliases, soleTable);
            if (column is null
                || (cursorColumn is not null && string.Equals(column, cursorColumn, StringComparison.OrdinalIgnoreCase)))
            {
                return false;
            }
        }

        // Same volatility rule as the watermark value side: the connector evaluates the pushed
        // predicate in a different query from the pipeline's own filter, so a function that varies
        // between the two would land a different row set than the SQL asked for.
        return FindNonDeterministicFunction(duck, CollectFunctionNames(conjunct)) is null;
    }

    /// <summary>Reduces every COLUMN_REF in <paramref name="node"/> to its bare column name, in place.
    /// Callers pass a clone — the statement's own AST must keep its qualifiers.</summary>
    private static JsonNode StripQualifiers(JsonNode node)
    {
        foreach (var columnRef in Descendants(node).OfType<JsonObject>().Where(o => ClassOf(o) == "COLUMN_REF"))
        {
            if (columnRef["column_names"] is JsonArray names && names.Count > 1)
            {
                columnRef["column_names"] = new JsonArray(names[^1]!.GetValue<string>());
            }
        }

        return node;
    }

    // -- walker helpers ------------------------------------------------------------------------

    private static string Escape(string value) => value.Replace("'", "''");

    // Guarded against nodes whose "class"/"type" field is itself an object (e.g. a CONSTANT's nested
    // type descriptor) rather than a string — MapComparisonScopes probes every JsonObject, not only
    // known statement nodes, so these must never throw on a non-string field.
    private static string? ClassOf(JsonNode? node) =>
        (node as JsonObject)?["class"] is JsonValue v && v.TryGetValue(out string? s) ? s : null;

    private static string? TypeOf(JsonNode? node) =>
        (node as JsonObject)?["type"] is JsonValue v && v.TryGetValue(out string? s) ? s : null;

    /// <summary>Every JsonObject/JsonArray descendant of <paramref name="node"/>, including
    /// <paramref name="node"/> itself, in document order.</summary>
    private static IEnumerable<JsonNode> Descendants(JsonNode? node)
    {
        if (node is null)
        {
            yield break;
        }

        yield return node;
        switch (node)
        {
            case JsonObject obj:
                foreach (var kv in obj)
                {
                    foreach (var child in Descendants(kv.Value))
                    {
                        yield return child;
                    }
                }

                break;
            case JsonArray arr:
                foreach (var item in arr)
                {
                    foreach (var child in Descendants(item))
                    {
                        yield return child;
                    }
                }

                break;
        }
    }

    /// <summary>Depth-first (document order): every node with class == "COMPARISON" whose subtree
    /// contains the sentinel constant. A sentinel nested under a non-comparison ancestor only (e.g.
    /// select list) yields nothing here and is caught by the "outside a comparison" fallback in
    /// <see cref="Analyze"/>. Materialized eagerly so later in-place mutation cannot disturb it.</summary>
    private static IReadOnlyList<JsonObject> FindComparisonsContaining(JsonNode node, string sentinel) =>
        [.. Descendants(node).OfType<JsonObject>()
            .Where(o => ClassOf(o) == "COMPARISON" && ContainsSentinel(o, sentinel))];

    private static bool ContainsSentinel(JsonNode? node, string sentinel) =>
        SentinelConstants(node, sentinel).Any();

    private static bool IsSentinelConstant(JsonObject node, string sentinel) =>
        ClassOf(node) == "CONSTANT"
        && node["value"]?["value"] is JsonValue v
        && v.TryGetValue(out string? s)
        && s == sentinel;

    /// <summary>Every CONSTANT node in <paramref name="node"/>'s subtree whose literal equals
    /// <paramref name="sentinel"/>, by reference — used to detect sentinel occurrences that fall
    /// outside every recognized comparison's subtree (see the occurrence-level check in
    /// <see cref="Analyze"/>).</summary>
    private static IEnumerable<JsonObject> SentinelConstants(JsonNode? node, string sentinel) =>
        Descendants(node).OfType<JsonObject>().Where(o => IsSentinelConstant(o, sentinel));

    private static bool ContainsColumnRef(JsonNode? node) =>
        Descendants(node).OfType<JsonObject>().Any(o => ClassOf(o) == "COLUMN_REF");

    /// <summary>Every distinct FUNCTION name in <paramref name="node"/>'s subtree (operators such as
    /// <c>-</c>/<c>+</c> and interval helpers serialize as FUNCTION nodes too, so this catches them
    /// all). Case-insensitive dedupe — DuckDB folds function names to lowercase, but be defensive.</summary>
    private static IReadOnlyList<string> CollectFunctionNames(JsonNode? node) =>
        [.. Descendants(node).OfType<JsonObject>()
            .Where(o => ClassOf(o) == "FUNCTION")
            .Select(o => o["function_name"] is JsonValue v && v.TryGetValue(out string? s) ? s : null)
            .Where(n => !string.IsNullOrEmpty(n))
            .Select(n => n!)
            .Distinct(StringComparer.OrdinalIgnoreCase)];

    /// <summary>Empirical volatility probe:
    /// asks DuckDB's own catalog which of the collected names are non-deterministic — total-or-error,
    /// same as every other shape rule in this reader. <c>duckdb_functions().stability</c> classifies
    /// each CATALOGED function as CONSISTENT (pure), CONSISTENT_WITHIN_QUERY (stable inside one query
    /// but varying between queries — e.g. now(), current_timestamp), or VOLATILE (e.g. random(),
    /// uuid()). Only CONSISTENT is safe here: the extraction bound and the pipeline predicate are two
    /// separate queries, so anything in the other two classes would evaluate differently between them.
    /// A name ABSENT from the catalog is not treated as deterministic either — json_serialize_sql
    /// parses without resolving to the catalog at all, so a typo'd or nonexistent function (e.g. the
    /// Postgres-ism <c>clock_timestamp()</c>, which DuckDB has no builtin for) would otherwise bind at
    /// EXPLAIN time and slip this guard with unverifiable determinism. Two in-memory catalog queries
    /// per comparison (non-consistent hit, then presence check for the remainder); deterministic;
    /// returns the lowest offending name (ordinal) for a deterministic message, tagged with whether it
    /// was found-but-non-consistent or missing entirely, or null when every name is CONSISTENT.</summary>
    private static FunctionDeterminismIssue? FindNonDeterministicFunction(DuckDbSync duck, IReadOnlyList<string> names)
    {
        if (names.Count == 0)
        {
            return null;
        }

        var lowerNames = names.Select(n => n.ToLowerInvariant()).ToList();
        var inList = string.Join(", ", lowerNames.Select(n => $"'{Escape(n)}'"));

        var nonConsistent = duck.Scalar(
            "select coalesce(min(lower(function_name)), '') from duckdb_functions() " +
            $"where stability in ('VOLATILE', 'CONSISTENT_WITHIN_QUERY') and lower(function_name) in ({inList})");
        if (!string.IsNullOrEmpty(nonConsistent))
        {
            return new FunctionDeterminismIssue(nonConsistent, IsAbsentFromCatalog: false);
        }

        // None of the names is a cataloged VOLATILE/CONSISTENT_WITHIN_QUERY function. Now check
        // whether every name is even cataloged at all — anything missing is unverifiable, not
        // deterministic-by-default.
        var known = duck.Scalar(
            "select coalesce(string_agg(distinct lower(function_name), chr(1)), '') from duckdb_functions() " +
            $"where lower(function_name) in ({inList})");
        var knownNames = known.Length == 0
            ? new HashSet<string>(StringComparer.Ordinal)
            : new HashSet<string>(known.Split('\u0001'), StringComparer.Ordinal);
        var absent = lowerNames.Where(n => !knownNames.Contains(n)).OrderBy(n => n, StringComparer.Ordinal).FirstOrDefault();
        return absent is null ? null : new FunctionDeterminismIssue(absent, IsAbsentFromCatalog: true);
    }

    /// <summary>Result of <see cref="FindNonDeterministicFunction"/>: the offending (lowercased)
    /// function name, and whether it was missing from <c>duckdb_functions()</c> entirely versus
    /// present but classified VOLATILE/CONSISTENT_WITHIN_QUERY.</summary>
    private readonly record struct FunctionDeterminismIssue(string Name, bool IsAbsentFromCatalog);

    /// <summary>Maps every COMPARISON node in the statement to the alias table of its NEAREST
    /// enclosing SELECT scope. Walks the AST tracking the current scope: entering a SELECT node
    /// (the outer statement or a cte_map entry's query) recomputes the scope from that node's own
    /// from_table, and any COMPARISON encountered while in that scope resolves against it. This is
    /// what lets a watermark() comparison sitting inside an ephemeral CTE body
    /// resolve its cursor qualifier against the CTE's FROM clause rather than the outer statement's
    /// __pz_cte__ reference. Keyed by reference so later in-place NULL-guard mutation is unaffected.</summary>
    private static Dictionary<JsonObject, Dictionary<string, string>> MapComparisonScopes(JsonNode statement)
    {
        var map = new Dictionary<JsonObject, Dictionary<string, string>>(ReferenceEqualityComparer.Instance);
        var root = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        Walk(statement, root);
        return map;

        void Walk(JsonNode? node, Dictionary<string, string> scope)
        {
            switch (node)
            {
                case JsonObject obj:
                    if (TypeOf(obj) == "SELECT_NODE")
                    {
                        scope = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                        CollectBaseTables(obj["from_table"], scope);
                    }
                    else if (ClassOf(obj) == "COMPARISON")
                    {
                        map[obj] = scope;
                    }

                    foreach (var kv in obj)
                    {
                        Walk(kv.Value, scope);
                    }

                    break;
                case JsonArray arr:
                    foreach (var item in arr)
                    {
                        Walk(item, scope);
                    }

                    break;
            }
        }
    }

    /// <summary>BASE_TABLE => aliases[alias.Length>0 ? alias : table_name] = table_name (the
    /// "staging." schema qualifier rides a separate schema_name field, so it never appears here);
    /// JOIN => recurse left + right.</summary>
    private static void CollectBaseTables(JsonNode? from, Dictionary<string, string> aliases)
    {
        if (from is not JsonObject obj)
        {
            return;
        }

        switch (TypeOf(obj))
        {
            case "BASE_TABLE":
                var table = obj["table_name"]?.GetValue<string>() ?? string.Empty;
                var alias = obj["alias"]?.GetValue<string>() ?? string.Empty;
                aliases[alias.Length > 0 ? alias : table] = table;
                break;
            case "JOIN":
                CollectBaseTables(obj["left"], aliases);
                CollectBaseTables(obj["right"], aliases);
                break;
        }
    }

    /// <summary>The shape gate. Exactly one side must be a bare COLUMN_REF and the OTHER side must
    /// contain the sentinel and no COLUMN_REF. Operator must normalize to a lower bound on the
    /// column: col GT/GE expr, or expr LT/LE col. On success: resolve the column's qualifier via
    /// <paramref name="aliases"/> (unqualified + exactly one base table => that table; unqualified +
    /// several => violation), regenerate the value side's SQL, record the comparison, and mutate
    /// this JSON node in place into CONJUNCTION_OR [ OPERATOR_IS_NULL(clone(valueExpr)), original
    /// comparison ] — the NULL-guard. On any failure: add a WatermarkShapeViolation whose Reason
    /// names the specific rule broken and states the accepted shape.</summary>
    private static void Classify(JsonObject comparison, string sentinel, Dictionary<string, string> aliases,
        DuckDbSync duck, List<WatermarkComparison> comparisons, List<WatermarkShapeViolation> violations)
    {
        void Reject(string reason) => violations.Add(new WatermarkShapeViolation(sentinel, reason));
        const string accepted = " Accepted shape: <column> > / >= / < / <= <expression> " +
            "(the watermark on the value side, no column).";

        var op = TypeOf(comparison);
        var left = comparison["left"];
        var right = comparison["right"];

        // The value side is whichever side carries the sentinel; the cursor side is the other.
        JsonNode? valueSide, cursorSide;
        bool cursorOnLeft;
        if (ContainsSentinel(left, sentinel))
        {
            valueSide = left;
            cursorSide = right;
            cursorOnLeft = false;
        }
        else
        {
            valueSide = right;
            cursorSide = left;
            cursorOnLeft = true;
        }

        // Rule: the cursor side must be a bare column reference.
        if (ClassOf(cursorSide) != "COLUMN_REF")
        {
            Reject("the cursor side of the watermark comparison must be a bare column reference, not an expression." + accepted);
            return;
        }

        // Rule: the value side must not reference a column.
        if (ContainsColumnRef(valueSide))
        {
            Reject("the watermark value expression must not reference a column." + accepted);
            return;
        }

        // Rule: the value side must be deterministic. A volatile function (now(), random(), …)
        // evaluates differently at extraction-bound time vs pipeline-predicate time, so rows landed in
        // staging can be excluded by the predicate while advancement (MAX(cursor)) moves past them —
        // permanently skipped, breaking effectively-once.
        var determinismIssue = FindNonDeterministicFunction(duck, CollectFunctionNames(valueSide));
        if (determinismIssue is { } issue)
        {
            var reason = issue.IsAbsentFromCatalog
                ? $"the watermark value expression uses function '{issue.Name}', which is not in DuckDB's function catalog — " +
                  "its determinism cannot be verified; use a catalog function with CONSISTENT stability."
                : $"the watermark value expression must be deterministic: function '{issue.Name}' is volatile — " +
                  "the extraction bound and the pipeline filter would evaluate it at different times.";
            Reject(reason + accepted);
            return;
        }

        // Rule: the operator must be an ordered bound on the cursor column — a floor or a ceiling.
        // Direction is RECORDED rather than normalized away: a ceiling is how max_window and until are
        // spelled in SQL. `inclusive` means the boundary value itself is included, whichever way the
        // bound runs.
        //   floor:   col GT/GE value  (cursor left)  or  value LT/LE col  (cursor right)
        //   ceiling: col LT/LE value  (cursor left)  or  value GT/GE col  (cursor right)
        bool inclusive;
        bool isUpper;
        switch (op)
        {
            case "COMPARE_GREATERTHAN" when cursorOnLeft:
            case "COMPARE_LESSTHAN" when !cursorOnLeft:
                (inclusive, isUpper) = (false, false);
                break;
            case "COMPARE_GREATERTHANOREQUALTO" when cursorOnLeft:
            case "COMPARE_LESSTHANOREQUALTO" when !cursorOnLeft:
                (inclusive, isUpper) = (true, false);
                break;
            case "COMPARE_LESSTHAN" when cursorOnLeft:
            case "COMPARE_GREATERTHAN" when !cursorOnLeft:
                (inclusive, isUpper) = (false, true);
                break;
            case "COMPARE_LESSTHANOREQUALTO" when cursorOnLeft:
            case "COMPARE_GREATERTHANOREQUALTO" when !cursorOnLeft:
                (inclusive, isUpper) = (true, true);
                break;
            default:
                Reject("the watermark comparison must be an ordered bound on the cursor column " +
                       "(>, >=, < or <=)." + accepted);
                return;
        }

        // Resolve the cursor column's qualifier to a base table.
        var columnNames = (JsonArray)cursorSide!["column_names"]!;
        var column = columnNames[^1]!.GetValue<string>();
        string columnTable;
        if (columnNames.Count >= 2)
        {
            var qualifier = columnNames[0]!.GetValue<string>();
            if (!aliases.TryGetValue(qualifier, out var resolved))
            {
                Reject($"could not resolve the cursor column's qualifier '{qualifier}' to a FROM-clause table." + accepted);
                return;
            }

            columnTable = resolved;
        }
        else if (aliases.Count == 1)
        {
            columnTable = aliases.Values.Single();
        }
        else
        {
            Reject("the cursor column is unqualified but the query has multiple base tables — qualify the cursor column." + accepted);
            return;
        }

        var valueExprSql = RegenerateExprSql(duck, valueSide!);
        comparisons.Add(new WatermarkComparison(sentinel, column, columnTable, inclusive, valueExprSql, isUpper));
        InjectNullGuard(comparison, valueSide!);
    }

    /// <summary>Mutates <paramref name="comparison"/> in place into
    /// CONJUNCTION_OR [ OPERATOR_IS_NULL(clone(valueExpr)), original comparison ]. Clones are taken
    /// before the object is cleared, so the original comparison and value expression survive.</summary>
    private static void InjectNullGuard(JsonObject comparison, JsonNode valueExpr)
    {
        var originalComparison = comparison.DeepClone();
        var isNull = new JsonObject
        {
            ["class"] = "OPERATOR",
            ["type"] = "OPERATOR_IS_NULL",
            ["alias"] = "",
            ["query_location"] = UnknownLocation,
            ["children"] = new JsonArray(valueExpr.DeepClone()),
        };

        foreach (var key in comparison.Select(kv => kv.Key).ToList())
        {
            comparison.Remove(key);
        }

        comparison["class"] = "CONJUNCTION";
        comparison["type"] = "CONJUNCTION_OR";
        comparison["alias"] = "";
        comparison["query_location"] = UnknownLocation;
        comparison["children"] = new JsonArray(isNull, originalComparison);
    }

    /// <summary>Wrap the expression subtree as the lone select-list item of a parsed `SELECT 1`
    /// skeleton, run json_deserialize_sql, strip the leading "SELECT ".</summary>
    private static string RegenerateExprSql(DuckDbSync duck, JsonNode expr)
    {
        var skeleton = JsonNode.Parse(duck.Scalar("select json_serialize_sql('select 1')"))!;
        skeleton["statements"]![0]!["node"]!["select_list"] = new JsonArray(expr.DeepClone());
        var sql = Deserialize(duck, skeleton);
        const string prefix = "SELECT ";
        return sql.StartsWith(prefix, StringComparison.Ordinal) ? sql[prefix.Length..] : sql;
    }

    private static string Deserialize(DuckDbSync duck, JsonNode root) =>
        duck.Scalar($"select json_deserialize_sql('{Escape(root.ToJsonString())}')");

    /// <summary>Thin synchronous wrapper over <see cref="DuckSession"/> — Analyze must be sync
    /// because DagCompiler.Compile is. A fresh in-memory, catalog-less session per call.</summary>
    private sealed class DuckDbSync : IDisposable
    {
        private readonly DuckSession _session;

        private DuckDbSync(DuckSession session) => _session = session;

        public static DuckDbSync OpenInMemory() => new(DuckSession.Open(":memory:"));

        public string Scalar(string sql) => _session.ScalarAsync<string>(sql).GetAwaiter().GetResult();

        public void Dispose() => _session.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }
}
