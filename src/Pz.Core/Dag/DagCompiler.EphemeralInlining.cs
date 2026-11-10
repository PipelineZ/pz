using Pz.Core.Model;
using Pz.Core.Templating;

namespace Pz.Core.Dag;

/// <summary>Ephemeral pipelines: dependency inheritance, CTE assembly and SQL normalization.</summary>
public static partial class DagCompiler
{
    /// <summary>
    /// <c>RenderResult.Dependencies</c> is a <see cref="HashSet{T}"/>, whose enumeration order
    /// .NET does not contractually guarantee stable across processes (randomized string
    /// hashing). Sorting by a canonical string key before deriving PZ0201/PZ0204 errors from it
    /// keeps the resulting error lists' ordering deterministic across runs.
    /// </summary>
    private static IEnumerable<DepRef> OrderDeterministically(IEnumerable<DepRef> deps) =>
        deps.OrderBy(DepKey, StringComparer.Ordinal);

    private static string DepKey(DepRef dep) => dep switch
    {
        DepRef.Pipeline p => $"pipeline:{p.Name}",
        DepRef.Source s => $"source:{s.SourceName}.{s.Dataset}",
        _ => throw new InvalidOperationException("unreachable"),
    };

    /// <summary>
    /// A pipeline's own rendered Dependencies, with any direct reference to an ephemeral
    /// pipeline replaced by that ephemeral's own Dependencies ("consumer inherits the
    /// ephemeral's dependencies"). Ephemeral chains are rejected earlier, so this is at
    /// most one level of substitution.
    /// </summary>
    private static IEnumerable<DepRef> EffectiveDependencies(
        PipelineDef pipeline, IReadOnlyDictionary<string, RenderResult> rendered,
        IReadOnlyDictionary<string, PipelineDef> pipelinesByName)
    {
        foreach (var dep in rendered[pipeline.Name].Dependencies)
        {
            if (dep is DepRef.Pipeline pipelineRef && pipelinesByName[pipelineRef.Name].Materialization == "ephemeral")
            {
                foreach (var inherited in rendered[pipelineRef.Name].Dependencies)
                {
                    yield return inherited;
                }
            }
            else
            {
                yield return dep;
            }
        }
    }

    /// <summary>
    /// A pipeline's own rendered <see cref="RenderResult.WatermarkRefs"/>, unioned with the
    /// WatermarkRefs of every ephemeral it directly consumes — the watermark analogue of
    /// <see cref="EffectiveDependencies"/>. A <c>watermark()</c> inside an ephemeral is CTE-inlined
    /// into this consumer's assembled SQL, so it must be attributed here. Determinism:
    /// the consumer's own refs first, then inherited refs in ephemeral-name order (Dependencies is a
    /// set, so its enumeration order is not stable). Ephemeral chains are rejected earlier, so this is
    /// at most one level of inheritance.
    /// </summary>
    private static IReadOnlyList<WatermarkRef> EffectiveWatermarkRefs(
        PipelineDef pipeline, IReadOnlyDictionary<string, RenderResult> rendered,
        IReadOnlyDictionary<string, PipelineDef> pipelinesByName)
    {
        var refs = new List<WatermarkRef>(rendered[pipeline.Name].WatermarkRefs);
        var ephemeralDeps = rendered[pipeline.Name].Dependencies
            .OfType<DepRef.Pipeline>()
            .Where(p => pipelinesByName[p.Name].Materialization == "ephemeral")
            .OrderBy(p => p.Name, StringComparer.Ordinal);
        foreach (var dep in ephemeralDeps)
        {
            refs.AddRange(rendered[dep.Name].WatermarkRefs);
        }

        return refs;
    }

    private static IReadOnlyList<NodeId> ResolveDependsOn(
        IEnumerable<DepRef> deps,
        IReadOnlyDictionary<string, NodeId> pipelineNodeIds,
        IReadOnlyDictionary<(string Source, string Dataset), NodeId> sourceNodeIds)
    {
        var ids = new HashSet<NodeId>();
        foreach (var dep in deps)
        {
            var id = dep switch
            {
                DepRef.Pipeline p => pipelineNodeIds[p.Name],
                DepRef.Source s => sourceNodeIds[(s.SourceName, s.Dataset)],
                _ => throw new InvalidOperationException("unreachable"),
            };
            ids.Add(id);
        }

        return ids.OrderBy(id => id.Value, StringComparer.Ordinal).ToList();
    }

    /// <summary>
    /// "Consumer SQL becomes `with __pz_cte__&lt;name&gt; as (&lt;ephemeral sql&gt;)` + consumer
    /// body; multiple ephemeral deps join their CTEs with `, ` sorted by name; if the consumer's
    /// own SQL already carries a WITH clause, the ephemeral CTEs become its leading entries." The
    /// CTE alias is prefixed with <c>__pz_cte__</c> so it matches what <see cref="TemplateRenderer"/>
    /// renders for <c>ref()</c> calls to ephemeral pipelines (which render <c>__pz_cte__&lt;name&gt;</c>,
    /// not <c>staging.&lt;name&gt;</c>) — otherwise the inlined CTE and the consumer body's reference to
    /// it would not match.
    /// <para>Assembly prefers <see cref="ISqlAstReader.PrependCtes"/>: reading the consumer's own
    /// <c>cte_map</c> off DuckDB's own parser is what gets a consumer that opens with a comment before
    /// WITH, or is itself WITH RECURSIVE, right without this stage sniffing consumer text for a "with"
    /// keyword. <paramref name="sqlAst"/> is null in tests that construct a project with no incremental
    /// SQL at all (every production caller supplies <c>DuckDbSqlAstReader</c>) — and
    /// <see cref="ISqlAstReader.PrependCtes"/> itself returns null if either side fails to parse — so a
    /// textual splice is the fallback: it handles the common case, and downstream tier-4 EXPLAIN/PREPARE reports a genuinely malformed pipeline
    /// with full context regardless of which route assembled it.</para>
    /// </summary>
    private static string BuildInlinedSql(
        PipelineDef pipeline, IReadOnlyDictionary<string, RenderResult> rendered,
        IReadOnlyDictionary<string, PipelineDef> pipelinesByName, ISqlAstReader? sqlAst)
    {
        var sql = rendered[pipeline.Name].Sql;
        var ephemeralNames = rendered[pipeline.Name].Dependencies
            .OfType<DepRef.Pipeline>()
            .Where(d => pipelinesByName[d.Name].Materialization == "ephemeral")
            .Select(d => d.Name)
            .Distinct()
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();

        if (ephemeralNames.Count == 0)
        {
            return sql;
        }

        // NormalizeSql here as well as at the assembled-SQL call site: an ephemeral body ending in its
        // own trailing `;` must not land inside `( ... ; )`, which DuckDB refuses as a second (empty)
        // statement nested in a parenthesized expression.
        var ctes = ephemeralNames
            .Select(name => (Alias: $"__pz_cte__{name}", Sql: NormalizeSql(rendered[name].Sql)))
            .ToList();

        if (sqlAst?.PrependCtes(sql, ctes) is { } merged)
        {
            return merged;
        }

        var cteList = string.Join(", ", ctes.Select(c => $"{c.Alias} as ({c.Sql})"));
        var trimmed = sql.TrimStart();

        if (trimmed.StartsWith("with", StringComparison.OrdinalIgnoreCase))
        {
            var rest = trimmed[4..].TrimStart();
            return $"with {cteList}, {rest}";
        }

        return $"with {cteList} {sql}";
    }

    /// <summary>LF-normalizes and trims trailing whitespace, then strips exactly one trailing
    /// semicolon (plus any whitespace before it) -- mirroring CheckExecutor's custom_sql handling, so a
    /// pipeline written the way most SQL tools format it (trailing `;`) is not a hard DuckDB parse
    /// error. A `;` anywhere else in the text is left alone: a second statement stays a second
    /// statement, and fails loudly rather than being silently truncated.</summary>
    private static string NormalizeSql(string sql)
    {
        var normalized = sql.Replace("\r\n", "\n").Replace('\r', '\n').TrimEnd();
        return normalized.EndsWith(';') ? normalized[..^1].TrimEnd() : normalized;
    }
}
