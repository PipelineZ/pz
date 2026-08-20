using System.Globalization;
using Apache.Arrow;
using Pz.Core.Dag;
using Pz.Core.Loading;
using Pz.Core.Validation;
using Pz.DuckDb;
using Pz.Engine.Execution;

namespace Pz.Engine.Checks;

/// <summary>
/// Executes a Check node's SQL against its owning pipeline's staging relation (see
/// <see cref="CheckNodeDef"/> — the pipeline is already materialized by the time this runs,
/// since a check node depends on its pipeline node and the dispatcher runs dependencies first).
/// <c>RowsMoved</c> on the returned <see cref="NodeResult"/> is the violation count (0 on pass).
/// A failing check surfaces <see cref="PzErrorCode.CheckFailed"/> (PZ0510) naming the violation
/// count plus up to 5 sample offending rows, read from the first <c>QueryArrowAsync</c> batch of
/// the same predicate that counted them — no second query shape per check type, no JSON-extension
/// dependency.
/// </summary>
public sealed class CheckExecutor : INodeExecutor
{
    public async Task<NodeResult> ExecuteAsync(DagNode node, RunContext ctx, CancellationToken ct)
    {
        var def = (CheckNodeDef)node.Definition;
        var check = def.Check;
        var relation = $"staging.{QuoteIdentifier(def.PipelineName)}";

        return check.Type switch
        {
            "not_null" => await RunNotNullAsync(node, ctx.Duck, def, relation, ct).ConfigureAwait(false),
            "unique" => await RunUniqueAsync(node, ctx.Duck, def, relation, ct).ConfigureAwait(false),
            "row_count" => await RunRowCountAsync(node, ctx.Duck, def, relation, ct).ConfigureAwait(false),
            "freshness" => await RunFreshnessAsync(node, ctx, def, relation, ct).ConfigureAwait(false),
            "accepted_values" => await RunAcceptedValuesAsync(node, ctx.Duck, def, relation, ct).ConfigureAwait(false),
            "custom_sql" => await RunCustomSqlAsync(node, ctx.Duck, def, ct).ConfigureAwait(false),
            _ => UnknownType(node, check.Type),
        };
    }

    /// <summary>violations = count of rows where any listed column is null.</summary>
    private static async Task<NodeResult> RunNotNullAsync(
        DagNode node, IDuckSession duck, CheckNodeDef def, string relation, CancellationToken ct)
    {
        var predicate = string.Join(" or ", def.Check.Columns.Select(c => $"{QuoteIdentifier(c)} is null"));
        var violations = await duck.ScalarAsync<long>(
            $"select count(*) from {relation} where {predicate}", ct).ConfigureAwait(false);

        if (violations == 0)
        {
            return Pass(node);
        }

        // The sample query itself is skipped when opted out -- not
        // just its rendering -- so no per-row data is ever read off the failing predicate, let alone
        // formatted into the message that run_results.json/NDJSON both derive from.
        if (!def.SampleValues)
        {
            return FailSuppressed(node, def, violations);
        }

        var sample = await SampleAsync(duck, $"select * from {relation} where {predicate} limit 5", ct)
            .ConfigureAwait(false);
        return Fail(node, def, violations, sample);
    }

    /// <summary>violations = count of key groups with count(*) &gt; 1. Uses an explicit
    /// <c>group by &lt;cols&gt;</c> (not <c>group by all</c>) for engine-version safety.</summary>
    private static async Task<NodeResult> RunUniqueAsync(
        DagNode node, IDuckSession duck, CheckNodeDef def, string relation, CancellationToken ct)
    {
        var columns = string.Join(", ", def.Check.Columns.Select(QuoteIdentifier));
        var dupeGroups = $"select {columns} from {relation} group by {columns} having count(*) > 1";
        var violations = await duck.ScalarAsync<long>($"select count(*) from ({dupeGroups}) q", ct)
            .ConfigureAwait(false);

        if (violations == 0)
        {
            return Pass(node);
        }

        if (!def.SampleValues)
        {
            return FailSuppressed(node, def, violations);
        }

        var sample = await SampleAsync(duck, $"{dupeGroups} limit 5", ct).ConfigureAwait(false);
        return Fail(node, def, violations, sample);
    }

    /// <summary>options {min?, max?} (absent = unbounded); violations = 1 when count(*) falls
    /// outside [min, max]. There is no per-row sample for this check type -- the "sample" is a
    /// short description of the actual count against the violated bound.</summary>
    private static async Task<NodeResult> RunRowCountAsync(
        DagNode node, IDuckSession duck, CheckNodeDef def, string relation, CancellationToken ct)
    {
        var count = await duck.ScalarAsync<long>($"select count(*) from {relation}", ct).ConfigureAwait(false);
        var min = ParseBound(def.Check.Options, "min");
        var max = ParseBound(def.Check.Options, "max");

        if ((min is null || count >= min) && (max is null || count <= max))
        {
            return Pass(node);
        }

        return Fail(node, def, 1, $"row_count={count}");
    }

    /// <summary>violations = 1 when max(column) is null (no rows -- emptiness
    /// IS staleness) or older than now - max_age; `now` comes exclusively from
    /// <see cref="RunContext.EffectiveTime"/> so tests drive a fake clock. Like row_count, the
    /// "sample" is a bound description, never per-row data, so SampleValues does not apply. Option
    /// shapes (exactly one column, parseable positive max_age) are loader-guaranteed (PZ0113).</summary>
    private static async Task<NodeResult> RunFreshnessAsync(
        DagNode node, RunContext ctx, CheckNodeDef def, string relation, CancellationToken ct)
    {
        var column = QuoteIdentifier(def.Check.Columns[0]);
        var maxAgeRaw = def.Check.Options["max_age"]!.ToString()!;
        DurationParser.TryParse(maxAgeRaw, out var maxAge);
        var cutoff = ctx.EffectiveTime.GetUtcNow().UtcDateTime - maxAge;
        var cutoffLiteral = cutoff.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture);

        var violations = await ctx.Duck.ScalarAsync<long>(
            $"select count(*) from (select max({column}) as m from {relation}) q " +
            $"where q.m is null or q.m < timestamp '{cutoffLiteral}'", ct).ConfigureAwait(false);

        if (violations == 0)
        {
            return Pass(node);
        }

        var maxText = await ctx.Duck.ScalarAsync<string>(
            $"select coalesce(cast(max({column}) as varchar), 'null (no rows or all-null column)') from {relation}", ct)
            .ConfigureAwait(false);
        return Fail(node, def, 1, $"max({def.Check.Columns[0]})={maxText}, max_age={maxAgeRaw}");
    }

    /// <summary>violations = count of ROWS whose column value is non-null and
    /// outside the list. NULLs pass deliberately (null-checking is not_null's
    /// job); the predicate spells `is not null and ... not in` out rather than leaning on SQL
    /// three-valued NOT IN semantics. Sample = up to 5 DISTINCT offending values, not whole rows —
    /// the useful answer for an enum check ("what unexpected values appeared?").</summary>
    private static async Task<NodeResult> RunAcceptedValuesAsync(
        DagNode node, IDuckSession duck, CheckNodeDef def, string relation, CancellationToken ct)
    {
        var column = QuoteIdentifier(def.Check.Columns[0]);
        var values = (List<object?>)def.Check.Options["values"]!;
        var literals = string.Join(", ", values.Select(FormatLiteral));
        var predicate = $"{column} is not null and {column} not in ({literals})";

        var violations = await duck.ScalarAsync<long>(
            $"select count(*) from {relation} where {predicate}", ct).ConfigureAwait(false);
        if (violations == 0)
        {
            return Pass(node);
        }

        if (!def.SampleValues)
        {
            return FailSuppressed(node, def, violations);
        }

        var sample = await SampleAsync(duck,
            $"select distinct {column} from {relation} where {predicate} limit 5", ct).ConfigureAwait(false);
        return Fail(node, def, violations, sample);
    }

    /// <summary>The user's SQL runs VERBATIM (no templating)
    /// against the staging DB and returns VIOLATING rows — violations = its row count,
    /// pass = zero rows. Trailing semicolons/whitespace are trimmed so the query wraps as a
    /// subquery. The check depends only on its owning pipeline; referencing other pipelines'
    /// tables is undefined ordering (documented). A malformed query surfaces DuckDB's own error on
    /// the check node.</summary>
    private static async Task<NodeResult> RunCustomSqlAsync(
        DagNode node, IDuckSession duck, CheckNodeDef def, CancellationToken ct)
    {
        var sql = def.Check.Options["sql"]!.ToString()!.TrimEnd().TrimEnd(';').TrimEnd();
        var violations = await duck.ScalarAsync<long>($"select count(*) from ({sql}) q", ct)
            .ConfigureAwait(false);
        if (violations == 0)
        {
            return Pass(node);
        }

        if (!def.SampleValues)
        {
            return FailSuppressed(node, def, violations);
        }

        var sample = await SampleAsync(duck, $"select * from ({sql}) q limit 5", ct).ConfigureAwait(false);
        return Fail(node, def, violations, sample);
    }

    /// <summary>YAML scalar -> typed DuckDB literal. Only loader-shaped scalars arrive here
    /// (PZ0113 refuses everything else at config time); strings use single-quote doubling, the
    /// codebase's literal injection-safety idiom.</summary>
    private static string FormatLiteral(object? value) => value switch
    {
        string s => "'" + s.Replace("'", "''") + "'",
        long l => l.ToString(CultureInfo.InvariantCulture),
        double d => d.ToString(CultureInfo.InvariantCulture),
        bool b => b ? "true" : "false",
        _ => throw new InvalidOperationException(
            $"accepted_values literal of type '{value?.GetType().ToString() ?? "null"}' survived loader validation."),
    };

    private static long? ParseBound(IReadOnlyDictionary<string, object?> options, string key) =>
        options.TryGetValue(key, out var value) && value is not null
            ? Convert.ToInt64(value, CultureInfo.InvariantCulture)
            : null;

    private static NodeResult UnknownType(DagNode node, string type)
    {
        var error = new PzError(PzErrorCode.CheckFailed,
            $"unknown check type '{type}'", null, null,
            "not_null | unique | row_count | freshness | accepted_values | custom_sql");
        return new NodeResult(node.Id, node.Kind, node.Name, NodeStatus.Failed, 0, TimeSpan.Zero, error);
    }

    private static NodeResult Pass(DagNode node) =>
        new(node.Id, node.Kind, node.Name, NodeStatus.Success, 0, TimeSpan.Zero, null);

    private static NodeResult Fail(DagNode node, CheckNodeDef def, long violations, string sample)
    {
        var message = $"check {def.Check.Type} failed on staging.{def.PipelineName}: " +
            $"{violations} violation(s); sample: {sample}";
        var error = new PzError(PzErrorCode.CheckFailed, message, null, null, null);
        return new NodeResult(node.Id, node.Kind, node.Name, NodeStatus.Failed, violations, TimeSpan.Zero, error);
    }

    /// <summary>The PII opt-out path -- violation COUNT still shown,
    /// no per-row VALUES. Since <see cref="NodeResult.Error"/> is the sole source both
    /// <c>RunResultsWriter</c> (run_results.json) and <c>RunEventPublisher</c> (the NDJSON event stream)
    /// read the failure message from, suppressing the sample here suppresses it from both.</summary>
    private static NodeResult FailSuppressed(DagNode node, CheckNodeDef def, long violations)
    {
        var message = $"check {def.Check.Type} failed on staging.{def.PipelineName}: " +
            $"{violations} violation(s) (samples disabled)";
        var error = new PzError(PzErrorCode.CheckFailed, message, null, null, null);
        return new NodeResult(node.Id, node.Kind, node.Name, NodeStatus.Failed, violations, TimeSpan.Zero, error);
    }

    /// <summary>Reads only the FIRST Arrow batch of <paramref name="sql"/> (a `limit 5` query never
    /// produces more than one batch in practice, but this only ever looks at the first regardless)
    /// and formats its rows as <c>{col=value, ...}</c> joined with <c>; </c>.</summary>
    private static async Task<string> SampleAsync(IDuckSession duck, string sql, CancellationToken ct)
    {
        await foreach (var batch in duck.QueryArrowAsync(sql, ct: ct).ConfigureAwait(false))
        {
            using (batch)
            {
                return FormatBatch(batch);
            }
        }

        return string.Empty;
    }

    private static string FormatBatch(RecordBatch batch)
    {
        var fields = batch.Schema.FieldsList;
        var rows = new List<string>(batch.Length);
        for (var r = 0; r < batch.Length; r++)
        {
            var cells = new List<string>(fields.Count);
            for (var c = 0; c < fields.Count; c++)
            {
                cells.Add($"{fields[c].Name}={FormatValue(batch.Column(c), r)}");
            }

            rows.Add("{" + string.Join(", ", cells) + "}");
        }

        return string.Join("; ", rows);
    }

    /// <summary>Same v0 type matrix <c>ArrowInterop.ToDuckDbType</c>/<c>NormalizeNativeArrowSchema</c>
    /// already commit to elsewhere in the engine; an unmapped array type throws NotSupportedException
    /// naming it, rather than silently rendering something misleading.</summary>
    private static string FormatValue(IArrowArray array, int index)
    {
        if (array.IsNull(index))
        {
            return "null";
        }

        return array switch
        {
            Int32Array a => a.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture),
            Int64Array a => a.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture),
            DoubleArray a => a.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture),
            Decimal128Array a => a.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture),
            StringArray a => a.GetString(index),
            BooleanArray a => a.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture),
            Date32Array a => a.GetDateOnly(index)!.Value.ToString("O", CultureInfo.InvariantCulture),
            TimestampArray a => a.GetTimestamp(index)!.Value.ToString("O", CultureInfo.InvariantCulture),
            _ => throw new NotSupportedException(
                $"Arrow array type '{array.GetType()}' has no sample formatter in the v0 type matrix."),
        };
    }

    /// <summary>Quotes a single (dot-free) identifier by reusing <c>ArrowInterop.QuoteQualified</c> --
    /// the engine's one existing identifier-quoting helper -- rather than duplicating quoting logic
    /// here. It splits on '.', which is a no-op for a plain name.</summary>
    private static string QuoteIdentifier(string identifier) => ArrowInterop.QuoteQualified(identifier);
}
