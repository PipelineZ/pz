using Pz.Core.Model;
using Pz.Core.Templating;
using Pz.Core.Validation;

namespace Pz.Core.Dag;

/// <summary>SQL-declared incremental: folds every pipeline's <c>watermark()</c>
/// comparisons -- parsed per pipeline by <see cref="ISqlAstReader"/> against the rendered
/// sentinels (<see cref="RenderResult.WatermarkRefs"/>) -- into synthesized <see cref="IncrementalDef"/>s
/// keyed by (source, dataset), plus the rewrite plumbing (NULL-guarded <c>RewrittenSql</c> and the
/// per-pipeline <see cref="WatermarkSubstitution"/> maps) DagCompiler/PipelineExecutor consume at
/// execution time. Pure function -- never throws; every structural problem becomes an aggregated
/// <see cref="PzError"/> in <see cref="Result.Errors"/> so validation reports all of them, never
/// fail-one-at-a-time (repo error philosophy).
///
/// Structural checks: <c>PZ0224</c> (a reader-reported shape violation, a <c>watermark()</c> call naming
/// a dataset the pipeline never reads, or a comparison whose resolved table doesn't trace to the claimed
/// dataset) and the "cursor absent from <c>columns:</c>" case of <c>PZ0227</c>.
///
/// The folding rules all fire per (source, dataset) group -- at most one PZ0225 per dataset (see the
/// group loop's ordering of checks): a YAML-declared <see cref="IncrementalDef"/> coexisting with a SQL
/// declaration (<c>PZ0225</c>, "either/or"; the windowed-YAML case takes precedence over the generic
/// either/or message when both apply, since a windowed dataset always also has a non-null
/// <see cref="IncrementalDef"/>), and a recognized cursor whose declared type falls outside the allowed
/// set (<c>PZ0227</c>'s "mistyped cursor" case). Consistency guard: any of these firing for a dataset
/// withholds that dataset's <see cref="Result.Synthesized"/> entry (there is no non-ambiguous
/// incremental to synthesize) but leaves already-recognized pipelines'
/// <see cref="Result.RewrittenSql"/> in place.
/// </summary>
public static class WatermarkInference
{
    public sealed record PipelineInput(PipelineDef Pipeline, RenderResult Rendered, string AssembledSql);

    /// <summary>WARNING: <paramref name="RewrittenSql"/> and <paramref name="Substitutions"/> must not be
    /// consumed unless <paramref name="Errors"/> is empty. On a PZ0227 (undeclared cursor), a pipeline's
    /// comparison can still be structurally recognized -- so <see cref="RewrittenSql"/> holds SQL with the
    /// raw <c>{{ watermark(...) }}</c> sentinel rewritten to a placeholder, but with NO corresponding entry
    /// in <see cref="Substitutions"/> to fill it back in. It is the compile phase's stop-on-errors gate
    /// (validation reports all errors, never proceeds past them) that makes this pairing safe -- callers
    /// downstream of this method must never see one without the other having been checked first.</summary>
    /// <param name="RewrittenSql">Per-pipeline SQL with watermark() calls replaced by sentinel
    /// placeholders. See the type-level warning: not safe to use unless <see cref="Errors"/> is empty.</param>
    /// <param name="Substitutions">Per-pipeline sentinel -> source/dataset/cursor-type substitution maps
    /// consumed to fill in <see cref="RewrittenSql"/>'s placeholders. See the type-level warning: not safe
    /// to use unless <see cref="Errors"/> is empty.</param>
    public sealed record Result(
        IReadOnlyDictionary<(string Source, string Dataset), IncrementalDef> Synthesized,
        IReadOnlyDictionary<string, string> RewrittenSql,
        IReadOnlyDictionary<string, IReadOnlyList<WatermarkSubstitution>> Substitutions,
        IReadOnlyList<PzError> Errors);

    // Ceilings (< / <=) are recognized too, so the hint names all four operators -- a hint advertising
    // only "> or >=" would send an author fixing a shape that is already legal.
    private const string AcceptedShapeHint =
        "accepted shape: <cursor column> > / >= / < / <= <expression containing {{ watermark(source, dataset) }}>";

    private static readonly string[] AllowedCursorTypes = ["int", "bigint", "decimal", "date", "timestamp"];
    private const string AllowedCursorTypesList = "int, bigint, decimal, date, timestamp";

    /// <summary>One comparison that passed structural validation (dependency + table-tracing checks) --
    /// the unit the per-dataset fold and the per-pipeline substitution map both consume.</summary>
    private sealed record RecognizedComparison(
        string Pipeline, string PipelineFilePath, string Source, string Dataset,
        string Column, bool Inclusive, string ValueExprSql, string Sentinel, bool IsUpper);

    public static Result Run(IReadOnlyList<PipelineInput> pipelines,
        IReadOnlyDictionary<string, ConnectionDef> sourcesByName, ISqlAstReader sqlAst)
    {
        var errors = new List<PzError>();
        var rewrittenSql = new Dictionary<string, string>();
        var recognizedByPipeline = new Dictionary<string, List<RecognizedComparison>>();
        var allRecognized = new List<RecognizedComparison>();

        // Step 1-3: analyze each pipeline (given order) and structurally validate every comparison.
        foreach (var input in pipelines)
        {
            var refs = input.Rendered.WatermarkRefs;
            if (refs.Count == 0)
            {
                continue;
            }

            var refsBySentinel = refs.GroupBy(r => r.Sentinel).ToDictionary(g => g.Key, g => g.First());
            // Explicit-ordering convention (see DagCompiler's OrderDeterministically): source sentinels
            // from the already-ordered refs list, not from Dictionary key enumeration, which is an
            // implementation detail and not a stable order guarantee.
            var sentinels = refs.Select(r => r.Sentinel).Distinct(StringComparer.Ordinal).ToList();
            var analysis = sqlAst.Analyze(input.AssembledSql, sentinels);

            foreach (var violation in analysis.Violations)
            {
                errors.Add(new PzError(PzErrorCode.UnrecognizedWatermarkExpression,
                    $"pipeline '{input.Pipeline.Name}': {violation.Reason}",
                    input.Pipeline.FilePath, null, AcceptedShapeHint));
            }

            var pipelineRecognized = new List<RecognizedComparison>();
            foreach (var comparison in analysis.Comparisons)
            {
                if (!refsBySentinel.TryGetValue(comparison.Sentinel, out var wref))
                {
                    // The reader is only ever asked about sentinels this pipeline's own watermark()
                    // calls produced, so a comparison keyed on an unknown sentinel cannot happen from a
                    // real ISqlAstReader; skip defensively rather than synthesizing off an unknown ref.
                    continue;
                }

                // By (source, dataset), not record equality: a DepRef.Source also carries the call
                // site's read options, and a source() written with kwargs is still the same read this
                // watermark() names.
                if (!input.Rendered.Dependencies.OfType<DepRef.Source>()
                        .Any(d => d.SourceName == wref.SourceName && d.Dataset == wref.Dataset))
                {
                    errors.Add(new PzError(PzErrorCode.UnrecognizedWatermarkExpression,
                        $"pipeline '{input.Pipeline.Name}' does not read dataset '{wref.SourceName}.{wref.Dataset}' " +
                        $"— add {{{{ source('{wref.SourceName}', '{wref.Dataset}') }}}} or remove the watermark() call",
                        input.Pipeline.FilePath, null, AcceptedShapeHint));
                    continue;
                }

                var expectedTable = StagingName.ForSourceLoad(wref.SourceName, wref.Dataset);
                if (comparison.ColumnTable != expectedTable)
                {
                    errors.Add(new PzError(PzErrorCode.UnrecognizedWatermarkExpression,
                        $"pipeline '{input.Pipeline.Name}': cursor column '{comparison.Column}' resolves to table " +
                        $"'{comparison.ColumnTable}', which does not trace to '{wref.SourceName}.{wref.Dataset}' " +
                        $"(expected '{expectedTable}')",
                        input.Pipeline.FilePath, null, AcceptedShapeHint));
                    continue;
                }

                pipelineRecognized.Add(new RecognizedComparison(input.Pipeline.Name, input.Pipeline.FilePath,
                    wref.SourceName, wref.Dataset, comparison.Column, comparison.Inclusive,
                    comparison.ValueExprSql, comparison.Sentinel, comparison.IsUpper));
            }

            if (pipelineRecognized.Count > 0)
            {
                rewrittenSql[input.Pipeline.Name] = analysis.RewrittenSql;
                recognizedByPipeline[input.Pipeline.Name] = pipelineRecognized;
                allRecognized.AddRange(pipelineRecognized);
            }
        }

        // Step 4: fold recognized comparisons per (source, dataset), sorted ordinal for a byte-stable
        // PZ0227 error order. First comparison encountered wins the cursor column.
        var synthesized = new Dictionary<(string Source, string Dataset), IncrementalDef>();
        var cursorTypeByDataset = new Dictionary<(string Source, string Dataset), string?>();

        var groups = allRecognized
            .GroupBy(r => (r.Source, r.Dataset))
            .OrderBy(g => g.Key.Source, StringComparer.Ordinal)
            .ThenBy(g => g.Key.Dataset, StringComparer.Ordinal);

        foreach (var group in groups)
        {
            var items = group.ToList();

            sourcesByName.TryGetValue(group.Key.Source, out var source);
            var dataset = source?.Datasets.FirstOrDefault(d => d.Name == group.Key.Dataset);
            var filePath = source?.FilePath ?? items[0].PipelineFilePath;
            var declaringPipelines = string.Join(", ",
                items.Select(i => i.Pipeline).Distinct(StringComparer.Ordinal).OrderBy(n => n, StringComparer.Ordinal));

            // A `sync:` dataset (opaque continuation token) referenced by
            // watermark() in pipeline SQL. DagCompiler's PZ0315 only ever sees the two YAML blocks
            // (incremental: + sync:) on the SAME dataset -- it cannot see a SQL-declared ordered cursor
            // synthesized here, so a `sync: {}` dataset with no YAML `incremental:` block sails through
            // that check and would otherwise become BOTH kinds at execution (PriorSyncState AND SQL
            // watermark bounds both stamped; both advancement passes persist). Checked before the
            // YAML either/or below so a sync dataset always gets this specific message.
            // `SyncMode is { Mode: SyncMode.Auto }`
            // only matches an EXPLICITLY declared `sync: {mode: auto}` block -- SyncMode is null
            // (so this pattern doesn't match) when the block is omitted ("implicit auto"). Pz.Core has no
            // connector-capability access, so an implicit-auto dataset that a connector resolves to Feed
            // at plan time can never trip this conflict -- the "declares 'sync:' ..." message below is
            // only ever true for an explicit declaration in the first place.
            // `SyncMode.Cdc` is likewise opaque
            // change-capture state, never an ordered cursor -- a declared `sync: {mode: cdc}` dataset
            // referenced by a SQL watermark() trips the same conflict as `mode: auto`.
            if (dataset?.SyncMode is { Mode: SyncMode.Auto or SyncMode.Cdc })
            {
                errors.Add(new PzError(PzErrorCode.SyncStateConflict,
                    $"source '{group.Key.Source}.{group.Key.Dataset}': dataset declares 'sync:' in " +
                    $"'{filePath}' and ALSO has a SQL watermark() declaration in pipeline(s) " +
                    $"'{declaringPipelines}' -- a dataset is either ordered-cursor incremental or opaque " +
                    "sync-state, not both",
                    filePath, null,
                    $"remove the watermark() call(s) in pipeline(s) '{declaringPipelines}', or remove the " +
                    "dataset's 'sync:' block and rely on the SQL watermark() declaration instead"));

                continue;
            }

            // PZ0225 "either/or": a YAML-declared incremental: and a SQL watermark() declaration
            // cannot coexist for the same dataset. A windowed YAML incremental (MaxWindow non-null) always
            // also has a non-null Incremental, so this branch would fire for it too -- the windowed
            // sub-case is checked first and reports its own message instead, keeping this at one PZ0225
            // per dataset.
            if (dataset?.SyncMode?.Incremental is { } incremental)
            {
                if (incremental.MaxWindow is not null)
                {
                    errors.Add(new PzError(PzErrorCode.ConflictingIncrementalDeclaration,
                        $"source '{group.Key.Source}.{group.Key.Dataset}': dataset declares a bounded " +
                        $"incremental window (max_window) in '{filePath}' and ALSO has a SQL watermark() " +
                        $"declaration in pipeline(s) '{declaringPipelines}' — windowed backfill is YAML-only",
                        filePath, null,
                        $"remove the watermark() call(s) in pipeline(s) '{declaringPipelines}' and rely on " +
                        "the YAML incremental: config for windowed backfill"));
                }
                else
                {
                    errors.Add(new PzError(PzErrorCode.ConflictingIncrementalDeclaration,
                        $"source '{group.Key.Source}.{group.Key.Dataset}': dataset declares incremental: in " +
                        $"'{filePath}' and ALSO has a SQL watermark() declaration in pipeline(s) " +
                        $"'{declaringPipelines}'",
                        filePath, null,
                        "pick one route: either the YAML incremental: config or SQL watermark(), not both"));
                }

                continue;
            }

            // There is no cross-pipeline cursor-disagreement check: PZ0349 refuses a source read by more
            // than one pipeline, so two pipelines cannot infer different cursor columns for one dataset.
            // The fold below handles several comparisons within ONE pipeline, which stays reachable.
            var cursor = items[0].Column;
            string? cursorType = dataset?.Columns is { } columns && columns.TryGetValue(cursor, out var declaredType)
                ? declaredType
                : null;

            // A columns: contract prunes reads to exactly its columns, so a cursor outside it would never
            // be extracted -- still PZ0227. No contract at all prunes nothing: the cursor IS extracted and
            // its type is discoverable at run time from the stored watermark (PipelineExecutor types the
            // literal from stored.TypeName and renders NULL on a first run), so compile carries a null
            // type rather than demanding a declaration.
            if (cursorType is null && dataset?.Columns is not null)
            {
                errors.Add(new PzError(PzErrorCode.WatermarkCursorUndeclared,
                    $"source '{group.Key.Source}.{group.Key.Dataset}': watermark() cursor '{cursor}' is not in the " +
                    "dataset's columns: contract",
                    filePath, null,
                    $"add '{cursor}' to columns: with its DuckDB type, or remove the columns: contract so the " +
                    "cursor's type is discovered at run time"));
                continue;
            }

            // Ordinal (exact-lowercase), matching the YAML route's stage-0 check
            // (DagCompiler.Array.IndexOf(AllowedCursorTypes, ...)) and every downstream consumer
            // (CursorLiterals.Typed, EvaluateSqlBoundsAsync, WindowMath, PipelineExecutor). A
            // case-insensitive check here would let a mixed-case 'TIMESTAMP' compile then fail at
            // runtime — PZ0227 must reject it at compile time, listing the allowed lowercase set.
            if (cursorType is not null && !AllowedCursorTypes.Contains(cursorType, StringComparer.Ordinal))
            {
                errors.Add(new PzError(PzErrorCode.WatermarkCursorUndeclared,
                    $"source '{group.Key.Source}.{group.Key.Dataset}': watermark() cursor '{cursor}' has " +
                    $"declared type '{cursorType}', which is not one of the allowed incremental cursor " +
                    $"types ({AllowedCursorTypesList})",
                    filePath, null,
                    $"change '{cursor}''s declared type in columns: to one of {AllowedCursorTypesList}, or " +
                    "remove the watermark() call(s)"));
                continue;
            }

            // A ceiling alone never resumes -- the first run would advance the watermark straight
            // to the ceiling and every later run would extract nothing. Checked here, inside the same group
            // loop as every other per-dataset rule, so it aggregates with them rather than short-circuiting.
            if (items.All(i => i.IsUpper))
            {
                var ceiling = items.OrderBy(i => i.ValueExprSql, StringComparer.Ordinal).First();
                // Show the author their own call, not the internal placeholder the sentinel rewrite left
                // behind -- `__pz_watermark__raw__orders__` appears in no file anyone wrote.
                var ceilingExpr = ceiling.ValueExprSql.Replace($"'{ceiling.Sentinel}'",
                    $"{{{{ watermark('{group.Key.Source}', '{group.Key.Dataset}') }}}}", StringComparison.Ordinal);
                errors.Add(new PzError(PzErrorCode.WatermarkCeilingWithoutFloor,
                    $"source '{group.Key.Source}.{group.Key.Dataset}': watermark() declares an upper bound " +
                    $"on cursor '{cursor}' ({ceilingExpr}) but no lower bound",
                    filePath, null,
                    $"add a lower bound, e.g. `{cursor} > {{{{ watermark('{group.Key.Source}', " +
                    $"'{group.Key.Dataset}') }}}}`, or drop the watermark() call so the comparison is " +
                    "pushed as an ordinary filter instead"));
                continue;
            }

            cursorTypeByDataset[group.Key] = cursorType;
            var bounds = items
                .Select(i => new SqlWatermarkBound(i.Pipeline, i.Inclusive, i.ValueExprSql, i.Sentinel, i.IsUpper))
                .ToList();
            synthesized[group.Key] = new IncrementalDef(cursor, DeclaredInSql: true, SqlBounds: bounds);
        }

        // There is no consumer-completeness pass (PZ0226, retired): the condition it flagged needs two
        // pipelines reading one dataset, which PZ0349 refuses outright in DagCompiler stage 6.

        // Step 5: per-pipeline substitution maps -- one entry per distinct sentinel the pipeline
        // recognized, omitted for sentinels whose dataset the group loop above rejected outright
        // (PZ0225/PZ0227 already reported the cause). A dataset accepted with no declared type is
        // present here with a null CursorType: PipelineExecutor types the literal from the stored
        // watermark instead, and renders NULL on a first run.
        var substitutions = new Dictionary<string, IReadOnlyList<WatermarkSubstitution>>();
        foreach (var (pipelineName, recognized) in recognizedByPipeline)
        {
            var subs = new List<WatermarkSubstitution>();
            var seenSentinels = new HashSet<string>();
            foreach (var r in recognized)
            {
                if (!seenSentinels.Add(r.Sentinel))
                {
                    continue;
                }

                if (cursorTypeByDataset.TryGetValue((r.Source, r.Dataset), out var cursorType))
                {
                    subs.Add(new WatermarkSubstitution(r.Sentinel, r.Source, r.Dataset, cursorType));
                }
            }

            if (subs.Count > 0)
            {
                substitutions[pipelineName] = subs;
            }
        }

        return new Result(synthesized, rewrittenSql, substitutions, errors);
    }
}
