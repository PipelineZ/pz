using System.Globalization;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Paths;
using Pz.Core.Incremental;
using Pz.Core.Model;
using Pz.Core.Templating;
using Pz.Core.Validation;

namespace Pz.Core.Dag;

/// <summary>
/// Turns a loaded+rendered <see cref="PzProject"/> into a <see cref="CompiledDag"/> of
/// content-addressed nodes.
/// </summary>
public static class DagCompiler
{
    /// <summary>Allowed <c>columns:</c> contract types for an <c>incremental: cursor</c>
    /// -- a subset of the full v0 contract-type matrix (int, bigint, double,
    /// decimal, varchar, boolean, date, timestamp; see Pz.Engine's <c>ContractTypes</c>, which
    /// Pz.Core cannot reference: Pz.Engine depends on Pz.Core, not the reverse). double/varchar/boolean
    /// have no well-defined "greater than" ordering a watermark cursor can rely on, so they're excluded.</summary>
    private static readonly string[] AllowedCursorTypes = CursorContract.AllowedTypes;

    private static readonly string AllowedCursorTypesText = string.Join(", ", AllowedCursorTypes);

    /// <summary>
    /// <paramref name="notices"/>, if provided, collects non-fatal informational messages produced
    /// while compiling -- currently only "cursor unverified" (see stage 0 below). Same channel as
    /// other advisory-not-error findings in the compile pipeline
    /// (<c>SqlDryCompiler.DryCompileResult.UndeclaredDatasets</c>, surfaced by <c>ValidateCommand</c>
    /// as "note: ..." lines).
    /// <para><paramref name="sqlAst"/> is the SQL parser seam watermark() inference (stage 6b)
    /// consumes — Pz.Cli passes <c>DuckDbSqlAstReader</c>; Pz.Core cannot reference Pz.DuckDb, so a test
    /// passes a stub. Only required when a pipeline actually calls watermark(): a project with no
    /// watermark() references compiles with <paramref name="sqlAst"/> null; one that uses watermark()
    /// without a reader throws <see cref="InvalidOperationException"/>.</para>
    /// </summary>
    public static CompiledDag Compile(PzProject project, RenderContext ctx, List<string>? notices = null,
        ISqlAstReader? sqlAst = null)
    {
        var pipelinesByName = project.Pipelines.ToDictionary(p => p.Name);
        // One map, because a connection is one place. Direction is
        // decided per call site, so the read loops below filter on Datasets and the write loops on
        // Outputs -- a connection used in one direction only simply has an empty list on the other.
        var connectionsByName = project.Connections.ToDictionary(s => s.Name);

        // 1. Render every pipeline (ephemeral included — its SQL is needed for inlining and
        //    its Dependencies are needed for both ref/source validation and inheritance).
        var rendered = new Dictionary<string, RenderResult>();
        foreach (var pipeline in project.Pipelines)
        {
            rendered[pipeline.Name] = TemplateRenderer.Render(pipeline, ctx);
        }

        // 1b. Extract the fixed `INSERT INTO {{ sink(...) }}` prefix from any pipeline carrying an
        //     inline sink() binding. PZ0208 covers every way a sink() call can
        //     be malformed: more than one call in a pipeline, a call on an ephemeral pipeline (which
        //     produces no node for a sink to depend on), or a call that isn't the pipeline's mandatory
        //     leading INSERT statement. On success `rendered[name]` is replaced by the extracted-SQL
        //     RenderResult (InlineBindings carried over unchanged via `with`) so every later stage —
        //     and the compiled pipeline node itself — sees only the SELECT that actually runs.
        var sinkCallErrors = new List<PzError>();
        foreach (var pipeline in project.Pipelines)
        {
            var result = rendered[pipeline.Name];
            if (result.InlineBindings.Count == 0)
            {
                continue;
            }

            if (pipeline.Materialization == "ephemeral")
            {
                var b = result.InlineBindings[0];
                sinkCallErrors.Add(new PzError(PzErrorCode.InvalidSinkCall,
                    $"pipeline '{pipeline.Name}' is ephemeral but calls sink('{b.Sink}', '{b.Output}') " +
                    "— ephemeral pipelines produce no node for a sink to depend on",
                    pipeline.FilePath, null, InsertFormHint));
                continue;
            }

            var duplicate = result.InlineBindings
                .GroupBy(b => (b.Sink, b.Output))
                .FirstOrDefault(g => g.Count() > 1);
            if (duplicate is not null)
            {
                sinkCallErrors.Add(new PzError(PzErrorCode.InvalidSinkCall,
                    $"pipeline '{pipeline.Name}' binds sink('{duplicate.Key.Sink}', '{duplicate.Key.Output}') " +
                    "more than once — list each sink output at most once",
                    pipeline.FilePath, null, InsertFormHint));
                continue;
            }

            var markers = result.InlineBindings
                .Select(b => $"__pz_sink__{b.Sink}__{b.Output}__").ToList();
            var extracted = TryExtractInsertPrefix(result.Sql, markers);
            if (extracted is null)
            {
                var first = result.InlineBindings[0];
                sinkCallErrors.Add(new PzError(PzErrorCode.InvalidSinkCall,
                    $"pipeline '{pipeline.Name}' calls sink('{first.Sink}', '{first.Output}') but not as the " +
                    "pipeline's leading INSERT INTO {{ sink(...) }} statement",
                    pipeline.FilePath, null, InsertFormHint));
                continue;
            }

            rendered[pipeline.Name] = result with { Sql = extracted };
        }

        if (sinkCallErrors.Count > 0)
        {
            throw new PzValidationException(sinkCallErrors);
        }

        // 1c. A sink's Outputs list is synthesized here from the inline sink() bindings, not loaded from
        //     YAML -- write options live at the call site. Every later stage (the mode/keys checks below,
        //     path templating, the node build, the delivery and CDC matrices, SinkWriteExecutor) reads
        //     sink.Outputs. Stages 1/1b therefore run BEFORE stage 0, which needs the rendered call sites.
        //     A binding naming an unknown sink is skipped here and reported by stage 5 as PZ0201.
        //     Ordering is by output name so the node list stays deterministic regardless of which
        //     pipeline rendered first.
        //     Keyed by (sink, output) and first-write-wins over pipelines visited in name order, so two
        //     pipelines claiming one output with DIFFERENT kwargs still yield one OutputDef -- stage 5
        //     then reports that as PZ0206 once rather than once per claimant.
        var outputsBySink = new Dictionary<(string Sink, string Output), OutputDef>();
        var writeSurfaceErrors = new List<PzError>();
        foreach (var pipeline in project.Pipelines
            .Where(p => p.Materialization != "ephemeral")
            .OrderBy(p => p.Name, StringComparer.Ordinal))
        {
            foreach (var binding in rendered[pipeline.Name].InlineBindings)
            {
                if (!connectionsByName.ContainsKey(binding.Sink))
                {
                    continue;
                }

                // An entity-side's options live in `entities:` OR at the call site -- never split,
                // never merged. There is deliberately no precedence
                // rule: a reader of one file must see the whole story for that entity-side.
                var yamlWrite = connectionsByName[binding.Sink].EntityWrites.GetValueOrDefault(binding.Output);
                if (yamlWrite is not null && binding.DeclaredAtCallSite)
                {
                    writeSurfaceErrors.Add(new PzError(PzErrorCode.WriteSurfaceSplit,
                        $"sink '{binding.Sink}.{binding.Output}' declares write options in " +
                        "connections.yml AND at its sink() call site.",
                        pipeline.FilePath, null,
                        "declare in one place: drop the kwargs, or drop the `write:` block"));
                    continue;
                }

                var w = yamlWrite ?? binding.Write;
                // Input stays "" here and is synthesized in stage 10 from the claimant pipeline.
                outputsBySink.TryAdd((binding.Sink, binding.Output),
                    new OutputDef(binding.Output, string.Empty, w.Mode, w.SchemaPolicy, w.Options,
                        w.Keys, w.Retry, w.AcceptDuplicates, w.OnDelete));
            }
        }

        //     1d. The read side of the same rule. An entity exists
        //     because something reads or writes it, so a source() naming an entity the connection does
        //     not declare synthesizes one from the call's kwargs -- exactly as 1c synthesizes an
        //     OutputDef. Declaring BOTH is PZ0341, the same either/or the write side enforces: there is
        //     no effective-config assembly in either direction.
        //     Ordered by pipeline then entity so the synthesized list is deterministic.
        //     Every pipeline, including ephemerals, and each one's OWN rendered dependencies rather than
        //     EffectiveDependencies: a source() call declares its entity wherever it is written, and
        //     following ephemeral chains here would resolve ref()s that stage 2 has not validated yet.
        var readsByConnection = new Dictionary<string, Dictionary<string, DatasetDef>>(StringComparer.Ordinal);
        foreach (var pipeline in project.Pipelines.OrderBy(p => p.Name, StringComparer.Ordinal))
        {
            foreach (var dep in rendered[pipeline.Name].Dependencies
                .OfType<DepRef.Source>()
                .OrderBy(d => d.Dataset, StringComparer.Ordinal))
            {
                if (!connectionsByName.TryGetValue(dep.SourceName, out var connection))
                {
                    continue; // unknown connection: stage 2 reports it as PZ0201
                }

                var declaredInYaml = connection.Datasets.Any(d => d.Name == dep.Dataset);
                if (declaredInYaml && dep.DeclaredAtCallSite)
                {
                    writeSurfaceErrors.Add(new PzError(PzErrorCode.WriteSurfaceSplit,
                        $"source '{dep.SourceName}.{dep.Dataset}' declares read options in " +
                        "connections.yml AND at its source() call site.",
                        pipeline.FilePath, null,
                        "declare in one place: drop the kwargs, or drop the `read:` block"));
                    continue;
                }

                if (declaredInYaml)
                {
                    continue;
                }

                if (!readsByConnection.TryGetValue(dep.SourceName, out var synthesized))
                {
                    synthesized = new Dictionary<string, DatasetDef>(StringComparer.Ordinal);
                    readsByConnection[dep.SourceName] = synthesized;
                }

                var r = dep.Read;
                synthesized.TryAdd(dep.Dataset,
                    new DatasetDef(dep.Dataset, r.Options, r.Columns, r.Sync, r.Retry));
            }
        }

        if (writeSurfaceErrors.Count > 0)
        {
            throw new PzValidationException(writeSurfaceErrors);
        }

        project = project with
        {
            Connections = [.. project.Connections.Select(s => s with
            {
                Outputs = [.. outputsBySink
                    .Where(kv => kv.Key.Sink == s.Name)
                    .OrderBy(kv => kv.Key.Output, StringComparer.Ordinal)
                    .Select(kv => kv.Value)],
                Datasets = readsByConnection.TryGetValue(s.Name, out var extra)
                    ? [.. s.Datasets, .. extra.OrderBy(kv => kv.Key, StringComparer.Ordinal).Select(kv => kv.Value)]
                    : s.Datasets,
            })],
        };
        connectionsByName = project.Connections.ToDictionary(s => s.Name);


        // 0. Incremental cursor + merge-keys validation. Runs after stage 1c because the write options
        //    it validates come from the sink() call site. All aggregate (report every violation, not
        //    just the first) before throwing, matching every other stage below.
        //    There is no PZ0228 "unknown write mode" check here: SinkFunction refuses a strategy
        //    outside replace/append/merge at the call site (PZ0334), so an OutputDef cannot reach here
        //    carrying one.
        var incrementalErrors = new List<PzError>();
        foreach (var sink in project.Connections)
        {
            foreach (var output in sink.Outputs)
            {
                var isMerge = output.Mode == "merge";
                if (isMerge && output.Keys.Count == 0)
                {
                    incrementalErrors.Add(new PzError(PzErrorCode.MergeRequiresKeys,
                        $"sink '{sink.Name}.{output.Name}' has strategy: 'merge' but declares no keys -- " +
                        "merge needs at least one key column to match rows by",
                        sink.FilePath, null,
                        $"sink('{sink.Name}', '{output.Name}', strategy: 'merge', keys: ['<column>'])"));
                }
                else if (!isMerge && output.Keys.Count > 0)
                {
                    incrementalErrors.Add(new PzError(PzErrorCode.KeysWithoutMerge,
                        $"sink '{sink.Name}.{output.Name}' declares keys: [{string.Join(", ", output.Keys)}] " +
                        $"but strategy is '{output.Mode}', not merge -- keys only applies to strategy: 'merge'",
                        sink.FilePath, null,
                        $"sink('{sink.Name}', '{output.Name}', strategy: 'merge', " +
                        $"keys: ['{string.Join("', '", output.Keys)}']), or drop keys"));
                }
            }
        }

        // Bounded-window semantic rules ride along in the same pass, source
        // by source, dataset by dataset -- they need the very same `declaredType` lookup PZ0212 already
        // computes. Datasets whose Incremental gets a successful canonicalization are collected into
        // `rewrittenConnections` and swapped into `project` below (only once the whole pass is error-free),
        // so every later stage -- including the SourceLoad node builder in stage 7 -- sees canonical
        // Initial/Until instead of raw user variants. NodeId hashing (stage 7) never reads Incremental,
        // so this rewrite cannot change any NodeId.
        var rewrittenConnections = new List<ConnectionDef>(project.Connections.Count);
        foreach (var source in project.Connections)
        {
            List<DatasetDef>? rewrittenDatasets = null;
            for (var di = 0; di < source.Datasets.Count; di++)
            {
                var dataset = source.Datasets[di];
                // No PZ0315 "declares both incremental: and sync:" check here: SyncModeDef carries
                // exactly one Mode, so declaring two read modes is not representable at the loader
                // (see PzErrorCode.SyncStateConflict's doc comment).
                if (dataset.SyncMode?.Incremental is not { } incremental)
                {
                    continue;
                }

                // A declared-descending feed with a page
                // cap can truncate mid-slice, and MAX(cursor) advancement would skip the unfetched
                // middle -- refuse at compile. Undeclared ordering + a cap only warns: the runtime
                // guard (HttpPartition) decides by observation. Reads `cursor_order`/`max_pages`
                // through the same option convention CursorContract documents for cursor/cursor_type.
                var cursorOrder = dataset.Options.TryGetValue("cursor_order", out var cursorOrderObj)
                    ? cursorOrderObj?.ToString()
                    : null;
                if (dataset.Options.ContainsKey("max_pages"))
                {
                    if (cursorOrder == "desc")
                    {
                        incrementalErrors.Add(new PzError(PzErrorCode.DescendingCursorTruncatable,
                            $"source '{source.Name}.{dataset.Name}': 'max_pages' on a descending " +
                            "incremental feed (cursor_order: desc) — a truncated crawl would advance " +
                            "the watermark past unfetched rows", source.FilePath, null,
                            "remove max_pages so slices run to completion, or point at an ascending endpoint"));
                    }
                    else if (cursorOrder is null)
                    {
                        notices?.Add($"source '{source.Name}.{dataset.Name}': 'max_pages' with " +
                            "undeclared cursor ordering — a descending feed fails at run time if a " +
                            "crawl truncates; declare cursor_order to check this at compile time");
                    }
                }

                var cursor = incremental.Cursor;
                string? declaredType = null;
                if (dataset.Columns is not { Count: > 0 } columns)
                {
                    notices?.Add(
                        $"source '{source.Name}.{dataset.Name}': cursor '{cursor}' unverified until --connect / first run");
                    // Raw-envelope connectors (http) type the cursor via `cursor`/`cursor_type`
                    // dataset options instead of a columns: contract — accept that route for the
                    // window math below (CursorContract documents the convention).
                    declaredType = CursorContract.ResolveDeclaredType(dataset);
                }
                else if (!columns.TryGetValue(cursor, out var cursorType))
                {
                    // A windowed dataset (max_window declared) hits the
                    // very same `declaredType is null` condition in rule 3 below, which reports PZ0213
                    // for this exact root cause -- PZ0213 subsumes PZ0212 here so the dataset gets ONE
                    // error, not two codes naming the same misconfiguration.
                    if (incremental.MaxWindow is null)
                    {
                        incrementalErrors.Add(new PzError(PzErrorCode.CursorInvalid,
                            $"source '{source.Name}.{dataset.Name}' declares incremental cursor '{cursor}' but it is " +
                            $"not in the dataset's columns: contract -- allowed cursor types: {AllowedCursorTypesText}",
                            source.FilePath, null,
                            $"add '{cursor}' to columns: with one of the allowed cursor types ({AllowedCursorTypesText})"));
                    }
                }
                else if (Array.IndexOf(AllowedCursorTypes, cursorType) < 0)
                {
                    // Same subsumption rationale as above.
                    if (incremental.MaxWindow is null)
                    {
                        incrementalErrors.Add(new PzError(PzErrorCode.CursorInvalid,
                            $"source '{source.Name}.{dataset.Name}' declares incremental cursor '{cursor}' as " +
                            $"'{cursorType}', which cannot be used as a cursor -- allowed cursor types: {AllowedCursorTypesText}",
                            source.FilePath, null,
                            $"declare '{cursor}' as one of the allowed cursor types ({AllowedCursorTypesText})"));
                    }
                }
                else
                {
                    declaredType = cursorType;
                }

                // Only fires when the window trio is present; a plain `incremental: {cursor: ...}`
                // dataset is untouched. All errors aggregate as PZ0213 naming
                // the file + dataset. On success, Initial/Until are REWRITTEN into canonical form so
                // the engine never re-parses user variants.
                if (incremental.MaxWindow is null && incremental.Initial is null && incremental.Until is null)
                {
                    continue;
                }

                var windowErrors = new List<string>();

                if (incremental.MaxWindow is null)
                {
                    windowErrors.Add("'initial'/'until' require 'max_window' — alone they configure nothing");
                }

                if (incremental.MaxWindow is not null && incremental.Initial is null)
                {
                    windowErrors.Add(
                        "'max_window' requires 'initial' — a windowed dataset must declare where the backfill starts");
                }

                if (incremental.MaxWindow is not null && declaredType is null)
                {
                    windowErrors.Add($"'max_window' requires cursor '{cursor}' to be declared in columns: (or, for " +
                        "a raw-envelope dataset, via the connector's cursor:/cursor_type: options) with an " +
                        $"allowed cursor type ({AllowedCursorTypesText}) — bounds must be computable before the first extraction");
                }

                // Scalar `query` = SQL-text query mode (postgres/sqlserver). A MAP-valued `query` is
                // the http connector's URL-parameter block — the very thing that pushes the window
                // bounds ({{ watermark }}/{{ window_upper }}) — and is fine to window.
                if (incremental.MaxWindow is not null
                    && dataset.Options.TryGetValue("query", out var queryOption) && queryOption is string)
                {
                    windowErrors.Add(
                        "'max_window' is not supported on a query-mode dataset — query mode ignores pushdown, " +
                        "so a windowed query dataset would silently extract everything; use table: or drop max_window");
                }

                string? canonicalInitial = null;
                string? canonicalUntil = null;
                if (declaredType is not null)
                {
                    if (incremental.MaxWindow is not null &&
                        !WindowMath.TryValidateWindow(declaredType, incremental.MaxWindow, out var windowError))
                    {
                        windowErrors.Add(windowError!);
                    }

                    if (incremental.Initial is not null &&
                        !WindowMath.TryCanonicalize(declaredType, incremental.Initial, out canonicalInitial))
                    {
                        // TryCanonicalize's `out` still assigns "" (not null) on failure -- null it back
                        // out so the Compare guard below correctly treats this as "no canonical value".
                        canonicalInitial = null;
                        windowErrors.Add($"'initial' value '{incremental.Initial}' is not a canonical {declaredType} value");
                    }

                    if (incremental.Until is not null &&
                        !WindowMath.TryCanonicalize(declaredType, incremental.Until, out canonicalUntil))
                    {
                        canonicalUntil = null;
                        windowErrors.Add($"'until' value '{incremental.Until}' is not a canonical {declaredType} value");
                    }

                    if (canonicalInitial is not null && canonicalUntil is not null &&
                        WindowMath.Compare(declaredType, canonicalUntil, canonicalInitial) <= 0)
                    {
                        windowErrors.Add("'until' must be greater than 'initial'");
                    }
                }

                foreach (var message in windowErrors)
                {
                    incrementalErrors.Add(new PzError(PzErrorCode.WindowConfigInvalid,
                        $"source '{source.Name}.{dataset.Name}': {message}", source.FilePath, null,
                        "sync:\n  mode: incremental\n  cursor: <column>\n  max_window: 1d\n  initial: \"2020-01-01\"\n  until: \"<YYYY-MM-DD>\"  # optional"));
                }

                if (windowErrors.Count == 0 && (canonicalInitial is not null || canonicalUntil is not null))
                {
                    rewrittenDatasets ??= source.Datasets.ToList();
                    rewrittenDatasets[di] = dataset with
                    {
                        SyncMode = dataset.SyncMode! with
                        {
                            Incremental = incremental with
                            {
                                Initial = canonicalInitial ?? incremental.Initial,
                                Until = canonicalUntil ?? incremental.Until,
                            },
                        },
                    };
                }
            }

            rewrittenConnections.Add(rewrittenDatasets is null ? source : source with { Datasets = rewrittenDatasets });
        }

        // A dataset's `path` may embed calendar tokens
        // ({yyyy}/{MM}/{dd}/{HH}/{mm}) substituted per-run from the incremental watermark. Runs
        // alongside PZ0212/PZ0213 above, reading `dataset.Incremental` from `project.Connections` --
        // the SAME pre-canonicalization view PZ0213's loop above just read it from (rewrittenConnections
        // isn't swapped into `project` until after this whole pass is error-free, below) -- so this
        // check's notion of "bounded window" cannot disagree with PZ0213's. All aggregated into the
        // same incrementalErrors list and thrown together. Source datasets only -- write-side
        // `partition_by` renders per-row and needs no window (PZ0219 below covers that config instead).
        foreach (var source in project.Connections)
        {
            foreach (var dataset in source.Datasets)
            {
                if (!dataset.Options.TryGetValue("path", out var pathObj) || pathObj?.ToString() is not { } path
                    || !PathTemplate.HasDateTokens(path))
                {
                    continue;
                }

                try
                {
                    PathTemplate.Validate(path, $"dataset '{dataset.Name}' in {source.FilePath}");
                }
                catch (PzConnectorException ex)
                {
                    incrementalErrors.Add(new PzError(PzErrorCode.TemplatedPathTokensInvalid, ex.Message,
                        source.FilePath, null, null));
                    // Fall through: still check the cursor/window below -- aggregate-all-errors rule.
                }

                var cursorType = ResolveCursorType(dataset);
                if (cursorType is not ("date" or "timestamp"))
                {
                    incrementalErrors.Add(new PzError(PzErrorCode.TemplatedPathCursorInvalid,
                        $"dataset '{dataset.Name}' in {source.FilePath}: a date-templated path requires an " +
                        "incremental date cursor (type date or timestamp); add `sync: {mode: incremental, cursor: <col>}` " +
                        "naming a date/timestamp column",
                        source.FilePath, null,
                        "sync:\n  mode: incremental\n  cursor: <column>  # date or timestamp"));
                }
                else if (!HasBoundedWindow(dataset.SyncMode?.Incremental))
                {
                    // Only fires once the cursor is confirmed date/timestamp -- otherwise
                    // TemplatedPathCursorInvalid above already names the one root cause (one error per
                    // root cause).
                    incrementalErrors.Add(new PzError(PzErrorCode.TemplatedPathWindowRequired,
                        $"dataset '{dataset.Name}' in {source.FilePath}: a date-templated path requires a " +
                        "bounded window so both watermark bounds are stamped on every run (including the " +
                        "first); add `initial:` and `max_window:` under `sync:`",
                        source.FilePath, null,
                        "sync:\n  mode: incremental\n  cursor: <column>\n  max_window: 1d\n  initial: \"2020-01-01\""));
                }
            }
        }

        // An opt-in `files_per_partition` source dataset option
        // (Azure connector coalesces that many consecutive matched blobs into one partition, amortizing
        // per-file dispatch/stream-open overhead for many-tiny-files datasets) must be a positive
        // integer when present. Aggregated into the same incrementalErrors list, never fail-fast --
        // mirrors the source-side path-templating pass just above.
        foreach (var source in project.Connections)
        {
            foreach (var dataset in source.Datasets)
            {
                if (!dataset.Options.TryGetValue("files_per_partition", out var fppObj) || fppObj is null)
                {
                    continue;
                }

                if (!TryParsePositiveInt(fppObj, out _))
                {
                    incrementalErrors.Add(new PzError(PzErrorCode.FilesPerPartitionInvalid,
                        $"source '{source.Name}.{dataset.Name}' in {source.FilePath}: files_per_partition " +
                        $"'{fppObj}' must be a positive integer",
                        source.FilePath, null,
                        "set files_per_partition to a positive integer, or remove it"));
                }
            }
        }

        // The write-side counterpart of the source-side pass above.
        // `partition_by:` names the columns an output is partitioned by. WHAT THAT PRODUCES is the
        // destination's business, and only two things are checkable connector-agnostically here.
        //
        // (1) The declaration is well formed -- a column name, or a list of them. Reading it as
        // `.ToString() is { Length: > 0 }` is the trap this parse closes: a deserialized YAML list
        // stringifies to its CLR type name, which is non-empty, so the presence check passes and
        // everything downstream reads a column named "System.Collections.Generic.List`1[System.Object]".
        //
        // (2) It agrees with `path:`. Calendar tokens in the path are pz's OWN layout rule -- one
        // timestamp column rendered into a folder -- so tokens with no partition_by have no column to
        // substitute from, and tokens with several columns have no way to choose. Both are PZ0219.
        //
        // partition_by WITHOUT tokens is deliberately NOT refused here: a format that records its own
        // partitioning (Delta, Iceberg, Hive-layout parquet) has no templated path to route into and is
        // correct as written. Whether the target connector can actually honour it is a capability
        // question, and only the planner holds the connector instance -- so that refusal is PZ0314
        // there, exactly as the source-side templating refusal already is.
        //
        // Whether a named column EXISTS is impossible to check here -- OutputDef carries no declared
        // column set at compile time (its schema is the upstream pipeline's runtime result) -- so that
        // check is deferred to runtime; PZ0220 is not claimed here. Malformed token sequences reuse
        // PZ0218, same as the source side, aggregated into the same incrementalErrors list.
        foreach (var sink in project.Connections)
        {
            foreach (var output in sink.Outputs)
            {
                var path = output.Options.TryGetValue("path", out var pathObj) ? pathObj?.ToString() : null;
                var hasDateTokens = path is not null && PathTemplate.HasDateTokens(path);

                if (!PartitionColumns.TryRead(output.Options, out var partitionColumns, out var problem))
                {
                    incrementalErrors.Add(new PzError(PzErrorCode.PartitionedOutputConfigInvalid,
                        $"sink '{sink.Name}.{output.Name}' in {sink.FilePath} declares partition_by: but " +
                        problem,
                        sink.FilePath, null,
                        "set partition_by: to a column name, or to a list of column names"));
                }
                else if (partitionColumns.Count == 0 && hasDateTokens)
                {
                    incrementalErrors.Add(new PzError(PzErrorCode.PartitionedOutputConfigInvalid,
                        $"sink '{sink.Name}.{output.Name}' in {sink.FilePath} has a date-templated path but " +
                        "declares no partition_by: -- there is no column to substitute the tokens from at write time",
                        sink.FilePath, null,
                        "add `partition_by: <column>` naming the column to partition by"));
                }
                else if (partitionColumns.Count > 1 && hasDateTokens)
                {
                    incrementalErrors.Add(new PzError(PzErrorCode.PartitionedOutputConfigInvalid,
                        $"sink '{sink.Name}.{output.Name}' in {sink.FilePath} declares partition_by: with " +
                        $"{partitionColumns.Count} columns and a date-templated path -- calendar tokens render " +
                        "from exactly one timestamp column, so there is no way to choose between them",
                        sink.FilePath, null,
                        "name a single timestamp column in partition_by:, or remove the calendar tokens from path:"));
                }

                if (!hasDateTokens)
                {
                    continue;
                }

                try
                {
                    PathTemplate.Validate(path!, $"output '{output.Name}' in {sink.FilePath}");
                }
                catch (PzConnectorException ex)
                {
                    incrementalErrors.Add(new PzError(PzErrorCode.TemplatedPathTokensInvalid, ex.Message,
                        sink.FilePath, null, null));
                }
            }
        }

        if (incrementalErrors.Count > 0)
        {
            throw new PzValidationException(incrementalErrors);
        }

        project = project with { Connections = rewrittenConnections };

        // 2. Validate every ref()/source() resolves -> PZ0201 (all collected, then throw).
        var refErrors = new List<PzError>();
        foreach (var pipeline in project.Pipelines)
        {
            foreach (var dep in OrderDeterministically(rendered[pipeline.Name].Dependencies))
            {
                switch (dep)
                {
                    case DepRef.Pipeline pipelineRef when !pipelinesByName.ContainsKey(pipelineRef.Name):
                        refErrors.Add(new PzError(PzErrorCode.UnresolvedRef,
                            $"pipeline '{pipeline.Name}' calls ref('{pipelineRef.Name}') but no pipeline named '{pipelineRef.Name}' exists",
                            pipeline.FilePath, null, null));
                        break;
                    // There is no ENTITY half to this check: an entity exists because something reads
                    // it, so stage 1d has already synthesized one for every referenced entity of a known
                    // connection. Whether that entity exists in the remote system stays --connect work:
                    // pz does not know a catalog offline.
                    case DepRef.Source sourceRef when !connectionsByName.ContainsKey(sourceRef.SourceName):
                        refErrors.Add(new PzError(PzErrorCode.UnresolvedRef,
                            $"pipeline '{pipeline.Name}' calls source('{sourceRef.SourceName}', '{sourceRef.Dataset}') but no connection named '{sourceRef.SourceName}' exists",
                            pipeline.FilePath, null,
                            $"declare it in connections.yml:\n  {sourceRef.SourceName}:\n    connector: <connector>"));
                        break;
                }
            }
        }

        if (refErrors.Count > 0)
        {
            throw new PzValidationException(refErrors);
        }

        // 3. Ephemeral pipelines may not declare checks -> PZ0205 (all collected, then throw).
        //    A check node depends on its pipeline's node, but ephemeral pipelines produce no
        //    node — so checks on an ephemeral pipeline would otherwise be silently dropped.
        var checksOnEphemeralErrors = new List<PzError>();
        foreach (var pipeline in project.Pipelines.Where(p => p.Materialization == "ephemeral" && p.Checks.Count > 0))
        {
            checksOnEphemeralErrors.Add(new PzError(PzErrorCode.ChecksOnEphemeral,
                $"pipeline '{pipeline.Name}' is ephemeral but declares {pipeline.Checks.Count} check(s) — ephemeral pipelines produce no node for checks to depend on",
                pipeline.FilePath, null,
                "materialize the pipeline as table/view, or remove its checks"));
        }

        if (checksOnEphemeralErrors.Count > 0)
        {
            throw new PzValidationException(checksOnEphemeralErrors);
        }

        // 4. Ephemeral pipelines may not chain -> PZ0204 (all collected, then throw).
        var chainErrors = new List<PzError>();
        foreach (var pipeline in project.Pipelines.Where(p => p.Materialization == "ephemeral"))
        {
            foreach (var dep in OrderDeterministically(rendered[pipeline.Name].Dependencies).OfType<DepRef.Pipeline>())
            {
                if (pipelinesByName[dep.Name].Materialization == "ephemeral")
                {
                    chainErrors.Add(new PzError(PzErrorCode.EphemeralChain,
                        $"ephemeral pipeline '{pipeline.Name}' references another ephemeral pipeline '{dep.Name}' — ephemeral chains are not allowed",
                        pipeline.FilePath, null, null));
                }
            }
        }

        if (chainErrors.Count > 0)
        {
            throw new PzValidationException(chainErrors);
        }

        // 5. Resolve every sink output's binding (inline `INSERT INTO {{ sink(...) }}` is the SOLE load
        //    path — there is no YAML `input:` binding). First collect every pipeline's
        //    inline claim(s), validating the SINK each names actually exists (PZ0201, same code as
        //    ref()/source() lookups) — a pipeline may claim N outputs via the array fan-out form (stage
        //    1b already guarantees no duplicate output within one pipeline). Then walk every output
        //    once: more than one inline claimant is PZ0206 (ERROR). Errors aggregate and throw.
        //    There is no output-existence check and no orphan-output warning: stage 1c builds an output
        //    precisely because a call site named it, so an output cannot exist without a writer, nor be
        //    named without existing.
        var sinkErrors = new List<PzError>();
        var sinkWarnings = new List<PzWarning>();
        var inlineClaimsByOutput = new Dictionary<(string Sink, string Output), List<string>>();
        var claimFilePaths = new Dictionary<(string Sink, string Output), string>();
        foreach (var pipeline in project.Pipelines.Where(p => p.Materialization != "ephemeral"))
        {
            foreach (var binding in rendered[pipeline.Name].InlineBindings)
            {
                // A kwarg pz does not know is a CONNECTOR write option and rides Options unchecked --
                // no connector publishes a write-option vocabulary to check against, and the
                // Abstractions ABI is fixed. That leaves a typo of a pz key
                // (`strategyy: 'merge'`) silently defaulting the strategy to append, on the surface
                // that decides delivery semantics. A near-miss cannot be REFUSED -- a connector may
                // legitimately name an option `keyz` -- but it can be named, so it stops being silent.
                foreach (var option in binding.Write.Options.Keys.OrderBy(k => k, StringComparer.Ordinal))
                {
                    var nearMiss = SinkFunction.NearMissKwarg(option);
                    if (nearMiss is not null)
                    {
                        sinkWarnings.Add(new PzWarning(PzErrorCode.InvalidSinkCall,
                            $"pipeline '{pipeline.Name}' passes sink('{binding.Sink}', '{binding.Output}', " +
                            $"{option}: ...), which pz does not recognize — it is being sent to the " +
                            $"connector as a write option. Did you mean '{nearMiss}'?",
                            pipeline.FilePath, null,
                            $"rename it to '{nearMiss}', or ignore this if '{option}' really is an " +
                            $"option of the '{project.Connections.FirstOrDefault(s => s.Name == binding.Sink)?.Connector}' connector"));
                    }
                }

                if (!connectionsByName.ContainsKey(binding.Sink))
                {
                    sinkErrors.Add(new PzError(PzErrorCode.UnresolvedRef,
                        $"pipeline '{pipeline.Name}' calls sink('{binding.Sink}', '{binding.Output}') but no sink named '{binding.Sink}' exists",
                        pipeline.FilePath, null, null));
                    continue;
                }

                var key = (binding.Sink, binding.Output);
                claimFilePaths.TryAdd(key, pipeline.FilePath);
                if (!inlineClaimsByOutput.TryGetValue(key, out var claimants))
                {
                    claimants = [];
                    inlineClaimsByOutput[key] = claimants;
                }

                claimants.Add(pipeline.Name);
            }
        }

        var sinkInputs = new Dictionary<(ConnectionDef Sink, OutputDef Output), SinkInputResolution>();
        foreach (var sink in project.Connections)
        {
            foreach (var output in sink.Outputs)
            {
                var claimants = inlineClaimsByOutput.TryGetValue((sink.Name, output.Name), out var list)
                    ? list.OrderBy(n => n, StringComparer.Ordinal).ToList()
                    : [];

                if (claimants.Count > 1)
                {
                    sinkErrors.Add(new PzError(PzErrorCode.SinkBindingConflict,
                        $"sink '{sink.Name}.{output.Name}' is claimed by multiple pipelines' sink() calls: {string.Join(", ", claimants)}",
                        claimFilePaths[(sink.Name, output.Name)], null,
                        "only one pipeline may bind a given sink output via sink()"));
                    continue;
                }

                sinkInputs[(sink, output)] = new SinkInputResolution.PipelineInput(claimants[0]);
            }
        }

        if (sinkErrors.Count > 0)
        {
            throw new PzValidationException(sinkErrors);
        }

        // 6. Determine which source datasets are actually referenced (directly, or inherited
        //    through an inlined ephemeral pipeline). Sinks no longer drain a source directly — a
        //    pass-through pipeline (`INSERT INTO {{ sink(...) }} select * from {{ source(...) }}`) is
        //    the only way a source reaches a sink, so its source ref is already captured below.
        //    Readers are collected per dataset, not merely counted, so
        //    a dataset read by two pipelines fails here -- BEFORE stage 6b's watermark inference, which
        //    is what lets WatermarkInference drop all cross-pipeline reasoning.
        var readersByDataset = new Dictionary<(string Source, string Dataset), List<string>>();
        foreach (var pipeline in project.Pipelines.Where(p => p.Materialization != "ephemeral"))
        {
            foreach (var dep in EffectiveDependencies(pipeline, rendered, pipelinesByName).OfType<DepRef.Source>())
            {
                var key = (dep.SourceName, dep.Dataset);
                if (!readersByDataset.TryGetValue(key, out var readers))
                {
                    readers = [];
                    readersByDataset[key] = readers;
                }

                // EffectiveDependencies yields a set per pipeline, so a self-join already contributes
                // once -- guarded anyway, because the rule counts pipelines, not references.
                if (!readers.Contains(pipeline.Name, StringComparer.Ordinal))
                {
                    readers.Add(pipeline.Name);
                }
            }
        }

        var multiReaderErrors = new List<PzError>();
        foreach (var (key, readers) in readersByDataset
            .Where(kv => kv.Value.Count > 1)
            .OrderBy(kv => kv.Key.Source, StringComparer.Ordinal)
            .ThenBy(kv => kv.Key.Dataset, StringComparer.Ordinal))
        {
            var sorted = readers.OrderBy(n => n, StringComparer.Ordinal).ToList();
            var staged = $"stg_{key.Source}_{key.Dataset}".Replace('.', '_');
            multiReaderErrors.Add(new PzError(PzErrorCode.SourceReadByMultiplePipelines,
                $"source '{key.Source}.{key.Dataset}' is read by {sorted.Count} pipelines: " +
                string.Join(", ", sorted),
                project.Pipelines.First(p => p.Name == sorted[0]).FilePath, null,
                "a source dataset is read by exactly one pipeline -- read it once and ref() that " +
                $"pipeline:\n  -- pipelines/{staged}.sql\n  select * from " +
                $"{{{{ source('{key.Source}', '{key.Dataset}') }}}}\nthen replace each " +
                $"source('{key.Source}', '{key.Dataset}') with ref('{staged}')"));
        }

        if (multiReaderErrors.Count > 0)
        {
            throw new PzValidationException(multiReaderErrors);
        }

        var referencedSources = new HashSet<(string Source, string Dataset)>(readersByDataset.Keys);

        // 6b. SQL-declared incremental inference. Runs at the point where every non-ephemeral
        //     pipeline's FINAL SQL exists (post prefix-extraction, post ephemeral-CTE assembly — the very
        //     same BuildInlinedSql+NormalizeSql stage 8 uses, computed once here and reused there) and
        //     before any node is built, so SourceLoad nodes can carry the synthesized IncrementalDef and
        //     Pipeline nodes the NULL-guarded rewrite. Errors aggregate-then-throw exactly like every
        //     neighboring stage. Only reached (Analyze called) when some pipeline actually renders a
        //     watermark() sentinel — a watermark-free project never needs a reader.
        var assembledSql = new Dictionary<string, string>();
        foreach (var pipeline in project.Pipelines.Where(p => p.Materialization != "ephemeral"))
        {
            assembledSql[pipeline.Name] = NormalizeSql(BuildInlinedSql(pipeline, rendered, pipelinesByName));
        }

        var watermarkSynthesized = new Dictionary<(string Source, string Dataset), IncrementalDef>();
        var watermarkRewrittenSql = new Dictionary<string, string>();
        var watermarkSubstitutions = new Dictionary<string, IReadOnlyList<WatermarkSubstitution>>();

        if (project.Pipelines.Any(p => rendered[p.Name].WatermarkRefs.Count > 0))
        {
            if (sqlAst is null)
            {
                throw new InvalidOperationException(
                    "watermark() requires an ISqlAstReader — wire DuckDbSqlAstReader (Pz.Cli does this; a test must pass a stub)");
            }

            // A watermark() rendered inside an ephemeral is CTE-inlined into each consumer's assembled
            // SQL but has no node of its own, so inference must analyze it against the CONSUMER.
            // Feed each non-ephemeral pipeline its EFFECTIVE reads: WatermarkRefs and Dependencies
            // unioned with those of the ephemerals it directly consumes (same one-level model as
            // EffectiveDependencies — ephemeral chains are rejected earlier). Analysis then attributes the
            // inlined sentinel to the consumer: bounds carry the consumer's name; RewrittenSql/Substitutions
            // land on the consumer. RenderResult is a record, so augment via `with` rather than mutating.
            var pipelineInputs = project.Pipelines
                .Where(p => p.Materialization != "ephemeral")
                .Select(p => new WatermarkInference.PipelineInput(
                    p,
                    rendered[p.Name] with
                    {
                        Dependencies = EffectiveDependencies(p, rendered, pipelinesByName).ToHashSet(),
                        WatermarkRefs = EffectiveWatermarkRefs(p, rendered, pipelinesByName),
                    },
                    assembledSql[p.Name]))
                .ToList();
            var inference = WatermarkInference.Run(pipelineInputs, connectionsByName, sqlAst);
            if (inference.Errors.Count > 0)
            {
                throw new PzValidationException(inference.Errors);
            }

            // Result maps are safe to consume now (Errors empty — see WatermarkInference.Result's contract).
            foreach (var kvp in inference.Synthesized) { watermarkSynthesized[kvp.Key] = kvp.Value; }
            foreach (var kvp in inference.RewrittenSql) { watermarkRewrittenSql[kvp.Key] = kvp.Value; }
            foreach (var kvp in inference.Substitutions) { watermarkSubstitutions[kvp.Key] = kvp.Value; }

            // Belt-and-braces loud guard (repo rule: no silent failures). With the ephemeral-inheritance
            // union above, every rendered watermark() sentinel is analyzed and attributed — this is
            // unreachable for known shapes. It exists so ANY future leak (a sentinel that reaches a
            // Pipeline node's executed SQL without ever being analyzed/rewritten) fails loudly with a PZ
            // code instead of dying as a cryptic DuckDB cast error at run time.
            var sentinelGuardErrors = new List<PzError>();
            foreach (var pipeline in project.Pipelines.Where(p => p.Materialization != "ephemeral"))
            {
                if (assembledSql[pipeline.Name].Contains("__pz_watermark__", StringComparison.Ordinal)
                    && !watermarkRewrittenSql.ContainsKey(pipeline.Name))
                {
                    sentinelGuardErrors.Add(new PzError(PzErrorCode.UnrecognizedWatermarkExpression,
                        $"pipeline '{pipeline.Name}': a watermark() call was rendered into this pipeline's " +
                        "assembled SQL, but the analyzer attributed no comparison to it",
                        pipeline.FilePath, null,
                        "a watermark() inside an ephemeral is attributed to each pipeline that consumes it; " +
                        "accepted shape: <cursor column> > or >= <expression containing " +
                        "{{ watermark(source, dataset) }}>"));
                }
            }

            if (sentinelGuardErrors.Count > 0)
            {
                throw new PzValidationException(sentinelGuardErrors);
            }
        }

        // 7. Build SourceLoad nodes (only for referenced datasets), in project declaration order.
        var sourceNodeIds = new Dictionary<(string Source, string Dataset), NodeId>();
        var nodes = new List<DagNode>();
        // Entity names may carry characters no SQL identifier can,
        // so StagingName folds them -- many-to-one. Two datasets of one source that fold together would
        // share a staging table and the second load would overwrite the first, so the pair is refused.
        var stagingCollisions = new List<PzError>();
        foreach (var source in project.Connections)
        {
            var staged = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (var dataset in source.Datasets)
            {
                if (!referencedSources.Contains((source.Name, dataset.Name)))
                {
                    continue;
                }

                var stagingName = StagingName.ForSourceLoad(source.Name, dataset.Name);
                if (!staged.TryAdd(stagingName, dataset.Name))
                {
                    stagingCollisions.Add(new PzError(PzErrorCode.DuplicateName,
                        $"source '{source.Name}': datasets '{staged[stagingName]}' and '{dataset.Name}' " +
                        "cannot be told apart in staging.",
                        source.FilePath, null,
                        "rename one of them -- pz folds every character outside [A-Za-z0-9_] to '_' when " +
                        "it stages a read, so these two names collide"));
                }

                var effectiveDataset = watermarkSynthesized.TryGetValue((source.Name, dataset.Name), out var synthesized)
                    ? dataset with { SyncMode = new SyncModeDef(SyncMode.Incremental, synthesized) }
                    : dataset;
                var hints = ReadHintsFor(source, effectiveDataset, readersByDataset, watermarkRewrittenSql,
                    assembledSql, sqlAst);

                // NodeId hashes only Options + Columns (never Incremental), so folding a SQL-declared
                // synthesized IncrementalDef into the dataset above cannot change this SourceLoad's Id
                // — the same invariant stage 0's window-canonicalization rewrite relies on. Hints DO
                // feed it: a different projection is a different extraction, and `pz retry` must not
                // resurrect a staged table whose columns no longer match. They are appended only when
                // something is actually pushable, so a project that pushes nothing keeps a
                // byte-identical Id.
                var canonicalParts = new List<string>
                {
                    "source-load", source.Name, dataset.Name,
                    CanonicalJson.Serialize(dataset.Options),
                    CanonicalJson.Serialize(dataset.Columns),
                };
                if (hints is not null)
                {
                    canonicalParts.Add(CanonicalJson.Serialize(hints.Columns));
                    canonicalParts.Add(hints.PredicateSql ?? "");
                }

                var id = NodeId.Compute(string.Join('\n', canonicalParts));
                sourceNodeIds[(source.Name, dataset.Name)] = id;
                nodes.Add(new DagNode(id, NodeKind.SourceLoad, stagingName,
                    [], null, new SourceDatasetDef(source, effectiveDataset, hints)));
            }
        }

        if (stagingCollisions.Count > 0)
        {
            throw new PzValidationException(stagingCollisions);
        }

        // 8. Build Pipeline nodes — every non-ephemeral pipeline, always (unlike sources, not
        //    filtered by reference). SQL is the ephemeral-inlined, LF-normalized, trailing-
        //    whitespace-trimmed rendered SQL. Node IDs don't depend on each other, so this
        //    pass can run before DependsOn is resolved.
        var pipelineNodeIds = new Dictionary<string, NodeId>();
        var pipelineSql = new Dictionary<string, string>();
        foreach (var pipeline in project.Pipelines.Where(p => p.Materialization != "ephemeral"))
        {
            // A watermark() pipeline's executed SQL is the reader's NULL-guarded rewrite; a plain pipeline
            // keeps its stage-6b assembled SQL verbatim. Both are sentinel-bearing (watermark() renders a
            // deterministic sentinel; the rewrite only wraps the comparison and leaves that sentinel in
            // place), so hashing the rewrite keeps the NodeId a pure function of stable text — the actual
            // run-varying watermark value is never substituted in until PipelineExecutor.
            var sql = watermarkRewrittenSql.TryGetValue(pipeline.Name, out var rewritten)
                ? NormalizeSql(rewritten)
                : assembledSql[pipeline.Name];
            pipelineSql[pipeline.Name] = sql;
            var canonical = string.Join('\n', "pipeline", pipeline.Name, pipeline.Materialization, sql);
            pipelineNodeIds[pipeline.Name] = NodeId.Compute(canonical);
        }

        foreach (var pipeline in project.Pipelines.Where(p => p.Materialization != "ephemeral"))
        {
            var dependsOn = ResolveDependsOn(
                EffectiveDependencies(pipeline, rendered, pipelinesByName), pipelineNodeIds, sourceNodeIds);
            nodes.Add(new DagNode(pipelineNodeIds[pipeline.Name], NodeKind.Pipeline, pipeline.Name,
                dependsOn, pipelineSql[pipeline.Name], pipeline)
            {
                WatermarkSubstitutions = watermarkSubstitutions.TryGetValue(pipeline.Name, out var subs) ? subs : [],
            });
        }

        // 9. Check nodes — one per check on a non-ephemeral pipeline, depending on that pipeline.
        foreach (var pipeline in project.Pipelines.Where(p => p.Materialization != "ephemeral"))
        {
            foreach (var check in pipeline.Checks)
            {
                // custom_sql has no columns; its loader-validated `name` option is the node's identity
                // in console output/--select. All other types use the type+columns convention. The
                // canonical hash below is the same either way.
                var name = check.Type == "custom_sql"
                    ? $"check_{pipeline.Name}_{check.Options["name"]}"
                    : check.Columns.Count > 0
                        ? $"check_{pipeline.Name}_{check.Type}_{string.Join('_', check.Columns)}"
                        : $"check_{pipeline.Name}_{check.Type}";
                var canonical = string.Join('\n', "check", pipeline.Name, check.Type,
                    string.Join(',', check.Columns), CanonicalJson.Serialize(check.Options));
                var id = NodeId.Compute(canonical);
                // Per-check override wins over the project default in BOTH directions; absent from both
                // -> true. Deliberately not part of `canonical` above -- a runtime execution flag, not
                // compile output.
                var sampleValues = check.SampleValues ?? project.Engine.CheckSamples;
                nodes.Add(new DagNode(id, NodeKind.Check, name, [pipelineNodeIds[pipeline.Name]], null,
                    new CheckNodeDef(pipeline.Name, check, sampleValues)));
            }
        }

        // 10. SinkWrite nodes, depending on their claimant pipeline's node. Every bound output is
        //     inline-bound, declaring no YAML `input:`, so its Output.Input is synthesized here from the
        //     resolution (the claimant pipeline's name) for every downstream consumer — the canonical
        //     hash below and SinkWriteExecutor's staging-relation lookup — that expects it populated.
        //     Only outputs with a claimant reach here, so isInlineBound is always true.
        foreach (var sink in project.Connections)
        {
            foreach (var output in sink.Outputs)
            {
                if (!sinkInputs.TryGetValue((sink, output), out var resolution))
                {
                    continue; // no resolved claimant — no SinkWrite node
                }

                var isInlineBound = string.IsNullOrEmpty(output.Input);
                var pipelineName = ((SinkInputResolution.PipelineInput)resolution).PipelineName;
                var effectiveOutput = isInlineBound ? output with { Input = pipelineName } : output;
                var inputNodeId = pipelineNodeIds[pipelineName];
                var canonical = string.Join('\n', "sink-write", sink.Name, output.Name, effectiveOutput.Input,
                    output.Mode, output.SchemaPolicy, CanonicalJson.Serialize(output.Options));
                var id = NodeId.Compute(canonical);
                nodes.Add(new DagNode(id, NodeKind.SinkWrite, $"{sink.Name}.{output.Name}",
                    [inputNodeId], null, new SinkOutputDef(sink, effectiveOutput, isInlineBound)));
            }
        }

        // 10b. Final stage: the (read x write) pairing matrix for an EXPLICITLY declared incremental
        //      dataset -- append without consent (PZ0214) and replace (PzErrorCode.IncompatiblePair)
        //      both discard/duplicate rows an incremental read relies on staying put across runs. Runs
        //      on the REAL DAG (never the selected subset) because the risk is a property of the
        //      project, not of one run -- so it must see every node/edge, not a --select-filtered slice.
        //      Aggregate like every other stage above.
        //      Deliberately `Incremental` only, not `Incremental or Auto` -- Pz.Core has no
        //      connector-capability access, so an explicitly declared `sync: {mode: auto}` block might
        //      resolve to a full read (needing no consent at all) as easily as a feed. That ambiguity is
        //      ExecutionPlanner's job alone (its feed-side PZ0214 guard, which DOES hold the opened
        //      connector and the ReadShapeResolver's actual resolved shape); Core only ever refuses the
        //      unambiguous case, an explicit `sync: {mode: incremental}` block.
        var deliveryErrors = new List<PzError>();
        var deliveryById = nodes.ToDictionary(n => n.Id);
        foreach (var sinkNode in nodes)
        {
            if (sinkNode.Definition is not SinkOutputDef sinkDef)
            {
                continue;
            }

            var checkAppendConsent = sinkDef.Output.Mode == "append" && !sinkDef.Output.AcceptDuplicates;
            var checkReplace = sinkDef.Output.Mode == "replace";
            if (!checkAppendConsent && !checkReplace)
            {
                continue;
            }

            var seen = new HashSet<NodeId>();
            var queue = new Queue<NodeId>(sinkNode.DependsOn);
            while (queue.Count > 0)
            {
                var id = queue.Dequeue();
                if (!seen.Add(id) || !deliveryById.TryGetValue(id, out var ancestor))
                {
                    continue;
                }

                if (ancestor.Definition is SourceDatasetDef src &&
                    src.Dataset.SyncMode is { Mode: SyncMode.Incremental })
                {
                    if (checkReplace)
                    {
                        deliveryErrors.Add(new PzError(PzErrorCode.IncompatiblePair,
                            $"output '{sinkDef.Sink.Name}.{sinkDef.Output.Name}': write.strategy 'replace' fed by incremental dataset " +
                            $"'{src.Source.Name}.{src.Dataset.Name}' would discard previously loaded rows each run.",
                            sinkDef.Sink.FilePath, null,
                            "use write.strategy: merge (effectively-once), or remove the dataset's sync block for a full re-read"));
                    }
                    else
                    {
                        deliveryErrors.Add(new PzError(PzErrorCode.IncrementalAppendUnacknowledged,
                            $"sink '{sinkDef.Sink.Name}.{sinkDef.Output.Name}' has write.strategy: append and is fed by " +
                            $"incremental dataset '{src.Source.Name}.{src.Dataset.Name}' -- delivery is " +
                            "at-least-once, so a retried or replayed slice can duplicate rows",
                            sinkDef.Sink.FilePath, null,
                            "use write.strategy: merge (with keys:) or write.strategy: replace, or set\n" +
                            "write:\n  strategy: append\n  duplicates: accept\n" +
                            "on the output to accept at-least-once delivery"));
                    }
                }

                foreach (var dep in ancestor.DependsOn)
                {
                    queue.Enqueue(dep);
                }
            }
        }

        if (deliveryErrors.Count > 0)
        {
            throw new PzValidationException(deliveryErrors);
        }

        // 10b2. CDC pairing matrix: the cdc column of the same published (read x write) matrix as
        //       stage 10b above. cdc is always EXPLICITLY declared
        //       (never auto-resolved), so -- unlike the feed column, which needs ExecutionPlanner's
        //       held-open connector -- this validates entirely at compile time. An output is
        //       "cdc-fed" iff its SinkWrite node's transitive DependsOn closure (UpstreamSourceLoads
        //       below) contains >=1 SourceLoad whose dataset declares `sync: {mode: cdc}`; delete-key
        //       routing is valid iff that closure contains EXACTLY one SourceLoad in total and it is
        //       the cdc one -- a join/multi-source pipeline cannot map delete keys to a single
        //       upstream row identity. `CdcDeleteOrigin` is stamped only in that valid case, via
        //       `with` on the already NodeId-computed node (see DagNode.cs) so it never feeds a hash.
        var cdcErrors = new List<PzError>();
        for (var i = 0; i < nodes.Count; i++)
        {
            if (nodes[i].Definition is not SinkOutputDef sinkDef)
            {
                continue;
            }

            var upstream = UpstreamSourceLoads(nodes[i], deliveryById);
            var cdcSources = upstream.Where(s => s.Dataset.SyncMode?.Mode == SyncMode.Cdc)
                .OrderBy(s => s.Source.Name, StringComparer.Ordinal).ThenBy(s => s.Dataset.Name, StringComparer.Ordinal)
                .ToList();
            var outputRef = $"{sinkDef.Sink.Name}.{sinkDef.Output.Name}";

            if (cdcSources.Count > 0 && sinkDef.Output.Mode is "replace" or "append")
            {
                var datasetNames = string.Join(", ", cdcSources.Select(s => $"{s.Source.Name}.{s.Dataset.Name}"));
                var reason = sinkDef.Output.Mode == "replace"
                    ? "would discard previously applied change events each run (cdc x replace)"
                    : "cdc delivery is change events, not rows -- append would land raw events as duplicate " +
                      "rows (cdc x append)";
                cdcErrors.Add(new PzError(PzErrorCode.IncompatiblePair,
                    $"output '{outputRef}': write.strategy '{sinkDef.Output.Mode}' fed by cdc dataset(s) " +
                    $"'{datasetNames}' -- {reason}.",
                    sinkDef.Sink.FilePath, null,
                    "use write.strategy: merge with on_delete"));
            }

            if (cdcSources.Count > 0 && sinkDef.Output.Mode == "merge" && sinkDef.Output.OnDelete is null)
            {
                var datasetNames = string.Join(", ", cdcSources.Select(s => $"{s.Source.Name}.{s.Dataset.Name}"));
                cdcErrors.Add(new PzError(PzErrorCode.CdcConsentMissing,
                    $"output '{outputRef}': write.strategy merge fed by cdc dataset(s) '{datasetNames}' has not " +
                    "declared write.on_delete -- deletes on the source table need an explicit routing choice.",
                    sinkDef.Sink.FilePath, null,
                    "declare write.on_delete: delete, soft, or ignore"));
            }

            if (sinkDef.Output.OnDelete is not null && cdcSources.Count == 0)
            {
                cdcErrors.Add(new PzError(PzErrorCode.CdcDeleteRouteInvalid,
                    $"output '{outputRef}': write.on_delete '{sinkDef.Output.OnDelete}' is declared but no " +
                    "upstream dataset declares sync: {mode: cdc} -- on_delete requires a cdc-fed input.",
                    sinkDef.Sink.FilePath, null,
                    "remove write.on_delete, or feed this output from a sync: {mode: cdc} dataset"));
            }
            else if (sinkDef.Output.OnDelete is "delete" or "soft" && cdcSources.Count > 0 && upstream.Count > 1)
            {
                var datasetNames = string.Join(", ", upstream.Select(s => $"{s.Source.Name}.{s.Dataset.Name}"));
                cdcErrors.Add(new PzError(PzErrorCode.CdcDeleteRouteInvalid,
                    $"output '{outputRef}': write.on_delete '{sinkDef.Output.OnDelete}' fed by a multi-source " +
                    $"pipeline ('{datasetNames}') -- delete keys cannot be routed through a multi-source pipeline.",
                    sinkDef.Sink.FilePath, null,
                    "route on_delete: delete/soft through a single-source pipeline reading only the cdc " +
                    "dataset, or use on_delete: ignore"));
            }
            else if (sinkDef.Output.OnDelete is "delete" or "soft" && cdcSources.Count == 1 && upstream.Count == 1)
            {
                var origin = cdcSources[0];
                nodes[i] = nodes[i] with
                {
                    Definition = sinkDef with { CdcDeleteOrigin = new CdcOrigin(origin.Source.Name, origin.Dataset.Name) },
                };
            }
        }

        if (cdcErrors.Count > 0)
        {
            throw new PzValidationException(cdcErrors);
        }

        // 10c. Dead-leaf detection: a non-ephemeral pipeline consumed by no ref()
        //      and loaded to no sink — its Pipeline node's Id appears in no other node's DependsOn set.
        //      A SinkWrite DependsOn its claimant pipeline, a downstream pipeline DependsOn an upstream
        //      via ref(), and a Check DependsOn its pipeline — so `consumedIds` captures "loaded",
        //      "ref'd", and "checked" alike; a dead leaf is in none. A non-blocking WARNING (PZ0223).
        var pipelineNodesByName = nodes.Where(n => n.Kind == NodeKind.Pipeline).ToDictionary(n => n.Name);
        var consumedIds = new HashSet<NodeId>(nodes.SelectMany(n => n.DependsOn));
        foreach (var pipeline in project.Pipelines.Where(p => p.Materialization != "ephemeral"))
        {
            if (!pipelineNodesByName.TryGetValue(pipeline.Name, out var node)) { continue; }
            if (!consumedIds.Contains(node.Id))
            {
                sinkWarnings.Add(new PzWarning(PzErrorCode.DeadLeafPipeline,
                    $"pipeline '{pipeline.Name}' computes a result nothing consumes — no sink loads it and no ref() reads it",
                    pipeline.FilePath, null,
                    "add an `INSERT INTO {{ sink(...) }}` load or a ref() consumer, or leave it if this is temporary"));
            }
        }

        // 11. Deterministic Kahn topological order; leftover nodes -> a cycle -> PZ0202.
        var ordered = TopologicalSortOrThrow(nodes);
        var compiled = new CompiledDag(ordered) { Warnings = sinkWarnings, Connections = project.Connections };

        // 12. Effectively-once advisory NOTICE -- non-fatal, same `notices`
        //     mechanism as the "cursor unverified" notice above. The effectively-once guarantee an
        //     `incremental:` dataset relies on holds only if every sink its lineage reaches is
        //     idempotent (mode: merge): a crash/retry, or a no-pushdown source re-running the same
        //     extract, must not duplicate rows downstream. Trace each incremental SourceLoad's
        //     structural descendants (the real project DAG, via the just-built CompiledDag) to every
        //     SinkWrite it reaches and warn on each non-merge one. Deliberately NOT a hard error --
        //     append-only sources with pushdown support can legitimately use mode: append, and a hard
        //     error would over-block those legitimate cases.
        foreach (var node in ordered.Where(n => n.Kind == NodeKind.SourceLoad))
        {
            // Same explicit-only scope as stage 10b above -- an implicit `mode: auto` dataset (SyncMode
            // null) that a connector resolves to Feed is a planner-only concern (no connector access
            // here); this advisory notice never fires for it, mirroring PZ0214's split. Only
            // `SyncMode.Auto` can reach this: an Incremental dataset cannot land on a non-merge sink --
            // PZ0335 refuses replace and PZ0214 refuses append-without-consent, while
            // append-with-consent is filtered by the AcceptDuplicates check below.
            if (node.Definition is not SourceDatasetDef sourceDef ||
                sourceDef.Dataset.SyncMode is not { Mode: SyncMode.Auto })
            {
                continue;
            }

            foreach (var sinkNode in compiled.Descendants(node.Id).Where(d => d.Kind == NodeKind.SinkWrite))
            {
                var sinkDef = (SinkOutputDef)sinkNode.Definition;
                // Consent recorded via accept_duplicates: true (PZ0214's own offered escape hatch,
                // stage 10b above) must silence this recurring nag -- PZ0214 already surfaced merge
                // as the alternative at decision time, so repeating that hint here would contradict
                // the consent the operator just gave.
                if (sinkDef.Output.Mode == "merge" || sinkDef.Output.AcceptDuplicates)
                {
                    continue;
                }

                notices?.Add(
                    $"incremental/sync dataset '{sourceDef.Source.Name}.{sourceDef.Dataset.Name}' feeds non-merge sink " +
                    $"'{sinkDef.Sink.Name}.{sinkDef.Output.Name}' (mode: {sinkDef.Output.Mode}); effectively-once " +
                    "is not guaranteed -- a crash or retry may duplicate rows. Use write.strategy: merge for idempotency.");
            }
        }

        return compiled;
    }

    /// <summary>What this dataset's SINGLE reading pipeline lets pz push down. The one-reader rule
    /// (PZ0349, enforced in stage 6) is what makes this well-defined — with two readers there would be
    /// no answer to "whose columns". Returns null, meaning push nothing, when no parser is wired, when
    /// the dataset somehow has no reader, or when the reader found nothing pushable; pushing nothing is
    /// always correct because the pipeline's own SQL still runs in DuckDB over whatever landed.</summary>
    private static ReadHintPlan? ReadHintsFor(
        ConnectionDef source, DatasetDef dataset,
        IReadOnlyDictionary<(string Source, string Dataset), List<string>> readersByDataset,
        IReadOnlyDictionary<string, string> rewrittenSql,
        IReadOnlyDictionary<string, string> assembledSql,
        ISqlAstReader? sqlAst)
    {
        if (sqlAst is null || !readersByDataset.TryGetValue((source.Name, dataset.Name), out var readers)
            || readers.Count != 1)
        {
            return null;
        }

        // cdc reads the connector's change stream, not the base table: its landed shape carries change
        // metadata the pipeline SQL never names, and a predicate over base-table columns has no meaning
        // against a change log. Push nothing.
        if (dataset.SyncMode?.Mode == SyncMode.Cdc)
        {
            return null;
        }

        var reader = readers[0];
        // The pipeline's EXECUTED SQL, exactly as stage 8 picks it: a watermark() pipeline runs the
        // reader's NULL-guarded rewrite, a plain one its assembled SQL.
        if (!(rewrittenSql.TryGetValue(reader, out var sql) || assembledSql.TryGetValue(reader, out sql)))
        {
            return null;
        }

        var cursor = dataset.SyncMode?.Incremental?.Cursor;
        var plan = sqlAst.ExtractReadHints(sql, StagingName.ForSourceLoad(source.Name, dataset.Name), cursor);

        // The cursor is never prunable even when the pipeline's SQL never selects it: watermark
        // advancement is a post-land MAX(cursor) against the staged table, so pruning it away would
        // leave the watermark unable to advance.
        if (cursor is not null && plan.Columns is { } columns
            && !columns.Contains(cursor, StringComparer.OrdinalIgnoreCase))
        {
            plan = plan with { Columns = [.. columns, cursor] };
        }

        return plan is { Columns: null, PredicateSql: null } ? null : plan;
    }

    /// <summary>The cdc pairing-matrix pass's closure walk -- every
    /// <see cref="SourceDatasetDef"/> reachable from a SinkWrite node's transitive DependsOn ancestry.
    /// Mirrors stage 10b's (and ExecutionPlanner's feed pass) HashSet-seen + Queue-over-DependsOn
    /// shape; a dataset reachable via two paths in a diamond DAG is collected once because `seen`
    /// gates on NodeId (one SourceLoad node per (source, dataset) pair), not on dataset identity.</summary>
    private static List<SourceDatasetDef> UpstreamSourceLoads(
        DagNode sinkNode, IReadOnlyDictionary<NodeId, DagNode> byId)
    {
        var result = new List<SourceDatasetDef>();
        var seen = new HashSet<NodeId>();
        var queue = new Queue<NodeId>(sinkNode.DependsOn);
        while (queue.Count > 0)
        {
            var id = queue.Dequeue();
            if (!seen.Add(id) || !byId.TryGetValue(id, out var ancestor))
            {
                continue;
            }

            if (ancestor.Definition is SourceDatasetDef src)
            {
                result.Add(src);
            }

            foreach (var dep in ancestor.DependsOn)
            {
                queue.Enqueue(dep);
            }
        }

        return result;
    }

    /// <summary>The declared <c>columns:</c> contract type of a dataset's <c>incremental.cursor</c>,
    /// or null if there is no incremental block, no columns contract, the cursor isn't in it, or its
    /// declared type isn't one of <see cref="AllowedCursorTypes"/> -- the same resolution PZ0212 above
    /// performs inline (it additionally emits PZ0212 for some of these null cases; this helper is
    /// read-only and used only by the path-templating checks below, which raise their own codes).</summary>
    private static string? ResolveCursorType(DatasetDef dataset) =>
        CursorContract.ResolveDeclaredType(dataset);

    /// <summary>The exact "bounded window" PZ0213 above recognizes as valid: <c>max_window</c> and
    /// <c>initial</c> both set (<c>until</c> is an optional additional upper bound -- PZ0213's rules 1/2
    /// require max_window+initial together for ANY window config to be error-free, so this must mirror
    /// that pairing exactly rather than treat until as a max_window substitute).</summary>
    private static bool HasBoundedWindow(IncrementalDef? incremental) =>
        incremental is { MaxWindow: not null, Initial: not null };

    /// <summary>Parses a `files_per_partition` option value (int, long, or string form) to a positive
    /// <c>int</c>, per PZ0222's rule -- rejects anything that doesn't parse to an integer AND anything
    /// that parses to zero or negative. Deliberately narrow: this is a compile-time gate only, mirroring
    /// no existing numeric-option parser elsewhere in DagCompiler (there is none to reuse).</summary>
    private static bool TryParsePositiveInt(object? value, out int result)
    {
        switch (value)
        {
            case int i:
                result = i;
                return i > 0;
            case long l when l is > 0 and <= int.MaxValue:
                result = (int)l;
                return true;
            case long:
                result = 0;
                return false;
            case string s when int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed):
                result = parsed;
                return parsed > 0;
            default:
                result = 0;
                return false;
        }
    }

    private const string InsertFormHint =
        "bind the sink inline as the pipeline's leading statement: INSERT INTO {{ sink('<sink>', '<output>') }} <query>";

    /// <summary>
    /// Recognizes the INSERT-form prefix at the very start of <paramref name="sql"/>: any run
    /// of whitespace and/or full-line `--` SQL comments, then (case-insensitive) `insert`, whitespace,
    /// `into`, whitespace, then either the scalar form (a single sink() marker) or the array fan-out
    /// form `[ m1, m2, ... ]` (markers in the pipeline's recorded <see cref="InlineSinkBinding"/> render
    /// order), then whitespace separating it from the query. Names are matched by exact ordinal
    /// comparison against markers built from the bindings — deliberately not a regex with named capture
    /// groups, since sink/output names may contain underscores that would make such a regex ambiguous
    /// Returns the query verbatim — CTEs and all, the fixed prefix ends right after the
    /// closing marker/bracket — or null if the shape doesn't match, including any marker
    /// recurring anywhere in what would be the extracted query (a malformed/duplicated call the
    /// duplicate-output check above can't catch alone).
    /// </summary>
    private static string? TryExtractInsertPrefix(string sql, IReadOnlyList<string> markers)
    {
        var i = 0;
        while (true) // leading whitespace and/or full-line `--` comments
        {
            var start = i;
            while (i < sql.Length && char.IsWhiteSpace(sql[i])) { i++; }
            if (i + 1 < sql.Length && sql[i] == '-' && sql[i + 1] == '-')
            {
                var newline = sql.IndexOf('\n', i);
                i = newline < 0 ? sql.Length : newline + 1;
            }
            if (i == start) { break; }
        }

        if (!TryMatchKeyword(sql, ref i, "insert") || !SkipRequiredWhitespace(sql, ref i) ||
            !TryMatchKeyword(sql, ref i, "into") || !SkipRequiredWhitespace(sql, ref i))
        {
            return null;
        }

        if (i < sql.Length && sql[i] == '[') // array form: [ m1, m2, ... ]
        {
            i++;
            for (var idx = 0; idx < markers.Count; idx++)
            {
                SkipInlineWhitespace(sql, ref i);
                if (!TryMatchMarker(sql, ref i, markers[idx])) { return null; }
                SkipInlineWhitespace(sql, ref i);
                if (idx < markers.Count - 1)
                {
                    if (i >= sql.Length || sql[i] != ',') { return null; }
                    i++;
                }
            }
            SkipInlineWhitespace(sql, ref i);
            if (i >= sql.Length || sql[i] != ']') { return null; }
            i++;
        }
        else // scalar form: exactly one marker, no brackets
        {
            if (markers.Count != 1 || !TryMatchMarker(sql, ref i, markers[0])) { return null; }
        }

        if (!SkipRequiredWhitespace(sql, ref i)) { return null; }

        var extracted = sql[i..];
        foreach (var marker in markers)
        {
            if (extracted.Contains(marker, StringComparison.Ordinal)) { return null; }
        }
        return extracted;
    }

    private static bool TryMatchMarker(string sql, ref int i, string marker)
    {
        if (i + marker.Length > sql.Length || string.CompareOrdinal(sql, i, marker, 0, marker.Length) != 0)
        {
            return false;
        }
        i += marker.Length;
        return true;
    }

    private static void SkipInlineWhitespace(string sql, ref int i)
    {
        while (i < sql.Length && char.IsWhiteSpace(sql[i])) { i++; }
    }

    private static bool TryMatchKeyword(string sql, ref int i, string keyword)
    {
        if (i + keyword.Length > sql.Length ||
            string.Compare(sql, i, keyword, 0, keyword.Length, StringComparison.OrdinalIgnoreCase) != 0)
        {
            return false;
        }

        i += keyword.Length;
        return true;
    }

    private static bool SkipRequiredWhitespace(string sql, ref int i)
    {
        var start = i;
        while (i < sql.Length && char.IsWhiteSpace(sql[i]))
        {
            i++;
        }

        return i > start;
    }

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
    /// own SQL already starts with `with` (case-insensitive), strip that keyword and join
    /// its CTE list with `, `." The CTE alias is prefixed with <c>__pz_cte__</c> so it matches
    /// what <see cref="TemplateRenderer"/> renders for <c>ref()</c> calls to ephemeral pipelines
    /// (which render <c>__pz_cte__&lt;name&gt;</c>, not <c>staging.&lt;name&gt;</c>) — otherwise
    /// the inlined CTE and the consumer body's reference to it would not match.
    /// </summary>
    private static string BuildInlinedSql(
        PipelineDef pipeline, IReadOnlyDictionary<string, RenderResult> rendered,
        IReadOnlyDictionary<string, PipelineDef> pipelinesByName)
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

        var cteList = string.Join(", ",
            ephemeralNames.Select(name => $"__pz_cte__{name} as ({rendered[name].Sql})"));
        var trimmed = sql.TrimStart();

        if (trimmed.StartsWith("with", StringComparison.OrdinalIgnoreCase))
        {
            var rest = trimmed[4..].TrimStart();
            return $"with {cteList}, {rest}";
        }

        return $"with {cteList} {sql}";
    }

    private static string NormalizeSql(string sql) =>
        sql.Replace("\r\n", "\n").Replace('\r', '\n').TrimEnd();

    /// <summary>How a sink output is fed. There is exactly one kind — an inline
    /// `sink()` claim by a pipeline — but the wrapper is retained so stage 5's resolution map and stage
    /// 10's node-building stay explicit about what a bound output carries.</summary>
    private abstract record SinkInputResolution
    {
        private SinkInputResolution() { }
        public sealed record PipelineInput(string PipelineName) : SinkInputResolution;
    }

    private static int KindRank(NodeKind kind) => kind switch
    {
        NodeKind.SourceLoad => 0,
        NodeKind.Pipeline => 1,
        NodeKind.Check => 2,
        NodeKind.SinkWrite => 3,
        _ => throw new ArgumentOutOfRangeException(nameof(kind)),
    };

    /// <summary>
    /// Kahn's algorithm; among ready nodes at each step, pick the lowest
    /// (kindRank, Name ordinal). If nodes remain once no more are ready, they form at least
    /// one cycle — walk their dependency edges (restricted to the remainder) to name one.
    /// </summary>
    private static List<DagNode> TopologicalSortOrThrow(List<DagNode> nodes)
    {
        var byId = nodes.ToDictionary(n => n.Id);
        var inDegree = nodes.ToDictionary(n => n.Id, n => n.DependsOn.Count);
        var children = new Dictionary<NodeId, List<NodeId>>();
        foreach (var node in nodes)
        {
            foreach (var dep in node.DependsOn)
            {
                if (!children.TryGetValue(dep, out var list))
                {
                    list = [];
                    children[dep] = list;
                }

                list.Add(node.Id);
            }
        }

        var comparer = Comparer<NodeId>.Create((a, b) =>
        {
            var na = byId[a];
            var nb = byId[b];
            var cmp = KindRank(na.Kind).CompareTo(KindRank(nb.Kind));
            if (cmp != 0)
            {
                return cmp;
            }

            cmp = string.CompareOrdinal(na.Name, nb.Name);
            return cmp != 0 ? cmp : string.CompareOrdinal(a.Value, b.Value);
        });

        var ready = new SortedSet<NodeId>(comparer);
        foreach (var node in nodes)
        {
            if (inDegree[node.Id] == 0)
            {
                ready.Add(node.Id);
            }
        }

        var result = new List<DagNode>(nodes.Count);
        while (ready.Count > 0)
        {
            var id = ready.Min;
            ready.Remove(id);
            result.Add(byId[id]);

            if (!children.TryGetValue(id, out var kids))
            {
                continue;
            }

            foreach (var kid in kids)
            {
                if (--inDegree[kid] == 0)
                {
                    ready.Add(kid);
                }
            }
        }

        if (result.Count == nodes.Count)
        {
            return result;
        }

        var remainder = nodes.Where(n => inDegree[n.Id] > 0).ToList();
        throw new PzValidationException([BuildCycleError(remainder, byId)]);
    }

    private static PzError BuildCycleError(List<DagNode> remainder, Dictionary<NodeId, DagNode> byId)
    {
        var remainderIds = remainder.Select(n => n.Id).ToHashSet();
        var start = remainder
            .OrderBy(n => KindRank(n.Kind))
            .ThenBy(n => n.Name, StringComparer.Ordinal)
            .First();

        var path = new List<NodeId>();
        var seen = new HashSet<NodeId>();
        var current = start.Id;
        while (seen.Add(current))
        {
            path.Add(current);
            var next = byId[current].DependsOn
                .Where(remainderIds.Contains)
                .OrderBy(id => byId[id].Name, StringComparer.Ordinal)
                .First();
            current = next;
        }

        var cycleStart = path.IndexOf(current);
        var cycleNames = path.Skip(cycleStart).Select(id => byId[id].Name).Append(byId[current].Name);
        var message = $"dependency cycle: {string.Join(" -> ", cycleNames)}";
        return new PzError(PzErrorCode.Cycle, message, null, null, null);
    }
}
