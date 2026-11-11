using Pz.Core.Model;
using Pz.Core.Templating;
using Pz.Core.Validation;

namespace Pz.Core.Dag;

/// <summary>
/// Turns a loaded+rendered <see cref="PzProject"/> into a <see cref="CompiledDag"/> of
/// content-addressed nodes.
/// </summary>
public static partial class DagCompiler
{
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
        //    its Dependencies are needed for both ref/source validation and inheritance). One
        //    pipeline's template is independent of every other's, so a broken template in pipeline A
        //    must not hide a broken template in pipeline B -- every pipeline gets a chance to render,
        //    and every failure is collected before throwing once. Stopping here (rather than seeding
        //    `rendered` with a placeholder for a failed pipeline and continuing into stage 1b) is the one
        //    genuine dependency below: every later stage indexes `rendered[pipeline.Name]` unconditionally
        //    for EVERY pipeline, so a project that fails to render cannot proceed to stages that assume
        //    it did.
        var rendered = new Dictionary<string, RenderResult>();
        var renderErrors = new List<PzError>();
        foreach (var pipeline in project.Pipelines)
        {
            try
            {
                rendered[pipeline.Name] = TemplateRenderer.Render(pipeline, ctx);
            }
            catch (PzValidationException ex)
            {
                renderErrors.AddRange(ex.Errors);
            }
        }

        if (renderErrors.Count > 0)
        {
            throw new PzValidationException(OrderErrors(renderErrors));
        }

        // 1a. `run_id`/`run_started_at` land in RENDERED SQL, which `.pz/target` writes to disk and
        //     which feeds this Pipeline node's NodeId (stage 8 below hashes the rendered text
        //     verbatim, same as every other pipeline) -- so a pipeline that embeds either constant
        //     gets a different NodeId on every run, and `pz retry` (which matches by NodeId against
        //     the failed run's results) can never treat two runs of that pipeline as the same node.
        //     A non-blocking WARNING, once per pipeline regardless of how many times the pipeline's
        //     SQL references either constant (RenderResult.UsesRunIdentity is a single bool, not a
        //     count).
        var templateWarnings = new List<PzWarning>();
        foreach (var pipeline in project.Pipelines.Where(p => rendered[p.Name].UsesRunIdentity)
            .OrderBy(p => p.Name, StringComparer.Ordinal))
        {
            templateWarnings.Add(new PzWarning(PzErrorCode.RunIdentityInRenderedSql,
                $"pipeline '{pipeline.Name}' renders run_id or run_started_at into its SQL -- this " +
                "changes the pipeline's NodeId every run, so `pz retry` can never match this node " +
                "against a prior run and will always re-run it",
                pipeline.FilePath, null,
                "remove run_id/run_started_at from the rendered SQL if this pipeline should be " +
                "retry-matchable, or ignore this if always re-running it is intended"));
        }

        // 1b. Strip the leading `INSERT INTO {{ sink(...) }}` from every pipeline that binds a sink inline.
        ExtractInlineSinkPrefixes(project, rendered);

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

        // 1d. The read side of the same rule. An entity exists
        //     because something reads or writes it, so a source() naming an entity the connection does
        //     not declare synthesizes one from the call's kwargs -- exactly as 1c synthesizes an
        //     OutputDef. Declaring BOTH is PZ0341, the same either/or the write side enforces: there is
        //     no effective-config assembly in either direction.
        //     Ordered by pipeline then entity so the synthesized list is deterministic.
        //     Every pipeline, including ephemerals, and each one's OWN rendered dependencies rather than
        //     EffectiveDependencies: a source() call declares its entity wherever it is written, and
        //     following ephemeral chains here would resolve ref()s that stage 2 has not validated yet.
        var readsByConnection = new Dictionary<string, Dictionary<string, DatasetDef>>(StringComparer.Ordinal);
        // The read-side twin of stage 5's sink() near-miss warning below, sharing the same rationale
        // (SourceFunction's doc comment) -- a kwarg one edit from a pz-owned name (`sync`/`retry`/
        // `columns`/`partition_column`/`partitions`) rides through as a connector read option unchecked,
        // silently, unless named here.
        var sourceWarnings = new List<PzWarning>();
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

                foreach (var option in dep.Read.Options.Keys.OrderBy(k => k, StringComparer.Ordinal))
                {
                    var nearMiss = SourceFunction.NearMissKwarg(option);
                    if (nearMiss is not null)
                    {
                        sourceWarnings.Add(new PzWarning(PzErrorCode.UnresolvedRef,
                            $"pipeline '{pipeline.Name}' passes source('{dep.SourceName}', '{dep.Dataset}', " +
                            $"{option}: ...), which pz does not recognize — it is being sent to the " +
                            $"connector as a read option. Did you mean '{nearMiss}'?",
                            pipeline.FilePath, null,
                            $"rename it to '{nearMiss}', or ignore this if '{option}' really is an " +
                            $"option of the '{connection.Connector}' connector"));
                    }
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
                    new DatasetDef(dep.Dataset, r.Options, r.Columns, r.Sync, r.Retry)
                    {
                        RedactedColumns = r.RedactedColumns,
                    });
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
        //    No "unknown write mode" check is needed: SinkFunction refuses a strategy outside
        //    replace/append/merge at the call site (PZ0334), so an OutputDef cannot reach here carrying one.
        var configErrors = new List<PzError>();
        CheckMergeKeys(project, configErrors);
        var rewrittenConnections = CheckIncrementalSources(project, notices, configErrors);
        CheckSourcePathTemplates(project, configErrors);
        CheckFilesPerPartition(project, configErrors);
        CheckPartitionedOutputs(project, configErrors);

        // 2. Validate every ref()/source() resolves -> PZ0201. Independent of stage 0 above: it reads
        //    only `rendered`, `pipelinesByName` (fixed at the top of Compile) and `connectionsByName`
        //    (fixed once, after stage 1c/1d's synthesis -- stage 0's rewrite below only canonicalizes
        //    Incremental fields on datasets already in that map, never adds/removes a connection or
        //    dataset name), so it neither needs nor is needed by stage 0's result.
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
                            pipeline.FilePath, null,
                            NearMissHint(pipelinesByName.Keys, pipelineRef.Name,
                                "check pipelines/ for the pipeline's file stem -- that is its name")));
                        break;
                    // There is no ENTITY half to this check: an entity exists because something reads
                    // it, so stage 1d has already synthesized one for every referenced entity of a known
                    // connection. Whether that entity exists in the remote system stays --connect work:
                    // pz does not know a catalog offline.
                    case DepRef.Source sourceRef when !connectionsByName.ContainsKey(sourceRef.SourceName):
                        refErrors.Add(new PzError(PzErrorCode.UnresolvedRef,
                            $"pipeline '{pipeline.Name}' calls source('{sourceRef.SourceName}', '{sourceRef.Dataset}') but no connection named '{sourceRef.SourceName}' exists",
                            pipeline.FilePath, null,
                            NearMissHint(connectionsByName.Keys, sourceRef.SourceName,
                                $"declare it in connections.yml:\n  {sourceRef.SourceName}:\n    connector: <connector>")));
                        break;
                }
            }
        }

        // 3. Ephemeral pipelines may not declare checks -> PZ0205. Independent of stages 0/2 above:
        //    reads only pipeline Materialization/Checks, untouched by either.
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

        // 4. Ephemeral pipelines may not chain -> PZ0204. Independent of stages 0/2/3: reads only
        //    `rendered` and `pipelinesByName`, neither touched by them.
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

        // Stages 0, 2, 3 and 4 above each stand alone -- none reads another's error list or a value
        // another one computes -- so they report together in ONE throw instead of stopping at whichever
        // happens to run first.
        var independentStageErrors = configErrors
            .Concat(refErrors).Concat(checksOnEphemeralErrors).Concat(chainErrors).ToList();
        if (independentStageErrors.Count > 0)
        {
            throw new PzValidationException(OrderErrors(independentStageErrors));
        }

        project = project with { Connections = rewrittenConnections };

        // 5. Resolve every sink output's binding (inline `INSERT INTO {{ sink(...) }}` is the SOLE load
        //    path). First collect every pipeline's
        //    inline claim(s), validating the SINK each names actually exists (PZ0201, same code as
        //    ref()/source() lookups) — a pipeline may claim N outputs via the array fan-out form (stage
        //    1b already guarantees no duplicate output within one pipeline). Then walk every output
        //    once: more than one inline claimant is PZ0206 (ERROR). Errors aggregate and throw.
        //    No output-existence check or orphan-output warning is needed: stage 1c builds an output
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
                        $"pipeline '{pipeline.Name}' calls sink('{binding.Sink}', '{binding.Output}') but no connection named '{binding.Sink}' exists",
                        pipeline.FilePath, null,
                        NearMissHint(connectionsByName.Keys, binding.Sink,
                            $"declare it in connections.yml:\n  {binding.Sink}:\n    connector: <connector>")));
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

        var claimantByOutput = new Dictionary<(ConnectionDef Sink, OutputDef Output), string>();
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

                claimantByOutput[(sink, output)] = claimants[0];
            }
        }

        if (sinkErrors.Count > 0)
        {
            throw new PzValidationException(sinkErrors);
        }

        // 6. Determine which source datasets are actually referenced (directly, or inherited
        //    through an inlined ephemeral pipeline). Sinks never drain a source directly — a
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
            assembledSql[pipeline.Name] = NormalizeSql(BuildInlinedSql(pipeline, rendered, pipelinesByName, sqlAst));
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
        // Case-insensitive: DuckDB folds an unquoted identifier's case, so two datasets differing only
        // by case land in the same relation exactly as a folded-together pair does.
        var stagingCollisions = new List<PzError>();
        // A non-ephemeral pipeline's CREATE OR REPLACE TABLE/VIEW targets `staging.<pipeline name>`
        // verbatim (PipelineExecutor) -- the identical relation a SourceLoad named
        // `src_<connection>__<entity>` stages to. Checked case-insensitively for the same reason as
        // the dataset-vs-dataset fold above.
        var pipelineStagingNames = project.Pipelines
            .Where(p => p.Materialization != "ephemeral")
            .ToDictionary(p => p.Name, p => p, StringComparer.OrdinalIgnoreCase);
        // One map for the whole project, not one per connection: the relation name is
        // `src_<connection>__<folded entity>`, so two connections reach one name by case alone, or by an
        // underscore that sits on either side of the separator (`a` + `_b`, `a_` + `b`).
        var stagedRelations = new Dictionary<string, (string Connection, string Dataset)>(StringComparer.OrdinalIgnoreCase);
        foreach (var source in project.Connections)
        {
            foreach (var dataset in source.Datasets)
            {
                if (!referencedSources.Contains((source.Name, dataset.Name)))
                {
                    continue;
                }

                var stagingName = StagingName.ForSourceLoad(source.Name, dataset.Name);
                if (!stagedRelations.TryAdd(stagingName, (source.Name, dataset.Name)))
                {
                    var first = stagedRelations[stagingName];
                    stagingCollisions.Add(new PzError(PzErrorCode.DuplicateName,
                        (first.Connection == source.Name
                            ? $"source '{source.Name}': datasets '{first.Dataset}' and '{dataset.Name}' "
                            : $"sources '{first.Connection}.{first.Dataset}' and '{source.Name}.{dataset.Name}' ") +
                        "cannot be told apart in staging.",
                        source.FilePath, null,
                        $"rename one of them -- both stage as '{stagingName}': pz folds every character " +
                        "outside [A-Za-z0-9_] to '_' when it stages a read, and DuckDB itself folds identifier case"));
                }

                if (pipelineStagingNames.TryGetValue(stagingName, out var collidingPipeline))
                {
                    stagingCollisions.Add(new PzError(PzErrorCode.PipelineNameCollidesWithStaging,
                        $"pipeline '{collidingPipeline.Name}' and source '{source.Name}.{dataset.Name}' " +
                        $"both stage to 'staging.{stagingName}' -- the pipeline's CREATE OR REPLACE and " +
                        "the source's landing would target the same DuckDB relation.",
                        collidingPipeline.FilePath, null,
                        $"rename the pipeline -- '{source.Name}.{dataset.Name}' always stages as " +
                        $"'{stagingName}'"));
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
                string optionsJson;
                try
                {
                    optionsJson = CanonicalJson.Serialize(dataset.Options);
                }
                catch (NotSupportedException)
                {
                    stagingCollisions.Add(new PzError(PzErrorCode.UnsupportedOptionValue,
                        $"source '{source.Name}.{dataset.Name}': option '{FindUnsupportedOptionKey(dataset.Options)}' " +
                        "has a value pz cannot hash into this node's identity",
                        source.FilePath, null,
                        "use a string/number/bool/date value, or a list/mapping of them, for this option"));
                    continue;
                }

                var canonicalParts = new List<string>
                {
                    "source-load", source.Name, dataset.Name,
                    optionsJson,
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
        var checkNames = new HashSet<string>(StringComparer.Ordinal);
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
                // Distinct checks can share the conventional name (two row_counts; `[a_b]` against
                // `[a, b]`). Later ones take a numeric suffix so --select stays unambiguous; the first
                // keeps the conventional name, so a project without collisions is unchanged.
                var conventionalName = name;
                for (var n = 2; !checkNames.Add(name); n++)
                {
                    name = $"{conventionalName}_{n}";
                }

                var canonical = string.Join('\n', "check", pipeline.Name, CheckIdentity.Canonical(check));
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
        //     inline-bound, so its Output.Input is synthesized here from the claimant pipeline's name
        //     for every downstream consumer — the canonical hash below and SinkWriteExecutor's
        //     staging-relation lookup — that expects it populated. Only outputs with a claimant reach
        //     here, so isInlineBound is always true.
        var sinkOptionErrors = new List<PzError>();
        foreach (var sink in project.Connections)
        {
            foreach (var output in sink.Outputs)
            {
                if (!claimantByOutput.TryGetValue((sink, output), out var pipelineName))
                {
                    continue; // no resolved claimant — no SinkWrite node
                }

                string outputOptionsJson;
                try
                {
                    outputOptionsJson = CanonicalJson.Serialize(output.Options);
                }
                catch (NotSupportedException)
                {
                    sinkOptionErrors.Add(new PzError(PzErrorCode.UnsupportedOptionValue,
                        $"output '{sink.Name}.{output.Name}': option '{FindUnsupportedOptionKey(output.Options)}' " +
                        "has a value pz cannot hash into this node's identity",
                        sink.FilePath, null,
                        "use a string/number/bool/date value, or a list/mapping of them, for this option"));
                    continue;
                }

                var isInlineBound = string.IsNullOrEmpty(output.Input);
                var effectiveOutput = isInlineBound ? output with { Input = pipelineName } : output;
                var inputNodeId = pipelineNodeIds[pipelineName];
                // Keys/AcceptDuplicates/OnDelete all change what a commit under this Id MEANS -- e.g.
                // merge `keys:` is half the join condition a future write against the same relation
                // performs, so a NodeId that ignored it would let `pz retry` carry forward a prior
                // commit made under different match semantics as though it were the same output.
                // Retry (RetryDef) is deliberately excluded: it governs how many attempts/backoff THIS
                // run gives the write, not what got committed, so changing it must not stop `pz retry`
                // from matching a prior success.
                var canonical = string.Join('\n', "sink-write", sink.Name, output.Name, effectiveOutput.Input,
                    output.Mode, output.SchemaPolicy, outputOptionsJson,
                    CanonicalJson.Serialize(output.Keys), output.AcceptDuplicates.ToString(),
                    output.OnDelete ?? "");
                var id = NodeId.Compute(canonical);
                nodes.Add(new DagNode(id, NodeKind.SinkWrite, $"{sink.Name}.{output.Name}",
                    [inputNodeId], null, new SinkOutputDef(sink, effectiveOutput, isInlineBound)));
            }
        }

        if (sinkOptionErrors.Count > 0)
        {
            throw new PzValidationException(sinkOptionErrors);
        }

        // 10b. The (read x write) pairing matrices over the finished node list; see CheckDeliveryMatrices.
        CheckDeliveryMatrices(nodes);

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
        var compiled = new CompiledDag(ordered)
        {
            Warnings = [.. project.Warnings, .. sourceWarnings, .. sinkWarnings, .. templateWarnings],
            Connections = project.Connections,
        };

        // 12. Advisory notices and warnings derived from the finished DAG.
        AddEffectivelyOnceNotices(compiled, notices);

        var coarse = CoarseWatermarkFormatWarnings(compiled, ordered);
        return coarse.Count == 0 ? compiled : compiled with { Warnings = [.. compiled.Warnings, .. coarse] };
    }

    /// <summary>Which top-level key in an options bag CanonicalJson.Serialize could not hash -- walked
    /// lazily, only once serializing the whole bag has already thrown, so the common case (nothing
    /// unsupported) pays no extra cost. A bad value nested inside a key's own list/mapping still names
    /// that top-level key, since Serialize(value) recurses into it and throws at the same depth.
    /// Never returns the value itself (secret hygiene) -- only the key CanonicalJson.Serialize(options)
    /// as a whole already proved is the problem.</summary>
    private static string FindUnsupportedOptionKey(IReadOnlyDictionary<string, object?> options)
    {
        foreach (var (key, value) in options)
        {
            try
            {
                CanonicalJson.Serialize(value);
            }
            catch (NotSupportedException)
            {
                return key;
            }
        }

        // Defensive: Serialize(options) threw but no individual top-level entry's re-serialization
        // does. Not reachable today (every code path that builds an options dictionary uses the same
        // entry shapes CanonicalJson already walks identically at the top and nested levels), but
        // this must still name something rather than throw a second, unhandled exception here.
        return "(unknown)";
    }

    /// <summary>The next step for an unresolved ref()/source()/sink() name: a one-edit-or-case-only
    /// near miss against every name actually declared, or <paramref name="fallback"/> (how to declare
    /// one) when nothing is close enough to guess.</summary>
    private static string NearMissHint(IEnumerable<string> known, string requested, string fallback)
    {
        var nearMiss = NearMiss.Find(known, requested);
        return nearMiss is null ? fallback : $"did you mean '{nearMiss}'?";
    }

    /// <summary>Deterministic order for an aggregated batch of errors from more than one independent
    /// stage: by file (nulls first -- a project-level error with no single file), then by line/position
    /// (unpositioned first), then by each error's original position in <paramref name="errors"/> as the
    /// final tie-break -- which is itself deterministic, since every stage below builds its own list by
    /// walking pipelines/connections in declaration or ordinal order before appending it here. This is
    /// what lets two stages' errors merge into one throw without their relative order depending on
    /// dictionary enumeration or anything else non-deterministic.</summary>
    private static IReadOnlyList<PzError> OrderErrors(IEnumerable<PzError> errors) =>
        [.. errors
            .Select((error, index) => (Error: error, Index: index))
            .OrderBy(e => e.Error.File, StringComparer.Ordinal)
            .ThenBy(e => e.Error.Line)
            .ThenBy(e => e.Index)
            .Select(e => e.Error)];

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
}
