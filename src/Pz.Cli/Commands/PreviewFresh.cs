using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Loading;
using Pz.Core.Model;
using Pz.Core.Templating;
using Pz.Core.Validation;
using Pz.DuckDb;
using Pz.Engine.Dispatch;
using Pz.Engine.Execution;
using Pz.Engine.Planning;
using Pz.Engine.Preview;
using Pz.Engine.Resilience;

namespace Pz.Cli.Commands;

/// <summary>A node failed while computing a fresh preview: exit 1, with that node's own error.</summary>
internal sealed class PreviewNodeFailedException(PzError error) : Exception(error.Message)
{
    public PzError Error { get; } = error;
}

/// <summary><c>pz preview --fresh</c>: compiles the current project and runs the node and its ancestors in a
/// throwaway in-memory DuckDB, every source read capped at the slice. Never a sink, a check, a state store
/// (no watermark, sync token or cdc slot is read or written), or a <c>.pz/runs</c> directory: rows live only
/// in that DuckDB and in this process.</summary>
internal static class PreviewFresh
{
    internal static async Task<PreviewResult> RunAsync(string projectDir, string? varsJson, string node, int limit,
        int slice, Action<string> notice, CancellationToken ct)
    {
        var env = SharedInputHelpers.SnapshotEnvironment();
        var project = ProjectLoader.Load(projectDir, env, SharedInputHelpers.ParseVars(varsJson));
        project = SharedInputHelpers.AnchorToProjectDir(project, projectDir);

        // An ephemeral pipeline is inlined into its readers and has no node; compiled as a view it gets one,
        // with its own ancestors.
        if (project.Pipelines.FirstOrDefault(p => p.Name == node) is { Materialization: "ephemeral" } ephemeral)
        {
            project = project with
            {
                Pipelines = [.. project.Pipelines.Select(p => p == ephemeral ? p with { Materialization = "view" } : p)],
            };
        }

        CompiledDag dag;
        var renderCtx = new RenderContext(project, Guid.NewGuid().ToString("N"), DateTimeOffset.UtcNow) { Env = env };
        using (var sqlAst = new DuckDbSqlAstReader())
        {
            dag = DagCompiler.Compile(project, renderCtx, [], sqlAst);
        }

        var target = dag.Nodes.FirstOrDefault(n => n.Name == node) ?? throw new PzValidationException([
            new PzError(PzErrorCode.SelectorNoMatch, $"no node named '{node}' in this project.", null, null,
                "pick a node name from `pz ls`")]);
        if (target.Kind is NodeKind.Check or NodeKind.SinkWrite)
        {
            throw new PzValidationException([new PzError(PzErrorCode.PreviewNodeNotPreviewable,
                $"'{node}' is a {(target.Kind == NodeKind.Check ? "check" : "sink")}, which has no rows of its own to preview.",
                null, null, "preview the pipeline that feeds it")]);
        }

        dag = AsSnapshots(dag);
        var marks = target.Kind == NodeKind.SourceLoad
            ? ((SourceDatasetDef)target.Definition).Dataset.RedactedColumns
            : ((PipelineDef)target.Definition).RedactedColumns;

        var tempRoot = Path.Combine(Path.GetTempPath(), "pz-preview", Guid.NewGuid().ToString("N"));
        try
        {
            var duckOptions = project.Engine.DuckDb is { } cfg
                ? new DuckOptions(cfg.MemoryLimit, cfg.Threads, cfg.TempDirectory)
                : null;
            await using var duck = DuckSession.Open(":memory:", duckOptions);
            var extensions = await DuckExtensions.LoadAsync(duck, project.Extensions, ct);
            if (extensions.Errors.Count > 0)
            {
                throw new PzValidationException(extensions.Errors);
            }

            await duck.ExecuteAsync("create schema if not exists staging", ct);

            var runId = $"preview-{Guid.NewGuid():N}";
            // No run id: a process connector's socket root then goes under the temp directory, owned and
            // deleted, rather than under <project>/.pz/runs/<id>, which would leave a trace.
            var (registry, host) = await ConnectorRegistryFactory.CreateAsync(
                project, projectDir, noLockCheck: false, ct, runId: null, connectorLog: (_, _, message) => notice(message));
            await using var connectorHost = host;

            var selection = new HashSet<NodeId> { target.Id };
            var plan = await new ExecutionPlanner(registry).PlanAsync(dag, project.Engine.ForceUniversal, ct,
                project.Engine, RunOrchestrator.EffectiveNodeIds(dag, selection));

            var ctx = new RunContext(duck, registry, new RunPaths(tempRoot, runId), NullRunEvents.Instance, plan,
                project.Engine.BatchBytes is { } batchBytes ? new BatchOptions(batchBytes) : null,
                Watermarks: null, FullRefresh: true, Notice: notice,
                RateLimiters: new RateLimiterRegistry(TimeProvider.System))
            {
                SourceSliceRows = slice,
                NodeTimeout = project.Engine.NodeTimeout,
            };
            var result = await new RunOrchestrator(new KindDispatchingExecutor(), ctx)
                .ExecuteAsync(dag, new RunOptions(project.Engine.Threads, FailFast: true, selection, project.Name), ct);

            if (result.Nodes.FirstOrDefault(n => n.Status == NodeStatus.Failed) is { } failed)
            {
                throw new PreviewNodeFailedException(failed.Error ?? new PzError(PzErrorCode.NodeFailed,
                    $"node '{failed.Name}' failed.", null, null, null));
            }

            var (columns, rows) = await PreviewSampler.SampleAsync(duck, $"staging.\"{target.Name}\"", marks, limit, ct);
            return new PreviewResult(node, "fresh", null, columns, rows, TotalRows: null, limit, Sliced: true);
        }
        finally
        {
            try { Directory.Delete(tempRoot, recursive: true); } catch { /* best-effort; may never have been created */ }
        }
    }

    /// <summary>A cdc dataset read fresh is a plain snapshot: no replication slot, no change window.</summary>
    private static CompiledDag AsSnapshots(CompiledDag dag) => dag with
    {
        Nodes = [.. dag.Nodes.Select(n => n.Definition is SourceDatasetDef { Dataset.SyncMode.Mode: SyncMode.Cdc } def
            ? n with { Definition = def with { Dataset = def.Dataset with { SyncMode = null } } }
            : n)],
    };
}
