using System.CommandLine;
using Pz.Core.Dag;
using Pz.Core.Loading;
using Pz.Core.Model;
using Pz.Core.Templating;
using Pz.Core.Validation;
using Pz.Engine.Artifacts;
using Pz.Engine.Execution;
using Pz.Engine.Validation;
using Pz.PackageManagement.Hosting;

namespace Pz.Cli.Commands;

/// <summary>`pz validate`: tiers 1-2 (load + compile) -> tier 3 (connector config JSON-Schema +
/// cross-field validation) -> tier 4 (SQL dry-compile against contract-derived empty tables) -> tier 5
/// (connectivity probes + schema drift), only with `--connect` and only if tiers 1-4 passed
/// (cheapest-first: a broken project never reaches a network probe). Each tier reports ALL its findings
/// before the next tier runs. Tiers 1-4 (and a plain `pz validate` invocation) write no artifacts (no
/// `.pz/target` mutation); tier 5 is the one exception -- it writes `.pz/target/schemas.json` for every
/// source dataset without a declared `columns:` contract (<see cref="SchemaCacheWriter"/>).
/// `InjectLocalFilesBaseDir` is applied ONLY to the project instance tier 5 uses to open connectors (so
/// `localfiles` datasets resolve real paths) -- tier 3 always validates connection/dataset config exactly
/// as the user wrote it, pre-injection.</summary>
internal static class ValidateCommand
{
    public static Command Create()
    {
        var projectOption = new Option<string?>("--project") { Description = "Project directory (default: current directory)" };
        var varsOption = new Option<string?>("--vars") { Description = "JSON object of var overrides" };
        var connectOption = new Option<bool>("--connect")
        {
            Description = "Also probe connectivity and detect schema drift (tier 5)",
        };
        var command = new Command("validate",
            "Validate config and SQL without running anything (tiers 1-4: shape, semantics, " +
            "connector option schemas, SQL dry-compile); with --connect, also probe live " +
            "connections and schema drift (tier 5).");
        command.Options.Add(projectOption);
        command.Options.Add(varsOption);
        command.Options.Add(connectOption);
        command.Options.Add(SharedOptions.NoLockCheck);
        command.SetAction((parseResult, ct) => Execute(
            parseResult.GetValue(projectOption) ?? Directory.GetCurrentDirectory(),
            parseResult.GetValue(varsOption),
            parseResult.GetValue(connectOption),
            parseResult.GetValue(SharedOptions.NoLockCheck),
            ct));
        return command;
    }

    internal static async Task<int> Execute(
        string projectDir, string? varsJson, bool connect, bool noLockCheck, CancellationToken ct)
    {
        PzProject project;
        CompiledDag dag;
        var compileNotices = new List<string>();
        try
        {
            var env = SharedInputHelpers.SnapshotEnvironment();
            var overrides = SharedInputHelpers.ParseVars(varsJson);
            project = ProjectLoader.Load(projectDir, env, overrides);
            var renderCtx = new RenderContext(project, Guid.NewGuid().ToString("N"), DateTimeOffset.UtcNow) { Env = env };
            // Tier 2: DAG compile catches PZ0201-0210 semantic errors. The DAG is kept for tier 4's
            // SQL dry-compile below.
            dag = DagCompiler.Compile(project, renderCtx, compileNotices, new Pz.DuckDb.DuckDbSqlAstReader());
            // Tiers 3-5 validate the EFFECTIVE connections, not the loaded ones: an entity may be
            // declared entirely at its source()/sink() call site, and a validator reading the loaded
            // project would skip it -- silently, which is exactly how drift detection would be lost on
            // a call-site contract.
            project = project with { Connections = dag.Connections };
        }
        catch (PzValidationException ex)
        {
            foreach (var error in ex.Errors)
            {
                Console.Error.WriteLine($"error {error}");
            }

            return ExitCodes.ConfigError;
        }

        SharedInputHelpers.WriteWarnings(dag.Warnings);

        foreach (var notice in compileNotices)
        {
            Console.WriteLine($"note: {notice}");
        }

        ConnectorRegistry registry;
        ConnectorHost? host;
        try
        {
            (registry, host) = await ConnectorRegistryFactory.CreateAsync(project, projectDir, noLockCheck, ct);
        }
        catch (PzValidationException ex)
        {
            foreach (var error in ex.Errors)
            {
                Console.Error.WriteLine($"error {error}");
            }

            return ExitCodes.ConfigError;
        }

        await using var connectorHost = host;

        // Tier 3: connector connection/dataset config schemas + cross-field ValidateAsync. Validated
        // as the user wrote it (pre-base_dir-injection) -- see ConnectorConfigValidator's doc comment.
        var tier3Errors = await ConnectorConfigValidator.ValidateAsync(project, registry, ct);
        if (tier3Errors.Count > 0)
        {
            foreach (var error in tier3Errors)
            {
                Console.Error.WriteLine($"error {error}");
            }

            return ExitCodes.ConfigError;
        }

        // Tier 4: SQL dry-compile against contract-derived empty tables (throwaway DuckDB session).
        var dry = await SqlDryCompiler.RunAsync(dag, ct);
        if (dry.Errors.Count > 0)
        {
            foreach (var error in dry.Errors)
            {
                Console.Error.WriteLine($"error {error}");
            }

            return ExitCodes.ConfigError;
        }

        foreach (var dataset in dry.UndeclaredDatasets)
        {
            var affected = CountSkippedDependents(dag, dataset, dry.SkippedPipelines);
            Console.WriteLine(
                $"note: dataset '{dataset}' has no columns: contract — {affected} pipeline(s) not dry-compiled");
        }

        if (connect)
        {
            // Tier 5: InjectLocalFilesBaseDir applies ONLY to the project instance used to open
            // connectors here -- tier 3 above already validated `project.Connections`/`project.Connections` exactly
            // as the user wrote them.
            var connectProject = InjectLocalFilesBaseDir(project, projectDir);
            var connectivity = await ConnectivityValidator.RunAsync(connectProject, registry, ct);
            SchemaCacheWriter.Write(connectivity.FetchedSchemas, Path.Combine(projectDir, ".pz", "target"));

            if (connectivity.Errors.Count > 0)
            {
                foreach (var error in connectivity.Errors)
                {
                    Console.Error.WriteLine($"error {error}");
                }

                return ExitCodes.ConfigError;
            }
        }

        // One count per CONNECTION, not per direction: a connector that reads and writes (postgres,
        // localfiles) would otherwise report every connection twice.
        var connectionsChecked = project.Connections.Count(
            c => registry.TryGetSource(c.Connector, out _) || registry.TryGetSink(c.Connector, out _));
        Console.WriteLine($"validation passed ({project.Pipelines.Count} pipelines, {connectionsChecked} connections checked)");
        return ExitCodes.Ok;
    }

    /// <summary>Same base_dir injection <see cref="RunCommand"/>/<see cref="PlanCommand"/> perform (see
    /// their doc comments for why this lives at the CLI-verb level) -- tier 5 opens `localfiles`
    /// connectors too, so it needs the same connection config the executor would see.</summary>
    private static PzProject InjectLocalFilesBaseDir(PzProject project, string projectDir)
    {
        var connections = project.Connections
            .Select(s => s.Connector is "localfiles" or "sqlite" ? s with { Connection = WithBaseDir(s.Connection, projectDir) } : s)
            .ToList();
        return project with { Connections = connections };
    }

    private static IReadOnlyDictionary<string, object?> WithBaseDir(
        IReadOnlyDictionary<string, object?> connection, string projectDir)
    {
        var merged = new Dictionary<string, object?>(connection) { ["base_dir"] = projectDir };
        return merged;
    }

    /// <summary>Approximates how many pipelines a given undeclared "source.dataset" notice covers: every
    /// pipeline reachable downstream (direct or transitive) from that source's SourceLoad node that ended
    /// up in <paramref name="skippedPipelines"/>. `DryCompileResult` deliberately carries no per-dataset
    /// attribution, so a pipeline depending on more than one undeclared
    /// dataset is counted once per such notice -- acceptable for an informational count.</summary>
    private static int CountSkippedDependents(CompiledDag dag, string datasetKey, IReadOnlyList<string> skippedPipelines)
    {
        var dotIndex = datasetKey.IndexOf('.');
        if (dotIndex < 0)
        {
            return 0;
        }

        var sourceName = datasetKey[..dotIndex];
        var datasetName = datasetKey[(dotIndex + 1)..];
        var sourceNodeName = $"src_{sourceName}__{datasetName}";
        var sourceNode = dag.Nodes.FirstOrDefault(n => n.Kind == NodeKind.SourceLoad && n.Name == sourceNodeName);
        if (sourceNode is null)
        {
            return 0;
        }

        var skippedSet = skippedPipelines.ToHashSet();
        return dag.Descendants(sourceNode.Id).Count(n => n.Kind == NodeKind.Pipeline && skippedSet.Contains(n.Name));
    }

}
