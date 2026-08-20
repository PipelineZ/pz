using System.CommandLine;
using Pz.Core.Dag;
using Pz.Core.Loading;
using Pz.Core.Model;
using Pz.Core.Templating;
using Pz.Core.Validation;
using Pz.Engine.Validation;

namespace Pz.Cli.Commands;

/// <summary>`pz test`: runs ONLY Check nodes plus whatever ancestors they need (their owning
/// pipeline and, transitively, its sources) — never sinks, since a check node has no descendants
/// and <see cref="RunCommand.ExecuteRun"/>'s selection expansion only pulls in ancestors, never
/// descendants. Reuses the exact same load/compile/dry-compile/run machinery `pz run` does via the
/// shared <see cref="RunCommand.ExecuteRun"/> overload, just with an explicit node-id selection
/// instead of one resolved from a `--select` expression.</summary>
internal static class TestCommand
{
    public static Command Create()
    {
        var projectOption = new Option<string?>("--project") { Description = "Project directory (default: current directory)" };
        var varsOption = new Option<string?>("--vars") { Description = "JSON object of var overrides" };
        var command = new Command("test",
            "Run data checks (not_null, unique, row_count, freshness, accepted_values, custom_sql) " +
            "and their required ancestors — the owning " +
            "pipeline and its sources — without executing any sink. --select narrows which checks run.");
        command.Options.Add(projectOption);
        command.Options.Add(varsOption);
        command.Options.Add(SharedOptions.Select);
        command.Options.Add(SharedOptions.NoLockCheck);
        command.Options.Add(SharedOptions.LogFormat);
        command.Options.Add(SharedOptions.OtelEndpoint);
        command.Options.Add(SharedOptions.StateUrl);
        command.SetAction((parseResult, ct) => Execute(
            parseResult.GetValue(projectOption) ?? Directory.GetCurrentDirectory(),
            parseResult.GetValue(varsOption),
            parseResult.GetValue(SharedOptions.Select),
            parseResult.GetValue(SharedOptions.NoLockCheck),
            parseResult.GetValue(SharedOptions.LogFormat),
            parseResult.GetValue(SharedOptions.OtelEndpoint),
            parseResult.GetValue(SharedOptions.StateUrl),
            ct));
        return command;
    }

    internal static async Task<int> Execute(
        string projectDir, string? varsJson, string? select, bool noLockCheck, string? logFormatRaw,
        string? otelEndpointRaw, string? stateUrlRaw, CancellationToken ct)
    {
        if (!RunCommand.TryParseLogFormat(logFormatRaw, out var logFormat))
        {
            Console.Error.WriteLine(
                $"error: invalid --log-format value '{logFormatRaw}' (expected 'text' or 'json')");
            return ExitCodes.ConfigError;
        }

        if (!RunCommand.TryResolveOtelEndpoint(otelEndpointRaw, out var otelEndpoint, out var otelError))
        {
            Console.Error.WriteLine($"error: {otelError}");
            return ExitCodes.ConfigError;
        }

        PzProject project;
        CompiledDag fullDag;
        var compileNotices = new List<string>();
        try
        {
            var env = SharedInputHelpers.SnapshotEnvironment();
            var overrides = SharedInputHelpers.ParseVars(varsJson);
            project = ProjectLoader.Load(projectDir, env, overrides);
            project = InjectLocalFilesBaseDir(project, projectDir);
            if (!StateUrlOverride.TryApply(project, stateUrlRaw, env, out project, out var stateUrlError))
            {
                Console.Error.WriteLine($"error: {stateUrlError}");
                return ExitCodes.ConfigError;
            }
            var renderCtx = new RenderContext(project, Guid.NewGuid().ToString("N"), DateTimeOffset.UtcNow) { Env = env };
            fullDag = DagCompiler.Compile(project, renderCtx, compileNotices, new Pz.DuckDb.DuckDbSqlAstReader());
        }
        catch (PzValidationException ex)
        {
            foreach (var error in ex.Errors)
            {
                Console.Error.WriteLine($"error {error}");
            }

            return ExitCodes.ConfigError;
        }

        foreach (var notice in compileNotices)
        {
            Console.WriteLine($"note: {notice}");
        }

        try
        {
            // Same implicit pre-run dry-compile `pz run` does (validation tier 4) -- a broken
            // pipeline is rejected before any real run starts, never as a node failure inside one.
            var dry = await SqlDryCompiler.RunAsync(fullDag, ct);
            if (dry.Errors.Count > 0)
            {
                foreach (var error in dry.Errors)
                {
                    Console.Error.WriteLine($"error {error}");
                }

                return ExitCodes.ConfigError;
            }

            IReadOnlySet<NodeId>? selectFilter;
            try
            {
                selectFilter = RunSelection.Resolve(fullDag, select);
            }
            catch (PzValidationException ex)
            {
                foreach (var error in ex.Errors)
                {
                    Console.Error.WriteLine($"error {error}");
                }

                return ExitCodes.ConfigError;
            }

            var checkIds = fullDag.Nodes
                .Where(n => n.Kind == NodeKind.Check)
                .Where(n => selectFilter is null || selectFilter.Contains(n.Id))
                .Select(n => n.Id)
                .ToHashSet();

            if (checkIds.Count == 0)
            {
                Console.WriteLine("no checks defined");
                return ExitCodes.Ok;
            }

            return await RunCommand.ExecuteRun(
                project, fullDag, projectDir, checkIds, failFast: false, noLockCheck, logFormat, ct,
                otelEndpoint: otelEndpoint);
        }
        catch (Exception ex)
        {
            // Mirrors RunCommand.Execute's outer catch: an unexpected exception must never surface
            // as a raw stack trace, and PzValidationException from --select parsing above is already
            // handled locally.
            Console.Error.WriteLine(
                $"error {PzErrorCode.UnexpectedEngineFailure}: unexpected engine failure — {ex.Message}");
            return ExitCodes.Fatal;
        }
    }

    /// <summary>Same base_dir injection <see cref="RunCommand"/>/<see cref="PlanCommand"/> perform
    /// (see their doc comments for why this lives at the CLI-verb level) -- a check's owning
    /// pipeline may itself depend on a `localfiles` source, which needs the same connection config
    /// the executor would see.</summary>
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

}
