using System.CommandLine;
using Pz.Core.Dag;
using Pz.Core.Loading;
using Pz.Core.Model;
using Pz.Core.Templating;
using Pz.Core.Validation;
using Pz.Engine.Artifacts;
using Pz.Engine.Execution;
using Pz.Engine.Validation;

namespace Pz.Cli.Commands;

/// <summary>The plan <see cref="RetryCommand.BuildRetryPlan"/> hands back once it has a prior run whose
/// failed/skipped nodes still resolve against the freshly recompiled dag: exactly the pieces
/// <see cref="RunCommand.ExecuteRun"/> and the console-line rendering at each call site need.
/// <see cref="ChangedNodeNotices"/> is intentionally NOT carried here — it is a genuine third out
/// value on <see cref="RetryCommand.BuildRetryPlan"/> (see that method's doc comment) because those
/// notices are printed even on the "nothing to retry (project changed)" path, before a
/// <see cref="RetryPlan"/> would exist.</summary>
internal sealed record RetryPlan(
    PriorRun Prior, IReadOnlySet<NodeId> Selection, ReuseManifest Reuse, IReadOnlyList<NodeResult> CarriedForward);

/// <summary>`pz retry`: re-runs the LAST run's failed+skipped nodes (plus
/// whatever ancestors they need to rebuild ephemeral per-run staging — the same selection-expansion
/// <see cref="RunCommand.ExecuteRun"/> already does for every selection) instead of the whole project.
/// No <c>--select</c>/<c>--vars</c>: a retry re-runs the PRIOR intent verbatim; changing vars means
/// `pz run`. Reuses the same load/compile/dry-compile shape as `pz run`/`pz test`, then feeds the
/// selected node ids into the shared <see cref="RunCommand.ExecuteRun"/> overload — independent
/// previously-succeeded branches are never re-executed, because they are never part of the selection and
/// selection-expansion only walks ancestors, never descendants/siblings.</summary>
internal static class RetryCommand
{
    public static Command Create()
    {
        var projectOption = new Option<string?>("--project") { Description = "Project directory (default: current directory)" };
        var failFastOption = new Option<bool>("--fail-fast") { Description = "Cancel remaining nodes as soon as one fails" };
        var fullRefreshOption = new Option<bool>("--full-refresh")
        {
            Description = "Ignore stored watermarks when reading incremental datasets this run -- " +
                "capture and watermark advancement still run, re-establishing them from the full extract.",
        };
        var command = new Command("retry",
            "Re-run the last run's failed and skipped nodes (plus required ancestors) against the " +
            "current project. No --select/--vars: retry re-runs the prior intent verbatim.");
        command.Options.Add(projectOption);
        command.Options.Add(failFastOption);
        command.Options.Add(fullRefreshOption);
        command.Options.Add(SharedOptions.NoLockCheck);
        command.Options.Add(SharedOptions.LogFormat);
        command.Options.Add(SharedOptions.OtelEndpoint);
        command.Options.Add(SharedOptions.StateUrl);
        command.SetAction((parseResult, ct) => Execute(
            parseResult.GetValue(projectOption) ?? Directory.GetCurrentDirectory(),
            parseResult.GetValue(failFastOption),
            parseResult.GetValue(SharedOptions.NoLockCheck),
            parseResult.GetValue(SharedOptions.LogFormat),
            parseResult.GetValue(SharedOptions.OtelEndpoint),
            parseResult.GetValue(SharedOptions.StateUrl),
            parseResult.GetValue(fullRefreshOption),
            ct));
        return command;
    }

    internal static async Task<int> Execute(
        string projectDir, bool failFast, bool noLockCheck, string? logFormatRaw, string? otelEndpointRaw,
        string? stateUrlRaw, bool fullRefresh, CancellationToken ct)
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
            project = ProjectLoader.Load(projectDir, env);
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
            // Same implicit pre-run dry-compile `pz run`/`pz test` perform (validation tier 4).
            var dry = await SqlDryCompiler.RunAsync(fullDag, ct);
            if (dry.Errors.Count > 0)
            {
                foreach (var error in dry.Errors)
                {
                    Console.Error.WriteLine($"error {error}");
                }

                return ExitCodes.ConfigError;
            }

            var plan = BuildRetryPlan(project, fullDag, projectDir, fullRefresh,
                out var refusal, out var nothingToRetry, out var changedNodeNotices);
            if (refusal is { } err)
            {
                Console.Error.WriteLine($"error {err.Code}: {err.Message}");
                return ExitCodes.ConfigError;
            }

            foreach (var notice in changedNodeNotices)
            {
                Console.Error.WriteLine(notice);
            }

            if (nothingToRetry is { } msg)
            {
                Console.WriteLine(msg);
                return ExitCodes.Ok;
            }

            if (plan!.Reuse.Count > 0)
            {
                Console.WriteLine(
                    $"note: reusing staged data for {plan.Reuse.Count} source load(s) from run {plan.Prior.RunId}");
            }

            if (plan.CarriedForward.Count > 0)
            {
                Console.WriteLine(
                    $"note: carrying forward {plan.CarriedForward.Count} committed sink write(s) from run {plan.Prior.RunId}");
            }

            return await RunCommand.ExecuteRun(
                project, fullDag, projectDir, plan.Selection, failFast, noLockCheck, logFormat, ct,
                otelEndpoint: otelEndpoint, fullRefresh: fullRefresh, reuse: plan.Reuse, carriedForward: plan.CarriedForward);
        }
        catch (PzConfigException ex)
        {
            // The state backend's own failures (PZ0125 credentials, PZ0518 unreachable, PZ0519 schema)
            // are config errors with a code and a next step, not "unexpected engine failure" -- they must
            // exit 2 with their own message rather than 3 with a generic one. Already secret-hygienic:
            // SqlStateConnection.Unavailable never includes the connection string.
            Console.Error.WriteLine($"error {ex.Error}");
            return ExitCodes.ConfigError;
        }
        catch (Exception ex)
        {
            // Mirrors RunCommand.Execute's outer catch: an unexpected exception must never surface as a
            // raw stack trace.
            Console.Error.WriteLine(
                $"error {PzErrorCode.UnexpectedEngineFailure}: unexpected engine failure — {ex.Message}");
            return ExitCodes.Fatal;
        }
    }

    /// <summary>The plan-building core of `pz retry`, shared with <c>pz_retry</c>'s MCP adapter: the
    /// prior-run read → status guards → selection build → <see cref="RetryReusePlanner.Plan"/>
    /// composition. Console-free by design (matches this project's other extracted-core
    /// convention, e.g. <c>Pz.Mcp.Handlers.ProjectPhases</c>) -- every line to print round-trips through
    /// one of the three out parameters, so the caller renders it verbatim instead of this method
    /// deciding how/where a caller shows it.
    ///
    /// Exactly one of three outcomes: (1) <paramref name="refusal"/> non-null -- the prior run cannot be
    /// retried at all (no prior run / still "running" / ended "fatal"); the returned <see cref="PzError"/>
    /// carries the message text to print, so callers do not need to special-case which refusal it is.
    /// (2) <paramref name="nothingToRetry"/> non-null -- a retryable prior run exists but there is
    /// genuinely nothing to re-run (it already succeeded, or every failed/skipped node has since changed
    /// and re-hashed); the string is printed verbatim. (3) a non-null <see cref="RetryPlan"/> -- both outs
    /// null, a real retry should proceed.
    ///
    /// <paramref name="changedNodeNotices"/> is populated whenever the method reaches the prior run's
    /// node loop, REGARDLESS of whether that loop ends in outcome (2) or (3) -- one "note: … changed
    /// since the failed run" line per stale node id, produced before the resulting selection is checked
    /// for emptiness, so the caller prints them in that order.</summary>
    internal static RetryPlan? BuildRetryPlan(
        PzProject project, CompiledDag fullDag, string projectDir, bool fullRefresh,
        out PzError? refusal, out string? nothingToRetry, out IReadOnlyList<string> changedNodeNotices)
    {
        refusal = null;
        nothingToRetry = null;
        changedNodeNotices = [];

        // The prior run comes from whichever artifact store `state:` resolved to, exactly as
        // `pz run`/`pz clean`/`pz state` compose it. Hardcoding LocalRunArtifactStore here would fail
        // with PZ0502 under the default remote configuration (state.artifacts defaults to true when the
        // backend is not local, so run_results.json is never written locally). EnsureSchema first so a
        // store that has never been written to reports "no prior run" rather than a missing-table failure.
        var backends = StateBackendFactory.Create(project, projectDir, TimeProvider.System);
        backends.EnsureSchema();
        var prior = backends.Artifacts.ReadLatest();
        if (prior is null)
        {
            refusal = new PzError(PzErrorCode.NoPriorRun,
                "no prior run found under .pz/runs — run 'pz run' first", null, null, "run 'pz run' first");
            return null;
        }

        // A crashed run or an orchestrator-level fatal is NOT retryable by
        // selecting on recorded node status alone -- a crash can leave every recorded node showing
        // "success" (intermediate snapshots are committed atomically per node, per
        // RunResultsWriter.WriteSnapshot) with the run's own last-written status still "running", and
        // a "fatal" run may likewise carry only-success recorded nodes if the fatal failure happened
        // between node completions. Both must refuse with a clean, actionable error rather than
        // silently reporting "nothing to retry" — that would mislead the caller into thinking the
        // prior run actually finished.
        if (prior.Status == "running")
        {
            refusal = new PzError(PzErrorCode.PriorRunIncomplete,
                $"prior run was interrupted ({prior.Nodes.Count} node(s) recorded before it stopped); " +
                "re-run 'pz run'", null, null, "re-run 'pz run'");
            return null;
        }

        if (prior.Status == "fatal")
        {
            refusal = new PzError(PzErrorCode.PriorRunFatal,
                "prior run ended fatally; re-run 'pz run'", null, null, "re-run 'pz run'");
            return null;
        }

        if (prior.Status == "success")
        {
            nothingToRetry = $"nothing to retry (run {prior.RunId} succeeded)";
            return null;
        }

        var currentIds = fullDag.Nodes.Select(n => n.Id.Value).ToHashSet(StringComparer.Ordinal);
        var selection = new HashSet<NodeId>();
        var notices = new List<string>();
        foreach (var node in prior.Nodes)
        {
            if (node.Status is not ("failed" or "skipped"))
            {
                continue;
            }

            if (currentIds.Contains(node.Id))
            {
                selection.Add(new NodeId(node.Id));
            }
            else
            {
                // id = content hash, so an edited-and-therefore-rehashed node simply
                // won't match. It is only picked back up if it's an ancestor/descendant of some
                // OTHER selected (still-matching) node, via the normal selection-expansion below.
                notices.Add($"note: {node.Name} changed since the failed run; run 'pz run' for a full pass if needed");
            }
        }

        changedNodeNotices = notices;

        if (selection.Count == 0)
        {
            nothingToRetry = "nothing to retry (project changed)";
            return null;
        }

        // Plan reuse + carry-forward from the prior run's artifact. Both degrade to empty when staging
        // is gone or --full-refresh was passed.
        var (reuse, carriedForward) = RetryReusePlanner.Plan(fullDag, prior, selection, projectDir, fullRefresh);
        return new RetryPlan(prior, selection, reuse, carriedForward);
    }

    /// <summary>Same base_dir injection <see cref="RunCommand"/>/<see cref="TestCommand"/> perform.</summary>
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
