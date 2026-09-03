using System.CommandLine;
using Pz.Core.Dag;
using Pz.Core.Loading;
using Pz.Core.Model;
using Pz.Core.Templating;
using Pz.Core.Validation;
using Pz.Engine.Artifacts;
using Pz.Engine.Dispatch;
using Pz.Engine.Execution;
using Pz.Engine.Planning;
using Pz.Engine.Resilience;
using Pz.PackageManagement.Hosting;

namespace Pz.Cli.Commands;

/// <summary>`pz plan`: shows the per-node <see cref="EdgeStrategy"/> the engine would use (native
/// scan/copy vs. the universal batch path) and why, then persists the same plan `pz run` would
/// compute to <c>.pz/target/plan.json</c> — without executing anything.</summary>
internal static class PlanCommand
{
    public static Command Create()
    {
        var projectOption = new Option<string?>("--project") { Description = "Project directory (default: current directory)" };
        var varsOption = new Option<string?>("--vars") { Description = "JSON object of var overrides" };
        var namesArgument = new Argument<string[]>("names")
        {
            Description = "Flow name(s): filter the printed table to each node plus every ancestor and descendant",
            Arity = ArgumentArity.ZeroOrMore,
        };
        var command = new Command("plan",
            "Show the per-node execution strategy (native scan/copy vs. the universal batch path) " +
            "the engine would use, and why; writes .pz/target/plan.json (always full-project -- " +
            "names/--select/--all filter only the printed rows).");
        command.Arguments.Add(namesArgument);
        command.Options.Add(projectOption);
        command.Options.Add(varsOption);
        command.Options.Add(SharedOptions.Select);
        command.Options.Add(SharedOptions.All);
        command.Options.Add(SharedOptions.NoLockCheck);
        command.SetAction((parseResult, ct) => Execute(
            parseResult.GetValue(projectOption) ?? Directory.GetCurrentDirectory(),
            parseResult.GetValue(varsOption),
            parseResult.GetValue(namesArgument) ?? [],
            parseResult.GetValue(SharedOptions.Select),
            parseResult.GetValue(SharedOptions.All),
            parseResult.GetValue(SharedOptions.NoLockCheck),
            ct));
        return command;
    }

    internal static async Task<int> Execute(
        string projectDir, string? varsJson, string[] names, string? select, bool all,
        bool noLockCheck, CancellationToken ct)
    {
        try
        {
            var env = SharedInputHelpers.SnapshotEnvironment();
            var overrides = SharedInputHelpers.ParseVars(varsJson);
            var project = ProjectLoader.Load(projectDir, env, overrides);
            project = SharedInputHelpers.AnchorToProjectDir(project, projectDir);
            var renderCtx = new RenderContext(project, Guid.NewGuid().ToString("N"), DateTimeOffset.UtcNow) { Env = env };
            var compileNotices = new List<string>();
            var fullDag = DagCompiler.Compile(project, renderCtx, compileNotices, new Pz.DuckDb.DuckDbSqlAstReader());
            SharedInputHelpers.WriteWarnings(fullDag.Warnings);
            foreach (var notice in compileNotices)
            {
                Console.WriteLine($"note: {notice}");
            }

            var runDag = new CompiledDag([.. fullDag.Nodes.Where(n => n.Kind != NodeKind.Check)]);

            // Resolved before planning: the planner takes the same effective set `pz run` with this
            // selection would execute, so a refusal on a node outside it is recorded, not raised --
            // `pz plan <flow>` shows exactly the plan `pz run <flow>` will use.
            var selection = RunSelection.Resolve(fullDag, names, select, all, gateBareMultiFlow: false);

            var (registry, host) = await ConnectorRegistryFactory.CreateAsync(project, projectDir, noLockCheck, ct);
            await using var connectorHost = host;
            var plan = await new ExecutionPlanner(registry)
                .PlanAsync(runDag, project.Engine.ForceUniversal, ct, project.Engine,
                    RunOrchestrator.EffectiveNodeIds(runDag, selection));
            var rows = selection is null ? plan.Nodes : [.. plan.Nodes.Where(n => selection.Contains(n.Id))];

            // Selection (positional names, --select, or --all) only narrows what
            // this table prints; plan.json (written below regardless) always covers the full project,
            // so make that explicit whenever a selection is active to avoid readers assuming plan.json
            // was itself filtered.
            if (selection is not null)
            {
                Console.WriteLine("note: table filtered by selection; plan.json covers the full project");
            }

            Console.WriteLine($"{"strategy",-13} {"node",-24} {"reason"}");
            foreach (var node in rows)
            {
                Console.WriteLine($"{StrategyName(node.Strategy),-13} {node.Name,-24} {node.Reason}{PushdownSuffix(node)}");
            }

            Console.WriteLine(FormatBudgetLine(plan.MemoryBudget));
            // The budget line above is a ceiling on what pz+DuckDB may hold, not a promise the workload
            // fits inside duckdb.memory_limit. Printed as its own `note:` line rather than folded into
            // the budget line, whose shape output assertions pin.
            if (plan.MemoryBudget.DuckDbThreadsDisclaimer is { } threadsNote)
            {
                Console.WriteLine($"note: {threadsNote}");
            }

            foreach (var source in project.Connections.OrderBy(s => s.Name, StringComparer.Ordinal))
            {
                if (source.Retry is not null)
                {
                    Console.WriteLine(FormatRetryLine("source", source.Name, null, source.Retry));
                }

                foreach (var dataset in source.Datasets
                    .Where(d => d.Retry is not null).OrderBy(d => d.Name, StringComparer.Ordinal))
                {
                    Console.WriteLine(FormatRetryLine(
                        "source", $"{source.Name}.{dataset.Name}", dataset.Retry, source.Retry));
                }
            }

            // An output's `retry:` is a sink() keyword argument, so
            // the loaded project no longer carries it -- the compiled SinkWrite nodes do. Ordered by
            // "<sink>.<output>", which is exactly the node Name, so the printed order is unchanged.
            var outputRetries = fullDag.Nodes
                .Where(n => n.Kind == NodeKind.SinkWrite)
                .Select(n => (SinkOutputDef)n.Definition!)
                .Where(d => d.Output.Retry is not null)
                .ToLookup(d => d.Sink.Name, StringComparer.Ordinal);

            foreach (var sink in project.Connections.OrderBy(s => s.Name, StringComparer.Ordinal))
            {
                if (sink.Retry is not null)
                {
                    Console.WriteLine(FormatRetryLine("sink", sink.Name, null, sink.Retry));
                }

                foreach (var def in outputRetries[sink.Name]
                    .OrderBy(d => d.Output.Name, StringComparer.Ordinal))
                {
                    Console.WriteLine(FormatRetryLine(
                        "sink", $"{sink.Name}.{def.Output.Name}", def.Output.Retry, sink.Retry));
                }
            }

            foreach (var source in project.Connections.OrderBy(s => s.Name, StringComparer.Ordinal))
            {
                if (source.MaxConcurrency is { } sourceCap)
                {
                    Console.WriteLine(FormatMaxConcurrencyLine("source", source.Name, sourceCap));
                }
            }

            foreach (var sink in project.Connections.OrderBy(s => s.Name, StringComparer.Ordinal))
            {
                if (sink.MaxConcurrency is { } sinkCap)
                {
                    Console.WriteLine(FormatMaxConcurrencyLine("sink", sink.Name, sinkCap));
                }
            }

            PlanWriter.Write(plan, Path.Combine(projectDir, ".pz", "target"));
            return ExitCodes.Ok;
        }
        catch (PzValidationException ex)
        {
            foreach (var error in ex.Errors)
            {
                Console.Error.WriteLine($"error {error}");
            }

            return ExitCodes.ConfigError;
        }
    }

    // Mirrors PlanWriter.StrategyName (internal to Pz.Engine, not visible across assemblies) so the
    // on-screen table uses the exact same strategy names as plan.json.
    /// <summary>Says what this read pushes to the source, so a pushed read is distinguishable from an
    /// unpushed one without watching the network. Counts only — the
    /// predicate's TEXT stays out of console output for the same reason it stays out of plan.json.</summary>
    private static string PushdownSuffix(PlannedNode node)
    {
        if (node.Pushdown is not { } pushdown)
        {
            return string.Empty;
        }

        var parts = new List<string>(2);
        if (pushdown.ColumnsPushed is { } columns)
        {
            parts.Add($"{columns} column{(columns == 1 ? "" : "s")}");
        }

        if (pushdown.PredicatePushed)
        {
            parts.Add("filter");
        }

        return parts.Count > 0 ? $" [pushed: {string.Join(" + ", parts)}]" : string.Empty;
    }

    private static string StrategyName(EdgeStrategy strategy) => strategy switch
    {
        EdgeStrategy.NativeScan => "native_scan",
        EdgeStrategy.NativeCopy => "native_copy",
        EdgeStrategy.ArrowStream => "arrow_stream",
        EdgeStrategy.DuckSql => "duck_sql",
        _ => throw new ArgumentOutOfRangeException(nameof(strategy), strategy, "unknown strategy"),
    };

    /// <summary>Renders the memory budget the planner already computed
    /// (same instance PlanWriter serializes to plan.json, so the console line and the artifact can
    /// never disagree).</summary>
    private static string FormatBudgetLine(MemoryBudget budget)
    {
        var duckdbPart = budget.DuckDbBytes is { } duckdbBytes
            ? FormatGiB(duckdbBytes)
            : $"unset ({budget.DuckDbDisclaimer})";
        return $"memory budget: ~{FormatGiB(budget.TotalBytes)} " +
            $"(duckdb {duckdbPart} + channels {FormatGiB(budget.ChannelBytes)} + overhead 256MB)";
    }

    private static string FormatGiB(long bytes) => $"{bytes / (1024.0 * 1024 * 1024):0.00} GB";

    /// <summary>One line per source/sink instance AND per dataset/output that DECLARES retry:, showing
    /// the effective (cascaded) policy. Console-only — plan.json is deliberately untouched
    /// (byte-stability).</summary>
    internal static string FormatRetryLine(string kind, string name, RetryDef? nearest, RetryDef? instance)
    {
        var policy = RetryPolicyResolver.Resolve(nearest, instance);
        return $"retry: {kind} {name} max_attempts={policy.MaxAttempts} " +
            $"base_delay={FormatDuration(policy.BaseDelay)} max_delay={FormatDuration(policy.MaxDelay)}";
    }

    /// <summary>One line per source/sink INSTANCE that declares
    /// `max_concurrency:` -- console-only (plan.json untouched, same rationale as
    /// <see cref="FormatRetryLine"/>), mirroring its placement/ordering (sources before sinks, ordinal
    /// name order). Unlike retry, `max_concurrency` is instance-only -- no per-dataset/output override to
    /// cascade -- so there is exactly one candidate line per source/sink, never a nested dataset/output
    /// loop.</summary>
    internal static string FormatMaxConcurrencyLine(string kind, string name, int cap) =>
        $"max_concurrency: {kind} {name} = {cap}";

    /// <summary>Inverse of DurationParser for display: the largest unit that divides the value exactly
    /// (90s stays "90s", never "1.5m" — DurationParser's grammar has no fractions, and these lines
    /// should round-trip as valid config values).</summary>
    internal static string FormatDuration(TimeSpan value)
    {
        var ms = (long)value.TotalMilliseconds;
        if (ms % 86_400_000 == 0 && ms > 0) return $"{ms / 86_400_000}d";
        if (ms % 3_600_000 == 0 && ms > 0) return $"{ms / 3_600_000}h";
        if (ms % 60_000 == 0 && ms > 0) return $"{ms / 60_000}m";
        if (ms % 1_000 == 0 && ms > 0) return $"{ms / 1_000}s";
        return $"{ms}ms";
    }


}
