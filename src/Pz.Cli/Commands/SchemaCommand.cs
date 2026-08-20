using System.CommandLine;
using Pz.Core.Dag;
using Pz.Core.Loading;
using Pz.Core.Templating;
using Pz.Core.Validation;
using Pz.Engine.Artifacts;
using Pz.Engine.Execution;
using Pz.Engine.State;

namespace Pz.Cli.Commands;

/// <summary>`pz schema accept [&lt;connection&gt;.&lt;entity&gt; ...]`.
/// Accepts the schema a `warn`-policy run last OBSERVED for one or more contract-less SourceLoad
/// datasets as the new baseline — the only remedy <see cref="Pz.Engine.Execution.SchemaDriftGate"/>'s
/// error hint (under `fail`) and its recurring warning (under `warn`) point at. Reads the latest run's
/// recorded <c>observed_schema</c> per SourceLoad and re-<see cref="SchemaDriftDiffer.Diff"/>s it against
/// the CURRENT baseline (never the source) — a dataset whose baseline still matches what was last
/// observed has nothing to accept, so it is silently skipped, not re-written. The source is never
/// contacted: this command compiles the project (to map the latest run's content-hash node ids back to
/// `&lt;connection&gt;.&lt;entity&gt;` names, exactly as `pz retry` maps ids back to selection) but never
/// executes a node, so a source host being unreachable cannot affect it.</summary>
internal static class SchemaCommand
{
    public static Command Create()
    {
        var schema = new Command("schema", "Inspect and accept observed schema drift for contract-less datasets");
        schema.Subcommands.Add(CreateAccept());
        return schema;
    }

    private static Command CreateAccept()
    {
        var projectOption = new Option<string?>("--project") { Description = "Project directory (default: current directory)" };
        var targetsArgument = new Argument<string[]>("targets")
        {
            Description = "One or more <connection>.<entity> datasets to accept (default: every dataset " +
                "the latest run recorded an observed schema for whose baseline differs from it)",
            Arity = ArgumentArity.ZeroOrMore,
        };
        var command = new Command("accept",
            "Accept the latest run's observed schema for one or more datasets as the new baseline. " +
            "Never contacts the source -- it only reads the latest run's recorded observed_schema and " +
            "the current baseline.");
        command.Options.Add(projectOption);
        command.Arguments.Add(targetsArgument);
        command.SetAction(parseResult => Execute(
            parseResult.GetValue(projectOption) ?? Directory.GetCurrentDirectory(),
            parseResult.GetValue(targetsArgument) ?? []));
        return command;
    }

    internal static int Execute(string projectDir, IReadOnlyList<string> targets)
    {
        CompiledDag dag;
        Pz.Core.Model.PzProject project;
        try
        {
            var env = SharedInputHelpers.SnapshotEnvironment();
            project = ProjectLoader.Load(projectDir, env);
            var renderCtx = new RenderContext(project, Guid.NewGuid().ToString("N"), DateTimeOffset.UtcNow) { Env = env };
            var compileNotices = new List<string>();
            dag = DagCompiler.Compile(project, renderCtx, compileNotices, new Pz.DuckDb.DuckDbSqlAstReader());
            foreach (var notice in compileNotices)
            {
                Console.WriteLine($"note: {notice}");
            }
        }
        catch (PzValidationException ex)
        {
            foreach (var error in ex.Errors)
            {
                Console.Error.WriteLine($"error {error}");
            }

            return ExitCodes.ConfigError;
        }

        StateBackends backends;
        try
        {
            backends = StateBackendFactory.Create(project, projectDir, TimeProvider.System);
            backends.EnsureSchema();
        }
        catch (PzConfigException ex)
        {
            Console.Error.WriteLine($"error {ex.Error}");
            return ExitCodes.ConfigError;
        }

        // Never opens a connector: the latest run's artifact and the current dag are all this reads.
        // A source's actual reachability plays no part in accept.
        var prior = backends.Artifacts.ReadLatest();
        var observedByKey = ObservedByKey(dag, prior);

        IReadOnlyList<string> keysToAccept;
        if (targets.Count > 0)
        {
            foreach (var target in targets)
            {
                if (!observedByKey.ContainsKey(target))
                {
                    Console.Error.WriteLine($"error {new PzError(PzErrorCode.SchemaAcceptTargetInvalid,
                        $"'{target}' has no recorded observed schema in the latest run.", null, null,
                        "run with on_source_drift: warn|fail first, then run pz schema accept again")}");
                    return ExitCodes.ConfigError;
                }
            }

            keysToAccept = targets.Distinct(StringComparer.Ordinal).ToList();
        }
        else
        {
            keysToAccept = observedByKey.Keys.Order(StringComparer.Ordinal).ToList();
        }

        var totalChanges = 0;
        foreach (var key in keysToAccept)
        {
            var observed = observedByKey[key];
            var baseline = backends.Schemas.Get(key);
            var changes = SchemaDriftDiffer.Diff(baseline?.Columns ?? [], observed.Columns);
            if (changes.Count == 0)
            {
                continue; // baseline already matches what was last observed -- nothing to accept here
            }

            foreach (var change in changes)
            {
                Console.WriteLine($"{key}: {DescribeChange(change)}");
            }

            backends.Schemas.Set(key, new SchemaBaseline(observed.Columns, observed.HintsHash, prior!.RunId));
            totalChanges += changes.Count;
        }

        Console.WriteLine(totalChanges == 0 ? "nothing to accept" : $"accepted {totalChanges} schema change(s)");
        return ExitCodes.Ok;
    }

    /// <summary>Maps the latest run's recorded <c>observed_schema</c> back to
    /// `&lt;connection&gt;.&lt;entity&gt;` keys, by matching each SourceLoad node's content-hash id
    /// against the freshly recompiled dag -- the same id-matching `pz retry`'s selection uses. A node
    /// whose id no longer matches (the dataset's declared read changed since that run) is silently
    /// excluded, exactly like a stale id in `pz retry`'s selection: there is no current dataset for accept
    /// to update. Empty when there is no prior run.</summary>
    private static Dictionary<string, ObservedSchema> ObservedByKey(CompiledDag dag, PriorRun? prior)
    {
        var result = new Dictionary<string, ObservedSchema>(StringComparer.Ordinal);
        if (prior is null)
        {
            return result;
        }

        var observedById = prior.Nodes
            .Where(n => n.Kind == "SourceLoad" && n.Observed is not null)
            .ToDictionary(n => n.Id, n => n.Observed!, StringComparer.Ordinal);

        foreach (var node in dag.Nodes)
        {
            if (node.Kind != NodeKind.SourceLoad || node.Definition is not SourceDatasetDef def)
            {
                continue;
            }

            if (observedById.TryGetValue(node.Id.Value, out var observed))
            {
                result[SchemaBaselineStore.Key(def.Source.Name, def.Dataset.Name)] = observed;
            }
        }

        return result;
    }

    /// <summary>Mirrors <c>SchemaDriftGate.Describe</c>'s per-change phrasing -- one line per change here
    /// rather than one joined sentence, per this command's output shape.</summary>
    private static string DescribeChange(SchemaDriftDiffer.Change change) => change.Kind switch
    {
        "added" => $"column '{change.Column}' added ({change.To})",
        "removed" => $"column '{change.Column}' removed (was {change.From})",
        _ => $"column '{change.Column}' retyped {change.From} -> {change.To}",
    };
}
