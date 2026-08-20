using System.CommandLine;
using Pz.Core.Dag;
using Pz.Core.Loading;
using Pz.Core.Model;
using Pz.Core.Templating;
using Pz.Core.Validation;

namespace Pz.Cli.Commands;

/// <summary>`pz ls`: compiles the project and prints one row per node in topological order, honoring
/// `--select` selection-exact (same semantics as `pz compile`). Read-only: no `.pz/target` artifacts
/// are written.</summary>
internal static class LsCommand
{
    public static Command Create()
    {
        var projectOption = new Option<string?>("--project") { Description = "Project directory (default: current directory)" };
        var varsOption = new Option<string?>("--vars") { Description = "JSON object of var overrides" };
        var command = new Command("ls", "List every node in the compiled DAG, in topological order");
        command.Options.Add(projectOption);
        command.Options.Add(varsOption);
        command.Options.Add(SharedOptions.Select);
        command.SetAction(parseResult => Execute(
            parseResult.GetValue(projectOption) ?? Directory.GetCurrentDirectory(),
            parseResult.GetValue(varsOption),
            parseResult.GetValue(SharedOptions.Select)));
        return command;
    }

    internal static int Execute(string projectDir, string? varsJson, string? select = null)
    {
        try
        {
            var env = SharedInputHelpers.SnapshotEnvironment();
            var overrides = SharedInputHelpers.ParseVars(varsJson);
            var project = ProjectLoader.Load(projectDir, env, overrides);
            var ctx = new RenderContext(project, Guid.NewGuid().ToString("N"), DateTimeOffset.UtcNow) { Env = env };
            var compileNotices = new List<string>();
            var dag = DagCompiler.Compile(project, ctx, compileNotices, new Pz.DuckDb.DuckDbSqlAstReader());
            foreach (var notice in compileNotices)
            {
                Console.WriteLine($"note: {notice}");
            }

            IEnumerable<DagNode> rows = dag.TopologicalOrder();
            if (!string.IsNullOrWhiteSpace(select))
            {
                var selected = Selector.Apply(dag, select);
                rows = dag.TopologicalOrder().Where(n => selected.Contains(n.Id));
            }

            Console.WriteLine($"{"kind",-10} {"name",-40} {"tags"}");
            foreach (var node in rows)
            {
                Console.WriteLine($"{KindName(node.Kind),-10} {node.Name,-40} {FormatTags(node)}");
            }

            return ExitCodes.Ok;
        }
        catch (PzValidationException ex)
        {
            foreach (var error in ex.Errors)
                Console.Error.WriteLine($"error {error}");
            return ExitCodes.ConfigError;
        }
    }

    // Mirrors ManifestWriter.KindName (internal to Pz.Core, not visible across assemblies) so the
    // on-screen `kind` column uses the exact same names as manifest.json.
    private static string KindName(NodeKind kind) => kind switch
    {
        NodeKind.SourceLoad => "source_load",
        NodeKind.Pipeline => "pipeline",
        NodeKind.Check => "check",
        NodeKind.SinkWrite => "sink_write",
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "unknown node kind"),
    };

    private static string FormatTags(DagNode node) =>
        node.Definition is PipelineDef pipeline && pipeline.Tags.Count > 0
            ? string.Join(",", pipeline.Tags)
            : "-";
}
