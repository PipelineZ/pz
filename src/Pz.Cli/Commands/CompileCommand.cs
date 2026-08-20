using System.CommandLine;
using Pz.Core.Artifacts;
using Pz.Core.Dag;
using Pz.Core.Loading;
using Pz.Core.Templating;
using Pz.Core.Validation;

namespace Pz.Cli.Commands;

internal static class CompileCommand
{
    public static Command Create()
    {
        var projectOption = new Option<string?>("--project") { Description = "Project directory (default: current directory)" };
        var varsOption = new Option<string?>("--vars") { Description = "JSON object of var overrides" };
        var command = new Command("compile", "Render pipelines, build the DAG, write .pz/target artifacts");
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
            var ctx = new RenderContext(project, Guid.NewGuid().ToString("N"),
                DateTimeOffset.UtcNow) { Env = env };
            var compileNotices = new List<string>();
            var dag = DagCompiler.Compile(project, ctx, compileNotices, new Pz.DuckDb.DuckDbSqlAstReader());
            foreach (var notice in compileNotices)
            {
                Console.WriteLine($"note: {notice}");
            }

            var nodesToWrite = dag.Nodes;
            if (!string.IsNullOrWhiteSpace(select))
            {
                var selected = Selector.Apply(dag, select);
                nodesToWrite = [.. dag.Nodes.Where(n => selected.Contains(n.Id))];
            }

            var targetDir = Path.Combine(projectDir, ".pz", "target");
            ManifestWriter.Write(dag, nodesToWrite, project, targetDir);
            Console.WriteLine($"Compiled {nodesToWrite.Count} nodes -> {targetDir}");
            return ExitCodes.Ok;
        }
        catch (PzValidationException ex)
        {
            foreach (var error in ex.Errors)
                Console.Error.WriteLine($"error {error}");
            return ExitCodes.ConfigError;
        }
    }
}
