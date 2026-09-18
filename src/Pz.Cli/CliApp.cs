using System.CommandLine;
using System.CommandLine.Invocation;
using Pz.Cli.Commands;
using Pz.Core.Validation;

namespace Pz.Cli;

public static class CliApp
{
    public static RootCommand Build()
    {
        var root = new RootCommand(
            "pz — a lightweight, developer-first batch data pipeline engine for SQL-based ETL/ELT, "
            + "powered by DuckDB, that can run anywhere without requiring a data platform");

        root.Subcommands.Add(InitCommand.Create());
        root.Subcommands.Add(CompileCommand.Create());
        root.Subcommands.Add(RunCommand.Create());
        root.Subcommands.Add(PlanCommand.Create());
        root.Subcommands.Add(RestoreCommand.Create());
        root.Subcommands.Add(ValidateCommand.Create());
        root.Subcommands.Add(TestCommand.Create());
        root.Subcommands.Add(RetryCommand.Create());
        root.Subcommands.Add(LsCommand.Create());
        root.Subcommands.Add(RunsCommand.Create());
        root.Subcommands.Add(ConnectorsCommand.Create());
        root.Subcommands.Add(ConnectorTestCommand.Create());
        root.Subcommands.Add(CdcCommand.Create());
        root.Subcommands.Add(CleanCommand.Create());
        root.Subcommands.Add(StateCommand.Create());
        root.Subcommands.Add(SchemaCommand.Create());
        root.Subcommands.Add(McpCommand.Create());

        return root;
    }

    /// <summary>The process entry point's whole body. Every verb handles the failures it anticipates;
    /// this is the net under them, so an exception nobody anticipated still ends as a coded fatal
    /// (exit 3) naming a next step rather than a raw stack trace.</summary>
    public static int Run(string[] args) =>
        Run(Build(), args, Console.Error, Environment.GetEnvironmentVariable);

    internal static int Run(RootCommand root, string[] args, TextWriter stderr, Func<string, string?> getEnv)
    {
        try
        {
            // System.CommandLine's own handler would print the stack and return 1, which the exit-code
            // contract reserves for node failures. Its own termination handling is switched off too:
            // left on, it answers SIGINT/SIGTERM by cancelling the verb's token and then force-exiting
            // two seconds later, whatever the verb is doing — which cuts a run off between a sink's
            // commit and the watermark that records it, leaves run_results.json at "running", and
            // orphans connector processes. A run owns its stop signals instead (StopSignals), and winds
            // down for as long as that takes.
            var parseResult = root.Parse(args);
            var config = new InvocationConfiguration
            {
                EnableDefaultExceptionHandler = false,
                ProcessTerminationTimeout = null,
            };

            // A usage error -- an unrecognized command/option, a missing required argument -- is a
            // config problem the caller can fix, not a node failure: System.CommandLine's own
            // ParseErrorAction hardcodes exit 1, the code this contract reserves for "one or more nodes
            // failed", so a CI script cannot tell "pz bogus" from a real failed run. Still invoked
            // normally (prints the same message + help it always has); only the exit code changes.
            // --help/--version resolve to their own action types and are unaffected.
            var isUsageError = parseResult.Action is ParseErrorAction;
            var exitCode = parseResult.Invoke(config);
            return isUsageError ? ExitCodes.ConfigError : exitCode;
        }
        catch (Exception ex)
        {
            stderr.WriteLine(
                $"error {PzErrorCode.UnexpectedEngineFailure}: internal error ({ex.GetType().Name}: {ex.Message}) — "
                + "this is a bug in pz, not in your project; please report it at "
                + "https://github.com/PipelineZ/pz/issues, re-running with PZ_DEBUG=1 to include the stack trace");
            if (getEnv("PZ_DEBUG") is { Length: > 0 } debug && debug != "0")
            {
                stderr.WriteLine(ex.ToString());
            }

            return ExitCodes.Fatal;
        }
    }
}
