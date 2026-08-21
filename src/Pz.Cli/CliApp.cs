using System.CommandLine;
using Pz.Cli.Commands;

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
        root.Subcommands.Add(ConnectorsCommand.Create());
        root.Subcommands.Add(CdcCommand.Create());
        root.Subcommands.Add(CleanCommand.Create());
        root.Subcommands.Add(StateCommand.Create());
        root.Subcommands.Add(SchemaCommand.Create());
        root.Subcommands.Add(McpCommand.Create());

        return root;
    }
}
