using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;
using Pz.Cli;
using Pz.TestSupport;

namespace Pz.State.SqlServer.Tests;

/// <summary>A real `pz run` against a real SQL Server watermark store, proven by deleting `.pz/state`
/// between two runs and showing the second run still extracts only the delta.
/// `state.backend`/`state.connection_string` are supplied via
/// <c>PZ_STATE_BACKEND</c>/<c>PZ_STATE_CONNECTION_STRING</c> rather than a `state:` block in
/// project.yml, so the fixture project needs no connections.yml entry for pz's own state.
///
/// The dataset declares a BOUNDED window (`max_window`/`initial`/`until`), not a plain incremental cursor:
/// `Pz.Connector.LocalFiles.CsvSource` deliberately reads the WHOLE file every run for a plain
/// (unwindowed) incremental cursor (relies on downstream merge dedup instead of narrowing extraction --
/// see that class's <c>WrapWindowed</c> doc), so a plain-incremental dataset's row count could never prove
/// the watermark's READ side actually came from SQL Server. Only the windowed path pushes the stored
/// cursor down into the native CSV scan as a real `cursor &gt; lower` predicate, which is what makes the
/// row-count assertions below load-bearing rather than coincidental.</summary>
[Collection(SqlServerFixture.CollectionName)]
public sealed class EndToEndRunTests(SqlServerFixture fixture) : IDisposable
{
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-sqlstate-e2e", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [SkippableFact]
    public void Second_run_extracts_only_the_delta_even_after_deleting_local_state()
    {
        DockerFacts.SkipUnlessDocker();

        WriteProject();
        WriteOrders(["1,alice,10.25", "2,bob,20.50", "3,alice,30.75", "4,carol,5.00"]);

        var connectionString = fixture.NewRawConnectionString();
        Environment.SetEnvironmentVariable("PZ_STATE_BACKEND", "sqlserver");
        Environment.SetEnvironmentVariable("PZ_STATE_CONNECTION_STRING", connectionString);
        try
        {
            var firstOut = CaptureOut(() => CliApp.Build().Parse(["run", "--project", _work]).Invoke(), out var firstExit);
            Assert.Equal(ExitCodes.Ok, firstExit);
            var firstRunId = ParseRunId(firstOut);
            Assert.Equal(4L, ReadSourceLoadRows(connectionString, firstRunId));

            // The watermark lives in SQL Server, not on local disk.
            var stateDir = Path.Combine(_work, ".pz", "state");
            Assert.False(Directory.Exists(stateDir), "local .pz/state must never be touched under the sqlserver backend");

            // Two new rows (ids 5, 6) appended after the first run's watermark (max id 4).
            WriteOrders(["1,alice,10.25", "2,bob,20.50", "3,alice,30.75", "4,carol,5.00", "5,dave,15.00", "6,erin,25.00"]);

            // Guard, not a precondition: deletes it if it somehow exists -- the assertion above already
            // proved it does not, for this backend.
            if (Directory.Exists(stateDir))
            {
                Directory.Delete(stateDir, recursive: true);
            }

            var secondOut = CaptureOut(() => CliApp.Build().Parse(["run", "--project", _work]).Invoke(), out var secondExit);
            Assert.Equal(ExitCodes.Ok, secondExit);
            var secondRunId = ParseRunId(secondOut);
            Assert.Equal(2L, ReadSourceLoadRows(connectionString, secondRunId));
        }
        finally
        {
            Environment.SetEnvironmentVariable("PZ_STATE_BACKEND", null);
            Environment.SetEnvironmentVariable("PZ_STATE_CONNECTION_STRING", null);
        }
    }

    /// <summary>`pz retry` must read the prior run from the configured artifact store. Reading it from
    /// a hardcoded <c>LocalRunArtifactStore</c> fails with PZ0502 "no prior run found" after a
    /// perfectly good run under the DEFAULT remote configuration (<c>state.artifacts</c> defaults to
    /// true when the backend is not local, and run_results.json is then never written); retry's
    /// selection is one of the consumers <c>IRunArtifactStore</c> exists for.
    ///
    /// The assertion is deliberately on retry's OWN selection output ("nothing to retry (run &lt;id&gt;
    /// succeeded)"): that sentence can only be printed by a retry that actually found and read the prior
    /// run in SQL Server, and it names the id, so it cannot pass by accident.</summary>
    [SkippableFact]
    public void Retry_finds_the_prior_run_in_the_remote_artifact_store()
    {
        DockerFacts.SkipUnlessDocker();

        WriteProject();
        WriteOrders(["1,alice,10.25", "2,bob,20.50"]);

        Environment.SetEnvironmentVariable("PZ_STATE_BACKEND", "sqlserver");
        Environment.SetEnvironmentVariable("PZ_STATE_CONNECTION_STRING", fixture.NewRawConnectionString());
        try
        {
            // No run yet: PZ0502 must still fire, with its own message, rather than a store failure.
            var emptyErr = CaptureErr(() => CliApp.Build().Parse(["retry", "--project", _work]).Invoke(), out var emptyExit);
            Assert.Equal(ExitCodes.ConfigError, emptyExit);
            Assert.Contains("no prior run found", emptyErr);

            var runOut = CaptureOut(() => CliApp.Build().Parse(["run", "--project", _work]).Invoke(), out var runExit);
            Assert.Equal(ExitCodes.Ok, runExit);
            var runId = ParseRunId(runOut);

            var retryOut = CaptureOut(() => CliApp.Build().Parse(["retry", "--project", _work]).Invoke(), out var retryExit);

            Assert.Equal(ExitCodes.Ok, retryExit);
            Assert.Contains($"nothing to retry (run {runId} succeeded)", retryOut);
        }
        finally
        {
            Environment.SetEnvironmentVariable("PZ_STATE_BACKEND", null);
            Environment.SetEnvironmentVariable("PZ_STATE_CONNECTION_STRING", null);
        }
    }

    private void WriteProject()
    {
        Directory.CreateDirectory(Path.Combine(_work, "pipelines"));
        Directory.CreateDirectory(Path.Combine(_work, "data"));

        File.WriteAllText(Path.Combine(_work, "project.yml"), """
            name: sqlstate_e2e
            version: 0.1.0
            connectors:
              - package: Pz.Connector.LocalFiles
                version: 0.1.0
            engine:
              threads: 1
            """);

        File.WriteAllText(Path.Combine(_work, "connections.yml"), """
            files:
              connector: localfiles
              entities:
                orders:
                  read:
                    path: data/orders.csv
                    format: csv
                    columns:
                      id: bigint
                      customer: varchar
                      amount: double
                    sync:
                      mode: incremental
                      cursor: id
                      initial: "0"
                      max_window: "1000"
                      until: "1000"

            lake:
              connector: localfiles
            """);

        File.WriteAllText(Path.Combine(_work, "pipelines", "totals.sql"), """
            INSERT INTO {{ sink('lake', 'totals', strategy: 'append', duplicates: 'accept', format: 'parquet', path: 'out/') }}
            select id, customer, amount
            from {{ source('files', 'orders') }}
            """);
    }

    private void WriteOrders(IEnumerable<string> rows) =>
        File.WriteAllLines(Path.Combine(_work, "data", "orders.csv"), ["id,customer,amount", .. rows]);

    private static string ParseRunId(string stdout)
    {
        var match = Regex.Match(stdout, @"run (\S+):");
        Assert.True(match.Success, $"expected a 'run <id>: ...' summary line in stdout, got: {stdout}");
        return match.Groups[1].Value;
    }

    private static long ReadSourceLoadRows(string connectionString, string runId)
    {
        using var sqlConnection = new SqlConnection(connectionString);
        sqlConnection.Open();
        using var command = new SqlCommand(
            "SELECT rows_moved FROM pz.run_nodes WHERE run_id = @run_id AND kind = 'SourceLoad'", sqlConnection);
        command.Parameters.AddWithValue("@run_id", runId);
        return (long)command.ExecuteScalar()!;
    }

    private static string CaptureErr(Func<int> action, out int exit)
    {
        var stderr = new StringWriter();
        var original = Console.Error;
        Console.SetError(stderr);
        try
        {
            exit = action();
        }
        finally
        {
            Console.SetError(original);
        }

        return stderr.ToString();
    }

    private static string CaptureOut(Func<int> action, out int exit)
    {
        var stdout = new StringWriter();
        var original = Console.Out;
        Console.SetOut(stdout);
        try
        {
            exit = action();
        }
        finally
        {
            Console.SetOut(original);
        }

        return stdout.ToString();
    }
}
