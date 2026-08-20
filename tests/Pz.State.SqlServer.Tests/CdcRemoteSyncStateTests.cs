using Microsoft.Data.SqlClient;
using Pz.Cli;
using Pz.Engine.State;
using Pz.TestSupport;

namespace Pz.State.SqlServer.Tests;

/// <summary>`pz cdc status`/`pz cdc drop` must read and clear sync-state in whichever backend
/// <c>state:</c> resolved to. With a hardcoded <c>SyncStateStore.Local</c>, under
/// <c>backend: sqlserver</c> status reports empty state and drop clears a local file the next run
/// never reads — the operator believes the position was dropped and expects a re-snapshot, but the run
/// resumes from the remote token.
///
/// The status test uses a localfiles cdc dataset: the "admin unsupported" row still prints the STORED
/// token, which is exactly the store read under test, with no cdc-capable server needed (the
/// ChangeCapture capability gate is plan-time, PZ0338 in ExecutionPlanner — `pz cdc status` never
/// plans). The drop test uses a sqlserver source pointed at this fixture's own container:
/// <c>SqlServerSource.GetChangeCaptureStatusAsync</c> reports (not throws) on a cdc-less database and
/// <c>DropChangeCaptureStateAsync</c> is a deliberate no-op, so the verb runs to its sync-state
/// clearing — the seam under test — against a completely ordinary database.</summary>
[Collection(SqlServerFixture.CollectionName)]
public sealed class CdcRemoteSyncStateTests(SqlServerFixture fixture) : IDisposable
{
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-sqlstate-cdc", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [SkippableFact]
    public void Cdc_status_reads_sync_state_from_the_configured_backend()
    {
        DockerFacts.SkipUnlessDocker();

        var connectionString = fixture.NewRawConnectionString();
        SeedRemoteSyncState(connectionString, "files.orders", "tok-remote-123");
        WriteLocalFilesCdcProject();

        Environment.SetEnvironmentVariable("PZ_STATE_BACKEND", "sqlserver");
        Environment.SetEnvironmentVariable("PZ_STATE_CONNECTION_STRING", connectionString);
        try
        {
            var stdout = CaptureOut(
                () => CliApp.Build().Parse(["cdc", "status", "--project", _work]).Invoke(),
                out var exit, out var stderr);

            Assert.True(exit == ExitCodes.Ok, $"expected exit 0, got {exit}; stderr: {stderr}");
            Assert.Contains("note: state backend: sqlserver (from PZ_STATE_BACKEND)", stdout);
            // The stored-token column comes from the remote store; a hardcoded local store would print
            // "-" here (no .pz/state exists in this fresh project dir).
            Assert.Contains("tok-remote-123", stdout);
            Assert.False(Directory.Exists(Path.Combine(_work, ".pz", "state")),
                "local .pz/state must never be touched under the sqlserver backend");
        }
        finally
        {
            Environment.SetEnvironmentVariable("PZ_STATE_BACKEND", null);
            Environment.SetEnvironmentVariable("PZ_STATE_CONNECTION_STRING", null);
        }
    }

    [SkippableFact]
    public void Cdc_drop_clears_sync_state_in_the_configured_backend()
    {
        DockerFacts.SkipUnlessDocker();

        var connectionString = fixture.NewRawConnectionString();
        // The source-side status probe (`GetChangeCaptureStatusAsync`, read before the drop) queries
        // msdb.dbo.cdc_jobs, which only exists once a capture job has been created on the instance --
        // so this sets up a real cdc-enabled table, the same way
        // Pz.Connector.SqlServer.Tests.SqlServerCdcTests does (agent enabled on the fixture container).
        // The cdc plumbing is scenery: the seam under test is only the sync-state clearing after it.
        EnableTableCdc(connectionString);
        SeedRemoteSyncState(connectionString, "ops.orders", "tok-to-drop");
        WriteSqlServerCdcProject(connectionString);

        Environment.SetEnvironmentVariable("PZ_STATE_BACKEND", "sqlserver");
        Environment.SetEnvironmentVariable("PZ_STATE_CONNECTION_STRING", connectionString);
        try
        {
            var stdout = CaptureOut(
                () => CliApp.Build().Parse(["cdc", "drop", "--project", _work, "ops.orders"]).Invoke(),
                out var exit, out var stderr);

            Assert.True(exit == ExitCodes.Ok, $"expected exit 0, got {exit}; stderr: {stderr}");
            Assert.Contains("configured state store", stdout);
            // The entry must vanish from the store the next run actually reads.
            Assert.Null(ReadRemoteSyncState(connectionString, "ops.orders"));
        }
        finally
        {
            Environment.SetEnvironmentVariable("PZ_STATE_BACKEND", null);
            Environment.SetEnvironmentVariable("PZ_STATE_CONNECTION_STRING", null);
        }
    }

    /// <summary>Writes through the same composition StateBackendFactory uses (scope "sync-state"), so
    /// the seeded entry is byte-identical to one a real run would have advanced.</summary>
    private static SyncStateStore RemoteSyncStateStore(string connectionString)
    {
        var connection = new SqlStateConnection(connectionString, "pz");
        SqlStateSchema.EnsureCurrent(connection);
        return new SyncStateStore(new SqlKeyedStateStore<SyncState>(
            connection, "sync-state", SyncStateStore.ReadEntry, SyncStateStore.WriteEntry));
    }

    private static void EnableTableCdc(string connectionString)
    {
        using var connection = new SqlConnection(connectionString);
        connection.Open();
        foreach (var statement in new[]
        {
            "CREATE TABLE dbo.orders (id INT NOT NULL PRIMARY KEY, amount INT NOT NULL)",
            "EXEC sys.sp_cdc_enable_db",
            "EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'orders', @role_name = NULL",
        })
        {
            using var command = new SqlCommand(statement, connection);
            command.ExecuteNonQuery();
        }
    }

    private static void SeedRemoteSyncState(string connectionString, string key, string token) =>
        RemoteSyncStateStore(connectionString).Set(key, new SyncState(token, "run-0"));

    private static string? ReadRemoteSyncState(string connectionString, string key) =>
        RemoteSyncStateStore(connectionString).Get(key)?.Token;

    private void WriteLocalFilesCdcProject()
    {
        Directory.CreateDirectory(_work);
        WriteProjectYml();
        File.WriteAllText(Path.Combine(_work, "connections.yml"), """
            files:
              connector: localfiles
              root: "./data"
              entities:
                orders:
                  read:
                    format: csv
                    sync:
                      mode: cdc
            """);
    }

    private void WriteSqlServerCdcProject(string connectionString)
    {
        Directory.CreateDirectory(_work);
        WriteProjectYml();

        var builder = new SqlConnectionStringBuilder(connectionString);
        var hostAndPort = builder.DataSource.Split(',');
        // The interpolated fragment lands AFTER the raw string literal's indentation stripping, so it
        // must carry the post-dedent two-space indent itself.
        var port = hostAndPort.Length > 1 ? $"\n  port: {hostAndPort[1]}" : string.Empty;
        File.WriteAllText(Path.Combine(_work, "connections.yml"), $"""
            ops:
              connector: sqlserver
              host: {hostAndPort[0]}{port}
              database: {builder.InitialCatalog}
              user: {builder.UserID}
              password: "{builder.Password}"
              encrypt: false
              entities:
                orders:
                  read:
                    sync:
                      mode: cdc
            """);
    }

    private void WriteProjectYml() =>
        File.WriteAllText(Path.Combine(_work, "project.yml"), """
            name: sqlstate_cdc
            version: 0.1.0
            engine:
              threads: 1
            """);

    private static string CaptureOut(Func<int> action, out int exit, out string stderr)
    {
        var stdout = new StringWriter();
        var stderrWriter = new StringWriter();
        var originalOut = Console.Out;
        var originalErr = Console.Error;
        Console.SetOut(stdout);
        Console.SetError(stderrWriter);
        try
        {
            exit = action();
        }
        finally
        {
            Console.SetOut(originalOut);
            Console.SetError(originalErr);
        }

        stderr = stderrWriter.ToString();
        return stdout.ToString();
    }
}
