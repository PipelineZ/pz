using Microsoft.Data.SqlClient;
using Pz.State.SqlServer;
using Pz.TestSupport;
using Testcontainers.MsSql;

namespace Pz.State.SqlServer.Tests;

/// <summary>One MsSql container shared across the whole collection -- startup is the expensive part,
/// same pattern as Pz.Connector.SqlServer.Tests.MsSqlContainerFixture. `NewConnection()` creates a
/// fresh, isolated database per call so tests never race each other over shared tables.
///
/// Constructor calls DockerFacts.SkipUnlessDocker before any Testcontainers call, so docker-less
/// machines SKIP the whole collection cleanly.</summary>
public sealed class SqlServerFixture : IAsyncLifetime
{
    public const string CollectionName = "sqlserver-state";

    private MsSqlContainer? _container;
    private string _masterConnectionString = "";

    public SqlServerFixture()
    {
        DockerFacts.SkipUnlessDocker();
    }

    public async Task InitializeAsync()
    {
        // MSSQL_AGENT_ENABLED: CdcRemoteSyncStateTests's drop test runs sp_cdc_enable_table, whose
        // capture-job creation needs the agent (same rationale as
        // Pz.Connector.SqlServer.Tests.MsSqlContainerFixture) -- harmless to every other suite here.
        _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest")
            .WithEnvironment("MSSQL_AGENT_ENABLED", "true")
            .Build();
        await _container.StartAsync().ConfigureAwait(false);

        var master = new SqlConnectionStringBuilder(_container.GetConnectionString())
        {
            TrustServerCertificate = true,
        };
        _masterConnectionString = master.ConnectionString;
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
        {
            await _container.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Creates a brand-new database on the shared container and returns a
    /// <see cref="SqlStateConnection"/> pointed at it, so callers never share tables across tests.</summary>
    public SqlStateConnection NewConnection(string schema = "pz") => new(NewRawConnectionString(), schema);

    /// <summary>Same fresh-database guarantee as <see cref="NewConnection"/>, but as a
    /// plain connection string -- for <c>EndToEndRunTests</c>, which drives a real `pz run` process
    /// through <c>PZ_STATE_CONNECTION_STRING</c>/project.yml rather than constructing a
    /// <see cref="SqlStateConnection"/> directly.</summary>
    public string NewRawConnectionString()
    {
        var builder = new SqlConnectionStringBuilder(_masterConnectionString) { InitialCatalog = CreateDatabase() };
        return builder.ConnectionString;
    }

    /// <summary>Creates a fresh database plus a login scoped to it that carries no DDL rights (just the
    /// `public` role) and returns a <see cref="SqlStateConnection"/> authenticated as that login. The
    /// login connects fine -- the credentials are valid and the server is reachable -- but any
    /// `CREATE SCHEMA`/`CREATE TABLE` it attempts fails with a permission-denied error. That is the
    /// exact seam PZ0519 ("migration failed partway") exists for, as distinct from PZ0518 ("cannot
    /// reach the store").</summary>
    public SqlStateConnection NewConnectionWithoutDdlRights(string schema = "pz")
    {
        const string password = "Pz_LowPriv!2026";
        var database = CreateDatabase();
        var login = $"pz_lowpriv_{Guid.NewGuid():N}";

        using (var connection = new SqlConnection(
                   new SqlConnectionStringBuilder(_masterConnectionString) { InitialCatalog = database }
                       .ConnectionString))
        {
            connection.Open();
            using var command = new SqlCommand(
                $"CREATE LOGIN [{login}] WITH PASSWORD = '{password}', CHECK_POLICY = OFF; " +
                $"CREATE USER [{login}] FOR LOGIN [{login}];",
                connection);
            command.ExecuteNonQuery();
        }

        var builder = new SqlConnectionStringBuilder(_masterConnectionString)
        {
            InitialCatalog = database,
            UserID = login,
            Password = password,
            IntegratedSecurity = false,
        };
        return new SqlStateConnection(builder.ConnectionString, schema);
    }

    private string CreateDatabase()
    {
        var database = $"pz_state_{Guid.NewGuid():N}";
        using var master = new SqlConnection(_masterConnectionString);
        master.Open();
        using var command = new SqlCommand($"CREATE DATABASE [{database}]", master);
        command.ExecuteNonQuery();
        return database;
    }

    /// <summary>Test-only backdoor that flips READ_COMMITTED_SNAPSHOT on
    /// for one connection's own database -- so a concurrency test can reproduce the locking behavior
    /// Azure SQL Database uses by default (RCSI is on there out of the box), rather than only the
    /// on-premises default. Runs from a fresh connection to `master`, not the target database itself:
    /// this ALTER DATABASE option requires exclusive access, which a connection already inside that
    /// database cannot reliably grant itself. `WITH ROLLBACK IMMEDIATE` forces it regardless of any
    /// other pooled connections .NET's `SqlClient` may be holding open against the same database.
    /// `database` is this fixture's own `pz_state_&lt;guid&gt;` name (never operator input), so direct
    /// interpolation matches <see cref="CreateDatabase"/>'s existing test-only convention.</summary>
    public void EnableReadCommittedSnapshot(SqlStateConnection connection)
    {
        string database;
        using (var probe = connection.Open())
        {
            database = probe.Database;
        }

        using var master = new SqlConnection(_masterConnectionString);
        master.Open();
        using var command = new SqlCommand(
            $"ALTER DATABASE [{database}] SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE;", master);
        command.ExecuteNonQuery();
    }

    /// <summary>One row of <c>{schema}.run_events</c>, read back for
    /// <c>SqlEventSinkTests</c>' ordering assertion.</summary>
    public sealed record StoredEvent(long Seq, DateTimeOffset At, string Event, string Payload);

    /// <summary>Reads every event persisted for <paramref name="runId"/>, ordered by <c>seq</c> -- the
    /// same ordering <c>SqlEventSink</c> documents as its contract, not insertion order.</summary>
    public IReadOnlyList<StoredEvent> ReadEvents(SqlStateConnection connection, string runId)
    {
        using var sqlConnection = connection.Open();
        using var command = new SqlCommand(
            "DECLARE @sql NVARCHAR(MAX) = N'SELECT seq, at, event, payload FROM ' + QUOTENAME(@schema) + " +
            "N'.run_events WHERE run_id = @run_id ORDER BY seq'; " +
            "EXEC sp_executesql @sql, N'@run_id NVARCHAR(64)', @run_id = @run_id;",
            sqlConnection);
        command.Parameters.AddWithValue("@schema", connection.Schema);
        command.Parameters.AddWithValue("@run_id", runId);

        var results = new List<StoredEvent>();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            results.Add(new StoredEvent(
                reader.GetInt64(0),
                new DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Utc)),
                reader.GetString(2),
                reader.GetString(3)));
        }

        return results;
    }

    /// <summary>Test-only backdoor: stamps `schema_version` directly, bypassing `EnsureCurrent`'s
    /// migration path, so tests can simulate a store built by a newer `pz`.</summary>
    public static void SetSchemaVersion(SqlStateConnection connection, int version)
    {
        using var sqlConnection = connection.Open();
        using var command = new SqlCommand(
            "DECLARE @sql NVARCHAR(MAX) = 'UPDATE ' + QUOTENAME(@schema) + '.schema_version SET version = ' + " +
            "CONVERT(nvarchar(10), @version); EXEC(@sql);",
            sqlConnection);
        command.Parameters.AddWithValue("@schema", connection.Schema);
        command.Parameters.AddWithValue("@version", version);
        command.ExecuteNonQuery();
    }
}

[CollectionDefinition(SqlServerFixture.CollectionName)]
public sealed class SqlServerCollection : ICollectionFixture<SqlServerFixture>;
