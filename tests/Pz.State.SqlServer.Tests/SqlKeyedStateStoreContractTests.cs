using Microsoft.Data.SqlClient;
using Pz.Engine.State;
using Pz.State.SqlServer;
using Pz.TestSupport;
using Pz.TestSupport.State;

namespace Pz.State.SqlServer.Tests;

[Collection(SqlServerFixture.CollectionName)]
public sealed class SqlKeyedStateStoreContractTests(SqlServerFixture fixture) : KeyedStateStoreContract
{
    /// <summary>Each store's backing connection, so <see cref="CorruptStoredState"/> can reach the row
    /// directly without <see cref="NewStore"/> handing back anything but a fresh, independent store.</summary>
    private readonly Dictionary<IKeyedStateStore<TestEntry>, SqlStateConnection> _connections = [];

    protected override IKeyedStateStore<TestEntry> NewStore()
    {
        DockerFacts.SkipUnlessDocker();
        var connection = fixture.NewConnection();
        SqlStateSchema.EnsureCurrent(connection);
        var store = new SqlKeyedStateStore<TestEntry>(connection, "contract",
            readEntry: static entry =>
            {
                var value = entry.GetProperty("value").GetString();
                var runId = entry.GetProperty("runId").GetString();
                return value is null || runId is null ? null : new TestEntry(value, runId);
            },
            writeEntry: static (writer, e) =>
            {
                writer.WriteString("value", e.Value);
                writer.WriteString("runId", e.RunId);
            });

        _connections[store] = connection;
        return store;
    }

    /// <summary>On SQL Server, "present but unreadable" is an unparseable `payload` column, written
    /// directly against the database rather than through the store.</summary>
    protected override void CorruptStoredState(IKeyedStateStore<TestEntry> store)
    {
        var connection = _connections[store];
        using var sqlConnection = connection.Open();
        using var command = new SqlCommand(
            "DECLARE @sql NVARCHAR(MAX) = N'UPDATE ' + QUOTENAME(@schema) + N'.state SET payload = @payload'; " +
            "EXEC sp_executesql @sql, N'@payload NVARCHAR(MAX)', @payload = @payload;",
            sqlConnection);
        command.Parameters.AddWithValue("@schema", connection.Schema);
        command.Parameters.Add("@payload", System.Data.SqlDbType.NVarChar, -1).Value = "{ not json at all";
        command.ExecuteNonQuery();
    }
}
