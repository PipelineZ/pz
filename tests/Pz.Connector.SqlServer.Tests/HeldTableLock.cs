using Microsoft.Data.SqlClient;

namespace Pz.Connector.SqlServer.Tests;

/// <summary>A table another session holds exclusively, for as long as this object lives. A write to
/// <see cref="Table"/> from any other connection blocks until that connection's own command timeout
/// fires -- it can never complete early, which a timed server-side wait can when the server's clock
/// is stepped under it.</summary>
internal sealed class HeldTableLock : IAsyncDisposable
{
    private readonly SqlConnection _holder;
    private readonly SqlTransaction _transaction;

    private HeldTableLock(SqlConnection holder, SqlTransaction transaction, string table)
    {
        _holder = holder;
        _transaction = transaction;
        Table = table;
    }

    public string Table { get; }

    /// <summary>A statement that needs a lock the holder will not release.</summary>
    public string BlockedStatement => $"insert into dbo.{Table} (id) values (1)";

    public static async Task<HeldTableLock> AcquireAsync(string connectionString)
    {
        var table = $"held_{Guid.NewGuid():N}"[..24];
        var holder = new SqlConnection(connectionString);
        await holder.OpenAsync();
        await MsSqlContainerFixture.ExecuteAsync(holder, $"create table dbo.{table} (id int)");

        var transaction = (SqlTransaction)await holder.BeginTransactionAsync();
        await using var take = new SqlCommand(
            $"select count(*) from dbo.{table} with (tablockx, holdlock)", holder, transaction);
        await take.ExecuteScalarAsync();
        return new HeldTableLock(holder, transaction, table);
    }

    public async ValueTask DisposeAsync()
    {
        await _transaction.RollbackAsync();
        await _transaction.DisposeAsync();
        await MsSqlContainerFixture.ExecuteAsync(_holder, $"drop table dbo.{Table}");
        await _holder.DisposeAsync();
    }
}
