using Microsoft.Data.SqlClient;
using Pz.Shared;
using Pz.TestSupport;

namespace Pz.Connector.SqlServer.Tests;

/// <summary>Docker-backed proof: a real deadlock victim and a real command timeout against a
/// live SQL Server both come back from Microsoft.Data.SqlClient with <c>SqlException.IsTransient ==
/// false</c> (every raise site in this connector used to forward that flag unchanged, so a
/// deadlock-victim write failed the run instead of being retried) -- <see cref="MsTransient"/>
/// reclassifies both by error number.</summary>
[Collection("sqlserver")]
public sealed class MsTransientDockerTests(MsSqlContainerFixture fixture)
{
    [SkippableFact]
    public async Task Real_deadlock_victim_has_driver_IsTransient_false_but_MsTransient_true()
    {
        DockerFacts.SkipUnlessDocker();

        var table = $"deadlock_{Guid.NewGuid():N}"[..24];
        await using (var setup = new SqlConnection(fixture.ConnectionString))
        {
            await setup.OpenAsync();
            await MsSqlContainerFixture.ExecuteAsync(setup,
                $"create table dbo.{table} (id int primary key, v int)");
            await MsSqlContainerFixture.ExecuteAsync(setup,
                $"insert into dbo.{table} (id, v) values (1, 0), (2, 0)");
        }

        await using var connA = new SqlConnection(fixture.ConnectionString);
        await using var connB = new SqlConnection(fixture.ConnectionString);
        await connA.OpenAsync();
        await connB.OpenAsync();

        var txA = connA.BeginTransaction();
        var txB = connB.BeginTransaction();

        // A locks row 1, B locks row 2, then each tries the other's row -- opposite lock order forces
        // a genuine deadlock; the server picks one session as the victim.
        await using (var cmd = new SqlCommand($"update dbo.{table} set v = 1 where id = 1", connA, txA))
        {
            await cmd.ExecuteNonQueryAsync();
        }
        await using (var cmd = new SqlCommand($"update dbo.{table} set v = 1 where id = 2", connB, txB))
        {
            await cmd.ExecuteNonQueryAsync();
        }

        var taskA = Task.Run(async () =>
        {
            await using var cmd = new SqlCommand($"update dbo.{table} set v = 2 where id = 2", connA, txA);
            await cmd.ExecuteNonQueryAsync();
        });
        var taskB = Task.Run(async () =>
        {
            await using var cmd = new SqlCommand($"update dbo.{table} set v = 2 where id = 1", connB, txB);
            await cmd.ExecuteNonQueryAsync();
        });

        SqlException? caught = null;
        try
        {
            await Task.WhenAll(taskA, taskB);
        }
        catch (SqlException ex)
        {
            caught = ex;
        }
        catch (AggregateException ex) when (ex.InnerException is SqlException sqlEx)
        {
            caught = sqlEx;
        }

        Assert.NotNull(caught);
        Assert.Equal(1205, caught!.Number);
        Assert.False(caught.IsTransient); // the driver's own signal misses the deadlock-victim case
        Assert.True(MsTransient.IsTransient(caught));
    }

    [SkippableFact]
    public async Task Real_command_timeout_has_driver_IsTransient_false_but_MsTransient_true()
    {
        DockerFacts.SkipUnlessDocker();

        // Blocked on a lock rather than a timed wait: the statement cannot finish before the timeout.
        await using var held = await HeldTableLock.AcquireAsync(fixture.ConnectionString);
        await using var conn = new SqlConnection(fixture.ConnectionString);
        await conn.OpenAsync();

        SqlException? caught = null;
        try
        {
            await using var cmd = new SqlCommand(held.BlockedStatement, conn) { CommandTimeout = 1 };
            await cmd.ExecuteNonQueryAsync();
        }
        catch (SqlException ex)
        {
            caught = ex;
        }

        Assert.NotNull(caught);
        Assert.Equal(-2, caught!.Number);
        Assert.False(caught.IsTransient); // the driver's own signal misses the client-side timeout case
        Assert.True(MsTransient.IsTransient(caught));
    }
}
