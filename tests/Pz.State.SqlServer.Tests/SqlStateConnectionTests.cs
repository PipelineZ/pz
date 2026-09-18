using System.Reflection;
using Microsoft.Data.SqlClient;
using Pz.Core.Validation;
using Pz.State.SqlServer;
using Pz.TestSupport;

namespace Pz.State.SqlServer.Tests;

/// <summary><see cref="SqlStateConnection.Execute{T}"/>'s retry-vs-give-up behaviour, proven
/// against a real docker connection but with a synthetic operation so the retry COUNT is deterministic
/// -- a genuine transient failure (deadlock/timeout) is exercised for real in
/// Pz.Connector.SqlServer.Tests' <c>MsTransientDockerTests</c>; this only needs to prove
/// <see cref="SqlStateConnection"/> calls the operation the right number of times and classifies the
/// result correctly, which a synthetic <see cref="SqlException"/> does without depending on timing to
/// provoke a real deadlock on every run.</summary>
[Collection(SqlServerFixture.CollectionName)]
public sealed class SqlStateConnectionTests(SqlServerFixture fixture)
{
    [SkippableFact]
    public void Execute_retries_a_transient_failure_and_returns_the_eventual_success()
    {
        DockerFacts.SkipUnlessDocker();
        var connection = new SqlStateConnection(fixture.NewRawConnectionString(), "pz", TimeProvider.System, maxAttempts: 3);
        var calls = 0;

        var result = connection.Execute(_ =>
        {
            calls++;
            if (calls < 3)
            {
                throw SqlExceptionFactory.Create(1205); // deadlock victim -- MsTransient-classified
            }

            return "ok";
        });

        Assert.Equal("ok", result);
        Assert.Equal(3, calls);
    }

    [SkippableFact]
    public void Execute_gives_up_after_the_attempt_budget_as_PZ0529()
    {
        DockerFacts.SkipUnlessDocker();
        var connection = new SqlStateConnection(fixture.NewRawConnectionString(), "pz", TimeProvider.System, maxAttempts: 2);
        var calls = 0;

        var ex = Assert.Throws<PzConfigException>(() => connection.Execute<object?>(_ =>
        {
            calls++;
            throw SqlExceptionFactory.Create(1205);
        }));

        Assert.Equal(2, calls); // exhausted the budget, not retried forever
        Assert.Equal(PzErrorCode.StateQueryFailed, ex.Error.Code);
        Assert.Contains("SqlException 1205", ex.Error.Message, StringComparison.Ordinal);
        // A deadlock that outlasted the budget is not a permissions problem, and the next step says so.
        Assert.Contains("after 2 attempts", ex.Error.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("permission", ex.Error.Hint, StringComparison.Ordinal);
    }

    [SkippableFact]
    public void Execute_does_not_retry_a_permanent_SqlException()
    {
        DockerFacts.SkipUnlessDocker();
        var connection = new SqlStateConnection(fixture.NewRawConnectionString(), "pz", TimeProvider.System, maxAttempts: 3);
        var calls = 0;

        var ex = Assert.Throws<PzConfigException>(() => connection.Execute<object?>(_ =>
        {
            calls++;
            throw SqlExceptionFactory.Create(208); // invalid object name -- permanent
        }));

        Assert.Equal(1, calls); // not retried at all
        Assert.Equal(PzErrorCode.StateQueryFailed, ex.Error.Code);
        Assert.Contains("SqlException 208", ex.Error.Message, StringComparison.Ordinal);
    }

    [SkippableFact]
    public void A_query_that_fails_after_a_real_successful_connect_is_PZ0529_not_PZ0518()
    {
        // A real "Invalid object name" from an actual query -- not synthetic -- against a live
        // connection, proving the split end to end: the store IS reachable (Open succeeded), only this
        // operation failed.
        DockerFacts.SkipUnlessDocker();
        var connection = new SqlStateConnection(fixture.NewRawConnectionString(), "pz");

        var ex = Assert.Throws<PzConfigException>(() => connection.Execute(sqlConnection =>
        {
            using var command = new SqlCommand("SELECT * FROM pz_no_such_table_at_all", sqlConnection);
            return command.ExecuteScalar();
        }));

        Assert.Equal(PzErrorCode.StateQueryFailed, ex.Error.Code);
        Assert.Contains("SqlException 208", ex.Error.Message, StringComparison.Ordinal);
    }
}

/// <summary>SqlClient exposes no public <see cref="SqlException"/> constructor -- builds one offline via
/// the same internal factory SqlClient itself uses (mirrors
/// <c>Pz.Connector.SqlServer.Tests.SqlExceptionFactory</c>; duplicated rather than shared because test
/// projects do not reference each other).</summary>
internal static class SqlExceptionFactory
{
    public static SqlException Create(int number, string message = "boom")
    {
        var sqlClientAssembly = typeof(SqlException).Assembly;
        var errorType = sqlClientAssembly.GetType("Microsoft.Data.SqlClient.SqlError")!;
        var collectionType = sqlClientAssembly.GetType("Microsoft.Data.SqlClient.SqlErrorCollection")!;

        var errorCtor = errorType.GetConstructor(BindingFlags.NonPublic | BindingFlags.Instance, null,
            [typeof(int), typeof(byte), typeof(byte), typeof(string), typeof(string), typeof(string), typeof(int), typeof(Exception)],
            null)!;
        var error = errorCtor.Invoke([number, (byte)0, (byte)0, "test-server", message, "test-proc", 0, null]);

        var collection = Activator.CreateInstance(collectionType, nonPublic: true)!;
        collectionType.GetMethod("Add", BindingFlags.NonPublic | BindingFlags.Instance)!
            .Invoke(collection, [error]);

        var createException = typeof(SqlException).GetMethod("CreateException",
            BindingFlags.NonPublic | BindingFlags.Static, null,
            [collectionType, typeof(string), typeof(Guid), typeof(Exception)], null)!;
        return (SqlException)createException.Invoke(null, [collection, "11.0.0", Guid.Empty, null])!;
    }
}
