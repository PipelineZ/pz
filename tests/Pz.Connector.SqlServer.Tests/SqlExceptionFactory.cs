using System.Reflection;
using Microsoft.Data.SqlClient;

namespace Pz.Connector.SqlServer.Tests;

/// <summary>SqlClient exposes no public <see cref="SqlException"/> constructor -- every real instance
/// comes from a live TDS error. Builds one offline via the same internal factory SqlClient itself uses,
/// so <see cref="MsTransient"/>'s per-number branches are directly testable without a docker container
/// (the docker-backed <c>MsTransientDockerTests</c> covers the two numbers a real server actually
/// produces in this repro: a deadlock victim and a command timeout).</summary>
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
