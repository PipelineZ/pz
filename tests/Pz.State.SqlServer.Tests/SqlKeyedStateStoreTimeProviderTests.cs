using Microsoft.Data.SqlClient;
using Pz.Core.Validation;
using Pz.State.SqlServer;
using Pz.TestSupport;
using Pz.TestSupport.State;

namespace Pz.State.SqlServer.Tests;

using TestEntry = KeyedStateStoreContract.TestEntry;

/// <summary>Proves <see cref="SqlKeyedStateStore{T}"/> stamps <c>updated_at</c> through an injected
/// <see cref="TimeProvider"/> rather than the ambient wall clock (<c>DateTime.UtcNow</c>), and refuses
/// -- rather than silently truncates -- a state key past the length its <c>sp_executesql</c> parameter
/// is declared with.</summary>
[Collection(SqlServerFixture.CollectionName)]
public sealed class SqlKeyedStateStoreTimeProviderTests(SqlServerFixture fixture)
{
    private sealed class FixedTimeProvider(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }

    private static SqlKeyedStateStore<TestEntry> NewStore(SqlStateConnection connection, TimeProvider? time = null) =>
        new(connection, "time-provider",
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
            },
            time: time);

    /// <summary>Reads <c>updated_at</c> straight off the table -- the store itself never surfaces this
    /// column, so the only way to observe what <see cref="SqlKeyedStateStore{T}.Set"/> actually stamped
    /// is a raw query, mirroring <c>SqlKeyedStateStoreConcurrencyTests</c>' pattern for reaching rows the
    /// public API does not expose.</summary>
    private static DateTime ReadUpdatedAt(SqlStateConnection connection, string scope, string key)
    {
        using var sqlConnection = connection.Open();
        using var command = new SqlCommand(
            "DECLARE @sql NVARCHAR(MAX) = N'SELECT updated_at FROM ' + QUOTENAME(@schema) + " +
            "N'.state WHERE scope = @scope AND state_key = @key'; " +
            "EXEC sp_executesql @sql, N'@scope NVARCHAR(32), @key NVARCHAR(512)', " +
            "@scope = @scope, @key = @key;",
            sqlConnection);
        command.Parameters.AddWithValue("@schema", connection.Schema);
        command.Parameters.AddWithValue("@scope", scope);
        command.Parameters.AddWithValue("@key", key);
        return (DateTime)command.ExecuteScalar()!;
    }

    [SkippableFact]
    public void Set_stamps_updated_at_from_the_injected_TimeProvider_not_the_wall_clock()
    {
        DockerFacts.SkipUnlessDocker();
        var connection = fixture.NewConnection();
        SqlStateSchema.EnsureCurrent(connection);

        var fixedNow = new DateTimeOffset(2026, 3, 14, 9, 26, 53, TimeSpan.Zero);
        var store = NewStore(connection, new FixedTimeProvider(fixedNow));

        store.Set("k", new TestEntry("v", "run-1"));

        Assert.Equal(fixedNow.UtcDateTime, ReadUpdatedAt(connection, "time-provider", "k"));
    }

    [SkippableFact]
    public void A_second_Set_re_stamps_updated_at_from_a_later_injected_instant()
    {
        DockerFacts.SkipUnlessDocker();
        var connection = fixture.NewConnection();
        SqlStateSchema.EnsureCurrent(connection);

        var first = new DateTimeOffset(2026, 3, 14, 9, 26, 53, TimeSpan.Zero);
        var second = new DateTimeOffset(2026, 3, 15, 10, 0, 0, TimeSpan.Zero);

        NewStore(connection, new FixedTimeProvider(first)).Set("k2", new TestEntry("v1", "run-1"));
        var store2 = NewStore(connection, new FixedTimeProvider(second));
        store2.Get("k2"); // establishes the expected version for the compare-and-swap update path
        store2.Set("k2", new TestEntry("v2", "run-2"));

        Assert.Equal(second.UtcDateTime, ReadUpdatedAt(connection, "time-provider", "k2"));
    }

    /// <summary>A key over SQL Server's declared <c>NVARCHAR(512)</c> would otherwise be assigned into
    /// that parameter silently truncated -- no warning, no error -- so a later <c>Get</c> with the full
    /// (untruncated) key would never find the row it just wrote. Refused client-side instead, before the
    /// value ever reaches a command: no docker/network round trip needed to prove it, since the guard
    /// fires before any connection is opened.</summary>
    [Fact]
    public void Set_refuses_a_key_over_the_512_character_limit_instead_of_truncating()
    {
        var store = NewStore(connection: null!);
        var overLong = new string('k', 513);

        var ex = Assert.Throws<PzConfigException>(() => store.Set(overLong, new TestEntry("v", "run-1")));

        Assert.Equal(PzErrorCode.SqlStateValueTooLong, ex.Error.Code);
        Assert.DoesNotContain(overLong, ex.Error.Message, StringComparison.Ordinal);
        Assert.Contains("512", ex.Error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Get_refuses_a_key_over_the_512_character_limit()
    {
        var store = NewStore(connection: null!);
        var overLong = new string('k', 513);

        var ex = Assert.Throws<PzConfigException>(() => store.Get(overLong));

        Assert.Equal(PzErrorCode.SqlStateValueTooLong, ex.Error.Code);
    }

    [Fact]
    public void Remove_refuses_a_key_over_the_512_character_limit()
    {
        var store = NewStore(connection: null!);
        var overLong = new string('k', 513);

        var ex = Assert.Throws<PzConfigException>(() => store.Remove(overLong));

        Assert.Equal(PzErrorCode.SqlStateValueTooLong, ex.Error.Code);
    }

    [Fact]
    public void A_key_at_exactly_the_limit_passes_the_guard()
    {
        // Proves the boundary is ">", not ">=" -- a 512-character key must reach past the length guard
        // (and therefore fail on the null connection this no-docker test passes with some other
        // exception), never PzConfigException/SqlStateValueTooLong.
        var store = NewStore(connection: null!);
        var atLimit = new string('k', 512);

        var ex = Record.Exception(() => store.Get(atLimit));

        Assert.NotNull(ex);
        Assert.IsNotType<PzConfigException>(ex);
    }
}
