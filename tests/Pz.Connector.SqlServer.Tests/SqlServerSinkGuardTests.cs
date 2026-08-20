using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.SqlServer.Tests;

/// <summary>Fail-fast guards that run before any connection is opened -- provable offline: if the
/// guard didn't fire first, the unreachable host would produce a different (transient/socket) error.</summary>
public class SqlServerSinkGuardTests
{
    private static readonly Schema IdNameSchema = new(
    [
        new Field("id", Int64Type.Default, nullable: false),
        new Field("name", StringType.Default, nullable: false),
    ], null);

    private static async ValueTask<ISink> OpenSinkAsync()
    {
        ISinkConnector connector = new SqlServerConnector();
        return await connector.OpenAsync(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["host"] = "unreachable.invalid", ["database"] = "db",
        }), CancellationToken.None);
    }

    [Fact]
    public async Task Unknown_mode_is_rejected_before_any_connection()
    {
        await using var sink = await OpenSinkAsync();
        var spec = new OutputSpec("ms", "out", "upsert", "fail_on_change", new Dictionary<string, object?>());
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(spec, IdNameSchema, CancellationToken.None));
        Assert.False(ex.IsTransient);
        Assert.Contains("'append'/'replace'/'merge'", ex.Message, StringComparison.Ordinal);
        Assert.Contains("upsert", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Merge_with_key_missing_from_schema_is_rejected_before_any_connection()
    {
        await using var sink = await OpenSinkAsync();
        var spec = new OutputSpec("ms", "out", "merge", "fail_on_change", new Dictionary<string, object?>())
        {
            Keys = ["id", "tenant"],
        };
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(spec, IdNameSchema, CancellationToken.None));
        Assert.False(ex.IsTransient);
        Assert.Contains("tenant", ex.Message, StringComparison.Ordinal);
        Assert.Contains("not present in the write schema", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Non_boolean_tablock_option_is_rejected_before_any_connection()
    {
        await using var sink = await OpenSinkAsync();
        var spec = new OutputSpec("ms", "out", "append", "fail_on_change",
            new Dictionary<string, object?> { ["tablock"] = "sideways" });
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(spec, IdNameSchema, CancellationToken.None));
        Assert.False(ex.IsTransient);
        Assert.Contains("'tablock' must be a boolean", ex.Message, StringComparison.Ordinal);
        Assert.Contains("sideways", ex.Message, StringComparison.Ordinal);
    }
}
