using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Snowflake.Tests;

public class SnowflakeSinkGuardTests
{
    private static Schema TwoCol() => new([
        new Field("id", Int64Type.Default, nullable: true),
        new Field("name", StringType.Default, nullable: true)], null);

    private static OutputSpec Spec(string mode, params string[] keys) =>
        new("sf", "PUBLIC.T", mode, "create", new Dictionary<string, object?>()) { Keys = keys };

    [Fact]
    public async Task Unknown_mode_is_rejected_nontransient()
    {
        var sink = new SnowflakeSink("account=a");
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(Spec("upsert"), TwoCol(), CancellationToken.None));
        Assert.False(ex.IsTransient);
        Assert.Contains("upsert", ex.Message);
    }

    [Fact]
    public async Task Merge_key_missing_from_schema_is_rejected()
    {
        var sink = new SnowflakeSink("account=a");
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(Spec("merge", "nope"), TwoCol(), CancellationToken.None));
        Assert.Contains("nope", ex.Message);
    }

    [Fact]
    public async Task Reserved_sequence_column_is_rejected_with_rename_hint()
    {
        var schema = new Schema([new Field("_pz_seq", Int64Type.Default, nullable: true)], null);
        var sink = new SnowflakeSink("account=a");
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(Spec("merge", "_pz_seq"), schema, CancellationToken.None));
        Assert.Contains("_pz_seq", ex.Message);
        Assert.Contains("rename", ex.Message);
    }
}
