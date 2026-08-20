using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Sqlite.Tests;

public sealed class SqliteConnectorTests : IDisposable
{
    private readonly string dir = Directory.CreateTempSubdirectory("pz-sqlite-connector-tests-").FullName;

    public void Dispose() => Directory.Delete(dir, recursive: true);

    private ConnectorConfig Config(string fileName = "app.db") => new(new Dictionary<string, object?>
    {
        ["path"] = Path.Combine(dir, fileName),
    });

    [Fact]
    public void Published_schemas_are_valid_json_schema()
    {
        var c = new SqliteConnector();
        foreach (var s in new[] { c.ConnectionConfigSchema, c.DatasetConfigSchema })
        {
            var schema = Json.Schema.JsonSchema.FromText(s); // throws on malformed
            Assert.NotNull(schema);
        }
    }

    [Fact]
    public void Connector_is_native_only_in_both_directions()
    {
        var c = new SqliteConnector();
        Assert.IsAssignableFrom<INativeOnlySource>(c);
        Assert.IsAssignableFrom<INativeOnlySink>(c);
        Assert.Equal(
            ConnectorCapabilities.NativeScan | ConnectorCapabilities.NativeCopy |
            ConnectorCapabilities.ReplaceWrites |
            ConnectorCapabilities.BoundedWindow | ConnectorCapabilities.InclusiveWatermarkBound,
            c.Capabilities);
        Assert.Equal("sqlite", c.Info.Name);
        Assert.Equal(ProtocolVersion.Major, c.Info.ProtocolMajor);
    }

    [Fact]
    public async Task PlanReadAsync_refuses_the_universal_tier_with_PZ0312()
    {
        await using var source = await ((ISourceConnector)new SqliteConnector()).OpenAsync(Config(), CancellationToken.None);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await source.PlanReadAsync(new DatasetSpec("appdb", "events", new Dictionary<string, object?>()), ReadHints.None, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.StartsWith("PZ0312", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task BeginWriteAsync_refuses_the_universal_tier_with_PZ0312()
    {
        await using var sink = await ((ISinkConnector)new SqliteConnector()).OpenAsync(Config(), CancellationToken.None);
        var spec = new OutputSpec("appdb", "events_out", "append", "fail_on_change", new Dictionary<string, object?>());
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await sink.BeginWriteAsync(spec, new Apache.Arrow.Schema([], null), CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.StartsWith("PZ0312", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task GetSchemaAsync_returns_the_declared_contract_as_the_schema()
    {
        await using var source = await ((ISourceConnector)new SqliteConnector()).OpenAsync(Config(), CancellationToken.None);
        var spec = new DatasetSpec("appdb", "events", new Dictionary<string, object?>
        {
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["happened_on"] = "date" },
        });

        var schema = await source.GetSchemaAsync(spec, CancellationToken.None);

        Assert.Collection(schema.Schema.FieldsList,
            f => { Assert.Equal("id", f.Name); Assert.Equal(ArrowTypeId.Int64, f.DataType.TypeId); },
            f => { Assert.Equal("happened_on", f.Name); Assert.Equal(ArrowTypeId.Date32, f.DataType.TypeId); });
    }

    [Fact]
    public async Task GetSchemaAsync_without_a_contract_is_a_clear_permanent_refusal()
    {
        await using var source = await ((ISourceConnector)new SqliteConnector()).OpenAsync(Config(), CancellationToken.None);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await source.GetSchemaAsync(new DatasetSpec("appdb", "events", new Dictionary<string, object?>()), CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("columns:", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Check_verifies_a_real_sqlite_header()
    {
        // The 16-byte SQLite magic ("SQLite format 3\0") followed by arbitrary page bytes — the check
        // reads the header only, it never parses pages.
        var path = Path.Combine(dir, "real.db");
        await File.WriteAllBytesAsync(path, [.. "SQLite format 3"u8, 0, 0x10, 0x00]);

        var check = await new SqliteConnector().CheckConnectionAsync(
            new ConnectorConfig(new Dictionary<string, object?> { ["path"] = path }), CancellationToken.None);

        Assert.True(check.Ok);
        Assert.Contains("verified", check.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Check_rejects_a_file_that_is_not_a_sqlite_database()
    {
        // The extension itself attaches such a file "fine" and fails only at first query — this
        // check is what surfaces the problem at validate time instead.
        var path = Path.Combine(dir, "bogus.db");
        await File.WriteAllTextAsync(path, "hello i am not sqlite");

        var check = await new SqliteConnector().CheckConnectionAsync(
            new ConnectorConfig(new Dictionary<string, object?> { ["path"] = path }), CancellationToken.None);

        Assert.False(check.Ok);
        Assert.StartsWith("permanent:", check.Message, StringComparison.Ordinal);
        Assert.Contains("not a SQLite database", check.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Check_accepts_a_missing_file_with_an_explicit_will_be_created_note()
    {
        // A write-first project (pz creating app.db from scratch) is legitimate; the note keeps a
        // typo'd source path from passing silently.
        var check = await new SqliteConnector().CheckConnectionAsync(Config("not-yet.db"), CancellationToken.None);

        Assert.True(check.Ok);
        Assert.Contains("does not exist yet", check.Message, StringComparison.Ordinal);
        Assert.Contains("created on first write", check.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Check_rejects_a_missing_parent_directory()
    {
        var config = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["path"] = Path.Combine(dir, "no-such-dir", "app.db"),
        });

        var check = await new SqliteConnector().CheckConnectionAsync(config, CancellationToken.None);

        Assert.False(check.Ok);
        Assert.StartsWith("permanent:", check.Message, StringComparison.Ordinal);
        Assert.Contains("directory", check.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Check_requires_path()
    {
        var check = await new SqliteConnector().CheckConnectionAsync(
            new ConnectorConfig(new Dictionary<string, object?>()), CancellationToken.None);

        Assert.False(check.Ok);
        Assert.Equal("permanent: sqlite connection requires 'path'", check.Message);
    }

    [Fact]
    public async Task Validate_has_no_cross_field_rules()
    {
        var result = await new SqliteConnector().ValidateAsync(Config(), CancellationToken.None);
        Assert.True(result.IsValid);
    }
}
