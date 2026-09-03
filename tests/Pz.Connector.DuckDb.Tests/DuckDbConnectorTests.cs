using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Execution;
using Pz.Engine.Validation;

namespace Pz.Connector.DuckDb.Tests;

public sealed class DuckDbConnectorTests : IDisposable
{
    private readonly string dir = Directory.CreateTempSubdirectory("pz-duckdb-connector-tests-").FullName;

    public void Dispose() => Directory.Delete(dir, recursive: true);

    private ConnectorConfig Config(string fileName = "app.duckdb", string? baseDir = null)
    {
        var values = new Dictionary<string, object?> { ["path"] = Path.Combine(dir, fileName) };
        if (baseDir is not null)
        {
            values["base_dir"] = baseDir;
        }

        return new ConnectorConfig(values);
    }

    [Fact]
    public void Published_schemas_are_valid_json_schema()
    {
        var c = new DuckDbConnector();
        foreach (var s in new[] { c.ConnectionConfigSchema, c.DatasetConfigSchema })
        {
            var schema = Json.Schema.JsonSchema.FromText(s); // throws on malformed
            Assert.NotNull(schema);
        }
    }

    [Fact]
    public void Connector_is_native_only_in_both_directions()
    {
        var c = new DuckDbConnector();
        Assert.IsAssignableFrom<INativeOnlySource>(c);
        Assert.IsAssignableFrom<INativeOnlySink>(c);
        Assert.Equal(
            ConnectorCapabilities.NativeScan | ConnectorCapabilities.NativeCopy |
            ConnectorCapabilities.ReplaceWrites | ConnectorCapabilities.Merge |
            ConnectorCapabilities.Transactional |
            ConnectorCapabilities.BoundedWindow | ConnectorCapabilities.InclusiveWatermarkBound,
            c.Capabilities);
        Assert.Equal("duckdb", c.Info.Name);
        Assert.Equal(ProtocolVersion.Major, c.Info.ProtocolMajor);
    }

    [Fact]
    public async Task Validate_accepts_an_ordinary_path()
    {
        var result = await new DuckDbConnector().ValidateAsync(Config(), CancellationToken.None);
        Assert.Empty(result.Errors);
    }

    [Fact]
    public async Task Validate_refuses_a_relative_path_under_pz_with_no_base_dir_injected()
    {
        // Tier-3 validation runs on the connection as the USER wrote it, before the host injects
        // base_dir -- so this is the config shape ConnectorConfigValidator actually calls ValidateAsync
        // with. A relative path is project-relative by definition, so the refusal must fire without
        // needing base_dir at all.
        var config = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["path"] = ".pz/runs/x/staging.duckdb",
        });

        var result = await new DuckDbConnector().ValidateAsync(config, CancellationToken.None);
        var error = Assert.Single(result.Errors);
        Assert.Contains(".pz", error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Validate_refuses_an_injected_absolute_path_inside_the_projects_pz_directory()
    {
        // .pz/ is the run's own staging/state area; attaching a database there is never intended.
        // Covers the post-injection shape: base_dir present, path resolved absolute.
        var config = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["path"] = ".pz/runs/x/staging.duckdb",
            ["base_dir"] = dir,
        });

        var result = await new DuckDbConnector().ValidateAsync(config, CancellationToken.None);
        var error = Assert.Single(result.Errors);
        Assert.Contains(".pz", error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Validate_accepts_a_relative_path_that_stays_outside_pz()
    {
        var config = new ConnectorConfig(new Dictionary<string, object?> { ["path"] = "warehouse/app.duckdb" });
        var result = await new DuckDbConnector().ValidateAsync(config, CancellationToken.None);
        Assert.Empty(result.Errors);
    }

    [Fact]
    public async Task Check_reports_a_missing_file_as_ok_with_a_will_be_created_note()
    {
        var check = await new DuckDbConnector().CheckConnectionAsync(Config("fresh.duckdb"), CancellationToken.None);
        Assert.True(check.Ok);
        Assert.Contains("created on first write", check.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Check_fails_permanently_when_the_parent_directory_is_missing()
    {
        var check = await new DuckDbConnector().CheckConnectionAsync(
            Config(Path.Combine("nope", "app.duckdb")), CancellationToken.None);
        Assert.False(check.Ok);
        Assert.StartsWith("permanent:", check.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Check_fails_permanently_on_a_non_duckdb_file()
    {
        await File.WriteAllTextAsync(Path.Combine(dir, "app.duckdb"), "this is not a database");
        var check = await new DuckDbConnector().CheckConnectionAsync(Config(), CancellationToken.None);
        Assert.False(check.Ok);
        Assert.Contains("header magic", check.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Check_accepts_a_file_with_the_duckdb_header_magic()
    {
        // Byte offset 8..12 of every DuckDB database file is "DUCK".
        var bytes = new byte[32];
        "DUCK"u8.CopyTo(bytes.AsSpan(8));
        await File.WriteAllBytesAsync(Path.Combine(dir, "app.duckdb"), bytes);

        var check = await new DuckDbConnector().CheckConnectionAsync(Config(), CancellationToken.None);
        Assert.True(check.Ok);
    }

    [Fact]
    public async Task PlanReadAsync_refuses_the_universal_tier_with_PZ0312()
    {
        await using var source = await ((ISourceConnector)new DuckDbConnector()).OpenAsync(Config(), CancellationToken.None);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await source.PlanReadAsync(new DatasetSpec("appdb", "events", new Dictionary<string, object?>()), ReadHints.None, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.StartsWith("PZ0312", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task BeginWriteAsync_refuses_the_universal_tier_with_PZ0312()
    {
        await using var sink = await ((ISinkConnector)new DuckDbConnector()).OpenAsync(Config(), CancellationToken.None);
        var spec = new OutputSpec("appdb", "events_out", "append", "fail_on_change", new Dictionary<string, object?>());
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await sink.BeginWriteAsync(spec, new Apache.Arrow.Schema([], null), CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.StartsWith("PZ0312", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task GetSchemaAsync_returns_the_declared_contract_as_the_schema()
    {
        await using var source = await ((ISourceConnector)new DuckDbConnector()).OpenAsync(Config(), CancellationToken.None);
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
    public async Task GetSchemaAsync_without_a_contract_is_a_permanent_refusal()
    {
        await using var source = await ((ISourceConnector)new DuckDbConnector()).OpenAsync(Config(), CancellationToken.None);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await source.GetSchemaAsync(new DatasetSpec("appdb", "events", new Dictionary<string, object?>()), CancellationToken.None));
        Assert.False(ex.IsTransient);
        Assert.Contains("columns: contract", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Native_scan_and_copy_share_one_attach_alias()
    {
        // TryGetNativeScan refuses a missing file (F5); this test is about the shared alias, not
        // that guard, so give it a file that exists.
        await File.WriteAllBytesAsync(Path.Combine(dir, "app.duckdb"), []);
        await using var source = await ((ISourceConnector)new DuckDbConnector()).OpenAsync(Config(), CancellationToken.None);
        await using var sink = await ((ISinkConnector)new DuckDbConnector()).OpenAsync(Config(), CancellationToken.None);

        Assert.True(source.TryGetNativeScan(new DatasetSpec("wh", "events", new Dictionary<string, object?>()), out var scan));
        Assert.True(sink.TryGetNativeCopy(new OutputSpec("wh", "events_out", "append", "fail_on_change", new Dictionary<string, object?>()), out var copy));

        Assert.Equal(scan!.SetupStatements, copy!.SetupStatements);
        Assert.Equal("attach", scan.Mechanism);
        Assert.Equal("duckdb insert", copy.Mechanism);
        Assert.StartsWith("pz_duckdb_wh_", scan.SqlFragment, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ConnectorConfigValidator_refuses_a_duckdb_connection_whose_path_lands_under_pz()
    {
        // End-to-end through the production tier-3 path: ConnectorConfigValidator.ValidateAsync calls
        // the REAL DuckDbConnector.ValidateAsync on the connection exactly as the user wrote it -- no
        // base_dir, because tier 3 runs before the host injects it. Proves the F2 guard fires on the
        // path a real `pz validate` run takes, not just via a direct connector call.
        var registry = new ConnectorRegistry();
        registry.AddSource("duckdb", new DuckDbConnector());

        var source = new ConnectionDef("appdb", "duckdb",
            new Dictionary<string, object?> { ["path"] = ".pz/runs/x/staging.duckdb" },
            [new DatasetDef("events", new Dictionary<string, object?> { ["columns"] = new Dictionary<string, object?> { ["id"] = "bigint" } }, null)],
            "connections.yml");
        var project = new PzProject("proj", "0.1.0", new EngineConfig(), new Dictionary<string, object?>(), [],
            [source], []);

        var errors = await ConnectorConfigValidator.ValidateAsync(project, registry, CancellationToken.None);

        var error = Assert.Single(errors);
        Assert.Equal("connections.yml", error.File);
        Assert.Contains(".pz", error.Message, StringComparison.Ordinal);
    }
}
