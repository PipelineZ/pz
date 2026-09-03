using System.Net;
using System.Net.Sockets;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Execution;
using Pz.Engine.Validation;

namespace Pz.Connector.DuckLake.Tests;

public sealed class DuckLakeConnectorTests : IDisposable
{
    private readonly string dir = Directory.CreateTempSubdirectory("pz-ducklake-connector-tests-").FullName;

    public void Dispose() => Directory.Delete(dir, recursive: true);

    private ConnectorConfig FileCatalog(string catalog = "duckdb", string file = "catalog.ducklake") =>
        new(new Dictionary<string, object?>
        {
            ["catalog"] = catalog,
            ["path"] = Path.Combine(dir, file),
            ["data_path"] = Path.Combine(dir, "data"),
        });

    [Fact]
    public void Published_schemas_are_valid_json_schema()
    {
        var c = new DuckLakeConnector();
        foreach (var s in new[] { c.ConnectionConfigSchema, c.DatasetConfigSchema })
        {
            Assert.NotNull(Json.Schema.JsonSchema.FromText(s));
        }
    }

    [Fact]
    public void Connector_is_native_only_in_both_directions()
    {
        var c = new DuckLakeConnector();
        Assert.IsAssignableFrom<INativeOnlySource>(c);
        Assert.IsAssignableFrom<INativeOnlySink>(c);
        Assert.Equal(
            ConnectorCapabilities.NativeScan | ConnectorCapabilities.NativeCopy |
            ConnectorCapabilities.ReplaceWrites | ConnectorCapabilities.Merge |
            ConnectorCapabilities.Transactional |
            ConnectorCapabilities.BoundedWindow | ConnectorCapabilities.InclusiveWatermarkBound,
            c.Capabilities);
        Assert.Equal("ducklake", c.Info.Name);
    }

    [Fact]
    public async Task Validate_aggregates_the_catalog_matrix()
    {
        var config = new ConnectorConfig(new Dictionary<string, object?> { ["catalog"] = "postgres", ["token"] = "x" });
        var result = await new DuckLakeConnector().ValidateAsync(config, CancellationToken.None);
        Assert.Equal(4, result.Errors.Count); // host, database, data_path missing; token stray
    }

    [Fact]
    public async Task Validate_refuses_a_relative_catalog_path_under_pz_with_no_base_dir_injected()
    {
        var config = new ConnectorConfig(new Dictionary<string, object?> { ["path"] = ".pz/runs/x/catalog.ducklake" });
        var result = await new DuckLakeConnector().ValidateAsync(config, CancellationToken.None);
        var error = Assert.Single(result.Errors);
        Assert.Contains(".pz", error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Validate_refuses_a_relative_data_path_under_pz()
    {
        var config = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["path"] = "lake/c.ducklake",
            ["data_path"] = ".pz/lake",
        });
        var result = await new DuckLakeConnector().ValidateAsync(config, CancellationToken.None);
        var error = Assert.Single(result.Errors);
        Assert.Contains("data_path", error, StringComparison.Ordinal);
        Assert.Contains(".pz", error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Validate_refuses_an_injected_absolute_path_inside_the_projects_pz_directory()
    {
        var config = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["path"] = Path.Combine(dir, ".pz", "runs", "x", "catalog.ducklake"),
            ["base_dir"] = dir,
        });
        var result = await new DuckLakeConnector().ValidateAsync(config, CancellationToken.None);
        Assert.Single(result.Errors);
    }

    [Fact]
    public async Task Validate_ignores_an_absolute_path_when_no_base_dir_is_injected()
    {
        // An absolute path is only comparable against .pz/ once the host injects base_dir --
        // pre-injection, tier-3 validation has no anchor to resolve it against.
        var config = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["path"] = Path.Combine(dir, ".pz", "x.ducklake"),
        });
        var result = await new DuckLakeConnector().ValidateAsync(config, CancellationToken.None);
        Assert.Empty(result.Errors);
    }

    [Fact]
    public async Task Validate_accepts_paths_that_stay_outside_pz()
    {
        var config = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["path"] = "lake/c.ducklake",
            ["data_path"] = "lake/data",
        });
        var result = await new DuckLakeConnector().ValidateAsync(config, CancellationToken.None);
        Assert.Empty(result.Errors);

        var objectStore = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["path"] = "lake/c.ducklake",
            ["data_path"] = "s3://b/",
        });
        var objectStoreResult = await new DuckLakeConnector().ValidateAsync(objectStore, CancellationToken.None);
        Assert.Empty(objectStoreResult.Errors);
    }

    [Fact]
    public async Task ConnectorConfigValidator_refuses_a_ducklake_connection_whose_path_lands_under_pz()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("ducklake", new DuckLakeConnector());

        var source = new ConnectionDef("lake", "ducklake",
            new Dictionary<string, object?> { ["path"] = ".pz/runs/x/catalog.ducklake" },
            [new DatasetDef("events", new Dictionary<string, object?> { ["columns"] = new Dictionary<string, object?> { ["id"] = "bigint" } }, null)],
            "connections.yml");
        var project = new PzProject("proj", "0.1.0", new EngineConfig(), new Dictionary<string, object?>(), [],
            [source], []);

        var errors = await ConnectorConfigValidator.ValidateAsync(project, registry, CancellationToken.None);

        var error = Assert.Single(errors);
        Assert.Equal("connections.yml", error.File);
        Assert.Contains(".pz", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Check_reports_a_missing_duckdb_catalog_file_as_ok_with_a_note()
    {
        var check = await new DuckLakeConnector().CheckConnectionAsync(FileCatalog(), CancellationToken.None);
        Assert.True(check.Ok);
        Assert.Contains("created on first write", check.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Check_fails_permanently_on_a_non_duckdb_catalog_file()
    {
        await File.WriteAllTextAsync(Path.Combine(dir, "catalog.ducklake"), "nope");
        var check = await new DuckLakeConnector().CheckConnectionAsync(FileCatalog(), CancellationToken.None);
        Assert.False(check.Ok);
        Assert.Contains("header magic", check.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Check_verifies_a_sqlite_catalog_files_header()
    {
        var bytes = new byte[32];
        "SQLite format 3\0"u8.CopyTo(bytes);
        await File.WriteAllBytesAsync(Path.Combine(dir, "catalog.sqlite"), bytes);
        var check = await new DuckLakeConnector().CheckConnectionAsync(FileCatalog("sqlite", "catalog.sqlite"), CancellationToken.None);
        Assert.True(check.Ok);
    }

    [Fact]
    public async Task Check_fails_permanently_when_the_catalog_parent_directory_is_missing()
    {
        var config = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["path"] = Path.Combine(dir, "missing", "c.ducklake"),
        });
        var check = await new DuckLakeConnector().CheckConnectionAsync(config, CancellationToken.None);
        Assert.False(check.Ok);
        Assert.StartsWith("permanent:", check.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Check_probes_a_quack_server_over_tcp()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        var config = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["catalog"] = "quack", ["uri"] = $"quack:127.0.0.1:{port}", ["token"] = "tok", ["data_path"] = dir,
        });

        var check = await new DuckLakeConnector().CheckConnectionAsync(config, CancellationToken.None);
        Assert.True(check.Ok);
        Assert.Contains("credentials are verified at run time", check.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Check_reports_an_unreachable_postgres_catalog_as_transient()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop(); // nobody listens there now
        var config = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["catalog"] = "postgres", ["host"] = "127.0.0.1", ["port"] = (long)port, ["database"] = "d", ["data_path"] = dir,
        });

        var check = await new DuckLakeConnector().CheckConnectionAsync(config, CancellationToken.None);
        Assert.False(check.Ok);
        Assert.StartsWith("transient:", check.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Check_has_no_offline_probe_for_motherduck()
    {
        var config = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["catalog"] = "motherduck", ["database"] = "d", ["token"] = "t", ["data_path"] = "s3://b/",
        });
        var check = await new DuckLakeConnector().CheckConnectionAsync(config, CancellationToken.None);
        Assert.True(check.Ok);
        Assert.StartsWith("not checked", check.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PlanReadAsync_and_BeginWriteAsync_refuse_the_universal_tier_with_PZ0312()
    {
        await using var source = await ((ISourceConnector)new DuckLakeConnector()).OpenAsync(FileCatalog(), CancellationToken.None);
        var readEx = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await source.PlanReadAsync(new DatasetSpec("wh", "events", new Dictionary<string, object?>()), ReadHints.None, CancellationToken.None));
        Assert.StartsWith("PZ0312", readEx.Message, StringComparison.Ordinal);

        await using var sink = await ((ISinkConnector)new DuckLakeConnector()).OpenAsync(FileCatalog(), CancellationToken.None);
        var writeEx = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await sink.BeginWriteAsync(new OutputSpec("wh", "o", "append", "fail_on_change", new Dictionary<string, object?>()),
                new Apache.Arrow.Schema([], null), CancellationToken.None));
        Assert.StartsWith("PZ0312", writeEx.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task GetSchemaAsync_returns_the_declared_contract_or_refuses()
    {
        await using var source = await ((ISourceConnector)new DuckLakeConnector()).OpenAsync(FileCatalog(), CancellationToken.None);
        var spec = new DatasetSpec("wh", "events", new Dictionary<string, object?>
        {
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["happened_on"] = "date" },
        });
        var schema = await source.GetSchemaAsync(spec, CancellationToken.None);
        Assert.Collection(schema.Schema.FieldsList,
            f => Assert.Equal(ArrowTypeId.Int64, f.DataType.TypeId),
            f => Assert.Equal(ArrowTypeId.Date32, f.DataType.TypeId));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await source.GetSchemaAsync(new DatasetSpec("wh", "events", new Dictionary<string, object?>()), CancellationToken.None));
        Assert.Contains("columns: contract", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Native_scan_and_copy_share_one_attach_and_time_travel_conflicts_surface_at_probe_time()
    {
        await File.WriteAllBytesAsync(Path.Combine(dir, "catalog.ducklake"), []);
        await using var source = await ((ISourceConnector)new DuckLakeConnector()).OpenAsync(FileCatalog(), CancellationToken.None);
        await using var sink = await ((ISinkConnector)new DuckLakeConnector()).OpenAsync(FileCatalog(), CancellationToken.None);

        Assert.True(source.TryGetNativeScan(new DatasetSpec("wh", "events", new Dictionary<string, object?>()), out var scan));
        Assert.True(sink.TryGetNativeCopy(new OutputSpec("wh", "o", "replace", "fail_on_change", new Dictionary<string, object?>()), out var copy));
        Assert.Equal(scan!.SetupStatements, copy!.SetupStatements);
        Assert.Equal("ducklake attach", scan.Mechanism);
        Assert.Equal("ducklake create-or-replace", copy.Mechanism);

        var both = new DatasetSpec("wh", "events", new Dictionary<string, object?> { ["version"] = 1L, ["timestamp"] = "2026-01-01" });
        Assert.Throws<PzConnectorException>(() => { source.TryGetNativeScan(both, out _); });
    }

    [Theory]
    [InlineData("duckdb", "catalog.ducklake")]
    [InlineData("sqlite", "catalog.sqlite")]
    public async Task A_read_against_a_missing_catalog_file_is_refused_and_creates_no_file(string catalog, string file)
    {
        await using var source = await ((ISourceConnector)new DuckLakeConnector()).OpenAsync(FileCatalog(catalog, file), CancellationToken.None);

        var ex = Assert.Throws<PzConnectorException>(() =>
        {
            source.TryGetNativeScan(new DatasetSpec("wh", "events", new Dictionary<string, object?>()), out _);
        });
        Assert.False(ex.IsTransient);
        Assert.Contains("events", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(dir, ex.Message, StringComparison.Ordinal);
        Assert.False(File.Exists(Path.Combine(dir, file)));
    }
}
