using System.Net;
using System.Net.Sockets;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Execution;
using Pz.Engine.Validation;

namespace Pz.Connector.Iceberg.Tests;

public sealed class IcebergConnectorTests : IDisposable
{
    private readonly string dir = Directory.CreateTempSubdirectory("pz-iceberg-connector-tests-").FullName;

    public void Dispose() => Directory.Delete(dir, recursive: true);

    private static ConnectorConfig Rest(params (string Key, object? Value)[] extra)
    {
        var dictionary = new Dictionary<string, object?> { ["catalog"] = "rest", ["endpoint"] = "http://127.0.0.1:1", ["warehouse"] = "wh" };
        foreach (var (key, value) in extra)
        {
            dictionary[key] = value; // last write wins, so a test can override a base key
        }

        return new(dictionary);
    }

    private ConnectorConfig Files(string? root = null) => new(new Dictionary<string, object?>
    {
        ["catalog"] = "files",
        ["root"] = root ?? dir,
    });

    private static DatasetSpec Spec(string entity = "raw.events", Dictionary<string, object?>? options = null) =>
        new("wh", entity, options ?? []);

    [Fact]
    public void Published_schemas_are_valid_json_schema()
    {
        var c = new IcebergConnector();
        foreach (var s in new[] { c.ConnectionConfigSchema, c.DatasetConfigSchema })
        {
            Assert.NotNull(Json.Schema.JsonSchema.FromText(s));
        }
    }

    [Fact]
    public void Connector_is_native_only_in_both_directions()
    {
        var c = new IcebergConnector();
        Assert.IsAssignableFrom<INativeOnlySource>(c);
        Assert.IsAssignableFrom<INativeOnlySink>(c);
        Assert.Equal(
            ConnectorCapabilities.NativeScan | ConnectorCapabilities.NativeCopy |
            ConnectorCapabilities.ReplaceWrites | ConnectorCapabilities.Merge |
            ConnectorCapabilities.Transactional |
            ConnectorCapabilities.BoundedWindow | ConnectorCapabilities.InclusiveWatermarkBound,
            c.Capabilities);
        Assert.Equal("iceberg", c.Info.Name);
    }

    [Fact]
    public async Task Validate_aggregates_the_catalog_matrix()
    {
        var config = new ConnectorConfig(new Dictionary<string, object?> { ["catalog"] = "files", ["endpoint"] = "http://c", ["storage_key_id"] = "AK" });
        var result = await new IcebergConnector().ValidateAsync(config, CancellationToken.None);
        Assert.Equal(3, result.Errors.Count); // root missing; endpoint stray; storage_secret_key missing
    }

    [Fact]
    public async Task Validate_refuses_a_relative_root_under_pz_with_no_base_dir_injected()
    {
        var result = await new IcebergConnector().ValidateAsync(Files(".pz/runs/x/lake"), CancellationToken.None);
        var error = Assert.Single(result.Errors);
        Assert.Contains(".pz", error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Validate_refuses_an_injected_absolute_root_inside_the_projects_pz_directory()
    {
        var config = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["catalog"] = "files",
            ["root"] = Path.Combine(dir, ".pz", "runs", "x", "lake"),
            ["base_dir"] = dir,
        });
        Assert.Single((await new IcebergConnector().ValidateAsync(config, CancellationToken.None)).Errors);
    }

    [Fact]
    public async Task Validate_ignores_an_absolute_root_when_no_base_dir_is_injected_and_accepts_roots_outside_pz()
    {
        Assert.Empty((await new IcebergConnector().ValidateAsync(Files(Path.Combine(dir, ".pz", "lake")), CancellationToken.None)).Errors);
        Assert.Empty((await new IcebergConnector().ValidateAsync(Files("lake/warehouse"), CancellationToken.None)).Errors);
        Assert.Empty((await new IcebergConnector().ValidateAsync(Files("s3://b/wh/"), CancellationToken.None)).Errors);
    }

    [Fact]
    public async Task ConnectorConfigValidator_refuses_an_iceberg_root_that_lands_under_pz()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("iceberg", new IcebergConnector());

        var source = new ConnectionDef("lake", "iceberg",
            new Dictionary<string, object?> { ["catalog"] = "files", ["root"] = ".pz/runs/x/lake" },
            [new DatasetDef("raw.events", new Dictionary<string, object?> { ["columns"] = new Dictionary<string, object?> { ["id"] = "bigint" } }, null)],
            "connections.yml");
        var project = new PzProject("proj", "0.1.0", new EngineConfig(), new Dictionary<string, object?>(), [],
            [source], []);

        var errors = await ConnectorConfigValidator.ValidateAsync(project, registry, CancellationToken.None);

        var error = Assert.Single(errors);
        Assert.Equal("connections.yml", error.File);
        Assert.Contains(".pz", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Check_probes_a_rest_endpoint_over_tcp()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;

        var check = await new IcebergConnector().CheckConnectionAsync(Rest(("endpoint", $"http://127.0.0.1:{port}/api")), CancellationToken.None);
        Assert.True(check.Ok);
        Assert.Contains("credentials are verified at run time", check.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Check_reports_an_unreachable_rest_endpoint_as_transient()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop(); // nobody listens there now

        var check = await new IcebergConnector().CheckConnectionAsync(Rest(("endpoint", $"http://127.0.0.1:{port}")), CancellationToken.None);
        Assert.False(check.Ok);
        Assert.StartsWith("transient:", check.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Check_reports_an_invalid_config_as_permanent()
    {
        var check = await new IcebergConnector().CheckConnectionAsync(new ConnectorConfig(new Dictionary<string, object?>()), CancellationToken.None);
        Assert.False(check.Ok);
        Assert.StartsWith("permanent:", check.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Check_verifies_a_local_files_root_directory()
    {
        var ok = await new IcebergConnector().CheckConnectionAsync(Files(), CancellationToken.None);
        Assert.True(ok.Ok);

        var missing = await new IcebergConnector().CheckConnectionAsync(Files(Path.Combine(dir, "missing")), CancellationToken.None);
        Assert.False(missing.Ok);
        Assert.StartsWith("permanent:", missing.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(dir, missing.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Check_has_no_offline_probe_for_object_store_roots_and_aws_catalogs()
    {
        var files = await new IcebergConnector().CheckConnectionAsync(Files("s3://b/wh/"), CancellationToken.None);
        Assert.True(files.Ok);
        Assert.StartsWith("not checked", files.Message, StringComparison.Ordinal);

        var glue = await new IcebergConnector().CheckConnectionAsync(
            new ConnectorConfig(new Dictionary<string, object?> { ["catalog"] = "glue" }), CancellationToken.None);
        Assert.True(glue.Ok);
        Assert.StartsWith("not checked", glue.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PlanReadAsync_and_BeginWriteAsync_refuse_the_universal_tier_with_PZ0312()
    {
        await using var source = await ((ISourceConnector)new IcebergConnector()).OpenAsync(Rest(), CancellationToken.None);
        var readEx = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await source.PlanReadAsync(Spec(), ReadHints.None, CancellationToken.None));
        Assert.StartsWith("PZ0312", readEx.Message, StringComparison.Ordinal);

        await using var sink = await ((ISinkConnector)new IcebergConnector()).OpenAsync(Rest(), CancellationToken.None);
        var writeEx = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await sink.BeginWriteAsync(new OutputSpec("wh", "raw.o", "append", "fail_on_change", new Dictionary<string, object?>()),
                new Apache.Arrow.Schema([], null), CancellationToken.None));
        Assert.StartsWith("PZ0312", writeEx.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task GetSchemaAsync_returns_the_declared_contract_or_refuses()
    {
        await using var source = await ((ISourceConnector)new IcebergConnector()).OpenAsync(Rest(), CancellationToken.None);
        var spec = Spec(options: new Dictionary<string, object?>
        {
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["happened_on"] = "date" },
        });
        var schema = await source.GetSchemaAsync(spec, CancellationToken.None);
        Assert.Collection(schema.Schema.FieldsList,
            f => Assert.Equal(ArrowTypeId.Int64, f.DataType.TypeId),
            f => Assert.Equal(ArrowTypeId.Date32, f.DataType.TypeId));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await source.GetSchemaAsync(Spec(), CancellationToken.None));
        Assert.Contains("columns: contract", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Native_scan_and_copy_share_one_attach_and_time_travel_conflicts_surface_at_probe_time()
    {
        await using var source = await ((ISourceConnector)new IcebergConnector()).OpenAsync(Rest(("token", "t")), CancellationToken.None);
        await using var sink = await ((ISinkConnector)new IcebergConnector()).OpenAsync(Rest(("token", "t")), CancellationToken.None);

        Assert.True(source.TryGetNativeScan(Spec(), out var scan));
        Assert.True(sink.TryGetNativeCopy(new OutputSpec("wh", "raw.o", "replace", "fail_on_change", new Dictionary<string, object?>()), out var copy));
        Assert.Equal(scan!.SetupStatements, copy!.SetupStatements);
        Assert.Equal("iceberg attach", scan.Mechanism);
        Assert.Equal("iceberg overwrite", copy.Mechanism);

        var both = Spec(options: new Dictionary<string, object?> { ["version"] = 1L, ["timestamp"] = "2026-01-01" });
        Assert.Throws<PzConnectorException>(() => { source.TryGetNativeScan(both, out _); });
    }

    [Fact]
    public async Task A_files_read_scans_an_existing_table_directory_and_refuses_a_missing_one()
    {
        Directory.CreateDirectory(Path.Combine(dir, "raw", "events"));
        await using var source = await ((ISourceConnector)new IcebergConnector()).OpenAsync(Files(), CancellationToken.None);

        Assert.True(source.TryGetNativeScan(Spec(), out var scan));
        Assert.Equal("iceberg scan", scan!.Mechanism);
        Assert.StartsWith("iceberg_scan('", scan.SqlFragment, StringComparison.Ordinal);
        Assert.Equal(["install iceberg", "load iceberg"], scan.SetupStatements);

        var ex = Assert.Throws<PzConnectorException>(() => { source.TryGetNativeScan(Spec("raw.missing"), out _); });
        Assert.False(ex.IsTransient);
        Assert.Contains("raw.missing", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(dir, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_files_write_is_refused_as_a_permanent_error()
    {
        await using var sink = await ((ISinkConnector)new IcebergConnector()).OpenAsync(Files(), CancellationToken.None);
        var ex = Assert.Throws<PzConnectorException>(() =>
        {
            sink.TryGetNativeCopy(new OutputSpec("wh", "raw.o", "append", "fail_on_change", new Dictionary<string, object?>()), out _);
        });
        Assert.False(ex.IsTransient);
        Assert.Contains("raw.o", ex.Message, StringComparison.Ordinal);
        Assert.Contains("read-only", ex.Message, StringComparison.Ordinal);
    }
}
