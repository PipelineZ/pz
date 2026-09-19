using Pz.Connectors.Abstractions;

namespace Pz.Connector.LocalFiles.Tests;

/// <summary>The connection says where the lake is, the entity says which dataset: the connection's
/// <c>root:</c> must actually be read, since every validation tier accepts it and a project setting it
/// would otherwise be silently ignored.
/// </summary>
public sealed class RootAndDefaultPathTests : IDisposable
{
    private readonly string _work =
        Path.Combine(Path.GetTempPath(), "pz-root-tests", Guid.NewGuid().ToString("N"));

    public RootAndDefaultPathTests() => Directory.CreateDirectory(_work);

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private static ConnectorConfig Config(string baseDir, string? root = null)
    {
        var values = new Dictionary<string, object?> { ["base_dir"] = baseDir };
        if (root is not null)
        {
            values["root"] = root;
        }

        return new ConnectorConfig(values);
    }

    private string WriteCsv(params string[] segments)
    {
        var path = Path.Combine([_work, .. segments]);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        File.WriteAllText(path, "id,email\n1,a@example.com\n");
        return path;
    }

    private static DatasetSpec Spec(string entity, string? path = null)
    {
        var options = new Dictionary<string, object?>
        {
            ["format"] = "csv",
            ["columns"] = (IReadOnlyDictionary<string, string>)new Dictionary<string, string>
            {
                ["id"] = "bigint",
                ["email"] = "varchar",
            },
        };
        if (path is not null)
        {
            options["path"] = path;
        }

        return new DatasetSpec("files", entity, options);
    }

    private static async Task<string> FirstColumnAsync(ConnectorConfig config, DatasetSpec spec)
    {
        await using var source = await ((ISourceConnector)new LocalFilesConnector())
            .OpenAsync(config, CancellationToken.None);
        var schema = await source.GetSchemaAsync(spec, CancellationToken.None);
        return schema.Schema.FieldsList[0].Name;
    }

    [Fact]
    public async Task A_relative_root_resolves_under_the_project_directory()
    {
        WriteCsv("lake", "orders.csv");

        Assert.Equal("id", await FirstColumnAsync(Config(_work, root: "lake"), Spec("orders")));
    }

    [Fact]
    public async Task An_absolute_root_wins_over_the_project_directory()
    {
        var elsewhere = Path.Combine(_work, "elsewhere");
        WriteCsv("elsewhere", "orders.csv");

        Assert.Equal("id",
            await FirstColumnAsync(Config(Path.Combine(_work, "unused"), root: elsewhere), Spec("orders")));
    }

    [Fact]
    public async Task An_entity_with_no_path_reads_entity_dot_format()
    {
        WriteCsv("customers.csv");

        Assert.Equal("id", await FirstColumnAsync(Config(_work), Spec("customers")));
    }

    [Fact]
    public async Task An_explicit_path_still_resolves_under_root()
    {
        WriteCsv("lake", "sub", "o.csv");

        Assert.Equal("id",
            await FirstColumnAsync(Config(_work, root: "lake"), Spec("orders", path: "sub/o.csv")));
    }

    [Fact]
    public async Task An_absolute_path_still_ignores_root()
    {
        var absolute = WriteCsv("outside", "o.csv");

        Assert.Equal("id",
            await FirstColumnAsync(Config(_work, root: "lake"), Spec("orders", path: absolute)));
    }

    [Fact]
    public async Task A_relative_path_escaping_root_via_csv_source_is_refused()
    {
        WriteCsv("outside", "o.csv");

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await FirstColumnAsync(Config(_work, root: "lake"), Spec("orders", path: "../outside/o.csv")));

        Assert.False(ex.IsTransient);
        Assert.StartsWith("PZ0365: connection 'files': dataset 'orders' resolves outside 'root:'", ex.Message,
            StringComparison.Ordinal);
        // Never echo the attempted value -- root:/path: are locations, not credentials, but the same
        // "name the option, not the value" hygiene applies.
        Assert.DoesNotContain(_work, ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("o.csv", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_relative_path_escaping_root_via_parquet_source_is_refused()
    {
        var options = new Dictionary<string, object?> { ["format"] = "parquet", ["path"] = "../outside/o.parquet" };
        var spec = new DatasetSpec("files", "orders", options);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await FirstColumnAsync(Config(_work, root: "lake"), spec));

        Assert.StartsWith("PZ0365: connection 'files': dataset 'orders' resolves outside 'root:'", ex.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_relative_path_escaping_root_via_native_only_source_is_refused()
    {
        var options = new Dictionary<string, object?> { ["format"] = "json", ["path"] = "../outside/o.json" };
        var spec = new DatasetSpec("files", "orders", options);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await FirstColumnAsync(Config(_work, root: "lake"), spec));

        Assert.StartsWith("PZ0365: connection 'files': dataset 'orders' resolves outside 'root:'", ex.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_relative_path_escaping_root_via_sink_is_refused()
    {
        await using var sink = await ((ISinkConnector)new LocalFilesConnector())
            .OpenAsync(Config(_work, root: "lake"), CancellationToken.None);
        var spec = new OutputSpec("lake", "curated", "replace", "fail_on_change",
            new Dictionary<string, object?> { ["format"] = "csv", ["path"] = "../outside" });

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await sink.BeginWriteAsync(spec, IdSchema, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.StartsWith("PZ0365: connection 'lake': output 'curated' resolves outside 'root:'", ex.Message,
            StringComparison.Ordinal);
    }

    /// <summary>A sibling directory whose name merely shares the root's name as a TEXT prefix
    /// ("lake2" against root "lake") must not be mistaken for "inside" -- a naive
    /// <c>fullCombined.StartsWith(fullRoot)</c> check without a trailing separator would wrongly let
    /// this one through, since "…/lake2/o.csv" does start with the raw string "…/lake".</summary>
    [Fact]
    public async Task A_path_escaping_into_a_root_name_prefixed_sibling_is_still_refused()
    {
        WriteCsv("lake2", "o.csv");

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await FirstColumnAsync(Config(_work, root: "lake"), Spec("orders", path: "../lake2/o.csv")));

        Assert.StartsWith("PZ0365: connection 'files': dataset 'orders' resolves outside 'root:'", ex.Message,
            StringComparison.Ordinal);
    }

    /// <summary>A relative path that stays inside root despite using ".." internally (it walks down and
    /// back up without ever leaving) is NOT refused -- the check is about where the path LANDS, not
    /// whether it contains the literal characters "..".</summary>
    [Fact]
    public async Task A_relative_path_using_dotdot_internally_but_staying_within_root_is_allowed()
    {
        WriteCsv("lake", "sub", "o.csv");

        Assert.Equal("id", await FirstColumnAsync(
            Config(_work, root: "lake"), Spec("orders", path: "sub/../sub/o.csv")));
    }

    [Fact]
    public async Task A_sink_with_no_path_writes_under_the_entity_name()
    {
        await using var sink = await ((ISinkConnector)new LocalFilesConnector())
            .OpenAsync(Config(_work, root: "lake"), CancellationToken.None);
        var spec = new OutputSpec("lake", "curated", "replace", "fail_on_change",
            new Dictionary<string, object?> { ["format"] = "csv" });

        await using (var session = await sink.BeginWriteAsync(spec, IdSchema, CancellationToken.None))
        {
            await session.CommitAsync(CancellationToken.None);
        }

        Assert.True(Directory.Exists(Path.Combine(_work, "lake", "curated")));
    }

    private static Apache.Arrow.Schema IdSchema =>
        new([new Apache.Arrow.Field("id", Apache.Arrow.Types.Int64Type.Default, nullable: false)], null);
}
