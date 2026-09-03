using Pz.Connectors.Abstractions;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.TestSupport;

namespace Pz.Connector.Quack.Tests;

/// <summary>Real-server proof of the data plane: a second in-process DuckDB session serves over
/// Quack; the connector attaches through the scoped secret, writes with every mode, reads with
/// watermark bounds and a contract-pruned projection, and a wrong token is a clean failure that
/// never echoes any token. Gated on PZ_TESTS_OFFLINE (extension install).</summary>
public sealed class QuackNativeEndToEndTests : IAsyncLifetime
{
    private readonly string dir = Directory.CreateTempSubdirectory("pz-quack-e2e-").FullName;
    private QuackTestServer? server;

    public async Task InitializeAsync()
    {
        if (Environment.GetEnvironmentVariable("PZ_TESTS_OFFLINE") != "1")
        {
            server = await QuackTestServer.StartAsync(dir);
        }
    }

    public async Task DisposeAsync()
    {
        if (server is not null)
        {
            await server.DisposeAsync();
        }

        try { Directory.Delete(dir, recursive: true); } catch (IOException) { /* best-effort */ }
    }

    private ConnectorConfig Config(string? token = null) =>
        new(new Dictionary<string, object?> { ["uri"] = server!.Uri, ["token"] = token ?? server!.Token });

    private async Task WriteAsync(DuckSession duck, string entity, string selectSql, string mode = "replace", IReadOnlyList<string>? keys = null)
    {
        await using var sink = await ((ISinkConnector)new QuackConnector()).OpenAsync(Config(), CancellationToken.None);
        Assert.True(sink.TryGetNativeCopy(new OutputSpec("wh", entity, mode, "fail_on_change", new Dictionary<string, object?>()) { Keys = keys ?? [] }, out var copy));
        foreach (var setup in copy!.SetupStatements)
        {
            await NativeSetup.ExecuteSetupAsync(duck, setup, CancellationToken.None);
        }

        var staging = "stage_" + Guid.NewGuid().ToString("N");
        await duck.ExecuteAsync($"create table {staging} as {selectSql}");
        await duck.ExecuteAsync(copy.CopySql.Replace("{{source}}", staging, StringComparison.Ordinal));
    }

    private async Task<string> ReadAsync(DuckSession duck, DatasetSpec spec, string? token = null)
    {
        await using var source = await ((ISourceConnector)new QuackConnector()).OpenAsync(Config(token), CancellationToken.None);
        Assert.True(source.TryGetNativeScan(spec, out var scan));
        foreach (var setup in scan!.SetupStatements)
        {
            await NativeSetup.ExecuteSetupAsync(duck, setup, CancellationToken.None);
        }

        var landed = "landed_" + Guid.NewGuid().ToString("N");
        await duck.ExecuteAsync($"create table {landed} as select * from {scan.SqlFragment}");
        return landed;
    }

    private static DatasetSpec Spec(string entity) => new("wh", entity, new Dictionary<string, object?>());

    [SkippableFact]
    public async Task Append_replace_merge_and_windowed_reads_run_on_the_server()
    {
        DockerFacts.SkipIfOffline();
        await using var duck = DuckSession.Open(Path.Combine(dir, "client.duckdb"));

        await WriteAsync(duck, "log", "(select 1 as id)", mode: "append");
        await WriteAsync(duck, "log", "(select 2 as id union all select 3)", mode: "append");
        Assert.Equal(3, await duck.ScalarAsync<long>($"select count(*) from {await ReadAsync(duck, Spec("log"))}"));

        var windowed = await ReadAsync(duck, Spec("log") with { WatermarkCursor = "id", WatermarkValue = "1", WatermarkUpperBound = "2" });
        Assert.Equal(2L, await duck.ScalarAsync<long>($"select id from {windowed}"));

        var pruned = await ReadAsync(duck, new DatasetSpec("wh", "log", new Dictionary<string, object?>
        {
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint" },
        }));
        Assert.Equal(1, await duck.ScalarAsync<long>($"select count(*) from information_schema.columns where table_name = '{pruned}'"));

        await WriteAsync(duck, "snap", "(select 1 as id union all select 2)");
        await WriteAsync(duck, "snap", "(select 9 as id)");
        Assert.Equal(9L, await duck.ScalarAsync<long>($"select id from {await ReadAsync(duck, Spec("snap"))}"));

        await WriteAsync(duck, "dim", "(select 1 as id, 'a' as name union all select 2, 'b')", mode: "merge", keys: ["id"]);
        await WriteAsync(duck, "dim", "(select 2 as id, 'B' as name union all select 3, 'c')", mode: "merge", keys: ["id"]);
        var dim = await ReadAsync(duck, Spec("dim"));
        Assert.Equal(3, await duck.ScalarAsync<long>($"select count(*) from {dim}"));
        Assert.Equal("B", await duck.ScalarAsync<string>($"select name from {dim} where id = 2"));

        // merge-by-replace's "not exists" branch selects unmatched target rows whole, so a later
        // batch that omits a column entirely still leaves untouched rows carrying it: id 1 never
        // appears in the second batch below, so its region survives even though that batch's SELECT
        // has no region column at all (UNION ALL BY NAME fills the gap with the matched side's null,
        // not the target's prior value — id 2 IS in the second batch, so its row comes wholesale from
        // the source and region goes null, same as a plain replace would do for that row).
        await WriteAsync(duck, "dim2", "(select 1 as id, 'a' as name, 'east' as region union all select 2, 'b', 'west')", mode: "merge", keys: ["id"]);
        await WriteAsync(duck, "dim2", "(select 2 as id, 'B' as name union all select 3 as id, 'c' as name)", mode: "merge", keys: ["id"]);
        var dim2 = await ReadAsync(duck, Spec("dim2"));
        Assert.Equal("east", await duck.ScalarAsync<string>($"select region from {dim2} where id = 1"));
        Assert.True(await duck.ScalarAsync<bool>($"select region is null from {dim2} where id = 2"));
    }

    [SkippableFact]
    public async Task A_wrong_token_fails_permanently_without_echoing_any_token()
    {
        DockerFacts.SkipIfOffline();
        await using var duck = DuckSession.Open(Path.Combine(dir, "client2.duckdb"));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => ReadAsync(duck, Spec("log"), token: "WRONG-TOKEN"));
        Assert.Contains("PZ0311", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("WRONG-TOKEN", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(server!.Token, ex.Message, StringComparison.Ordinal);
        Assert.False(ex.IsTransient);
    }
}
