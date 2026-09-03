using Pz.Connectors.Abstractions;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.TestSupport;

namespace Pz.Connector.DuckLake.Tests;

/// <summary>The lake's catalog on a DuckDB server reached over Quack: attach through the scoped
/// secret, write, merge, read, and a wrong token is a clean permanent failure that never echoes the
/// token. Server path: in-process, via <see cref="QuackTestServer"/> (a second <see cref="DuckSession"/>
/// running <c>quack_serve</c> on a background thread) — <c>quack_serve</c> returned immediately and a
/// client attach over the loopback port succeeded, so the CLI-spawn fallback was not needed. Gated on
/// PZ_TESTS_OFFLINE (extension installs).</summary>
public sealed class DuckLakeQuackCatalogTests : IAsyncLifetime
{
    private readonly string dir = Directory.CreateTempSubdirectory("pz-ducklake-quack-").FullName;
    private QuackTestServer? server;

    public async Task InitializeAsync()
    {
        if (Environment.GetEnvironmentVariable("PZ_TESTS_OFFLINE") == "1")
        {
            return;
        }

        server = await QuackTestServer.StartAsync(dir);
    }

    public async Task DisposeAsync()
    {
        if (server is not null)
        {
            await server.DisposeAsync();
        }

        try { Directory.Delete(dir, recursive: true); } catch (IOException) { /* best-effort */ }
    }

    private ConnectorConfig Config(string? token = null) => new(new Dictionary<string, object?>
    {
        ["catalog"] = "quack",
        ["uri"] = server!.Uri,
        ["token"] = token ?? server!.Token,
        ["data_path"] = Path.Combine(dir, "data"),
    });

    private async Task WriteAsync(DuckSession duck, string entity, string selectSql, string mode = "replace", IReadOnlyList<string>? keys = null)
    {
        await using var sink = await ((ISinkConnector)new DuckLakeConnector()).OpenAsync(Config(), CancellationToken.None);
        Assert.True(sink.TryGetNativeCopy(new OutputSpec("wh", entity, mode, "fail_on_change", new Dictionary<string, object?>()) { Keys = keys ?? [] }, out var copy));
        foreach (var setup in copy!.SetupStatements)
        {
            await NativeSetup.ExecuteSetupAsync(duck, setup, CancellationToken.None);
        }

        var staging = "stage_" + Guid.NewGuid().ToString("N");
        await duck.ExecuteAsync($"create table {staging} as {selectSql}");
        await duck.ExecuteAsync(copy.CopySql.Replace("{{source}}", staging, StringComparison.Ordinal));
    }

    private async Task<string> ReadAsync(DuckSession duck, string entity, string? token = null)
    {
        await using var source = await ((ISourceConnector)new DuckLakeConnector()).OpenAsync(Config(token), CancellationToken.None);
        Assert.True(source.TryGetNativeScan(new DatasetSpec("wh", entity, new Dictionary<string, object?>()), out var scan));
        foreach (var setup in scan!.SetupStatements)
        {
            await NativeSetup.ExecuteSetupAsync(duck, setup, CancellationToken.None);
        }

        var landed = "landed_" + Guid.NewGuid().ToString("N");
        await duck.ExecuteAsync($"create table {landed} as select * from {scan.SqlFragment}");
        return landed;
    }

    [SkippableFact]
    public async Task Write_merge_and_read_through_the_quack_catalog()
    {
        DockerFacts.SkipIfOffline();
        await using var duck = DuckSession.Open(Path.Combine(dir, "client.duckdb"));

        await WriteAsync(duck, "events", "(select 1 as id, 'a' as name union all select 2, 'b')", mode: "merge", keys: ["id"]);
        await WriteAsync(duck, "events", "(select 2 as id, 'B' as name union all select 3, 'c')", mode: "merge", keys: ["id"]);

        var landed = await ReadAsync(duck, "events");
        Assert.Equal(3, await duck.ScalarAsync<long>($"select count(*) from {landed}"));
        Assert.Equal("B", await duck.ScalarAsync<string>($"select name from {landed} where id = 2"));
    }

    [SkippableFact]
    public async Task A_wrong_token_is_a_permanent_failure_that_never_echoes_the_token()
    {
        DockerFacts.SkipIfOffline();
        await using var duck = DuckSession.Open(Path.Combine(dir, "client2.duckdb"));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => ReadAsync(duck, "events", token: "WRONG-TOKEN"));
        Assert.Contains("PZ0311", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("WRONG-TOKEN", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(server!.Token, ex.Message, StringComparison.Ordinal);
        Assert.False(ex.IsTransient);
    }
}
