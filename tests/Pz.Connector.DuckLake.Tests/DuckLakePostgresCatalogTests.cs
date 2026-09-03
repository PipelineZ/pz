using Pz.Connectors.Abstractions;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.TestSupport;

namespace Pz.Connector.DuckLake.Tests;

/// <summary>The lake's catalog in Postgres, credentials riding the postgres secret referenced from
/// the ducklake secret (metadata_path is empty by construction): write, read, and a wrong password
/// fails without echoing it. SKIPs without docker; also gated on PZ_TESTS_OFFLINE.</summary>
[Collection("ducklake-postgres")]
public sealed class DuckLakePostgresCatalogTests(DuckLakePostgresCatalogFixture pg) : IDisposable
{
    private readonly string dir = Directory.CreateTempSubdirectory("pz-ducklake-pg-").FullName;

    public void Dispose()
    {
        try { Directory.Delete(dir, recursive: true); } catch (IOException) { /* best-effort */ }
    }

    private ConnectorConfig Config(string? password = null) => new(new Dictionary<string, object?>
    {
        ["catalog"] = "postgres",
        ["host"] = pg.Host, ["port"] = (long)pg.Port, ["database"] = pg.Database,
        ["user"] = pg.User, ["password"] = password ?? pg.Password,
        ["data_path"] = Path.Combine(dir, "data"),
    });

    private async Task RunSetupAsync(DuckSession duck, IReadOnlyList<string> statements)
    {
        foreach (var setup in statements)
        {
            await NativeSetup.ExecuteSetupAsync(duck, setup, CancellationToken.None);
        }
    }

    [SkippableFact]
    public async Task Write_then_read_through_the_postgres_catalog()
    {
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();
        await using var duck = DuckSession.Open(Path.Combine(dir, "client.duckdb"));

        await using var sink = await ((ISinkConnector)new DuckLakeConnector()).OpenAsync(Config(), CancellationToken.None);
        Assert.True(sink.TryGetNativeCopy(new OutputSpec("wh", "events", "replace", "fail_on_change", new Dictionary<string, object?>()), out var copy));
        await RunSetupAsync(duck, copy!.SetupStatements);
        await duck.ExecuteAsync("create table stage as select 1 as id, 'a' as name union all select 2, 'b'");
        await duck.ExecuteAsync(copy.CopySql.Replace("{{source}}", "stage", StringComparison.Ordinal));

        await using var source = await ((ISourceConnector)new DuckLakeConnector()).OpenAsync(Config(), CancellationToken.None);
        Assert.True(source.TryGetNativeScan(new DatasetSpec("wh", "events", new Dictionary<string, object?>()) { WatermarkCursor = "id", WatermarkValue = "1" }, out var scan));
        await RunSetupAsync(duck, scan!.SetupStatements);
        await duck.ExecuteAsync($"create table landed as select * from {scan.SqlFragment}");

        Assert.Equal(1, await duck.ScalarAsync<long>("select count(*) from landed"));
        Assert.Equal("b", await duck.ScalarAsync<string>("select name from landed"));
    }

    [SkippableFact]
    public async Task A_wrong_password_fails_without_echoing_it()
    {
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();
        await using var duck = DuckSession.Open(Path.Combine(dir, "client2.duckdb"));

        await using var source = await ((ISourceConnector)new DuckLakeConnector()).OpenAsync(Config("WRONG-PASSWORD"), CancellationToken.None);
        Assert.True(source.TryGetNativeScan(new DatasetSpec("wh", "events", new Dictionary<string, object?>()), out var scan));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => RunSetupAsync(duck, scan!.SetupStatements));
        Assert.Contains("PZ0311", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("WRONG-PASSWORD", ex.Message, StringComparison.Ordinal);
        Assert.False(ex.IsTransient);
    }
}
