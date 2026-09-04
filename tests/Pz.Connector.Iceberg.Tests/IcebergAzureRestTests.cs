using Pz.Connectors.Abstractions;
using Pz.DuckDb;
using Pz.Engine.Execution;

namespace Pz.Connector.Iceberg.Tests;

/// <summary>Write round-trip against a REAL Iceberg REST catalog whose warehouse is on Azure
/// storage (ADLS Gen2 / Blob). Opt-in: no emulator can host it — Azurite has no DFS endpoint and
/// every Azure-capable catalog writes through the DFS API — so the test skips unless
/// PZ_ICEBERG_AZURE_ENDPOINT is set. DuckDB's azure extension implements the directory and write
/// operations the iceberg extension's insert needs; this is the proof that the whole chain holds
/// for the catalog it is pointed at.</summary>
public sealed class IcebergAzureRestTests : IDisposable
{
    private readonly string dir = Directory.CreateTempSubdirectory("pz-iceberg-azure-rest-").FullName;
    private readonly string ns = Environment.GetEnvironmentVariable("PZ_ICEBERG_AZURE_NAMESPACE") is { Length: > 0 } n ? n : "pz_ci";
    private readonly string table = "t" + Guid.NewGuid().ToString("N")[..8];

    public void Dispose()
    {
        try { Directory.Delete(dir, recursive: true); } catch (IOException) { /* best-effort cleanup */ }
    }

    private static string? Env(string name) => Environment.GetEnvironmentVariable(name) is { Length: > 0 } v ? v : null;

    private static ConnectorConfig Config()
    {
        var values = new Dictionary<string, object?>
        {
            ["catalog"] = "rest",
            ["endpoint"] = Env("PZ_ICEBERG_AZURE_ENDPOINT"),
            ["warehouse"] = Env("PZ_ICEBERG_AZURE_WAREHOUSE"),
            ["storage"] = "azure",
        };
        if (Env("PZ_ICEBERG_AZURE_TOKEN") is { } token)
        {
            values["token"] = token;
        }

        if (Env("PZ_ICEBERG_AZURE_ACCOUNT_NAME") is { } account && Env("PZ_ICEBERG_AZURE_ACCOUNT_KEY") is { } key)
        {
            values["storage_auth"] = "account_key";
            values["storage_account_name"] = account;
            values["storage_account_key"] = key;
        }

        return new(values);
    }

    private static async Task RunSetupAsync(DuckSession duck, IReadOnlyList<string> statements)
    {
        foreach (var setup in statements)
        {
            await NativeSetup.ExecuteSetupAsync(duck, setup, CancellationToken.None);
        }
    }

    [SkippableFact]
    public async Task Replace_then_append_then_read_round_trips_on_azure_storage()
    {
        Skip.If(Env("PZ_ICEBERG_AZURE_ENDPOINT") is null, "PZ_ICEBERG_AZURE_ENDPOINT is not set");
        Skip.If(Env("PZ_ICEBERG_AZURE_WAREHOUSE") is null, "PZ_ICEBERG_AZURE_WAREHOUSE is not set");
        await using var duck = DuckSession.Open(Path.Combine(dir, "scratch.duckdb"));
        var connector = new IcebergConnector();

        foreach (var (mode, rows) in new[] { ("replace", "select 1 as id union all select 2"), ("append", "select 3 as id") })
        {
            await using var sink = await ((ISinkConnector)connector).OpenAsync(Config(), CancellationToken.None);
            var spec = new OutputSpec("wh", $"{ns}.{table}", mode, "fail_on_change", new Dictionary<string, object?>()) { Keys = [] };
            Assert.True(sink.TryGetNativeCopy(spec, out var copy));
            await RunSetupAsync(duck, copy!.SetupStatements);
            var staging = "stage_" + Guid.NewGuid().ToString("N");
            await duck.ExecuteAsync($"create table {staging} as {rows}");
            await duck.ExecuteAsync(copy.CopySql.Replace("{{source}}", staging, StringComparison.Ordinal));
        }

        await using var source = await ((ISourceConnector)connector).OpenAsync(Config(), CancellationToken.None);
        Assert.True(source.TryGetNativeScan(new DatasetSpec("wh", $"{ns}.{table}", new Dictionary<string, object?>()), out var scan));
        await RunSetupAsync(duck, scan!.SetupStatements);
        await duck.ExecuteAsync($"create table landed as select * from {scan.SqlFragment}");
        Assert.Equal(3, await duck.ScalarAsync<long>("select count(*) from landed"));

        await duck.ExecuteAsync($"drop table {IcebergSql.Alias("wh")}.\"{ns}\".\"{table}\"");
    }
}
