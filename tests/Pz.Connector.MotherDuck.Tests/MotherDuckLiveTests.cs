using Pz.Connectors.Abstractions;
using Pz.DuckDb;
using Pz.Engine.Execution;

namespace Pz.Connector.MotherDuck.Tests;

/// <summary>The only proof of the documentation-derived MotherDuck behaviors (alias-less attach,
/// the session token setting, MERGE and CREATE OR REPLACE on MotherDuck tables). Runs ONLY when
/// PZ_MOTHERDUCK_TOKEN and PZ_MOTHERDUCK_DATABASE are set — never in CI. Writes and drops tables
/// prefixed <c>pz_live_</c> in that database. Every setup statement routes through one
/// <see cref="NativeSetupLedger"/> per DuckSession, modeling the engine's per-run once-only
/// re-issue rule (the motherduck extension refuses a repeat <c>set motherduck_token</c> after its
/// first attach).</summary>
public sealed class MotherDuckLiveTests : IDisposable
{
    private static readonly string? Token = Environment.GetEnvironmentVariable("PZ_MOTHERDUCK_TOKEN");
    private static readonly string? Database = Environment.GetEnvironmentVariable("PZ_MOTHERDUCK_DATABASE");

    private readonly string dir = Directory.CreateTempSubdirectory("pz-motherduck-live-").FullName;
    private readonly string suffix = Guid.NewGuid().ToString("N")[..8];

    public void Dispose()
    {
        try { Directory.Delete(dir, recursive: true); } catch (IOException) { /* best-effort */ }
    }

    private static void SkipUnlessLive() =>
        Skip.If(Token is null || Database is null, "PZ_MOTHERDUCK_TOKEN / PZ_MOTHERDUCK_DATABASE not set");

    private static ConnectorConfig Config() =>
        new(new Dictionary<string, object?> { ["database"] = Database, ["token"] = Token });

    private async Task WriteAsync(DuckSession duck, NativeSetupLedger ledger, string entity, string selectSql, string mode = "replace", IReadOnlyList<string>? keys = null)
    {
        await using var sink = await ((ISinkConnector)new MotherDuckConnector()).OpenAsync(Config(), CancellationToken.None);
        Assert.True(sink.TryGetNativeCopy(new OutputSpec("wh", entity, mode, "fail_on_change", new Dictionary<string, object?>()) { Keys = keys ?? [] }, out var copy));
        foreach (var setup in copy!.SetupStatements)
        {
            await ledger.ExecuteOnceAsync(setup, CancellationToken.None);
        }

        var staging = "stage_" + Guid.NewGuid().ToString("N");
        await duck.ExecuteAsync($"create table {staging} as {selectSql}");
        await duck.ExecuteAsync(copy.CopySql.Replace("{{source}}", staging, StringComparison.Ordinal));
    }

    private async Task<string> ReadAsync(DuckSession duck, NativeSetupLedger ledger, DatasetSpec spec)
    {
        await using var source = await ((ISourceConnector)new MotherDuckConnector()).OpenAsync(Config(), CancellationToken.None);
        Assert.True(source.TryGetNativeScan(spec, out var scan));
        foreach (var setup in scan!.SetupStatements)
        {
            await ledger.ExecuteOnceAsync(setup, CancellationToken.None);
        }

        var landed = "landed_" + Guid.NewGuid().ToString("N");
        await duck.ExecuteAsync($"create table {landed} as select * from {scan.SqlFragment}");
        return landed;
    }

    [SkippableFact]
    public async Task Append_replace_merge_and_windowed_reads_round_trip_through_motherduck()
    {
        SkipUnlessLive();
        await using var duck = DuckSession.Open(Path.Combine(dir, "client.duckdb"));
        var ledger = new NativeSetupLedger(duck);
        var table = $"pz_live_{suffix}";
        try
        {
            await WriteAsync(duck, ledger, table, "(select 1 as id, 'a' as name)", mode: "append");
            await WriteAsync(duck, ledger, table, "(select 2 as id, 'b' as name union all select 3, 'c')", mode: "append");
            var all = await ReadAsync(duck, ledger, new DatasetSpec("wh", table, new Dictionary<string, object?>()));
            Assert.Equal(3, await duck.ScalarAsync<long>($"select count(*) from {all}"));

            var windowed = await ReadAsync(duck, ledger, new DatasetSpec("wh", table, new Dictionary<string, object?>())
            {
                WatermarkCursor = "id", WatermarkValue = "1", WatermarkUpperBound = "2",
            });
            Assert.Equal(2L, await duck.ScalarAsync<long>($"select id from {windowed}"));

            await WriteAsync(duck, ledger, table, "(select 2 as id, 'B' as name union all select 4, 'd')", mode: "merge", keys: ["id"]);
            var merged = await ReadAsync(duck, ledger, new DatasetSpec("wh", table, new Dictionary<string, object?>()));
            Assert.Equal(4, await duck.ScalarAsync<long>($"select count(*) from {merged}"));
            Assert.Equal("B", await duck.ScalarAsync<string>($"select name from {merged} where id = 2"));

            // Same batch replayed: MERGE is idempotent (update-on-match, insert-on-no-match), so
            // repeating it leaves the row count and the matched row's value unchanged.
            await WriteAsync(duck, ledger, table, "(select 2 as id, 'B' as name union all select 4, 'd')", mode: "merge", keys: ["id"]);
            var mergedAgain = await ReadAsync(duck, ledger, new DatasetSpec("wh", table, new Dictionary<string, object?>()));
            Assert.Equal(4, await duck.ScalarAsync<long>($"select count(*) from {mergedAgain}"));
            Assert.Equal("B", await duck.ScalarAsync<string>($"select name from {mergedAgain} where id = 2"));

            // An empty source batch matches nothing and inserts nothing: the target is untouched.
            await WriteAsync(duck, ledger, table, "(select 1 as id, 'x' as name where false)", mode: "merge", keys: ["id"]);
            var mergedEmpty = await ReadAsync(duck, ledger, new DatasetSpec("wh", table, new Dictionary<string, object?>()));
            Assert.Equal(4, await duck.ScalarAsync<long>($"select count(*) from {mergedEmpty}"));

            // Duplicate keys within one source batch: unlike quack's merge-by-replace rewrite (which
            // dedups the source with row_number() before unioning against the target), MotherDuck's
            // real MERGE INTO matches each source row against the pre-statement target snapshot. id 5
            // has no existing target row, so BOTH source rows independently take the "not matched"
            // branch and both insert — callers should supply batches with unique keys. This is
            // observed behaviour of the live extension, not a guarantee this sink makes or enforces;
            // the connector's plain generated SQL is unchanged since the docs make no dedup promise
            // either.
            await WriteAsync(duck, ledger, table, "(select 5 as id, 'p' as name union all select 5, 'q')", mode: "merge", keys: ["id"]);
            var mergedDupes = await ReadAsync(duck, ledger, new DatasetSpec("wh", table, new Dictionary<string, object?>()));
            Assert.Equal(2, await duck.ScalarAsync<long>($"select count(*) from {mergedDupes} where id = 5"));

            await WriteAsync(duck, ledger, table, "(select 9 as id, 'z' as name)");
            var replaced = await ReadAsync(duck, ledger, new DatasetSpec("wh", table, new Dictionary<string, object?>()));
            Assert.Equal(1, await duck.ScalarAsync<long>($"select count(*) from {replaced}"));
        }
        finally
        {
            await duck.ExecuteAsync($"drop table if exists {MotherDuckSql.QualifiedTable(MotherDuckSql.Database(Config()), table)}");
        }
    }

    [SkippableFact]
    public async Task A_wrong_token_fails_without_echoing_it()
    {
        SkipUnlessLive();
        await using var duck = DuckSession.Open(Path.Combine(dir, "client2.duckdb"));
        var ledger = new NativeSetupLedger(duck);
        var config = new ConnectorConfig(new Dictionary<string, object?> { ["database"] = Database, ["token"] = "WRONG-TOKEN" });
        await using var source = await ((ISourceConnector)new MotherDuckConnector()).OpenAsync(config, CancellationToken.None);
        Assert.True(source.TryGetNativeScan(new DatasetSpec("wh", "x", new Dictionary<string, object?>()), out var scan));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            foreach (var setup in scan!.SetupStatements)
            {
                await ledger.ExecuteOnceAsync(setup, CancellationToken.None);
            }
        });
        Assert.Contains("PZ0311", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("WRONG-TOKEN", ex.Message, StringComparison.Ordinal);
        Assert.False(ex.IsTransient);
    }
}
