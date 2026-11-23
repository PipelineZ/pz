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

    private ConnectorConfig Config(string? token = null, string? uri = null) =>
        new(new Dictionary<string, object?> { ["uri"] = uri ?? server!.Uri, ["token"] = token ?? server!.Token });

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

    private async Task<string> ReadAsync(DuckSession duck, DatasetSpec spec, string? token = null, string? uri = null)
    {
        await using var source = await ((ISourceConnector)new QuackConnector()).OpenAsync(Config(token, uri), CancellationToken.None);
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

        // Two source rows sharing a key within one batch collapse to one connector-determined
        // survivor (QuackSql.TryCopySql dedups with qualify row_number() on the way into the scratch
        // table) — the sink contract's Absorb behaviour.
        await WriteAsync(duck, "dupes", "(select 1 as id, 'a' as name union all select 1, 'b' union all select 2, 'c')", mode: "merge", keys: ["id"]);
        var dupes = await ReadAsync(duck, Spec("dupes"));
        Assert.Equal(2, await duck.ScalarAsync<long>($"select count(*) from {dupes}"));
        Assert.Equal(1, await duck.ScalarAsync<long>($"select count(*) from {dupes} where id = 1"));

        // An empty batch merges nothing: every target row survives untouched.
        await WriteAsync(duck, "dim", "(select 1 as id, 'a' as name where false)", mode: "merge", keys: ["id"]);
        var dimAfterEmptyMerge = await ReadAsync(duck, Spec("dim"));
        Assert.Equal(3, await duck.ScalarAsync<long>($"select count(*) from {dimAfterEmptyMerge}"));
        Assert.Equal("B", await duck.ScalarAsync<string>($"select name from {dimAfterEmptyMerge} where id = 2"));

        // The merge's scratch table never outlives the transaction that created it.
        Assert.Equal(0L, await ServerScratchTablesAsync());
    }

    private Task<long> ServerScratchTablesAsync() =>
        server!.Session.ScalarAsync<long>("select count(*) from duckdb_tables() where table_name like 'pz_merge_%'");

    // quack_serve serves the server session's in-memory catalog, so server-side setup is qualified
    // with `memory.`.

    /// <summary>The merge runs on the server against the target itself, so the target keeps its
    /// primary key and NOT NULL constraints, which the server goes on enforcing.</summary>
    [SkippableFact]
    public async Task Merge_keeps_the_targets_constraints()
    {
        DockerFacts.SkipIfOffline();
        await server!.Session.ExecuteAsync("create table memory.keyed (id integer primary key, name varchar not null)");
        await server.Session.ExecuteAsync("insert into memory.keyed values (1, 'a'), (2, 'b')");
        await using var duck = DuckSession.Open(Path.Combine(dir, "client-keyed.duckdb"));

        await WriteAsync(duck, "keyed", "(select 2 as id, 'B' as name union all select 3, 'c')", mode: "merge", keys: ["id"]);

        Assert.Equal("1a,2B,3c", await server.Session.ScalarAsync<string>(
            "select string_agg(id || name, ',' order by id) from memory.keyed"));
        Assert.Equal("NOT NULL,PRIMARY KEY", await server.Session.ScalarAsync<string>(
            "select string_agg(distinct constraint_type, ',' order by constraint_type) from duckdb_constraints() where table_name = 'keyed'"));
        Assert.Equal(0L, await ServerScratchTablesAsync());
    }

    /// <summary>A merge that fails on the server (here a batch violating the target's NOT NULL) rolls
    /// back whole: the target is unchanged and no scratch table is left behind.</summary>
    [SkippableFact]
    public async Task A_failed_merge_leaves_the_target_and_server_unchanged()
    {
        DockerFacts.SkipIfOffline();
        await server!.Session.ExecuteAsync("create table memory.strict (id integer, name varchar not null)");
        await server.Session.ExecuteAsync("insert into memory.strict values (1, 'a')");
        await using var duck = DuckSession.Open(Path.Combine(dir, "client-strict.duckdb"));

        await Assert.ThrowsAnyAsync<Exception>(() => WriteAsync(duck, "strict",
            "(select 1 as id, null::varchar as name union all select 2, 'b')", mode: "merge", keys: ["id"]));
        await duck.ExecuteAsync("rollback"); // the engine's own recovery after a failed native copy

        Assert.Equal("1a", await server.Session.ScalarAsync<string>(
            "select string_agg(id || name, ',' order by id) from memory.strict"));
        Assert.Equal(0L, await ServerScratchTablesAsync());
    }

    /// <summary>The merge writes every target column, so a batch that omits one fails rather than
    /// silently nulling it on matched rows.</summary>
    [SkippableFact]
    public async Task A_batch_missing_a_target_column_fails_the_merge()
    {
        DockerFacts.SkipIfOffline();
        await using var duck = DuckSession.Open(Path.Combine(dir, "client-cols.duckdb"));
        await WriteAsync(duck, "wide", "(select 1 as id, 'a' as name, 'east' as region)", mode: "merge", keys: ["id"]);

        var ex = await Assert.ThrowsAnyAsync<Exception>(() =>
            WriteAsync(duck, "wide", "(select 1 as id, 'A' as name)", mode: "merge", keys: ["id"]));
        Assert.Contains("3 columns but 2 values", ex.Message, StringComparison.Ordinal);
        await duck.ExecuteAsync("rollback");

        Assert.Equal("east", await server!.Session.ScalarAsync<string>("select region from memory.wide where id = 1"));
        Assert.Equal(0L, await ServerScratchTablesAsync());
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

    // A bracketed IPv6 literal goes through the whole chain: parser, canonical secret scope, attach.
    [SkippableFact]
    public async Task A_bracketed_ipv6_uri_reaches_a_server_listening_on_ipv6_loopback()
    {
        DockerFacts.SkipIfOffline();
        Skip.IfNot(System.Net.Sockets.Socket.OSSupportsIPv6, "no IPv6 loopback on this host");
        await using var v6 = await QuackTestServer.StartAsync(Directory.CreateTempSubdirectory("pz-quack-v6-").FullName, host: "[::1]");
        await using var duck = DuckSession.Open(Path.Combine(dir, "client3.duckdb"));

        await duck.ExecuteAsync("install quack");
        await duck.ExecuteAsync("load quack");
        await duck.ExecuteAsync($"create or replace secret v6_seed (type quack, token '{v6.Token}', scope '{v6.Uri}')");
        await duck.ExecuteAsync($"attach '{v6.Uri}' as v6_seed");
        await duck.ExecuteAsync("create table v6_seed.ping as select 42 as answer");
        await duck.ExecuteAsync("detach v6_seed");

        var landed = await ReadAsync(duck, Spec("ping"), uri: v6.Uri);
        Assert.Equal(42L, await duck.ScalarAsync<long>($"select answer from {landed}"));
    }
}
