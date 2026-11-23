using Pz.Core.Validation;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.Engine.Staging;
using Pz.TestSupport;

namespace Pz.Engine.Tests.Staging;

/// <summary>A finished run's staging served over quack: a client attaches it and reads it, but cannot
/// write to it or reach anything else in the serving process. Installs the quack extension; skipped
/// offline.</summary>
[Trait("Category", "DuckDbExtension")]
public sealed class StagingServerTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "pz-staging-serve-tests", Guid.NewGuid().ToString("N"));

    public StagingServerTests() => DockerFacts.SkipIfOffline();

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); } catch { /* best-effort cleanup */ }
    }

    /// <summary>A run dir whose staging holds <c>staging.orders</c> (5 rows), a view over it, and a
    /// <c>pz_meta</c> table standing in for pz's own bookkeeping.</summary>
    private async Task<string> RunDir()
    {
        var dir = Path.Combine(_root, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        await using var duck = DuckSession.Open(Path.Combine(dir, "staging.duckdb"));
        await duck.ExecuteAsync("create schema staging");
        await duck.ExecuteAsync("create table staging.orders as select range as id from range(5)");
        await duck.ExecuteAsync("create view staging.big_orders as select id from staging.orders where id > 2");
        await duck.ExecuteAsync("create schema pz_meta");
        await duck.ExecuteAsync("create table pz_meta.preview_nodes as select 1 as x");
        return dir;
    }

    private static async Task<DuckSession> AttachAsync(StagingServer server)
    {
        var client = DuckSession.Open(":memory:");
        await client.ExecuteAsync("install quack");
        await client.ExecuteAsync("load quack");
        await client.ExecuteAsync(
            $"create secret pz_staging (type quack, token '{server.Token}', scope '{server.Uri}')");
        await client.ExecuteAsync($"attach '{server.Uri}' as run");
        return client;
    }

    [SkippableFact]
    public async Task A_client_attaches_and_reads_the_runs_tables_and_views()
    {
        await using var server = await StagingServer.StartAsync(await RunDir(), "localhost", null, default);
        await using var client = await AttachAsync(server);

        Assert.Equal(["staging.big_orders", "staging.orders"], server.Tables);
        Assert.StartsWith("quack:localhost:", server.Uri, StringComparison.Ordinal);
        Assert.Equal(5L, await client.ScalarAsync<long>("select count(*) from run.staging.orders"));
        Assert.Equal(2L, await client.ScalarAsync<long>("select count(*) from run.staging.big_orders"));
        await Assert.ThrowsAnyAsync<Exception>(() => client.ScalarAsync<long>("select count(*) from run.pz_meta.preview_nodes"));
    }

    [SkippableFact]
    public async Task A_client_can_neither_write_nor_reach_the_filesystem()
    {
        var dir = await RunDir();
        await using var server = await StagingServer.StartAsync(dir, "localhost", null, default);
        await using var client = await AttachAsync(server);
        string Remote(string sql) =>
            $"select * from quack_query('{server.Uri}', '{sql.Replace("'", "''")}', token := '{server.Token}')";

        await Assert.ThrowsAnyAsync<Exception>(() => client.ExecuteAsync("create table run.staging.x as select 1"));
        await Assert.ThrowsAnyAsync<Exception>(() => client.ExecuteAsync("delete from run.staging.orders"));
        var leak = Path.Combine(dir, "leak.csv");
        await Assert.ThrowsAnyAsync<Exception>(() => client.ExecuteAsync(Remote($"copy (select 1) to '{leak}'")));
        await Assert.ThrowsAnyAsync<Exception>(() => client.ExecuteAsync(Remote("select * from read_text('/etc/hostname')")));
        await Assert.ThrowsAnyAsync<Exception>(() => client.ExecuteAsync(Remote("set enable_external_access = true")));
        Assert.False(File.Exists(leak));
        Assert.Equal(5L, await client.ScalarAsync<long>("select count(*) from run.staging.orders"));
    }

    [SkippableFact]
    public async Task A_wrong_token_is_refused()
    {
        await using var server = await StagingServer.StartAsync(await RunDir(), "localhost", null, default);
        await using var client = DuckSession.Open(":memory:");
        await client.ExecuteAsync("install quack");
        await client.ExecuteAsync("load quack");
        await client.ExecuteAsync($"create secret pz_staging (type quack, token 'not-the-token', scope '{server.Uri}')");

        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await client.ExecuteAsync($"attach '{server.Uri}' as run");
            await client.ScalarAsync<long>("select count(*) from run.staging.orders");
        });
    }

    [SkippableFact]
    public async Task Each_serve_gets_a_fresh_token()
    {
        var dir = await RunDir();
        string first;
        await using (var server = await StagingServer.StartAsync(dir, "localhost", null, default))
        {
            first = server.Token;
        }

        await using var again = await StagingServer.StartAsync(dir, "localhost", null, default);
        Assert.NotEqual(first, again.Token);
        Assert.Equal(48, again.Token.Length);
    }

    // The run dir's lock is held while serving, so retention cannot delete the file and no run starts
    // there; it is released when the server stops.
    [SkippableFact]
    public async Task The_run_dir_lock_is_held_while_serving()
    {
        var dir = await RunDir();
        await using (await StagingServer.StartAsync(dir, "localhost", null, default))
        {
            Assert.Null(RunDirLock.TryAcquire(dir));
        }

        using var after = RunDirLock.TryAcquire(dir);
        Assert.NotNull(after);
    }

    [SkippableFact]
    public async Task A_run_in_progress_is_refused_PZ0543()
    {
        var dir = await RunDir();
        using var running = RunDirLock.Acquire(dir);

        var ex = await Assert.ThrowsAsync<PzValidationException>(() => StagingServer.StartAsync(dir, "localhost", null, default));

        var error = Assert.Single(ex.Errors);
        Assert.Equal(PzErrorCode.StagingServeUnavailable, error.Code);
        Assert.Contains("still in progress", error.Message, StringComparison.Ordinal);
    }

    [SkippableFact]
    public async Task A_missing_run_or_cleaned_staging_is_PZ0543()
    {
        var missing = await Assert.ThrowsAsync<PzValidationException>(() =>
            StagingServer.StartAsync(Path.Combine(_root, "nope"), "localhost", null, default));
        Assert.Contains("does not exist", Assert.Single(missing.Errors).Message, StringComparison.Ordinal);

        var cleaned = Path.Combine(_root, "cleaned");
        Directory.CreateDirectory(cleaned);
        var noStaging = await Assert.ThrowsAsync<PzValidationException>(() =>
            StagingServer.StartAsync(cleaned, "localhost", null, default));
        Assert.Equal(PzErrorCode.StagingServeUnavailable, Assert.Single(noStaging.Errors).Code);
    }
}

/// <summary>The host rules, which need no network.</summary>
public sealed class StagingServerHostTests
{
    [Theory]
    [InlineData("localhost", true)]
    [InlineData("127.0.0.1", true)]
    [InlineData("::1", true)]
    [InlineData("[::1]", true)]
    [InlineData("0.0.0.0", true)]
    [InlineData("db-host.internal", true)]
    [InlineData("", false)]
    [InlineData("local'host", false)]
    [InlineData("host name", false)]
    [InlineData("host:1234", false)]
    public void Only_a_host_name_or_ip_address_is_a_valid_host(string host, bool valid) =>
        Assert.Equal(valid, StagingServer.IsValidHost(host));

    [Theory]
    [InlineData("localhost", true)]
    [InlineData("127.0.0.1", true)]
    [InlineData("[::1]", true)]
    [InlineData("0.0.0.0", false)]
    [InlineData("db-host.internal", false)]
    public void Loopback_hosts_are_recognized(string host, bool loopback) =>
        Assert.Equal(loopback, StagingServer.IsLoopback(host));
}
