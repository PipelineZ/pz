using Pz.Connectors.Abstractions;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.TestSupport;

namespace Pz.Connector.MySql.Tests;

/// <summary>Real-MySQL (Testcontainers) proof of the native-only connector's whole data plane: every
/// test drives the REAL <see cref="ISource"/>/<see cref="ISink"/> surface returned from
/// <see cref="MySqlConnector.OpenAsync"/> -- TryGetNativeScan/TryGetNativeCopy fragments run through
/// <see cref="NativeSetup.ExecuteSetupAsync"/> against a live <see cref="DuckSession"/> and a
/// mysql:8.4 container, never a hand-rolled attach string. Since the connector ships with zero .NET
/// MySQL driver, every seed and every read-back goes through the connector's own native SQL too --
/// there is no other way to talk to the server in this test project. Each test picks its own unique
/// table name so tests are order-independent within the one shared container/database.</summary>
[Collection("mysql")]
public sealed class MySqlNativeEndToEndTests(MySqlContainerFixture fixture)
{
    private static string Table() => "t_" + Guid.NewGuid().ToString("N");

    /// <summary>Opens a fresh scratch DuckDB file, deleted on dispose. Callers must have already run
    /// the two skip guards and <see cref="MySqlContainerFixture.EnsureStartedAsync"/>.</summary>
    private static Task<TempDuckSession> OpenDuckAsync()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"pz-mysql-e2e-{Guid.NewGuid():N}.duckdb");
        return Task.FromResult(new TempDuckSession(DuckSession.Open(dbPath), dbPath));
    }

    /// <summary>Writes <paramref name="selectSql"/>'s rows into MySQL table <paramref name="table"/>
    /// through the connector's own <see cref="ISink.TryGetNativeCopy"/> -- the ONLY way this test
    /// project talks to MySQL, matching the connector's zero-driver design. Materializes the seed rows
    /// into a real DuckDB staging table first and substitutes it for <c>{{source}}</c> exactly as
    /// <c>SinkWriteExecutor</c> does, rather than inlining the SELECT.</summary>
    private async Task SeedAsync(DuckSession duck, string table, string selectSql, string mode = "replace")
    {
        await using var sink = await ((ISinkConnector)new MySqlConnector()).OpenAsync(fixture.Config, CancellationToken.None);
        var spec = new OutputSpec("wh", table, mode, "fail_on_change", new Dictionary<string, object?>());
        Assert.True(sink.TryGetNativeCopy(spec, out var copy));

        foreach (var setup in copy!.SetupStatements)
        {
            await NativeSetup.ExecuteSetupAsync(duck, setup, CancellationToken.None);
        }

        var staging = "stage_" + Guid.NewGuid().ToString("N");
        await duck.ExecuteAsync($"create table {staging} as {selectSql}");
        await duck.ExecuteAsync(copy.CopySql.Replace("{{source}}", staging, StringComparison.Ordinal));
    }

    /// <summary>Runs the connector's <see cref="ISource.TryGetNativeScan"/> for <paramref name="spec"/>
    /// through <see cref="NativeSetup.ExecuteSetupAsync"/> and materializes the resulting
    /// <c>mysql_query(...)</c> fragment into a fresh DuckDB table, returning its name.</summary>
    private async Task<string> MaterializeScanAsync(DuckSession duck, DatasetSpec spec)
    {
        await using var source = await ((ISourceConnector)new MySqlConnector()).OpenAsync(fixture.Config, CancellationToken.None);
        Assert.True(source.TryGetNativeScan(spec, out var scan));

        foreach (var setup in scan!.SetupStatements)
        {
            await NativeSetup.ExecuteSetupAsync(duck, setup, CancellationToken.None);
        }

        var landed = "landed_" + Guid.NewGuid().ToString("N");
        await duck.ExecuteAsync($"create table {landed} as select * from {scan.SqlFragment}");
        return landed;
    }

    [SkippableFact]
    public async Task Read_round_trips_row_count_values_and_types()
    {
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();
        await fixture.EnsureStartedAsync();

        await using var session = await OpenDuckAsync();
        var duck = session.Duck;
        var table = Table();

        // The injection-safety pattern exercised for real: a NULL and a quote-bearing string must
        // both land through mysql_query('alias', '...') unharmed.
        await SeedAsync(duck, table, """
            (select 1 as id, 'O''Brien' as name, date '2026-03-27' as placed_on,
                    timestamp '2026-03-27 10:30:00' as created_at
             union all
             select 2, NULL, date '2026-03-28', timestamp '2026-03-28 11:15:30')
            """);

        var landed = await MaterializeScanAsync(duck, new DatasetSpec("wh", table, new Dictionary<string, object?>()));

        Assert.Equal(2, await duck.ScalarAsync<long>($"select count(*) from {landed}"));
        Assert.Equal("O'Brien", await duck.ScalarAsync<string>($"select name from {landed} where id = 1"));
        Assert.True(await duck.ScalarAsync<bool>($"select name is null from {landed} where id = 2"));
        Assert.Equal(new DateOnly(2026, 3, 27),
            await duck.ScalarAsync<DateOnly>($"select placed_on from {landed} where id = 1"));
        Assert.Equal("2026-03-27 10:30:00", await duck.ScalarAsync<string>(
            $"select strftime(created_at, '%Y-%m-%d %H:%M:%S') from {landed} where id = 1"));
    }

    [SkippableFact]
    public async Task Declared_columns_contract_prunes_the_read()
    {
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();
        await fixture.EnsureStartedAsync();

        await using var session = await OpenDuckAsync();
        var duck = session.Duck;
        var table = Table();

        await SeedAsync(duck, table,
            "(select 1 as id, 'alice' as name, 999 as extra_col union all select 2, 'bob', 998)");

        var spec = new DatasetSpec("wh", table, new Dictionary<string, object?>
        {
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["name"] = "varchar" },
        });
        var landed = await MaterializeScanAsync(duck, spec);

        Assert.Equal(2, await duck.ScalarAsync<long>(
            $"select count(*) from information_schema.columns where table_name = '{landed}'"));
        Assert.Equal(0, await duck.ScalarAsync<long>(
            $"select count(*) from information_schema.columns where table_name = '{landed}' and column_name = 'extra_col'"));
        Assert.Equal(2, await duck.ScalarAsync<long>($"select count(*) from {landed}"));
    }

    [SkippableFact]
    public async Task Query_option_reads_an_arbitrary_mysql_select()
    {
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();
        await fixture.EnsureStartedAsync();

        await using var session = await OpenDuckAsync();
        var duck = session.Duck;
        var table = Table();

        await SeedAsync(duck, table,
            "(select 1 as id, 10 as amount union all select 2, 20 union all select 3, 30)");

        var spec = new DatasetSpec("wh", table, new Dictionary<string, object?>
        {
            ["query"] = $"SELECT COUNT(*) AS cnt, CAST(SUM(amount) AS SIGNED) AS total FROM {table}",
        });
        var landed = await MaterializeScanAsync(duck, spec);

        Assert.Equal(3L, await duck.ScalarAsync<long>($"select cnt from {landed}"));
        Assert.Equal(60L, await duck.ScalarAsync<long>($"select total from {landed}"));
    }

    [SkippableFact]
    public async Task Watermark_pushdown_extracts_only_rows_past_the_cursor()
    {
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();
        await fixture.EnsureStartedAsync();

        await using var session = await OpenDuckAsync();
        var duck = session.Duck;
        var table = Table();

        await SeedAsync(duck, table,
            "(select 1 as id union all select 2 union all select 3 union all select 4 union all select 5)");

        var spec = new DatasetSpec("wh", table, new Dictionary<string, object?>())
        {
            WatermarkCursor = "id",
            WatermarkValue = "2",
        };
        var landed = await MaterializeScanAsync(duck, spec);

        Assert.Equal(3, await duck.ScalarAsync<long>($"select count(*) from {landed}"));
        Assert.Equal("3,4,5", await duck.ScalarAsync<string>(
            $"select string_agg(id::varchar, ',' order by id) from {landed}"));
    }

    [SkippableFact]
    public async Task Watermark_upper_bound_narrows_the_window_further()
    {
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();
        await fixture.EnsureStartedAsync();

        await using var session = await OpenDuckAsync();
        var duck = session.Duck;
        var table = Table();

        await SeedAsync(duck, table,
            "(select 1 as id union all select 2 union all select 3 union all select 4 union all select 5)");

        var spec = new DatasetSpec("wh", table, new Dictionary<string, object?>())
        {
            WatermarkCursor = "id",
            WatermarkValue = "2",
            WatermarkUpperBound = "4",
        };
        var landed = await MaterializeScanAsync(duck, spec);

        Assert.Equal(2, await duck.ScalarAsync<long>($"select count(*) from {landed}"));
        Assert.Equal("3,4", await duck.ScalarAsync<string>(
            $"select string_agg(id::varchar, ',' order by id) from {landed}"));
    }

    [SkippableFact]
    public async Task Sink_append_creates_the_table_then_accumulates_rows()
    {
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();
        await fixture.EnsureStartedAsync();

        await using var session = await OpenDuckAsync();
        var duck = session.Duck;
        var table = Table();

        // First append: no pre-existing table -- CREATE IF NOT EXISTS must carry the shape.
        await SeedAsync(duck, table, "(select 1 as id, 'a' as name union all select 2, 'b')", mode: "append");
        var landed1 = await MaterializeScanAsync(duck, new DatasetSpec("wh", table, new Dictionary<string, object?>()));
        Assert.Equal(2, await duck.ScalarAsync<long>($"select count(*) from {landed1}"));

        // Second append accumulates (at-least-once shape, not a replace).
        await SeedAsync(duck, table, "(select 3 as id, 'c' as name)", mode: "append");
        var landed2 = await MaterializeScanAsync(duck, new DatasetSpec("wh", table, new Dictionary<string, object?>()));
        Assert.Equal(3, await duck.ScalarAsync<long>($"select count(*) from {landed2}"));
        Assert.Equal("1,2,3", await duck.ScalarAsync<string>(
            $"select string_agg(id::varchar, ',' order by id) from {landed2}"));
    }

    [SkippableFact]
    public async Task Sink_replace_swaps_contents_and_is_idempotent()
    {
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();
        await fixture.EnsureStartedAsync();

        await using var session = await OpenDuckAsync();
        var duck = session.Duck;
        var table = Table();

        await SeedAsync(duck, table, "(select 1 as id union all select 2)", mode: "replace");
        var afterFirst = await MaterializeScanAsync(duck, new DatasetSpec("wh", table, new Dictionary<string, object?>()));
        Assert.Equal(2, await duck.ScalarAsync<long>($"select count(*) from {afterFirst}"));

        const string secondBatch = "(select 10 as id union all select 20 union all select 30)";
        await SeedAsync(duck, table, secondBatch, mode: "replace");
        // Running the same replace again must be idempotent -- no accumulation, no failure.
        await SeedAsync(duck, table, secondBatch, mode: "replace");

        var afterSecond = await MaterializeScanAsync(duck, new DatasetSpec("wh", table, new Dictionary<string, object?>()));
        Assert.Equal(3, await duck.ScalarAsync<long>($"select count(*) from {afterSecond}"));
        Assert.Equal("10,20,30", await duck.ScalarAsync<string>(
            $"select string_agg(id::varchar, ',' order by id) from {afterSecond}"));
    }

    [SkippableFact]
    public async Task Sink_write_round_trips_through_source_scan_with_mixed_types()
    {
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();
        await fixture.EnsureStartedAsync();

        await using var session = await OpenDuckAsync();
        var duck = session.Duck;
        var table = Table();

        // Both directions of the native plane against one server: written via TryGetNativeCopy,
        // read back via TryGetNativeScan -- no other path touches MySQL in this test project.
        await SeedAsync(duck, table,
            "(select 1 as id, 'alice' as name, 12.5 as amount union all select 2, 'bob', 7.25)");

        var landed = await MaterializeScanAsync(duck, new DatasetSpec("wh", table, new Dictionary<string, object?>()));

        Assert.Equal(2, await duck.ScalarAsync<long>($"select count(*) from {landed}"));
        Assert.Equal("alice", await duck.ScalarAsync<string>($"select name from {landed} where id = 1"));
        Assert.Equal(12.5, await duck.ScalarAsync<double>($"select amount from {landed} where id = 1"));
        Assert.Equal("bob", await duck.ScalarAsync<string>($"select name from {landed} where id = 2"));
        Assert.Equal(7.25, await duck.ScalarAsync<double>($"select amount from {landed} where id = 2"));
    }

    [SkippableFact]
    public async Task A_wrong_password_connect_failure_never_leaks_the_password()
    {
        // A failed ATTACH connect (wrong password) is NOT a parser error -- it is a runtime IO error
        // the mysql extension throws only once TYPE mysql actually tries to connect, so it needs a
        // real server (unlike SecretRedactionTests' offline malformed-CREATE-SECRET variant). The
        // error echoes the attach path in DOUBLE quotes, a shape NativeStatementRedactor's
        // single-quote masking does not catch; the attach path is always '', so the echo is
        // credential-free by construction. Walks the FULL exception chain, since NativeSetup wraps
        // the DuckDB IOException as PzConnectorException.InnerException.
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();
        await fixture.EnsureStartedAsync();

        const string wrongPassword = "DEFINITELY_THE_WRONG_PASSWORD_9f3c1a";
        var wrongPasswordConfig = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["host"] = fixture.Hostname,
            ["port"] = fixture.Port,
            ["database"] = fixture.Database,
            ["user"] = fixture.Username,
            ["password"] = wrongPassword,
        });

        await using var session = await OpenDuckAsync();
        var duck = session.Duck;

        await using var source = await ((ISourceConnector)new MySqlConnector()).OpenAsync(
            wrongPasswordConfig, CancellationToken.None);
        Assert.True(source.TryGetNativeScan(
            new DatasetSpec("wh", "orders", new Dictionary<string, object?>()), out var scan));

        PzConnectorException? thrown = null;
        foreach (var setup in scan!.SetupStatements)
        {
            try
            {
                await NativeSetup.ExecuteSetupAsync(duck, setup, CancellationToken.None);
            }
            catch (PzConnectorException ex)
            {
                thrown = ex;
                break;
            }
        }

        Assert.NotNull(thrown);
        for (Exception? current = thrown; current is not null; current = current.InnerException)
        {
            Assert.DoesNotContain(wrongPassword, current.Message, StringComparison.Ordinal);
        }
    }

    [SkippableFact]
    public async Task CheckConnectionAsync_probes_a_live_server_and_reports_not_ok_for_a_closed_port()
    {
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();
        await fixture.EnsureStartedAsync();

        var connector = new MySqlConnector();

        var ok = await connector.CheckConnectionAsync(fixture.Config, CancellationToken.None);
        Assert.True(ok.Ok);
        Assert.Contains("server version", ok.Message, StringComparison.OrdinalIgnoreCase);

        var closed = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["host"] = fixture.Hostname,
            ["port"] = 1, // nothing listens here on the container host
        });
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var bad = await connector.CheckConnectionAsync(closed, timeout.Token);
        Assert.False(bad.Ok);
    }
}

/// <summary>A scratch DuckDB file, deleted on dispose (best-effort).</summary>
internal sealed class TempDuckSession(DuckSession duck, string dbPath) : IAsyncDisposable
{
    public DuckSession Duck => duck;

    public async ValueTask DisposeAsync()
    {
        await duck.DisposeAsync().ConfigureAwait(false);
        try
        {
            File.Delete(dbPath);
        }
        catch
        {
            // Suppressed by design: best-effort temp-file cleanup.
        }
    }
}
