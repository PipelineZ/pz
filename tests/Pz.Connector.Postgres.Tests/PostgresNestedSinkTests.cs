using Apache.Arrow;
using Npgsql;
using Pz.Connectors.Abstractions;
using Pz.DuckDb;

namespace Pz.Connector.Postgres.Tests;

/// <summary>Nested columns into postgres: a list of a flat type becomes a postgres array of that type (null
/// elements kept), a struct, a map or a list of nested values becomes <c>jsonb</c>. Batches come from DuckDB's
/// own Arrow export through pz's egress, as the engine hands them to the sink.</summary>
[Collection("postgres")]
public sealed class PostgresNestedSinkTests(PostgresContainerFixture fixture) : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-pg-nested", Guid.NewGuid().ToString("N"));
    private DuckSession _duck = null!;

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _duck = DuckSession.Open(Path.Combine(_dir, "staging.duckdb"));
        await _duck.ExecuteAsync("""
            create table t as select
                range as id,
                [range, null]::BIGINT[] as ints,
                ['a''b', null, 'c'] as strs,
                [1.5::DECIMAL(38,9), null] as decs,
                [DATE '2026-03-04'] as dates,
                [TIMESTAMP '2026-01-02 03:04:05.123456'] as stamps,
                [true, null] as flags,
                [2.5::DOUBLE] as dbls,
                [1, 2]::INTEGER[2] as pair,
                {'name': 'n' || range, 'tags': ['x']} as rec,
                map(['k'], [range::INTEGER]) as m,
                [[1], [2, 3]] as lol,
                []::INTEGER[] as empty
            from range(2)
            """);
        await _duck.ExecuteAsync("insert into t (id) values (2)");
    }

    public async Task DisposeAsync()
    {
        await _duck.DisposeAsync();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort */ }
    }

    private ConnectorConfig Config => new(new Dictionary<string, object?>
    {
        ["host"] = fixture.Host,
        ["port"] = fixture.Port,
        ["database"] = fixture.Database,
        ["user"] = fixture.User,
        ["password"] = fixture.Password,
    });

    private async Task<long> WriteAsync(string table)
    {
        ISinkConnector connector = new PostgresConnector();
        Assert.True(connector.Capabilities.HasFlag(ConnectorCapabilities.NestedTypes));
        await using var sink = await connector.OpenAsync(Config, CancellationToken.None);
        var schema = await _duck.GetResultSchemaAsync("select * from t");
        await using var session = await sink.BeginWriteAsync(
            new OutputSpec("pg", table, "append", "fail_on_change", new Dictionary<string, object?>()), schema,
            CancellationToken.None);
        await foreach (var batch in _duck.QueryArrowAsync("select * from t order by id"))
        {
            using (batch)
            {
                await session.WriteBatchAsync(batch, CancellationToken.None);
            }
        }

        return (await session.CommitAsync(CancellationToken.None)).RowsWritten;
    }

    [SkippableFact]
    public async Task Lists_land_as_arrays_and_structs_maps_and_nested_lists_as_jsonb()
    {
        const string table = "sink_nested";
        await ExecuteAsync($"drop table if exists public.{table}");

        Assert.Equal(3L, await WriteAsync(table));

        var types = await QueryAsync(
            $"select column_name || ':' || udt_name from information_schema.columns where table_name = '{table}' order by ordinal_position");
        Assert.Equal(
            ["id:int8", "ints:_int8", "strs:_text", "decs:_numeric", "dates:_date", "stamps:_timestamptz", "flags:_bool",
             "dbls:_float8", "pair:_int4", "rec:jsonb", "m:jsonb", "lol:jsonb", "empty:_int4"],
            types);
        var rows = await QueryAsync(
            $"select concat_ws(' | ', id, ints, strs, decs, dates, stamps, flags, dbls, pair, rec, m, lol, empty) from public.{table} order by id");
        Assert.Equal(
            [
                """0 | {0,NULL} | {a'b,NULL,c} | {1.500000000,NULL} | {2026-03-04} | {"2026-01-02 03:04:05.123456+00"} | {t,NULL} | {2.5} | {1,2} | {"name": "n0", "tags": ["x"]} | {"k": 0} | [[1], [2, 3]] | {}""",
                """1 | {1,NULL} | {a'b,NULL,c} | {1.500000000,NULL} | {2026-03-04} | {"2026-01-02 03:04:05.123456+00"} | {t,NULL} | {2.5} | {1,2} | {"name": "n1", "tags": ["x"]} | {"k": 1} | [[1], [2, 3]] | {}""",
                "2",
            ],
            rows);
    }

    [SkippableFact]
    public async Task An_existing_target_with_matching_array_and_jsonb_columns_passes_the_drift_check()
    {
        const string table = "sink_nested_existing";
        await ExecuteAsync($"drop table if exists public.{table}");
        await ExecuteAsync($"""
            create table public.{table} (id bigint, ints int8[], strs text[], decs numeric(20,4)[], dates date[],
                stamps timestamptz[], flags bool[], dbls float8[], pair integer[], rec jsonb, m jsonb, lol jsonb, empty int4[])
            """);

        Assert.Equal(3L, await WriteAsync(table));
        Assert.Equal(["3"], await QueryAsync($"select count(*) from public.{table}"));
    }

    [SkippableFact]
    public async Task An_array_element_type_mismatch_fails_the_drift_check_naming_both_types()
    {
        const string table = "sink_nested_drift";
        await ExecuteAsync($"drop table if exists public.{table}");
        await ExecuteAsync($"""
            create table public.{table} (id bigint, ints integer[], strs text[], decs numeric[], dates date[],
                stamps timestamptz[], flags boolean[], dbls double precision[], pair integer[], rec jsonb, m jsonb,
                lol jsonb, empty integer[])
            """);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => WriteAsync(table));

        Assert.False(ex.IsTransient);
        Assert.Contains("'ints'", ex.Message, StringComparison.Ordinal);
        Assert.Contains("'integer[]', expected 'bigint[]'", ex.Message, StringComparison.Ordinal);
    }

    private async Task ExecuteAsync(string sql)
    {
        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    private async Task<List<string>> QueryAsync(string sql)
    {
        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        await using (var tz = new NpgsqlCommand("set timezone = 'UTC'", connection))
        {
            await tz.ExecuteNonQueryAsync();
        }

        await using var command = new NpgsqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync();
        var rows = new List<string>();
        while (await reader.ReadAsync())
        {
            rows.Add(reader.GetValue(0).ToString()!);
        }

        return rows;
    }
}
