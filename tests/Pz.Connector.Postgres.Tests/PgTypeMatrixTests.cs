using Npgsql;
using Pz.Connectors.Abstractions.Batches;
using Pz.DuckDb;

namespace Pz.Connector.Postgres.Tests;

/// <summary>Full-stack type-matrix roundtrip: a real postgres row set (the full matrix, seeded
/// by <see cref="PostgresContainerFixture"/>) flows through <see cref="DataReaderSource.ReadBatchesAsync"/>
/// into a DuckDB table via <see cref="IDuckSession.IngestArrowAsync"/>, then every column's value (and
/// every column's NULL row) is asserted back out via <see cref="IDuckSession.ScalarAsync{T}"/>. This is
/// the one place in the suite that exercises <see cref="DataReaderSource"/>'s date32 branch (postgres
/// <c>date</c> reads as CLR <see cref="DateOnly"/> via Npgsql) -- <c>System.Data.DataTable</c> cannot
/// carry a <see cref="DateOnly"/> column, so the unit-test suite in Pz.Connectors.Abstractions.Tests
/// documents that gap and defers date32 coverage to here.</summary>
[Collection("postgres")]
public sealed class PgTypeMatrixTests(PostgresContainerFixture fixture)
{
    [SkippableFact]
    public async Task Pg_type_matrix_roundtrips_to_duckdb()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-pg-matrix-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            await using var connection = new NpgsqlConnection(fixture.ConnectionString);
            await connection.OpenAsync().ConfigureAwait(false);
            await using var command = new NpgsqlCommand("select * from public.matrix order by id", connection);
            await using var reader = await command.ExecuteReaderAsync().ConfigureAwait(false);

            var schema = DataReaderSource.BuildArrowSchema(reader);
            var batches = DataReaderSource.ReadBatchesAsync(reader, targetBatchBytes: 32 * 1024 * 1024);

            await using var duck = DuckSession.Open(Path.Combine(dir, "t.duckdb"));
            var rows = await duck.IngestArrowAsync("main.matrix", schema, batches);

            Assert.Equal(2, rows);

            // Row id = 1: every column carries a concrete value.
            Assert.Equal(42, await duck.ScalarAsync<int>("select c_int from main.matrix where id = 1"));
            Assert.Equal(9_000_000_000L, await duck.ScalarAsync<long>("select c_bigint from main.matrix where id = 1"));
            Assert.Equal(1.5, await duck.ScalarAsync<double>("select c_double from main.matrix where id = 1"));
            Assert.Equal(12345.123456789m, await duck.ScalarAsync<decimal>("select c_numeric from main.matrix where id = 1"));
            Assert.Equal("hello", await duck.ScalarAsync<string>("select c_text from main.matrix where id = 1"));
            Assert.True(await duck.ScalarAsync<bool>("select c_bool from main.matrix where id = 1"));
            Assert.Equal("2026-03-27", await duck.ScalarAsync<string>(
                "select strftime(c_date, '%Y-%m-%d') from main.matrix where id = 1"));
            Assert.Equal("2026-03-27 10:30:00", await duck.ScalarAsync<string>(
                "select strftime(c_timestamp, '%Y-%m-%d %H:%M:%S') from main.matrix where id = 1"));
            Assert.Equal("2026-03-27 10:30:00", await duck.ScalarAsync<string>(
                "select strftime(c_timestamptz, '%Y-%m-%d %H:%M:%S') from main.matrix where id = 1"));

            // Row id = 2: every typed column is NULL.
            Assert.Equal(1L, await duck.ScalarAsync<long>("""
                select count(*) from main.matrix
                where id = 2
                  and c_int is null and c_bigint is null and c_double is null and c_numeric is null
                  and c_text is null and c_bool is null and c_date is null and c_timestamp is null
                  and c_timestamptz is null
                """));
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    /// <summary>A one-dimensional postgres array of any matrix type reads as a DuckDB list of that type, NULL
    /// elements and NULL arrays kept.</summary>
    [SkippableFact]
    public async Task Pg_arrays_read_as_lists()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-pg-matrix-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            await using var connection = new NpgsqlConnection(fixture.ConnectionString);
            await connection.OpenAsync();
            await using var command = new NpgsqlCommand("""
                select * from (values
                    (1, array[1, null, 3]::integer[], array[9000000000]::bigint[], array[1.5]::float8[],
                     array[12.5, null]::numeric[], array['a', null]::text[], array[true]::boolean[],
                     array['2026-03-27']::date[], array['2026-03-27 10:30:00']::timestamp[],
                     array['2026-03-27 10:30:00+00']::timestamptz[]),
                    (2, null, '{}', null, null, null, null, null, null, null)
                ) as t(id, ints, bigs, dbls, nums, texts, flags, dates, stamps, stamptzs)
                order by id
                """, connection);
            await using var reader = await command.ExecuteReaderAsync();

            Type? Element(int i) => PgTypeMap.ArrayElement(reader.GetDataTypeName(i));
            var schema = DataReaderSource.BuildArrowSchema(reader, Element);
            await using var duck = DuckSession.Open(Path.Combine(dir, "t.duckdb"));
            Assert.Equal(2, await duck.IngestArrowAsync("main.arrays", schema,
                DataReaderSource.ReadBatchesAsync(reader, 32 * 1024 * 1024, Element)));

            Assert.Equal(
                "INTEGER[],BIGINT[],DOUBLE[],DECIMAL(38,9)[],VARCHAR[],BOOLEAN[],DATE[],TIMESTAMP[],TIMESTAMP[]",
                await duck.ScalarAsync<string>(
                    "select string_agg(column_type, ',' order by rowid) from (select *, row_number() over () as rowid from (describe main.arrays)) where column_name <> 'id'"));
            Assert.Equal(
                "[1, NULL, 3] | [9000000000] | [1.5] | [12.500000000, NULL] | [a, NULL] | [true] | [2026-03-27] | ['2026-03-27 10:30:00'] | ['2026-03-27 10:30:00']",
                await duck.ScalarAsync<string>(
                    "select concat_ws(' | ', ints::VARCHAR, bigs::VARCHAR, dbls::VARCHAR, nums::VARCHAR, texts::VARCHAR, flags::VARCHAR, dates::VARCHAR, stamps::VARCHAR, stamptzs::VARCHAR) from main.arrays where id = 1"));
            Assert.Equal("NULL | [] | NULL", await duck.ScalarAsync<string>(
                "select coalesce(ints::varchar, 'NULL') || ' | ' || bigs::varchar || ' | ' || coalesce(texts::varchar, 'NULL') from main.arrays where id = 2"));
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }
}
