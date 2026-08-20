using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Npgsql;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;

namespace Pz.Connector.Postgres.Tests;

/// <summary>Postgres sink, append/replace modes only -- see <see cref="PostgresSink"/>'s doc comment
/// for merge. Every test below drives the real <see
/// cref="PostgresConnector"/> as an <see cref="ISinkConnector"/> against the shared Testcontainers
/// instance (<see cref="PostgresContainerFixture"/>), each against its own uniquely-named target table so
/// tests never interfere with each other or with the fixture's shared "orders"/"matrix" tables.</summary>
[Collection("postgres")]
public sealed class PostgresSinkTests(PostgresContainerFixture fixture)
{
    private ConnectorConfig ValidConfig => new(new Dictionary<string, object?>
    {
        ["host"] = fixture.Host,
        ["port"] = fixture.Port,
        ["database"] = fixture.Database,
        ["user"] = fixture.User,
        ["password"] = fixture.Password,
    });

    // Full v0 type matrix plus a non-null "id" column for stable row identification/
    // ordering (a null row would otherwise be indistinguishable from any other all-null row).
    private static readonly Schema FullMatrixSchema = new(
    [
        new Field("id", Int64Type.Default, nullable: false),
        new Field("c_int", Int32Type.Default, nullable: true),
        new Field("c_bigint", Int64Type.Default, nullable: true),
        new Field("c_double", DoubleType.Default, nullable: true),
        new Field("c_numeric", new Decimal128Type(38, 9), nullable: true),
        new Field("c_text", StringType.Default, nullable: true),
        new Field("c_bool", BooleanType.Default, nullable: true),
        new Field("c_date", Date32Type.Default, nullable: true),
        new Field("c_ts", new TimestampType(TimeUnit.Microsecond, "+00:00"), nullable: true),
    ], null);

    [SkippableFact]
    public async Task Append_roundtrips_all_v0_types()
    {
        const string table = "sink_append_matrix";
        await DropAsync(table);

        ISinkConnector connector = new PostgresConnector();
        await using var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None);

        var spec = new OutputSpec("pg", table, "append", "fail_on_change",
            new Dictionary<string, object?>());

        var row1 = new object?[]
        {
            1L, 42, 9_000_000_000L, 1.5, 12345.123456789m, "hello", true,
            new DateOnly(2026, 3, 27), new DateTimeOffset(2026, 3, 27, 10, 30, 0, TimeSpan.Zero),
        };
        var row2 = new object?[] { 2L, null, null, null, null, null, null, null, null };

        await using (var session = await sink.BeginWriteAsync(spec, FullMatrixSchema, CancellationToken.None))
        {
            var builder = new ArrowBatchBuilder(FullMatrixSchema);
            builder.AppendRow(row1);
            builder.AppendRow(row2);
            using var batch = builder.Flush()!;
            await session.WriteBatchAsync(batch, CancellationToken.None);

            var result = await session.CommitAsync(CancellationToken.None);
            Assert.Equal(2, result.RowsWritten);
        }

        var expectedDigest = DigestRows([row1, row2]);
        var (actualCount, actualDigest) = await ReadBackDigestAsync(table);

        Assert.Equal(2, actualCount);
        Assert.Equal(expectedDigest, actualDigest);
    }

    [SkippableFact]
    public async Task Replace_is_transactional()
    {
        const string table = "sink_replace_tx";
        await DropAsync(table);
        await ExecuteAsync($"create table public.{table} (id bigint primary key, val integer not null)");
        await ExecuteAsync($"insert into public.{table} (id, val) values (100, 999)");

        var schema = new Schema(
        [
            new Field("id", Int64Type.Default, nullable: false),
            new Field("val", Int32Type.Default, nullable: false),
        ], null);

        ISinkConnector connector = new PostgresConnector();
        await using var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var spec = new OutputSpec("pg", table, "replace", "fail_on_change",
            new Dictionary<string, object?>());

        await using var reader = new NpgsqlConnection(fixture.ConnectionString);
        await reader.OpenAsync().ConfigureAwait(false);

        await using (var session = await sink.BeginWriteAsync(spec, schema, CancellationToken.None))
        {
            var builder = new ArrowBatchBuilder(schema);
            for (var i = 1; i <= 5; i++)
            {
                builder.AppendRow([(long)i, i * 10]);
            }

            using var batch = builder.Flush()!;
            await session.WriteBatchAsync(batch, CancellationToken.None);

            // Mid-write, pre-commit: a concurrent reader on a SEPARATE connection must still see the OLD
            // data -- truncate/insert only happen inside CommitAsync's finalize step, same transaction.
            var midWriteVal = await ScalarAsync<int>(reader, $"select val from public.{table} where id = 100");
            var midWriteCount = await ScalarAsync<long>(reader, $"select count(*) from public.{table}");
            Assert.Equal(999, midWriteVal);
            Assert.Equal(1L, midWriteCount);

            await session.CommitAsync(CancellationToken.None);
        }

        // Post-commit: the same reader connection now sees only the new rows.
        var postCommitCount = await ScalarAsync<long>(reader, $"select count(*) from public.{table}");
        var oldRowGone = await ScalarAsync<long>(reader, $"select count(*) from public.{table} where id = 100");
        Assert.Equal(5L, postCommitCount);
        Assert.Equal(0L, oldRowGone);
    }

    [SkippableFact]
    public async Task Abort_leaves_target_untouched()
    {
        const string table = "sink_abort";
        await DropAsync(table);
        await ExecuteAsync($"create table public.{table} (id bigint primary key, val integer not null)");
        await ExecuteAsync($"insert into public.{table} (id, val) values (1, 111)");

        var schema = new Schema(
        [
            new Field("id", Int64Type.Default, nullable: false),
            new Field("val", Int32Type.Default, nullable: false),
        ], null);

        ISinkConnector connector = new PostgresConnector();
        await using var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var spec = new OutputSpec("pg", table, "append", "fail_on_change",
            new Dictionary<string, object?>());

        await using (var session = await sink.BeginWriteAsync(spec, schema, CancellationToken.None))
        {
            var builder = new ArrowBatchBuilder(schema);
            builder.AppendRow([2L, 222]);
            using var batch = builder.Flush()!;
            await session.WriteBatchAsync(batch, CancellationToken.None);

            await session.AbortAsync(CancellationToken.None);
        }

        await using var reader = new NpgsqlConnection(fixture.ConnectionString);
        await reader.OpenAsync().ConfigureAwait(false);
        var count = await ScalarAsync<long>(reader, $"select count(*) from public.{table}");
        var survivingVal = await ScalarAsync<int>(reader, $"select val from public.{table} where id = 1");
        Assert.Equal(1L, count);
        Assert.Equal(111, survivingVal);
    }

    [SkippableFact]
    public async Task Mid_write_failure_rolls_back()
    {
        const string table = "sink_midwrite";
        await DropAsync(table);
        await ExecuteAsync($"create table public.{table} (id bigint primary key, val integer not null)");

        var schema = new Schema(
        [
            new Field("id", Int64Type.Default, nullable: false),
            new Field("val", Int32Type.Default, nullable: false),
        ], null);
        // Deliberately narrower than `schema` -- a batch built against this one has fewer columns than
        // the write session expects, so writing it makes the session's per-column loop index past the
        // batch's actual column count, throwing mid-WriteBatchAsync (a stand-in for "something went
        // wrong mid-write" -- the ABI's own doc comment requires Abort to be safe after exactly this).
        var narrowerSchema = new Schema([new Field("id", Int64Type.Default, nullable: false)], null);

        ISinkConnector connector = new PostgresConnector();
        await using var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var spec = new OutputSpec("pg", table, "append", "fail_on_change",
            new Dictionary<string, object?>());

        await using (var session = await sink.BeginWriteAsync(spec, schema, CancellationToken.None))
        {
            var goodBuilder = new ArrowBatchBuilder(schema);
            goodBuilder.AppendRow([1L, 10]);
            using var goodBatch = goodBuilder.Flush()!;
            await session.WriteBatchAsync(goodBatch, CancellationToken.None);

            var badBuilder = new ArrowBatchBuilder(narrowerSchema);
            badBuilder.AppendRow([2L]);
            using var badBatch = badBuilder.Flush()!;

            await Assert.ThrowsAnyAsync<Exception>(
                async () => await session.WriteBatchAsync(badBatch, CancellationToken.None));

            await session.AbortAsync(CancellationToken.None);
        }

        var count = await CountAsync(table);
        Assert.Equal(0L, count);
    }

    [SkippableFact]
    public async Task Missing_target_created_with_ddl_map()
    {
        const string table = "sink_missing_target";
        await DropAsync(table);

        ISinkConnector connector = new PostgresConnector();
        await using var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var spec = new OutputSpec("pg", table, "append", "fail_on_change",
            new Dictionary<string, object?>());

        await using (var session = await sink.BeginWriteAsync(spec, FullMatrixSchema, CancellationToken.None))
        {
            await session.CommitAsync(CancellationToken.None);
        }

        var columns = await LoadColumnTypesAsync(table);
        Assert.Equal("bigint", columns["id"].DataType);
        Assert.Equal("integer", columns["c_int"].DataType);
        Assert.Equal("bigint", columns["c_bigint"].DataType);
        Assert.Equal("double precision", columns["c_double"].DataType);
        Assert.Equal("numeric", columns["c_numeric"].DataType);
        Assert.Equal(38, columns["c_numeric"].Precision);
        Assert.Equal(9, columns["c_numeric"].Scale);
        Assert.Equal("text", columns["c_text"].DataType);
        Assert.Equal("boolean", columns["c_bool"].DataType);
        Assert.Equal("date", columns["c_date"].DataType);
        Assert.Equal("timestamp with time zone", columns["c_ts"].DataType);
    }

    [SkippableFact]
    public async Task Schema_drift_errors_cleanly()
    {
        const string table = "sink_drift";
        await DropAsync(table);
        // "c_int" declared as text instead of integer -- a genuine type mismatch fail_on_change must
        // catch against information_schema.columns.
        await ExecuteAsync($"""
            create table public.{table} (
                id bigint, c_int text, c_bigint bigint, c_double double precision, c_numeric numeric(38,9),
                c_text text, c_bool boolean, c_date date, c_ts timestamptz
            )
            """);

        ISinkConnector connector = new PostgresConnector();
        await using var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var spec = new OutputSpec("pg", table, "append", "fail_on_change",
            new Dictionary<string, object?>());

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(spec, FullMatrixSchema, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("c_int", ex.Message, StringComparison.Ordinal);
        Assert.Contains("integer", ex.Message, StringComparison.Ordinal);
        Assert.Contains("text", ex.Message, StringComparison.Ordinal);
    }

    [SkippableFact]
    public async Task Evolve_policy_errors_cleanly()
    {
        const string table = "sink_evolve";
        await DropAsync(table);

        ISinkConnector connector = new PostgresConnector();
        await using var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var spec = new OutputSpec("pg", table, "append", "evolve",
            new Dictionary<string, object?>());

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(spec, FullMatrixSchema, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("evolve", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("not supported", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    private async Task DropAsync(string table) =>
        await ExecuteAsync($"drop table if exists public.{table}");

    private async Task ExecuteAsync(string sql)
    {
        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync().ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    private async Task<long> CountAsync(string table)
    {
        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync().ConfigureAwait(false);
        return await ScalarAsync<long>(connection, $"select count(*) from public.{table}");
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var result = await command.ExecuteScalarAsync().ConfigureAwait(false);
        return (T)Convert.ChangeType(result!, typeof(T), CultureInfo.InvariantCulture);
    }

    private async Task<Dictionary<string, (string DataType, int? Precision, int? Scale)>> LoadColumnTypesAsync(string table)
    {
        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync().ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            """
            select column_name, data_type, numeric_precision, numeric_scale
            from information_schema.columns
            where table_schema = 'public' and table_name = @table
            """,
            connection);
        command.Parameters.AddWithValue("table", table);
        await using var reader = await command.ExecuteReaderAsync().ConfigureAwait(false);

        var columns = new Dictionary<string, (string, int?, int?)>(StringComparer.Ordinal);
        while (await reader.ReadAsync().ConfigureAwait(false))
        {
            columns[reader.GetString(0)] = (
                reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetInt32(2),
                reader.IsDBNull(3) ? null : reader.GetInt32(3));
        }

        return columns;
    }

    private async Task<(long Count, string Digest)> ReadBackDigestAsync(string table)
    {
        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync().ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            "select id, c_int, c_bigint, c_double, c_numeric, c_text, c_bool, c_date, c_ts " +
            $"from public.{table} order by id", connection);
        await using var reader = await command.ExecuteReaderAsync().ConfigureAwait(false);

        var rows = new List<string>();
        while (await reader.ReadAsync().ConfigureAwait(false))
        {
            var values = new object?[reader.FieldCount];
            for (var i = 0; i < reader.FieldCount; i++)
            {
                values[i] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }

            rows.Add(CanonicalRow(values));
        }

        return (rows.Count, Digest(rows));
    }

    private static string DigestRows(IEnumerable<object?[]> rows) =>
        Digest(rows.Select(CanonicalRow));

    private static string Digest(IEnumerable<string> canonicalRows)
    {
        var sorted = canonicalRows.OrderBy(r => r, StringComparer.Ordinal).ToArray();
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(string.Join('\n', sorted)));
        return Convert.ToHexString(bytes);
    }

    private static string CanonicalRow(object?[] values) =>
        string.Join('', values.Select(CanonicalValue));

    private static string CanonicalValue(object? value) => value switch
    {
        null => "<NULL>",
        int i => i.ToString(CultureInfo.InvariantCulture),
        long l => l.ToString(CultureInfo.InvariantCulture),
        double d => d.ToString("R", CultureInfo.InvariantCulture),
        decimal m => m.ToString(CultureInfo.InvariantCulture),
        bool b => b.ToString(CultureInfo.InvariantCulture),
        string s => s,
        DateOnly d => d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
        DateTimeOffset dto => dto.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture),
        // Npgsql's NpgsqlDataReader.GetValue(i) surfaces a timestamptz column as CLR DateTime (Kind=Utc),
        // not DateTimeOffset -- normalize both to the same canonical instant-with-offset rendering so the
        // written-side DateTimeOffset input and the read-back DateTime agree.
        DateTime dt => new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc))
            .ToString("O", CultureInfo.InvariantCulture),
        _ => value.ToString() ?? "<NULL>",
    };
}
