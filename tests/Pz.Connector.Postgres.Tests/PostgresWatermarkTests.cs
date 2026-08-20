using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Apache.Arrow;
using Npgsql;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;

namespace Pz.Connector.Postgres.Tests;

/// <summary>Postgres watermark pushdown -- <see cref="PostgresSource.BuildSelect"/>
/// ANDs <c>{Quote(cursor)} &gt; '{value}'</c> into the generated SELECT when <see
/// cref="DatasetSpec.WatermarkCursor"/> is set. Every proof below reads via the real
/// <see cref="PostgresConnector"/> against the shared Testcontainers instance (<see
/// cref="PostgresContainerFixture"/>) and compares a content digest against an ORACLE query that encodes
/// the expected filter directly (never going through the watermark code path itself), mirroring
/// <c>PostgresPartitioningTests</c>' digest-vs-oracle proof style -- so a match proves the pushdown
/// selects the exact right row set, not merely a plausible one. Each test creates its own uniquely-named
/// table (table: mode is required -- query: mode deliberately ignores hints/watermark, so
/// generate_series-only fixtures used by the partitioning suite can't exercise this path).</summary>
[Collection("postgres")]
public sealed class PostgresWatermarkTests(PostgresContainerFixture fixture)
{
    private ConnectorConfig ValidConfig => new(new Dictionary<string, object?>
    {
        ["host"] = fixture.Host,
        ["port"] = fixture.Port,
        ["database"] = fixture.Database,
        ["user"] = fixture.User,
        ["password"] = fixture.Password,
    });

    [SkippableFact]
    public async Task Watermarked_read_extracts_only_newer_rows()
    {
        const string table = "wm_only_newer";
        await ExecuteAsync($"create table if not exists public.{table} (id bigint primary key, val integer not null)");
        await ExecuteAsync($"truncate table public.{table}");
        await ExecuteAsync($"insert into public.{table} (id, val) select i, i * 2 from generate_series(1, 100) i");

        ISourceConnector connector = new PostgresConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);

        var spec = new DatasetSpec("pg", table, new Dictionary<string, object?>())
        {
            WatermarkCursor = "id",
            WatermarkValue = "60",
        };

        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
        var (count, digest) = await ReadRowsAndDigestAsync(partitions);

        var (oracleCount, oracleDigest) = await ReadOracleAsync(source, $"select * from public.{table} where id > 60");

        Assert.Equal(40, count);
        Assert.Equal(oracleCount, count);
        Assert.Equal(oracleDigest, digest);
    }

    [SkippableFact]
    public async Task Watermark_composes_with_pushdown_predicate_and_partitions()
    {
        const string table = "wm_compose_test";
        const int rowCount = 2000;
        await ExecuteAsync($"create table if not exists public.{table} (id integer primary key, val integer not null)");
        await ExecuteAsync($"truncate table public.{table}");
        await ExecuteAsync($"insert into public.{table} (id, val) select i, i * 3 from generate_series(1, {rowCount}) i");

        ISourceConnector connector = new PostgresConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);

        var spec = new DatasetSpec("pg", table, new Dictionary<string, object?>
        {
            ["partition_column"] = "id",
            ["partitions"] = 4,
        })
        {
            WatermarkCursor = "id",
            WatermarkValue = "500",
        };
        var hints = new ReadHints(PredicateSql: "val % 2 = 0");

        var partitions = await source.PlanReadAsync(spec, hints, CancellationToken.None);
        Assert.Equal(4, partitions.Count);
        var (count, digest) = await ReadRowsAndDigestAsync(partitions);

        var (oracleCount, oracleDigest) = await ReadOracleAsync(
            source, $"select * from public.{table} where id > 500 and val % 2 = 0");

        Assert.True(count > 0);
        Assert.Equal(oracleCount, count);
        Assert.Equal(oracleDigest, digest);
    }

    // The watermark AND must compose correctly with a pushdown predicate that contains a TOP-LEVEL
    // OR. SQL's AND binds tighter than OR, so an un-parenthesized join `where (id < 5 or id > 90 and
    // id > 50)` parses as `id < 5 or (id > 90 and id > 50)` -- which still admits every `id < 5` row,
    // leaking rows AT/BELOW the watermark (ids 1..4). With each term self-parenthesized the
    // composition is `(id < 5 or id > 90) and (id > 50)`, and the watermark AND binds across the whole
    // OR group. The oracle encodes exactly that intended grouping and is routed through query: mode
    // (which ignores hints/watermark), so it never touches the watermark code path under test.
    [SkippableFact]
    public async Task Watermark_composes_with_disjunctive_pushdown_predicate()
    {
        const string table = "wm_disjunctive_test";
        await ExecuteAsync($"create table if not exists public.{table} (id integer primary key, val integer not null)");
        await ExecuteAsync($"truncate table public.{table}");
        await ExecuteAsync($"insert into public.{table} (id, val) select i, i * 3 from generate_series(1, 100) i");

        ISourceConnector connector = new PostgresConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);

        var spec = new DatasetSpec("pg", table, new Dictionary<string, object?>())
        {
            WatermarkCursor = "id",
            WatermarkValue = "50",
        };
        var hints = new ReadHints(PredicateSql: "id < 5 or id > 90");

        var partitions = await source.PlanReadAsync(spec, hints, CancellationToken.None);
        var (count, digest) = await ReadRowsAndDigestAsync(partitions);

        // Intended set: (id < 5 OR id > 90) AND id > 50  ==  ids 91..100 (the id < 5 rows are all <= the
        // watermark and must be excluded). An un-parenthesized composition would ALSO leak ids 1..4.
        var (oracleCount, oracleDigest) = await ReadOracleAsync(
            source, $"select * from public.{table} where (id < 5 or id > 90) and id > 50");

        Assert.Equal(10, count);
        Assert.Equal(oracleCount, count);
        Assert.Equal(oracleDigest, digest);
    }

    // The bounded-window upper bound (WatermarkUpperBound) must compose with partitioned reads
    // exactly like the lower-bound watermark above -- it lives inside the same SELECT the min/max
    // partition-boundary probe wraps, so no partitioning-side code is involved: 100 rows (cursor
    // 1..100), lower=20/upper=40, 4 partitions over the SAME cursor column used as partition_column
    // -- exactly rows 21..40 must come back, as one multiset, across every partition.
    [SkippableFact]
    public async Task Window_upper_bound_composes_with_partitioned_read()
    {
        const string table = "wm_window_partition_test";
        await ExecuteAsync($"create table if not exists public.{table} (id integer primary key, val integer not null)");
        await ExecuteAsync($"truncate table public.{table}");
        await ExecuteAsync($"insert into public.{table} (id, val) select i, i * 5 from generate_series(1, 100) i");

        ISourceConnector connector = new PostgresConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);

        var spec = new DatasetSpec("pg", table, new Dictionary<string, object?>
        {
            ["partition_column"] = "id",
            ["partitions"] = 4,
        })
        {
            WatermarkCursor = "id",
            WatermarkValue = "20",
            WatermarkUpperBound = "40",
        };

        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
        Assert.Equal(4, partitions.Count);
        var (count, digest) = await ReadRowsAndDigestAsync(partitions);

        var (oracleCount, oracleDigest) = await ReadOracleAsync(
            source, $"select * from public.{table} where id > 20 and id <= 40");

        Assert.Equal(20, count);
        Assert.Equal(oracleCount, count);
        Assert.Equal(oracleDigest, digest);
    }

    // The four supported cursor CLR shapes with a genuine end-to-end Postgres round trip: 50 rows
    // spanning distinct cursor values, watermark set to the value ON an actual row (row 30) --
    // proving the predicate is strictly `>` (that row excluded), not `>=` (which would include it).
    // The timestamptz case is the load-bearing one: a UTC-canonical, offset-less value string filters
    // a `timestamptz` column correctly only because postgres infers the literal's type from the column
    // and the session's default TimeZone is UTC (as on the postgres:16-alpine image
    // PostgresContainerFixture uses) -- no explicit "+00" suffix is added here, deliberately unlike
    // the partition-boundary literals in PostgresSource.BuildTimestamptzBoundaries.
    [SkippableTheory]
    [InlineData("bigint", "c_bigint bigint", "i", "30")]
    [InlineData("timestamp", "c_timestamp timestamp", "timestamp '2026-01-01 00:00:00' + (i || ' minutes')::interval", "2026-01-01T00:30:00.000000")]
    [InlineData("timestamptz", "c_timestamptz timestamptz", "timestamptz '2026-01-01 00:00:00+00' + (i || ' minutes')::interval", "2026-01-01T00:30:00.000000")]
    [InlineData("date", "c_date date", "(date '2026-01-01' + (i || ' days')::interval)::date", "2026-01-31")]
    public async Task Watermark_type_matrix(string typeLabel, string columnDdl, string columnExpr, string watermarkValue)
    {
        var table = $"wm_matrix_{typeLabel}";
        var column = columnDdl.Split(' ')[0];
        await ExecuteAsync($"drop table if exists public.{table}");
        await ExecuteAsync($"create table public.{table} (id integer primary key, {columnDdl} not null)");
        await ExecuteAsync($"insert into public.{table} (id, {column}) select i, {columnExpr} from generate_series(1, 50) i");

        ISourceConnector connector = new PostgresConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);

        var spec = new DatasetSpec("pg", table, new Dictionary<string, object?>())
        {
            WatermarkCursor = column,
            WatermarkValue = watermarkValue,
        };

        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
        var (count, digest) = await ReadRowsAndDigestAsync(partitions);

        // Row 30 (the boundary value itself) must be EXCLUDED -- strictly `>`, not `>=` -- so exactly
        // the 20 rows with id 31..50 survive.
        var (oracleCount, oracleDigest) = await ReadOracleAsync(source, $"select * from public.{table} where id > 30");

        Assert.Equal(20, count);
        Assert.Equal(oracleCount, count);
        Assert.Equal(oracleDigest, digest);

        var (boundaryCount, _) = await ReadOracleAsync(source, $"select * from public.{table} where id = 30");
        Assert.Equal(1, boundaryCount); // sanity: the boundary row genuinely exists in the fixture
    }

    private async Task ExecuteAsync(string sql)
    {
        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync().ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    private static async Task<(long RowCount, string Digest)> ReadOracleAsync(ISource source, string oracleQuery)
    {
        var oracleSpec = new DatasetSpec("pg", "oracle", new Dictionary<string, object?> { ["query"] = oracleQuery });
        var oraclePartitions = await source.PlanReadAsync(oracleSpec, ReadHints.None, CancellationToken.None);
        return await ReadRowsAndDigestAsync(oraclePartitions);
    }

    /// <summary>Order-insensitive SHA-256 digest over every row's canonical rendering, across every
    /// partition. Mirrors <c>PostgresPartitioningTests.ReadRowsAndDigestAsync</c>.</summary>
    private static async Task<(long RowCount, string Digest)> ReadRowsAndDigestAsync(IReadOnlyList<IDatasetPartition> partitions)
    {
        var rows = new List<string>();
        foreach (var partition in partitions)
        {
            await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                for (var row = 0; row < batch.Length; row++)
                {
                    var rowBuilder = new StringBuilder();
                    for (var col = 0; col < batch.ColumnCount; col++)
                    {
                        if (col > 0)
                        {
                            rowBuilder.Append('');
                        }

                        rowBuilder.Append(CanonicalScalarValue(batch.Column(col), row));
                    }

                    rows.Add(rowBuilder.ToString());
                }

                batch.Dispose();
            }
        }

        rows.Sort(StringComparer.Ordinal);
        var digestBytes = SHA256.HashData(Encoding.UTF8.GetBytes(string.Join('\n', rows)));
        return (rows.Count, Convert.ToHexString(digestBytes));
    }

    private static string CanonicalScalarValue(IArrowArray array, int index)
    {
        if (array.IsNull(index))
        {
            return "<NULL>";
        }

        return array switch
        {
            Int32Array a => a.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture),
            Int64Array a => a.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture),
            DoubleArray a => a.GetValue(index)!.Value.ToString("R", CultureInfo.InvariantCulture),
            Decimal128Array a => a.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture),
            BooleanArray a => a.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture),
            Date32Array a => a.GetDateTime(index)!.Value.ToString("O", CultureInfo.InvariantCulture),
            TimestampArray a => a.GetTimestamp(index)!.Value.ToString("O", CultureInfo.InvariantCulture),
            StringArray a => a.GetString(index),
            _ => throw new NotSupportedException($"unsupported array type {array.GetType()} in watermark test digest"),
        };
    }
}
