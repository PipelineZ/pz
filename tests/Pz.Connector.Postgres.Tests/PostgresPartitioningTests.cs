using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;

namespace Pz.Connector.Postgres.Tests;

/// <summary>Postgres range-partitioned parallel reads. Every dataset here is a <c>query:</c>-mode
/// <c>generate_series</c> expression (no new fixture
/// tables needed) against the shared Testcontainers postgres instance (<see cref="PostgresContainerFixture"/>).
/// The content-digest helper below mirrors <c>SourceConnectorAcceptanceTests.ReadRowsAndDigest</c>'s
/// order-insensitive multiset proof pattern.</summary>
[Collection("postgres")]
public sealed class PostgresPartitioningTests(PostgresContainerFixture fixture)
{
    private ConnectorConfig ValidConfig => new(new Dictionary<string, object?>
    {
        ["host"] = fixture.Host,
        ["port"] = fixture.Port,
        ["database"] = fixture.Database,
        ["user"] = fixture.User,
        ["password"] = fixture.Password,
    });

    // 500k rows, integer partition_column with ~1/13th of the rows NULLed out -- large enough that
    // equal-width [lo,hi) boundary math over 4 partitions genuinely exercises multiple boundary crossings,
    // not just one.
    private static readonly Dictionary<string, object?> FiveHundredKQuery = new()
    {
        ["query"] = "select i as id, case when i % 13 = 0 then null else i end as part_col, i * 2 as val " +
            "from generate_series(1, 500000) i",
    };

    [SkippableFact]
    public async Task Partitioned_read_union_equals_single_read()
    {
        ISourceConnector connector = new PostgresConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);

        var fourPartitionSpec = new DatasetSpec("pg", "partition-500k", new Dictionary<string, object?>(FiveHundredKQuery)
        {
            ["partition_column"] = "part_col",
            ["partitions"] = 4,
        });
        var onePartitionSpec = new DatasetSpec("pg", "partition-500k", new Dictionary<string, object?>(FiveHundredKQuery)
        {
            ["partition_column"] = "part_col",
            ["partitions"] = 1,
        });

        var fourPartitions = await source.PlanReadAsync(fourPartitionSpec, ReadHints.None, CancellationToken.None);
        Assert.Equal(4, fourPartitions.Count);
        var (fourCount, fourDigest) = await ReadRowsAndDigestAsync(fourPartitions);

        var onePartition = await source.PlanReadAsync(onePartitionSpec, ReadHints.None, CancellationToken.None);
        Assert.Single(onePartition);
        var (oneCount, oneDigest) = await ReadRowsAndDigestAsync(onePartition);

        Assert.Equal(500_000, oneCount);
        Assert.Equal(oneCount, fourCount);
        Assert.Equal(oneDigest, fourDigest);
    }

    [SkippableFact]
    public async Task Null_partition_values_are_not_lost()
    {
        ISourceConnector connector = new PostgresConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);

        const int rowCount = 1_000;
        var spec = new DatasetSpec("pg", "null-partition-test", new Dictionary<string, object?>
        {
            ["query"] = $"select i as id, case when i % 3 = 0 then null else i end as part_col " +
                $"from generate_series(1, {rowCount}) i",
            ["partition_column"] = "part_col",
            ["partitions"] = 4,
        });

        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
        Assert.Equal(4, partitions.Count);

        var (total, nullCount) = await CountRowsAndNullsAsync(partitions, columnIndex: 1);

        var expectedNulls = Enumerable.Range(1, rowCount).Count(i => i % 3 == 0);
        Assert.Equal(rowCount, total);
        Assert.Equal(expectedNulls, nullCount);
        Assert.True(nullCount > 0, "fixture must actually contain NULL partition-column rows");
    }

    [SkippableFact]
    public async Task Temporal_partition_column_works()
    {
        ISourceConnector connector = new PostgresConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);

        const string queryTemplate = "select i as id, timestamptz '2026-01-01T00:00:00Z' + (i || ' seconds')::interval as ts " +
            "from generate_series(1, 2000) i";

        var fourPartitionSpec = new DatasetSpec("pg", "temporal-partition-test", new Dictionary<string, object?>
        {
            ["query"] = queryTemplate,
            ["partition_column"] = "ts",
            ["partitions"] = 4,
        });
        var onePartitionSpec = new DatasetSpec("pg", "temporal-partition-test", new Dictionary<string, object?>
        {
            ["query"] = queryTemplate,
            ["partition_column"] = "ts",
            ["partitions"] = 1,
        });

        var fourPartitions = await source.PlanReadAsync(fourPartitionSpec, ReadHints.None, CancellationToken.None);
        Assert.Equal(4, fourPartitions.Count);
        var (fourCount, fourDigest) = await ReadRowsAndDigestAsync(fourPartitions);

        var onePartition = await source.PlanReadAsync(onePartitionSpec, ReadHints.None, CancellationToken.None);
        var (oneCount, oneDigest) = await ReadRowsAndDigestAsync(onePartition);

        Assert.Equal(2000, oneCount);
        Assert.Equal(oneCount, fourCount);
        Assert.Equal(oneDigest, fourDigest);
    }

    // The 4 boundary-literal type branches Partitioned_read_union_equals_single_read (int) and
    // Temporal_partition_column_works (timestamptz) leave unproven -- double, decimal, date, and
    // timestamp-without-tz all dispatch to their own Build*Boundaries method (see PostgresSource.ComputeBoundaryLiterals),
    // each with its own literal-formatting logic (G17 for double, plain ToString for decimal, `date '...'`/`timestamp
    // '...'` literals for the temporal two). A modest ~2000-row fixture per type, with roughly 1/13th NULLed out and
    // the column's own natural min/max riding the first/last generated row (a boundary-edge value), reused against
    // the same 4-partition-vs-1-partition content-digest proof: any literal a Build*Boundaries method renders that
    // postgres parses back to a shifted or malformed value would move a row across a partition boundary and change
    // the digest.
    [SkippableTheory]
    [InlineData("double", "case when i % 13 = 0 then null else (i * 0.1234567)::double precision end")]
    [InlineData("decimal", "case when i % 13 = 0 then null else (i * 0.01)::numeric(10,4) end")]
    [InlineData("date", "case when i % 13 = 0 then null else (date '2020-01-01' + (i || ' days')::interval)::date end")]
    [InlineData("timestamp", "case when i % 13 = 0 then null else (timestamp '2020-01-01 00:00:00' + (i || ' seconds')::interval) end")]
    public async Task Boundary_literal_types_produce_matching_digests(string typeLabel, string partColExpr)
    {
        const int rowCount = 2000;
        ISourceConnector connector = new PostgresConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);

        var query = $"select i as id, {partColExpr} as part_col from generate_series(1, {rowCount}) i";
        var fourPartitionSpec = new DatasetSpec("pg", $"boundary-{typeLabel}", new Dictionary<string, object?>
        {
            ["query"] = query,
            ["partition_column"] = "part_col",
            ["partitions"] = 4,
        });
        var onePartitionSpec = fourPartitionSpec with
        {
            Options = new Dictionary<string, object?>(fourPartitionSpec.Options) { ["partitions"] = 1 },
        };

        var fourPartitions = await source.PlanReadAsync(fourPartitionSpec, ReadHints.None, CancellationToken.None);
        Assert.Equal(4, fourPartitions.Count);
        var (fourCount, fourDigest) = await ReadRowsAndDigestAsync(fourPartitions);

        var onePartition = await source.PlanReadAsync(onePartitionSpec, ReadHints.None, CancellationToken.None);
        Assert.Single(onePartition);
        var (oneCount, oneDigest) = await ReadRowsAndDigestAsync(onePartition);

        Assert.Equal(rowCount, oneCount);
        Assert.Equal(oneCount, fourCount);
        Assert.Equal(oneDigest, fourDigest);
    }

    [SkippableFact]
    public async Task Degenerate_range_yields_single_partition()
    {
        ISourceConnector connector = new PostgresConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);

        // min == max: every row shares the same partition_column value.
        var constantSpec = new DatasetSpec("pg", "degenerate-constant", new Dictionary<string, object?>
        {
            ["query"] = "select 42 as val from generate_series(1, 5) i",
            ["partition_column"] = "val",
            ["partitions"] = 4,
        });
        var constantPartitions = await source.PlanReadAsync(constantSpec, ReadHints.None, CancellationToken.None);
        Assert.Single(constantPartitions);
        var (constantCount, _) = await ReadRowsAndDigestAsync(constantPartitions);
        Assert.Equal(5, constantCount);

        // Empty table: min/max both NULL.
        var emptySpec = new DatasetSpec("pg", "degenerate-empty", new Dictionary<string, object?>
        {
            ["query"] = "select i as val from generate_series(1, 0) i",
            ["partition_column"] = "val",
            ["partitions"] = 4,
        });
        var emptyPartitions = await source.PlanReadAsync(emptySpec, ReadHints.None, CancellationToken.None);
        Assert.Single(emptyPartitions);
        var (emptyCount, _) = await ReadRowsAndDigestAsync(emptyPartitions);
        Assert.Equal(0, emptyCount);
    }

    [SkippableTheory]
    [InlineData(0)]
    [InlineData(17)]
    public async Task Invalid_partition_count_is_a_named_error(int partitions)
    {
        ISourceConnector connector = new PostgresConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var spec = new DatasetSpec("pg", "orders", new Dictionary<string, object?>
        {
            ["partition_column"] = "id",
            ["partitions"] = partitions,
        });

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("between 1 and 16", ex.Message, StringComparison.Ordinal);
        Assert.Contains(partitions.ToString(CultureInfo.InvariantCulture), ex.Message, StringComparison.Ordinal);
    }

    [SkippableFact]
    public async Task Non_orderable_partition_column_is_a_named_error()
    {
        ISourceConnector connector = new PostgresConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var spec = new DatasetSpec("pg", "orders", new Dictionary<string, object?>
        {
            ["partition_column"] = "name", // text -- not orderable
            ["partitions"] = 4,
        });

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("'name'", ex.Message, StringComparison.Ordinal);
        Assert.Contains("not orderable", ex.Message, StringComparison.Ordinal);
        Assert.Contains("integer", ex.Message, StringComparison.Ordinal);
    }

    [SkippableFact]
    public async Task Unknown_partition_column_is_a_named_error()
    {
        ISourceConnector connector = new PostgresConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var spec = new DatasetSpec("pg", "orders", new Dictionary<string, object?>
        {
            ["partition_column"] = "does_not_exist",
            ["partitions"] = 4,
        });

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("does_not_exist", ex.Message, StringComparison.Ordinal);
    }

    private static async Task<(long RowCount, long NullCount)> CountRowsAndNullsAsync(
        IReadOnlyList<IDatasetPartition> partitions, int columnIndex)
    {
        long rows = 0;
        long nulls = 0;
        foreach (var partition in partitions)
        {
            await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                var column = batch.Column(columnIndex);
                for (var i = 0; i < batch.Length; i++)
                {
                    if (column.IsNull(i))
                    {
                        nulls++;
                    }
                }

                rows += batch.Length;
                batch.Dispose();
            }
        }

        return (rows, nulls);
    }

    /// <summary>Order-insensitive SHA-256 digest over every row's canonical rendering, across every
    /// partition -- proves a multi-partition read is the exact same MULTISET as a single-partition read
    /// (not merely the same count), catching a dropped/duplicated row at a partition boundary that a
    /// count-only comparison would miss. Mirrors <c>SourceConnectorAcceptanceTests.ReadRowsAndDigest</c>.</summary>
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
            _ => throw new NotSupportedException($"unsupported array type {array.GetType()} in partitioning test digest"),
        };
    }
}
