using Apache.Arrow;
using Pz.DuckDb;

namespace Pz.DuckDb.Tests;

public sealed class ArrowEgressTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));
    public ArrowEgressTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, recursive: true); } catch { } }
    private DuckSession Open() => DuckSession.Open(Path.Combine(_dir, "t.duckdb"));

    [Fact]
    public async Task Egress_roundtrip_matches_ingest()
    {
        await using var duck = Open();
        // DuckDB infers plain numeric literals like `1.5` as DECIMAL (verified: `typeof(range * 1.5)` is
        // `DECIMAL(21,1)`, not DOUBLE), so the multiplication needs an explicit ::double cast to actually
        // produce the DOUBLE column this test's `(DoubleArray)b.Column(2)` cast below expects.
        await duck.ExecuteAsync(
            "create table t as select range as id, 'name-' || range as name, range * 1.5::double as amount, " +
            "range % 2 = 0 as flag from range(500)");

        long rows = 0; double amountSum = 0; long flagged = 0;
        await foreach (var batch in duck.QueryArrowAsync("select * from t order by id"))
        {
            using var b = batch;
            var ids = (Apache.Arrow.Int64Array)b.Column(0);
            var amounts = (Apache.Arrow.DoubleArray)b.Column(2);
            var flags = (Apache.Arrow.BooleanArray)b.Column(3);
            for (var i = 0; i < b.Length; i++)
            {
                rows++;
                amountSum += amounts.GetValue(i)!.Value;
                if (flags.GetValue(i)!.Value) flagged++;
            }
            _ = ids;
        }

        Assert.Equal(500, rows);
        Assert.Equal(500 * 499 / 2 * 1.5, amountSum, precision: 6);
        Assert.Equal(250, flagged);
    }

    [Fact]
    public async Task Egress_result_schema_matches_batches()
    {
        await using var duck = Open();
        await duck.ExecuteAsync("create table s as select 1::int as a, 'x' as b, now()::timestamp as c");
        var schema = await duck.GetResultSchemaAsync("select * from s");

        await foreach (var batch in duck.QueryArrowAsync("select * from s"))
        {
            using var b = batch;
            Assert.Equal(schema.FieldsList.Select(f => f.Name), b.Schema.FieldsList.Select(f => f.Name));
            Assert.Equal(schema.FieldsList.Select(f => f.DataType.TypeId), b.Schema.FieldsList.Select(f => f.DataType.TypeId));
        }
    }

    [Fact]
    public async Task Large_result_streams_and_cancels_early()
    {
        await using var duck = Open();
        using var cts = new CancellationTokenSource();

        var firstBatchRows = 0L; var batches = 0;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var batch in duck.QueryArrowAsync(
                "select range as id, 'padpadpadpadpad-' || range as pad from range(5000000)",
                targetBatchBytes: 1024 * 1024, ct: cts.Token))
            {
                using var b = batch;
                batches++;
                if (batches == 1) { firstBatchRows = b.Length; }
                if (batches == 3) { cts.Cancel(); }
            }
        });

        Assert.True(firstBatchRows > 0);
        Assert.True(batches < 50, $"cancellation was not honored promptly (saw {batches} batches)");
    }

    [Fact]
    public async Task Egress_roundtrip_decimal_date_and_nulls()
    {
        await using var duck = Open();
        await duck.ExecuteAsync(
            "create table t2 (" +
            "c_int integer, c_long bigint, c_double double, c_dec decimal(38,9), " +
            "c_str varchar, c_bool boolean, c_date date, c_ts timestamp)");
        await duck.ExecuteAsync(
            "insert into t2 values " +
            "(1, 100, 1.5, 12345.123456789, 'row1', true, '2026-01-01', '2026-01-01 00:00:01'), " +
            "(null, null, null, null, null, null, null, null), " +
            "(3, 300, 3.5, -67890.987654321, 'row3', false, '2026-03-03', '2026-03-03 03:03:03'), " +
            "(null, null, null, null, null, null, null, null), " +
            "(5, 500, 5.5, 11111.111111111, 'row5', true, '2026-05-05', '2026-05-05 05:05:05')");

        // Row order: 0=value, 1=NULL, 2=value, 3=NULL, 4=value — nulls interleaved as required.
        var nullRows = new[] { 1, 3 };
        var expectedInt = new int?[] { 1, null, 3, null, 5 };
        var expectedLong = new long?[] { 100, null, 300, null, 500 };
        var expectedDouble = new double?[] { 1.5, null, 3.5, null, 5.5 };
        var expectedDec = new decimal?[]
        {
            12345.123456789m, null, -67890.987654321m, null, 11111.111111111m,
        };
        var expectedStr = new string?[] { "row1", null, "row3", null, "row5" };
        var expectedBool = new bool?[] { true, null, false, null, true };
        var expectedDate = new DateOnly?[]
        {
            new(2026, 1, 1), null, new(2026, 3, 3), null, new(2026, 5, 5),
        };
        var expectedTs = new DateTimeOffset?[]
        {
            new(2026, 1, 1, 0, 0, 1, TimeSpan.Zero), null,
            new(2026, 3, 3, 3, 3, 3, TimeSpan.Zero), null,
            new(2026, 5, 5, 5, 5, 5, TimeSpan.Zero),
        };

        var rows = 0;
        await foreach (var batch in duck.QueryArrowAsync("select * from t2 order by rowid"))
        {
            using var b = batch;
            var ints = (Apache.Arrow.Int32Array)b.Column(0);
            var longs = (Apache.Arrow.Int64Array)b.Column(1);
            var doubles = (Apache.Arrow.DoubleArray)b.Column(2);
            var decs = (Apache.Arrow.Decimal128Array)b.Column(3);
            var strs = (Apache.Arrow.StringArray)b.Column(4);
            var bools = (Apache.Arrow.BooleanArray)b.Column(5);
            var dates = (Apache.Arrow.Date32Array)b.Column(6);
            var timestamps = (Apache.Arrow.TimestampArray)b.Column(7);

            for (var i = 0; i < b.Length; i++)
            {
                var expectNull = System.Array.IndexOf(nullRows, rows) >= 0;

                Assert.Equal(expectNull, ints.IsNull(i));
                Assert.Equal(expectNull, longs.IsNull(i));
                Assert.Equal(expectNull, doubles.IsNull(i));
                Assert.Equal(expectNull, decs.IsNull(i));
                Assert.Equal(expectNull, strs.IsNull(i));
                Assert.Equal(expectNull, bools.IsNull(i));
                Assert.Equal(expectNull, dates.IsNull(i));
                Assert.Equal(expectNull, timestamps.IsNull(i));

                Assert.Equal(expectedInt[rows], ints.GetValue(i));
                Assert.Equal(expectedLong[rows], longs.GetValue(i));
                Assert.Equal(expectedDouble[rows], doubles.GetValue(i));
                Assert.Equal(expectedDec[rows], decs.GetValue(i));
                Assert.Equal(expectedStr[rows], expectNull ? null : strs.GetString(i));
                Assert.Equal(expectedBool[rows], bools.GetValue(i));
                Assert.Equal(expectedDate[rows], dates.GetDateOnly(i));
                Assert.Equal(expectedTs[rows], timestamps.GetTimestamp(i));

                rows++;
            }
        }

        Assert.Equal(5, rows);
    }

    [Fact]
    public async Task GetResultSchemaAsync_reports_decimal_precision_and_scale()
    {
        // Regression test for a bug where GetResultSchemaAsync's `limit 0` schema peek silently reported
        // precision=0/scale=0 for a DECIMAL column -- any zero-row result did, not just this wrap -- because
        // the prior implementation derived DECIMAL precision/scale from the first fetched Arrow data chunk's
        // vector (DuckDBDataReader.GetSchemaTable()), and a zero-row result has no data chunk to read it
        // from. The real, unwrapped query (with actual rows) reported the correct precision/scale via the
        // same lookup, which is what let this go unnoticed: the bug was specific to schema-only peeks over
        // an empty result, not to DECIMAL handling in general.
        await using var duck = Open();
        await duck.ExecuteAsync(
            "create table t2 (" +
            "c_int integer, c_long bigint, c_double double, c_dec decimal(38,9), " +
            "c_str varchar, c_bool boolean, c_date date, c_ts timestamp)");
        await duck.ExecuteAsync("insert into t2 values (1, 100, 1.5, 12345.123456789, 'row1', true, '2026-01-01', '2026-01-01 00:00:01')");

        var schema = await duck.GetResultSchemaAsync("select * from t2");

        var dec = (Apache.Arrow.Types.Decimal128Type)schema.FieldsList.Single(f => f.Name == "c_dec").DataType;
        Assert.Equal(38, dec.Precision);
        Assert.Equal(9, dec.Scale);

        // The timestamp normalization this fix also needs (DuckDB's native Arrow export reports an empty
        // timezone for a plain TIMESTAMP column) -- confirms it still matches the "+00:00" convention every
        // other Arrow schema this codebase produces for that DuckDB column type uses.
        var ts = (Apache.Arrow.Types.TimestampType)schema.FieldsList.Single(f => f.Name == "c_ts").DataType;
        Assert.Equal("+00:00", ts.Timezone);
    }

    [Fact]
    public async Task GetResultSchemaAsync_decimal_precision_and_scale_survive_an_actually_empty_result()
    {
        // The narrowest reproduction of the bug this fix addresses: a genuinely zero-row result (not just
        // the `limit 0` wrap GetResultSchemaAsync always applies), which is exactly the shape the schema
        // peek always produces.
        await using var duck = Open();
        await duck.ExecuteAsync("create table t4 (c_dec decimal(38,9))");

        var schema = await duck.GetResultSchemaAsync("select * from t4 where 1 = 0");

        var dec = (Apache.Arrow.Types.Decimal128Type)schema.FieldsList.Single(f => f.Name == "c_dec").DataType;
        Assert.Equal(38, dec.Precision);
        Assert.Equal(9, dec.Scale);
    }

    [Fact]
    public async Task GetResultSchemaAsync_unsupported_type_throws_named_error()
    {
        await using var duck = Open();

        var ex = await Assert.ThrowsAsync<NotSupportedException>(
            async () => await duck.GetResultSchemaAsync("select [1, 2, 3] as xs"));

        Assert.Contains("xs", ex.Message);
    }

    [Fact]
    public async Task Egress_unsupported_type_throws_named_error()
    {
        await using var duck = Open();

        var ex = await Assert.ThrowsAsync<NotSupportedException>(async () =>
        {
            await foreach (var batch in duck.QueryArrowAsync("select [1, 2, 3] as xs"))
            {
                using var b = batch;
            }
        });

        Assert.Contains("xs", ex.Message);
        Assert.Contains("List", ex.Message);
    }

    [Fact]
    public async Task Egress_first_batch_arrives_before_reader_exhausted()
    {
        await using var duck = Open();

        long firstBatchRowsSoFar = -1;
        duck.OnEgressBatchProducedForTests = rowsSoFar =>
        {
            if (firstBatchRowsSoFar < 0) { firstBatchRowsSoFar = rowsSoFar; }
        };

        try
        {
            await foreach (var batch in duck.QueryArrowAsync(
                "select range as id from range(5000000)", targetBatchBytes: 256 * 1024))
            {
                using var b = batch;
                break; // only the first batch's arrival matters — see assertion below
            }
        }
        finally
        {
            duck.OnEgressBatchProducedForTests = null;
        }

        Assert.True(firstBatchRowsSoFar > 0, "no batch was produced before the reader stopped");
        // A full-buffering producer reads all 5,000,000 rows before its first channel write; a genuinely
        // streaming producer's first batch lands after only a small slice of the result has been read.
        Assert.True(firstBatchRowsSoFar < 1_000_000,
            $"first batch only arrived after {firstBatchRowsSoFar} of 5,000,000 rows were read — looks like full buffering");
    }
}
