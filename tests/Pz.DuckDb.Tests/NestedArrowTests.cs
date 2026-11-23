using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions.Memory;
using Pz.DuckDb;

namespace Pz.DuckDb.Tests;

/// <summary>STRUCT, LIST, MAP and fixed-size ARRAY columns of the flat types cross pz's Arrow path both ways:
/// DuckDB's export, coalesced across chunks into pooled memory, ingested back as the same DuckDB types with
/// every value (and every null, at every level) intact. VARIANT, UNION and other-unit timestamps stay
/// refused.</summary>
public sealed class NestedArrowTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-nested-arrow-tests", Guid.NewGuid().ToString("N"));

    public NestedArrowTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private const string NestedTable = """
        create table t as select
            range as id,
            case when range % 11 = 0 then null else [range, range + 1, null][1:(range % 4)::INTEGER] end as ints,
            case when range % 13 = 0 then null else ['s' || range, null] end as strs,
            case when range % 17 = 0 then null else
                {'a': range::INTEGER, 'b': case when range % 3 = 0 then null else 'x' || range end,
                 'l': [range * 1.5], 'd': {'when': DATE '2026-01-01' + (range % 30)::INTEGER}} end as rec,
            case when range % 19 = 0 then null else map(['k' || (range % 5)], [range::INTEGER]) end as m,
            case when range % 23 = 0 then null else [range::INTEGER, -range::INTEGER]::INTEGER[2] end as pair,
            [{'k': range % 7, 'v': [true, null]}, null] as items,
            [[range], [], null] as nested_lists,
            {'ts': TIMESTAMP '2026-01-01 00:00:00' + to_seconds(range)} as stamp
        from range(10000)
        """;

    [Theory]
    [InlineData(64 * 1024)]          // many output batches, each several DuckDB chunks coalesced
    [InlineData(32 * 1024 * 1024)]   // one output batch from every chunk
    public async Task Nested_columns_round_trip_through_egress_coalescing_and_ingest(int targetBatchBytes)
    {
        await using var duck = DuckSession.Open(Path.Combine(_dir, $"rt{targetBatchBytes}.duckdb"));
        var allocator = new PooledNativeAllocator();
        duck.EgressAllocator = allocator;
        await duck.ExecuteAsync(NestedTable);

        var batches = new List<RecordBatch>();
        await foreach (var batch in duck.QueryArrowAsync("select * from t order by id", targetBatchBytes))
        {
            batches.Add(batch);
        }

        Assert.True(allocator.RentedBytes > 0); // every output buffer is pooled memory, not a DuckDB chunk's
        async IAsyncEnumerable<RecordBatch> Feed()
        {
            foreach (var batch in batches)
            {
                yield return batch;
                await Task.Yield();
            }
        }

        // Egress batches carry DuckDB's own schema (a naive timestamp stays naive); the schema peek is what
        // marks timestamps UTC for sinks.
        Assert.Equal(10000L, await duck.IngestArrowAsync("back", batches[0].Schema, Feed()));
        Assert.Equal(0, allocator.RentedBytes); // ingest disposed every batch: nothing leaked, nothing shared

        Assert.Equal(
            await duck.ScalarAsync<string>("select string_agg(column_type, ',' order by column_name) from (describe t)"),
            await duck.ScalarAsync<string>("select string_agg(column_type, ',' order by column_name) from (describe back)"));
        Assert.Equal(0L, await duck.ScalarAsync<long>(
            "select count(*) from ((select * from t except all select * from back) union all (select * from back except all select * from t))"));
    }

    [Fact]
    public async Task The_schema_peek_carries_nested_types_with_utc_timestamps_at_depth()
    {
        await using var duck = DuckSession.Open(Path.Combine(_dir, "peek.duckdb"));

        var schema = await duck.GetResultSchemaAsync(
            "select [1] as l, {'a': 1, 'ts': TIMESTAMP '2026-01-01'} as s, map(['k'], [1.5]) as m, [1, 2]::INTEGER[2] as f");

        Assert.IsType<ListType>(schema.FieldsList[0].DataType);
        var @struct = Assert.IsType<StructType>(schema.FieldsList[1].DataType);
        Assert.Equal("+00:00", Assert.IsType<TimestampType>(@struct.Fields[1].DataType).Timezone);
        Assert.IsType<MapType>(schema.FieldsList[2].DataType);
        Assert.IsType<FixedSizeListType>(schema.FieldsList[3].DataType);
    }

    [Theory]
    [InlineData("'{\"x\": 1}'::JSON::VARIANT")]
    [InlineData("union_value(n := 1)")]
    [InlineData("{'ts': TIMESTAMP_NS '2026-01-01'}")]
    [InlineData("[1.5::FLOAT]")]
    [InlineData("{'v': '{}'::JSON::VARIANT}")]
    public async Task Variant_union_and_nested_unmapped_leaves_stay_refused(string expression)
    {
        await using var duck = DuckSession.Open(Path.Combine(_dir, "refused.duckdb"));

        var ex = await Assert.ThrowsAsync<UnsupportedArrowTypeException>(() =>
            duck.GetResultSchemaAsync($"select 1 as ok, {expression} as c"));

        Assert.Equal("c", Assert.Single(ex.Columns).Name);
    }
}
