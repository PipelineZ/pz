using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.Connectors.Abstractions;

namespace Pz.Engine.Tests.Execution;

/// <summary>An output whose staged input has columns pz's Arrow path does not carry (STRUCT, LIST,
/// VARIANT, SMALLINT, FLOAT…) fails PZ0546 before the connector opens a write session, naming every such
/// column with its DuckDB type and the cast that fixes it, instead of an engine failure mid-drain.</summary>
public sealed class ArrowPathColumnTypeTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-arrow-type-tests", Guid.NewGuid().ToString("N"));
    private DuckSession _duck = null!;

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _duck = DuckSession.Open(Path.Combine(_dir, "staging.duckdb"));
        await _duck.ExecuteAsync("create schema if not exists staging");
    }

    public async Task DisposeAsync()
    {
        await _duck.DisposeAsync();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private RunContext Context(ISinkConnector sink)
    {
        var registry = new ConnectorRegistry();
        registry.AddSink("cdcdelete", sink);
        return new RunContext(_duck, registry, new RunPaths(_dir, "test-run"), NullRunEvents.Instance,
            Batch: new BatchOptions(TargetBatchBytes: 256));
    }

    private static DagNode SinkNode(string strategy = "append", string? duplicates = "accept", params string[] keys)
    {
        var sink = new ConnectionDef("cap", "cdcdelete", new Dictionary<string, object?>(), [], "sinks/cap.yml")
        {
            Outputs = [new OutputDef("out", "stg_orders", strategy, "fail_on_change", new Dictionary<string, object?>(),
                keys, Duplicates: duplicates, OrderBy: strategy == "merge" ? ["id"] : null)],
        };
        return new DagNode(new NodeId("eeeeeeeeeeeeeeee"), NodeKind.SinkWrite, "cap.out", [], null,
            new SinkOutputDef(sink, sink.Outputs[0]));
    }

    [Fact]
    public async Task Unsupported_columns_fail_PZ0546_before_a_session_opens_naming_each_with_its_cast()
    {
        await _duck.ExecuteAsync("create table staging.stg_orders as select 1 as id, {'a': 1} as s, [1, 2] as l, " +
            "'{\"x\": 1}'::JSON::VARIANT as rec, 2::SMALLINT as small, 1.5::FLOAT as f, 'ok' as name");
        var sink = new PlainSinkConnector();

        var result = await new SinkWriteExecutor().ExecuteAsync(SinkNode(), Context(sink), default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        var error = result.Error!;
        Assert.Equal(PzErrorCode.SinkColumnTypeUnsupported, error.Code);
        Assert.Equal("sinks/cap.yml", error.File);
        foreach (var expected in new[] { "rec (VARIANT)", "small (SMALLINT)", "f (FLOAT)" })
        {
            Assert.Contains(expected, error.Message, StringComparison.Ordinal);
        }

        // STRUCT and LIST cross the Arrow path; they are refused separately, by this sink's missing NestedTypes.
        Assert.DoesNotContain("name", error.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("STRUCT", error.Message, StringComparison.Ordinal);
        Assert.Equal("cast in the pipeline: rec::JSON::VARCHAR, small::INTEGER, f::DOUBLE", error.Hint);
        Assert.Null(sink.Session);
    }

    [Fact]
    public async Task Nested_columns_to_a_sink_without_NestedTypes_fail_PZ0546_naming_them()
    {
        await _duck.ExecuteAsync("create table staging.stg_orders as select 1 as id, {'a': 1} as s, [1, 2] as l, 'ok' as name");
        var sink = new PlainSinkConnector();

        var result = await new SinkWriteExecutor().ExecuteAsync(SinkNode(), Context(sink), default);

        var error = result.Error!;
        Assert.Equal(PzErrorCode.SinkColumnTypeUnsupported, error.Code);
        Assert.Contains("does not accept nested column(s) s (STRUCT(a INTEGER)), l (INTEGER[])", error.Message, StringComparison.Ordinal);
        Assert.Equal("cast in the pipeline: s::JSON::VARCHAR, l::JSON::VARCHAR", error.Hint);
        Assert.Null(sink.Session);
    }

    [Fact]
    public async Task Nested_columns_reach_a_sink_that_declares_NestedTypes()
    {
        await _duck.ExecuteAsync("create table staging.stg_orders as select range as id, {'a': range, 'b': [range]} as s, " +
            "map(['k'], [range]) as m from range(3000)");
        var sink = new Pz.Connectors.TestKit.Reference.InMemoryConnector();

        var result = await new SinkWriteExecutor().ExecuteAsync(SinkNode(), Context(sink), default);

        Assert.True(result.Status == NodeStatus.Success, result.Error?.ToString());
        Assert.Equal(3000L, result.RowsMoved);
    }

    [Fact]
    public async Task The_suggested_casts_make_the_output_writable()
    {
        await _duck.ExecuteAsync("create table staging.stg_orders as select 1 as id, ({'a': 1})::JSON::VARCHAR as s, " +
            "('{\"x\": 1}'::JSON::VARIANT)::JSON::VARCHAR as rec, (2::SMALLINT)::INTEGER as small");
        var sink = new PlainSinkConnector();

        var result = await new SinkWriteExecutor().ExecuteAsync(SinkNode(), Context(sink), default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.NotNull(sink.Session);
    }

    [Fact]
    public async Task Merge_latest_reads_order_by_columns_without_tripping_on_unsupported_ones()
    {
        await _duck.ExecuteAsync("create table staging.stg_orders as select * from (values " +
            "(1, 1, {'a': 1}), (1, 2, {'a': 2})) t(k, id, s)");
        var sink = new PlainSinkConnector();

        var result = await new SinkWriteExecutor().ExecuteAsync(SinkNode("merge", "latest", "k"), Context(sink), default);

        Assert.Equal(PzErrorCode.SinkColumnTypeUnsupported, result.Error!.Code);
        Assert.Contains("s (STRUCT(a INTEGER))", result.Error.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("tags", "VARCHAR[]", "tags::JSON::VARCHAR")]
    [InlineData("m", "MAP(VARCHAR, INTEGER)", "m::JSON::VARCHAR")]
    [InlineData("u", "UNION(a INTEGER, b VARCHAR)", "u::JSON::VARCHAR")]
    [InlineData("pair", "INTEGER[2]", "pair::JSON::VARCHAR")]
    [InlineData("t", "TINYINT", "t::INTEGER")]
    [InlineData("u32", "UINTEGER", "u32::BIGINT")]
    [InlineData("u64", "UBIGINT", "u64::DECIMAL(38,0)")]
    [InlineData("ts", "TIMESTAMP_NS", "ts::TIMESTAMP")]
    [InlineData("b", "BLOB", "base64(b)")]
    [InlineData("e", "ENUM('a', 'b')", "e::VARCHAR")]
    [InlineData("at", "TIME", "at::VARCHAR")]
    [InlineData("order total", "FLOAT", "\"order total\"::DOUBLE")]
    public void Each_type_gets_a_cast_the_Arrow_path_carries(string name, string type, string cast) =>
        Assert.Equal(cast, StagedColumns.CastFor(name, type));

    [Fact]
    public async Task The_schema_peek_names_every_unsupported_column_at_once()
    {
        var ex = await Assert.ThrowsAsync<UnsupportedArrowTypeException>(() =>
            _duck.GetResultSchemaAsync("select 1 as ok, 2::SMALLINT as a, 1.5::FLOAT as b, 3::BIGINT as c, '{}'::JSON::VARIANT as d, [1] as e"));

        Assert.Equal(["a", "b", "d"], ex.Columns.Select(c => c.Name));
    }

    [Fact]
    public async Task Egress_names_every_unsupported_column_at_once()
    {
        var ex = await Assert.ThrowsAsync<UnsupportedArrowTypeException>(async () =>
        {
            await foreach (var batch in _duck.QueryArrowAsync("select 1 as ok, 2::SMALLINT as a, '{}'::JSON::VARIANT as b, {'x': 1} as c"))
            {
                batch.Dispose();
            }
        });

        Assert.Equal(["a", "b"], ex.Columns.Select(c => c.Name));
    }
}
