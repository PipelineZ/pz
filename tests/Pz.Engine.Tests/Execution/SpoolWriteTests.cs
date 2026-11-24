using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.Engine.Planning;

namespace Pz.Engine.Tests.Execution;

/// <summary>The spool tier: DuckDB writes the staged output to local files in the sink's format, nested columns
/// intact (a null list stays null, an empty one empty), one file per partition folder rendered from the column's
/// UTC value whatever the session time zone, and the files are gone once the sink has delivered them or failed.</summary>
public sealed class SpoolWriteTests : IAsyncLifetime
{
    private static readonly NodeId Node = new("5555555555555555");

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-spool-tests", Guid.NewGuid().ToString("N"));
    private DuckSession _duck = null!;

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _duck = DuckSession.Open(Path.Combine(_dir, "staging.duckdb"));
        await _duck.ExecuteAsync("create schema if not exists staging");
        await _duck.ExecuteAsync("""
            create table staging.stg_events as select * from (values
                (1, ['a', 'b'], {'name': 'ann', 'n': 1}, timestamptz '2024-01-15 12:00:00+00'),
                (2, [], null, timestamptz '2024-02-01 02:00:00+00'),
                (3, null, {'name': null, 'n': 3}, timestamptz '2024-02-20 00:00:00+00')
            ) t(id, tags, person, "at")
            """);
        // Not UTC: 2024-02-01 02:00 UTC is still January here, so a folder rendered in session time would be wrong.
        await _duck.ExecuteAsync("set TimeZone = 'America/New_York'");
    }

    public async Task DisposeAsync()
    {
        await _duck.DisposeAsync();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort */ }
    }

    private RunContext Context(ISinkConnector sink)
    {
        var registry = new ConnectorRegistry();
        registry.AddSink("spooler", sink);
        var plan = new ExecutionPlan(
            [new PlannedNode(Node, NodeKind.SinkWrite, "lake.out", EdgeStrategy.Spool, 1, "spool")],
            new MemoryBudget(1L << 30, null, 1L << 28, 1L << 28, 1L << 31));
        return new RunContext(_duck, registry, new RunPaths(_dir, "20260101T000000000Z-spol"), NullRunEvents.Instance, Plan: plan);
    }

    private static DagNode SinkNode()
    {
        var sink = new ConnectionDef("lake", "spooler", new Dictionary<string, object?>(), [], "connections.yml")
        {
            Outputs = [new OutputDef("out", "stg_events", "replace", "fail_on_change", new Dictionary<string, object?>())],
        };
        return new DagNode(Node, NodeKind.SinkWrite, "lake.out", [], null, new SinkOutputDef(sink, sink.Outputs[0]));
    }

    private string SpoolDir => Path.Combine(_dir, ".pz", "runs", "20260101T000000000Z-spol", "spool", Node.Value);

    [Fact]
    public async Task Nested_columns_land_in_the_spooled_parquet_file()
    {
        var sink = new SpoolingSink(_duck, new SpoolPlan("FORMAT parquet", "parquet"));
        var ctx = Context(sink);

        var result = await new SinkWriteExecutor().ExecuteAsync(SinkNode(), ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(3, result.RowsMoved);
        Assert.Equal(NodeFacts.Spool, ctx.FactsFor(Node).Strategy);
        var file = Assert.Single(sink.Delivered);
        Assert.Equal("", file.Folder);
        Assert.Equal(3, file.Rows);
        Assert.Equal("1|[a, b]|{'name': ann, 'n': 1} 2|[]|NULL 3|NULL|{'name': NULL, 'n': 3}", file.Content);
        Assert.False(Directory.Exists(SpoolDir));
    }

    [Fact]
    public async Task A_partitioned_output_spools_one_file_per_folder_rendered_in_UTC()
    {
        var sink = new SpoolingSink(_duck, new SpoolPlan("FORMAT parquet", "parquet")
        {
            PartitionColumn = "at",
            PathTemplate = "out/{yyyy}/{MM}",
        });

        var result = await new SinkWriteExecutor().ExecuteAsync(SinkNode(), Context(sink), default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(["out/2024/01", "out/2024/02"], sink.Delivered.Select(f => f.Folder));
        Assert.Equal([1L, 2L], sink.Delivered.Select(f => f.Rows));
        Assert.StartsWith("2|[]|NULL 3|", sink.Delivered[1].Content, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Csv_spools_nested_columns_as_JSON_text()
    {
        var sink = new SpoolingSink(_duck, new SpoolPlan("FORMAT csv, HEADER true", "csv") { NestedAsJson = true },
            readAs: "read_csv('{0}', all_varchar = true)");

        await new SinkWriteExecutor().ExecuteAsync(SinkNode(), Context(sink), default);

        Assert.StartsWith("1|[\"a\",\"b\"]|{\"name\":\"ann\",\"n\":1}", Assert.Single(sink.Delivered).Content, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_null_partition_value_fails_before_delivery_and_leaves_no_spool()
    {
        await _duck.ExecuteAsync("update staging.stg_events set \"at\" = null where id = 2");
        var sink = new SpoolingSink(_duck, new SpoolPlan("FORMAT parquet", "parquet") { PartitionColumn = "at", PathTemplate = "{yyyy}" });

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            new SinkWriteExecutor().ExecuteAsync(SinkNode(), Context(sink), default));

        Assert.Equal("partition_by column 'at' has a null value in 1 row(s); a null partition value cannot be routed to a partition folder", ex.Message);
        Assert.Empty(sink.Delivered);
        Assert.False(Directory.Exists(SpoolDir));
    }

    private sealed record Delivery(string Folder, long Rows, string Content);

    private sealed class SpoolingSink(DuckSession duck, SpoolPlan plan, string readAs = "read_parquet('{0}')")
        : ISinkConnector, ISink, ISpoolingSink
    {
        public List<Delivery> Delivered { get; } = [];

        public ConnectorInfo Info => new("spooler", "0.0.0", ProtocolVersion.Major);
        public ConnectorCapabilities Capabilities => ConnectorCapabilities.SpoolWrites;
        public string ConnectionConfigSchema => "{}";
        public string DatasetConfigSchema => "{}";

        public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) => new(ValidationResult.Success);

        public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) => new(new ConnectionCheck(true));

        public ValueTask<ISink> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(this);

        public bool TryGetNativeCopy(OutputSpec spec, [NotNullWhen(true)] out NativeCopy? copy)
        {
            copy = null;
            return false;
        }

        public bool TryGetSpool(OutputSpec spec, [NotNullWhen(true)] out SpoolPlan? spool)
        {
            spool = plan;
            return true;
        }

        public async ValueTask<WriteResult> CommitSpoolAsync(OutputSpec spec, IReadOnlyList<SpooledFile> files, CancellationToken ct)
        {
            foreach (var file in files)
            {
                var source = string.Format(System.Globalization.CultureInfo.InvariantCulture, readAs, file.LocalPath.Replace("'", "''", StringComparison.Ordinal));
                var content = await duck.ScalarAsync<string>(
                    $"select string_agg(concat_ws('|', id, coalesce(tags::varchar, 'NULL'), coalesce(person::varchar, 'NULL')), ' ' order by id) from {source}", ct);
                Delivered.Add(new Delivery(file.Folder, file.Rows, content));
            }

            return new WriteResult(files.Sum(f => f.Rows), files.Count);
        }

        public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct) =>
            throw new InvalidOperationException("the spool tier must not open an Arrow session");

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
