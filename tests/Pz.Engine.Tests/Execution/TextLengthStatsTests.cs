using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Execution;

namespace Pz.Engine.Tests.Execution;

/// <summary>A sink declaring <see cref="ConnectorCapabilities.TextLengthStats"/>
/// receives <see cref="OutputSpec.MaxTextLengths"/> — per-string-column max length observed in the staged
/// relation, computed in one scan on the universal path only. Sinks without the flag see byte-identical
/// behavior (null stats, no scan).</summary>
public sealed class TextLengthStatsTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-text-length-stats-tests", Guid.NewGuid().ToString("N"));
    private DuckSession _duck = null!;

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _duck = DuckSession.Open(Path.Combine(_dir, "staging.duckdb"));
        await _duck.ExecuteAsync("create schema if not exists staging");
        await _duck.ExecuteAsync(
            "create table staging.stg_orders as select * from " +
            "(values (1, 'abcde', NULL::varchar, 1.5), (2, 'ab', NULL, 2.5)) t(id, note, empty_col, amount)");
    }

    public async Task DisposeAsync()
    {
        await _duck.DisposeAsync();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort */ }
    }

    private RunContext Context(ISinkConnector sink) => new(
        _duck, Registry(sink), new RunPaths(_dir, "test-run"), NullRunEvents.Instance,
        Batch: new BatchOptions(TargetBatchBytes: 256));

    private static ConnectorRegistry Registry(ISinkConnector sink)
    {
        var reg = new ConnectorRegistry();
        reg.AddSink("spec-recorder", sink);
        return reg;
    }

    private static DagNode SinkNode()
    {
        var sink = new ConnectionDef("api", "spec-recorder", new Dictionary<string, object?>(), [],
            "sinks/api.yml") { Outputs = [new OutputDef("out", "stg_orders", "append", "fail_on_change", new Dictionary<string, object?>())] };
        return new DagNode(new NodeId("eeeeeeeeeeeeeeee"), NodeKind.SinkWrite, "api.out",
            [], null, new SinkOutputDef(sink, sink.Outputs[0]));
    }

    [Fact]
    public async Task Flag_declaring_sink_receives_max_lengths_for_string_columns_only()
    {
        var sink = new SpecRecordingSink(ConnectorCapabilities.TextLengthStats);
        await new SinkWriteExecutor().ExecuteAsync(SinkNode(), Context(sink), default);
        var stats = sink.SeenSpec!.MaxTextLengths;
        Assert.NotNull(stats);
        Assert.Equal(5, stats!["note"]);
        Assert.False(stats.ContainsKey("empty_col")); // all-null => key absent
        Assert.False(stats.ContainsKey("id"));        // non-string => never measured
        Assert.False(stats.ContainsKey("amount"));
    }

    [Fact]
    public async Task Sink_without_the_flag_receives_null_stats()
    {
        var sink = new SpecRecordingSink(ConnectorCapabilities.None);
        await new SinkWriteExecutor().ExecuteAsync(SinkNode(), Context(sink), default);
        Assert.Null(sink.SeenSpec!.MaxTextLengths);
    }

    [Fact]
    public async Task Empty_relation_yields_an_empty_map_not_null()
    {
        await _duck.ExecuteAsync("delete from staging.stg_orders");
        var sink = new SpecRecordingSink(ConnectorCapabilities.TextLengthStats);
        await new SinkWriteExecutor().ExecuteAsync(SinkNode(), Context(sink), default);
        Assert.NotNull(sink.SeenSpec!.MaxTextLengths);
        Assert.Empty(sink.SeenSpec!.MaxTextLengths!);
    }

    private sealed class SpecRecordingSink(ConnectorCapabilities capabilities) : ISinkConnector, ISink
    {
        public OutputSpec? SeenSpec;
        public ConnectorInfo Info => new("spec-recorder", "0.0.0", ProtocolVersion.Major);
        public ConnectorCapabilities Capabilities => capabilities;
        public string ConnectionConfigSchema => "{}";
        public string DatasetConfigSchema => "{}";
        public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
            new(ValidationResult.Success);
        public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
            new(new ConnectionCheck(true));
        public ValueTask<ISink> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(this);
        public bool TryGetNativeCopy(OutputSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeCopy? copy)
        { copy = null; return false; }
        public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct)
        { SeenSpec = spec; return new(new NullSession()); }
        public ValueTask DisposeAsync() => default;

        private sealed class NullSession : ISinkWriteSession
        {
            private long _rows;
            public ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct) { _rows += batch.Length; return default; }
            public ValueTask<WriteResult> CommitAsync(CancellationToken ct) => new(new WriteResult(_rows, 1));
            public ValueTask AbortAsync(CancellationToken ct) => default;
            public ValueTask DisposeAsync() => default;
        }
    }
}
