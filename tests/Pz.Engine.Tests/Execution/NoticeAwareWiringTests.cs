using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Execution;

namespace Pz.Engine.Tests.Execution;

/// <summary>Proves the engine hands an <see cref="INoticeAware"/> source/sink the run's
/// <see cref="RunContext.Notice"/> callback exactly once, immediately after <c>OpenAsync</c> -- the
/// same site and ordering <see cref="OperationGateWiringTests"/> proves for
/// <see cref="IOperationGateAware"/> -- and that a run with no notice sink (<c>Notice: null</c>, the
/// default) never calls <c>UseNotice</c> at all rather than handing it a no-op delegate.</summary>
public sealed class NoticeAwareWiringTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-notice-tests", Guid.NewGuid().ToString("N"));
    private DuckSession _duck = null!;

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _duck = DuckSession.Open(Path.Combine(_dir, "staging.duckdb"));
        await _duck.ExecuteAsync("create schema if not exists staging");
        await _duck.ExecuteAsync("create table staging.stg_orders as select * from (values (1), (2)) t(id)");
    }

    public async Task DisposeAsync()
    {
        await _duck.DisposeAsync();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private RunContext SourceContext(ISourceConnector connector, Action<string>? notice)
    {
        var reg = new ConnectorRegistry();
        reg.AddSource("noticestub", connector);
        return new RunContext(_duck, reg, new RunPaths(_dir, "test-run"), NullRunEvents.Instance, Notice: notice);
    }

    private RunContext SinkContext(ISinkConnector connector, Action<string>? notice)
    {
        var reg = new ConnectorRegistry();
        reg.AddSink("noticestub", connector);
        return new RunContext(_duck, reg, new RunPaths(_dir, "test-run"), NullRunEvents.Instance, Notice: notice);
    }

    private static DagNode SourceNode()
    {
        var source = new ConnectionDef("mem", "noticestub", new Dictionary<string, object?>(),
            [new DatasetDef("numbers", new Dictionary<string, object?>(), null)], "sources/mem.yml");
        return new DagNode(new NodeId("eeeeeeeeeeeeeeee"), NodeKind.SourceLoad, "src_mem__numbers",
            [], null, new SourceDatasetDef(source, source.Datasets[0]));
    }

    private static DagNode SinkNode()
    {
        var sink = new ConnectionDef("api", "noticestub", new Dictionary<string, object?>(), [],
            "sinks/api.yml") { Outputs = [new OutputDef("out", "stg_orders", "append", "fail_on_change", new Dictionary<string, object?>())] };
        return new DagNode(new NodeId("eeeeeeeeeeeeeeee"), NodeKind.SinkWrite, "api.out",
            [], null, new SinkOutputDef(sink, sink.Outputs[0]));
    }

    [Fact]
    public async Task Source_receives_the_runs_notice_callback_exactly_once()
    {
        var source = new NoticeAwareFakeSource();
        var received = new List<string>();

        var result = await new SourceLoadExecutor().ExecuteAsync(
            SourceNode(), SourceContext(new NoticeStubConnector(source), received.Add), default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(1, source.UseNoticeCalls);
        source.FireNotice("unpinned host key");
        Assert.Equal(["unpinned host key"], received);
    }

    [Fact]
    public async Task Source_with_no_run_level_notice_sink_is_never_asked_for_one()
    {
        var source = new NoticeAwareFakeSource();

        var result = await new SourceLoadExecutor().ExecuteAsync(
            SourceNode(), SourceContext(new NoticeStubConnector(source), notice: null), default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(0, source.UseNoticeCalls);
    }

    [Fact]
    public async Task Sink_receives_the_runs_notice_callback_exactly_once()
    {
        var sink = new NoticeAwareFakeSink();
        var received = new List<string>();

        var result = await new SinkWriteExecutor().ExecuteAsync(
            SinkNode(), SinkContext(sink, received.Add), default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(1, sink.UseNoticeCalls);
        sink.FireNotice("unpinned host key");
        Assert.Equal(["unpinned host key"], received);
    }

    /// <summary>A source/sink that does not implement <see cref="INoticeAware"/> at all is untouched --
    /// the existing-connector regression the gate-wiring tests pin the same way.</summary>
    [Fact]
    public async Task Non_notice_aware_source_is_untouched()
    {
        var source = new PlainNoticeUnawareSource();

        var result = await new SourceLoadExecutor().ExecuteAsync(
            SourceNode(), SourceContext(new NoticeStubConnector(source), _ => Assert.Fail("never called")), default);

        Assert.Equal(NodeStatus.Success, result.Status);
    }

    private sealed class NoticeStubConnector(ISource source) : ISourceConnector
    {
        public ConnectorInfo Info => new("noticestub", "0.1.0", ProtocolVersion.Major);
        public ConnectorCapabilities Capabilities => ConnectorCapabilities.None;
        public string ConnectionConfigSchema => "{}";
        public string DatasetConfigSchema => "{}";

        public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
            new(ValidationResult.Success);

        public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
            new(new ConnectionCheck(true));

        public ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(source);
    }

    private sealed class NoticeAwareFakeSource : ISource, INoticeAware
    {
        private Action<string>? _notice;
        private int _useNoticeCalls;

        public int UseNoticeCalls => Volatile.Read(ref _useNoticeCalls);

        public void UseNotice(Action<string> notice)
        {
            Interlocked.Increment(ref _useNoticeCalls);
            _notice = notice;
        }

        public void FireNotice(string message) => _notice!(message);

        public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct) =>
            new(new DatasetSchema(NoticeFakeSchema.Schema));

        public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
        {
            scan = null;
            return false;
        }

        public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
            new(new IDatasetPartition[] { new OneRowPartition() });

        public ValueTask DisposeAsync() => default;

        private sealed class OneRowPartition : IDatasetPartition
        {
            public async IAsyncEnumerable<RecordBatch> ReadAsync(
                BatchOptions options, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
            {
                await Task.Yield();
                yield return NoticeFakeSchema.BuildBatch(1);
            }
        }
    }

    private sealed class PlainNoticeUnawareSource : ISource
    {
        public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct) =>
            new(new DatasetSchema(NoticeFakeSchema.Schema));

        public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
        {
            scan = null;
            return false;
        }

        public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
            new(System.Array.Empty<IDatasetPartition>());

        public ValueTask DisposeAsync() => default;
    }

    private sealed class NoticeAwareFakeSink : ISinkConnector, ISink, INoticeAware
    {
        private Action<string>? _notice;
        private int _useNoticeCalls;

        public int UseNoticeCalls => Volatile.Read(ref _useNoticeCalls);

        public void UseNotice(Action<string> notice)
        {
            Interlocked.Increment(ref _useNoticeCalls);
            _notice = notice;
        }

        public void FireNotice(string message) => _notice!(message);

        public ConnectorInfo Info => new("noticestub", "0.0.0", ProtocolVersion.Major);
        public ConnectorCapabilities Capabilities => ConnectorCapabilities.None;
        public string ConnectionConfigSchema => "{}";
        public string DatasetConfigSchema => "{}";

        public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
            new(ValidationResult.Success);

        public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
            new(new ConnectionCheck(true));

        public ValueTask<ISink> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(this);

        public bool TryGetNativeCopy(OutputSpec spec, [NotNullWhen(true)] out NativeCopy? copy)
        {
            copy = null;
            return false;
        }

        public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct) =>
            new(new Session());

        public ValueTask DisposeAsync() => default;

        private sealed class Session : ISinkWriteSession
        {
            private long _rows;

            public ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
            {
                _rows += batch.Length;
                return default;
            }

            public ValueTask<WriteResult> CommitAsync(CancellationToken ct) =>
                new(new WriteResult(_rows, 1));

            public ValueTask AbortAsync(CancellationToken ct) => default;

            public ValueTask DisposeAsync() => default;
        }
    }
}

/// <summary>Single int64 <c>id</c> column -- the minimal schema this file's fake source stages.</summary>
internal static class NoticeFakeSchema
{
    public static readonly Schema Schema = new([new Field("id", Int64Type.Default, nullable: false)], null);

    public static RecordBatch BuildBatch(long id)
    {
        var builder = new Int64Array.Builder();
        builder.Append(id);
        return new RecordBatch(Schema, [builder.Build()], 1);
    }
}
