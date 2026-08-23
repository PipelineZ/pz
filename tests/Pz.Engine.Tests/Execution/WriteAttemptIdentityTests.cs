using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.Engine.Resilience;

namespace Pz.Engine.Tests.Execution;

/// <summary>Nothing reaching a sink used to distinguish a first attempt at a write from a retry of it,
/// so a sink whose destination CAN record a durable progress marker had no way to use one — which is
/// what keeps <c>append</c> at at-least-once for every such sink. <see cref="OutputSpec.Attempt"/>
/// carries that identity now; these facts pin what it is filled with.</summary>
public sealed class WriteAttemptIdentityTests : IAsyncLifetime
{
    private const string RunId = "20260101T000000000Z-abcd";
    private static readonly NodeId Node = new("eeeeeeeeeeeeeeee");

    private readonly string _dir = Path.Combine(
        Path.GetTempPath(), "pz-write-attempt-tests", Guid.NewGuid().ToString("N"));
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
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort */ }
    }

    private RunContext Context(ISinkConnector sink)
    {
        var registry = new ConnectorRegistry();
        registry.AddSink("attempt-recorder", sink);
        return new RunContext(_duck, registry, new RunPaths(_dir, RunId), NullRunEvents.Instance);
    }

    private static DagNode SinkNode()
    {
        var sink = new ConnectionDef("api", "attempt-recorder", new Dictionary<string, object?>(), [],
            "connections.yml")
        {
            Outputs = [new OutputDef("out", "stg_orders", "append", "fail_on_change", new Dictionary<string, object?>())],
        };
        return new DagNode(Node, NodeKind.SinkWrite, "api.out", [], null, new SinkOutputDef(sink, sink.Outputs[0]));
    }

    [Fact]
    public async Task A_sink_is_told_which_node_run_and_attempt_it_is_writing_for()
    {
        var sink = new AttemptRecordingSink();

        await new SinkWriteExecutor().ExecuteAsync(SinkNode(), Context(sink), default);

        var attempt = Assert.Single(sink.SeenAttempts);
        Assert.NotNull(attempt);
        Assert.Equal(Node.Value, attempt!.Node);
        Assert.Equal(RunId, attempt.Run);
        Assert.Equal(1, attempt.Ordinal);
    }

    /// <summary>The case duplicates actually come from: a commit that reached the destination and then
    /// failed to report back. The engine can only treat that as a failure and retry, so the retry has to
    /// arrive carrying the SAME write identity and a HIGHER ordinal — otherwise a sink cannot tell it
    /// apart from a genuinely new write.</summary>
    [Fact]
    public async Task A_retry_keeps_the_write_identity_and_advances_the_ordinal()
    {
        var sink = new AttemptRecordingSink { FailCommitsBefore = 3 };
        var executor = new KindDispatchingExecutor(
            new RetryPolicy(MaxAttempts: 3, BaseDelay: TimeSpan.Zero, MaxDelay: TimeSpan.Zero),
            delay: (_, _) => Task.CompletedTask);

        var result = await executor.ExecuteAsync(SinkNode(), Context(sink), default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(3, sink.SeenAttempts.Count);
        Assert.All(sink.SeenAttempts, a =>
        {
            Assert.Equal(Node.Value, a!.Node);
            Assert.Equal(RunId, a.Run);
        });
        Assert.Equal([1, 2, 3], sink.SeenAttempts.Select(a => a!.Ordinal));
    }

    /// <summary>Additive: a sink that ignores <see cref="OutputSpec.Attempt"/> behaves exactly as
    /// before, and the rest of the spec is untouched by the stamping.</summary>
    [Fact]
    public async Task Stamping_leaves_the_rest_of_the_spec_alone()
    {
        var sink = new AttemptRecordingSink();

        await new SinkWriteExecutor().ExecuteAsync(SinkNode(), Context(sink), default);

        Assert.Equal("api", sink.SeenSpec!.Sink);
        Assert.Equal("out", sink.SeenSpec.Output);
        Assert.Equal("append", sink.SeenSpec.Mode);
    }

    private sealed class AttemptRecordingSink : ISinkConnector, ISink
    {
        private int _commits;

        public readonly List<WriteAttempt?> SeenAttempts = [];
        public OutputSpec? SeenSpec;

        /// <summary>Fail the first N-1 commits transiently, so the engine's retry loop runs.</summary>
        public int FailCommitsBefore { get; init; }

        public ConnectorInfo Info => new("attempt-recorder", "0.0.0", ProtocolVersion.Major);
        public ConnectorCapabilities Capabilities => ConnectorCapabilities.None;
        public string ConnectionConfigSchema => "{}";
        public string DatasetConfigSchema => "{}";

        public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
            new(ValidationResult.Success);

        public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
            new(new ConnectionCheck(true));

        public ValueTask<ISink> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(this);

        public bool TryGetNativeCopy(
            OutputSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeCopy? copy)
        {
            copy = null;
            return false;
        }

        public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct)
        {
            SeenSpec = spec;
            SeenAttempts.Add(spec.Attempt);
            return new ValueTask<ISinkWriteSession>(new Session(this));
        }

        public ValueTask DisposeAsync() => default;

        private sealed class Session(AttemptRecordingSink owner) : ISinkWriteSession
        {
            private long _rows;

            public ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
            {
                _rows += batch.Length;
                return default;
            }

            public ValueTask<WriteResult> CommitAsync(CancellationToken ct)
            {
                if (++owner._commits < owner.FailCommitsBefore)
                {
                    throw new PzConnectorException("injected transient commit failure", isTransient: true);
                }

                return new ValueTask<WriteResult>(new WriteResult(_rows, 1));
            }

            public ValueTask AbortAsync(CancellationToken ct) => default;

            public ValueTask DisposeAsync() => default;
        }
    }
}
