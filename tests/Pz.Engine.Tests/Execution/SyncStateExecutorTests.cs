using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.Engine.Dispatch;
using Pz.Engine.State;

namespace Pz.Engine.Tests.Execution;

/// <summary>End-to-end wiring: a `sync:` dataset's prior opaque token flows from
/// <see cref="RunContext.SyncState"/> onto <see cref="DatasetSpec.PriorSyncState"/>, and the connector's
/// post-enumeration candidate (<see cref="ISyncStatePartition.TryGetSyncStateCandidate"/>) is captured
/// onto the success <see cref="NodeResult.SyncStateCandidate"/>. Mirrors the self-contained fake-connector
/// harness in <see cref="StreamingSourceDrainTests"/> (own <see cref="ISource"/>/<see cref="ISourceConnector"/>
/// stubs, real <see cref="DuckSession"/>) rather than the InMemoryConnector-based harness elsewhere, since
/// InMemoryConnector has no sync-state support.</summary>
public sealed class SyncStateExecutorTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-sync-exec-tests", Guid.NewGuid().ToString("N"));
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

    private RunContext Context(ISourceConnector connector, SyncStateStore? syncState = null, string runId = "test-run")
    {
        var reg = new ConnectorRegistry();
        reg.AddSource("syncstub", connector);
        return new RunContext(_duck, reg, new RunPaths(_dir, runId), NullRunEvents.Instance, SyncState: syncState);
    }

    private static DagNode SourceLoadNode()
    {
        var source = new ConnectionDef("src", "syncstub", new Dictionary<string, object?>(),
            [new DatasetDef("orders", new Dictionary<string, object?>(), null, new SyncModeDef(SyncMode.Auto, null))],
            "sources/src.yml");
        return new DagNode(new NodeId("aaaaaaaaaaaaaaaa"), NodeKind.SourceLoad, "src_src__orders",
            [], null, new SourceDatasetDef(source, source.Datasets[0]));
    }

    [Fact]
    public async Task Prior_sync_state_is_read_and_candidate_is_captured()
    {
        var store = SyncStateStore.Local(Path.Combine(_dir, "state"));
        store.Set(SyncStateStore.Key("src", "orders"), new SyncState("seed-token", "seed-run"));

        var source = new SyncStubSource(partitionCount: 1, candidate: "new-token");
        var ctx = Context(new SyncStubConnector(source), syncState: store);

        var result = await new KindDispatchingExecutor().ExecuteAsync(SourceLoadNode(), ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal("seed-token", source.ObservedPriorSyncState);
        Assert.NotNull(result.SyncStateCandidate);
        Assert.Equal("new-token", result.SyncStateCandidate!.Token);
        Assert.Equal("test-run", result.SyncStateCandidate.RunId);
    }

    [Fact]
    public async Task Full_refresh_skips_the_prior_sync_state_read()
    {
        var store = SyncStateStore.Local(Path.Combine(_dir, "state"));
        store.Set(SyncStateStore.Key("src", "orders"), new SyncState("seed-token", "seed-run"));

        var source = new SyncStubSource(partitionCount: 1, candidate: "new-token");
        var reg = new ConnectorRegistry();
        reg.AddSource("syncstub", new SyncStubConnector(source));
        var ctx = new RunContext(_duck, reg, new RunPaths(_dir, "test-run"), NullRunEvents.Instance,
            SyncState: store, FullRefresh: true);

        var result = await new KindDispatchingExecutor().ExecuteAsync(SourceLoadNode(), ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Null(source.ObservedPriorSyncState);
        // Full-refresh only gates the READ side (no prior token replayed) -- capture still runs, so
        // the very next (non-full-refresh) run has a fresh token to seed from instead of starting
        // over a second time.
        Assert.NotNull(result.SyncStateCandidate);
        Assert.Equal("new-token", result.SyncStateCandidate!.Token);
    }

    [Fact]
    public async Task Two_partitions_on_a_sync_dataset_fails_with_PZ0316()
    {
        var source = new SyncStubSource(partitionCount: 2, candidate: "new-token");
        var ctx = Context(new SyncStubConnector(source));

        var result = await new KindDispatchingExecutor().ExecuteAsync(SourceLoadNode(), ctx, default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.NotNull(result.Error);
        Assert.Equal(PzErrorCode.SyncPartitionedReadConflict, result.Error!.Code);
        Assert.Contains("src", result.Error.Message);
        Assert.Contains("orders", result.Error.Message);
    }

    [Fact]
    public async Task No_candidate_when_partition_reports_none()
    {
        var source = new SyncStubSource(partitionCount: 1, candidate: null);
        var ctx = Context(new SyncStubConnector(source));

        var result = await new KindDispatchingExecutor().ExecuteAsync(SourceLoadNode(), ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Null(result.SyncStateCandidate);
    }
}

/// <summary>Fake <see cref="ISource"/> for a `sync:` dataset: <see cref="PlanReadAsync"/> records the
/// spec's <see cref="DatasetSpec.PriorSyncState"/> it was handed (<see cref="ObservedPriorSyncState"/>)
/// and returns <paramref name="partitionCount"/> single-batch partitions, each reporting
/// <paramref name="candidate"/> as its post-enumeration sync-state candidate. Every dataset driven
/// through this file is feed-shaped by construction --
/// unconditionally implements <see cref="INaturalReadShapeSource"/>, resolving Feed, so
/// <see cref="ReadShapeResolver"/> routes these tests through the sync-state machinery
/// (prior-token replay, PZ0316's runtime guard, candidate capture).</summary>
internal sealed class SyncStubSource(int partitionCount, string? candidate) : ISource, INaturalReadShapeSource
{
    public string? ObservedPriorSyncState { get; private set; }

    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct) =>
        new(new DatasetSchema(StubSchema.IdSchema));

    public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
    {
        scan = null;
        return false;
    }

    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct)
    {
        ObservedPriorSyncState = spec.PriorSyncState;
        IReadOnlyList<IDatasetPartition> partitions = Enumerable.Range(0, partitionCount)
            .Select(i => (IDatasetPartition)new SyncStubPartition(i, candidate))
            .ToList();
        return new(partitions);
    }

    public NaturalReadShape GetNaturalReadShape(DatasetSpec spec) => NaturalReadShape.Feed;

    public ValueTask DisposeAsync() => default;
}

/// <summary>One partition yielding a single one-row batch, implementing <see cref="ISyncStatePartition"/>
/// so the executor can poll it post-enumeration. <paramref name="candidate"/> null means "no new token
/// this run" (<see cref="TryGetSyncStateCandidate"/> returns false).</summary>
internal sealed class SyncStubPartition(long id, string? candidate) : IDatasetPartition, ISyncStatePartition
{
    public async IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
    {
        await Task.Yield();
        yield return StubSchema.BuildBatch(id);
    }

    public bool TryGetSyncStateCandidate(out string? candidate2)
    {
        candidate2 = candidate;
        return candidate is not null;
    }
}

/// <summary>Source connector wrapping a pre-built source, advertising no capabilities -- a sync dataset's
/// connector must not declare <see cref="ConnectorCapabilities.PartitionedRead"/> (PZ0316 at plan time),
/// but these tests drive the runtime guard directly through the executor, bypassing the planner.</summary>
internal sealed class SyncStubConnector(ISource source) : ISourceConnector
{
    public ConnectorInfo Info => new("syncstub", "0.1.0", ProtocolVersion.Major);
    public ConnectorCapabilities Capabilities => ConnectorCapabilities.None;
    public string ConnectionConfigSchema => "{}";
    public string DatasetConfigSchema => "{}";

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
        new(ValidationResult.Success);

    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new ConnectionCheck(true));

    public ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(source);
}
