using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.Engine.State;

namespace Pz.Engine.Tests.Execution;

/// <summary>Engine landing for cdc: a `sync: {mode: cdc}` dataset lands
/// raw change rows into <c>&lt;staging&gt;__changes</c>, then the engine collapses them in DuckDB to
/// last-event-per-key upserts (canonical <c>&lt;staging&gt;</c>) plus a <c>&lt;staging&gt;__deletes</c>
/// side table of net-deleted keys, and reports per-op raw counts as <see cref="NodeResult.Cdc"/>.
/// Stub-connector harness mirrors <see cref="SyncStateExecutorTests"/> (own <see cref="ISource"/>/
/// <see cref="ISourceConnector"/> stubs + a real <see cref="DuckSession"/>); the partition implements
/// both <see cref="ISyncStatePartition"/> and <see cref="IChangeCapturePartition"/> and emits rows
/// carrying the `_pz_op`/`_pz_lsn`/`_pz_changed_at` change-row envelope.</summary>
public sealed class CdcLandingTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-cdc-landing-tests", Guid.NewGuid().ToString("N"));
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

    private const string Canonical = "staging.src_src__orders";
    private const string Changes = "staging.src_src__orders__changes";
    private const string Deletes = "staging.src_src__orders__deletes";

    private RunContext Context(ISourceConnector connector, SyncStateStore? syncState = null,
        ReuseManifest? reuse = null, Action<string>? notice = null, bool fullRefresh = false) =>
        new(_duck, Registry(connector), new RunPaths(_dir, "test-run"), NullRunEvents.Instance,
            SyncState: syncState, Reuse: reuse, Notice: notice, FullRefresh: fullRefresh);

    private static ConnectorRegistry Registry(ISourceConnector connector)
    {
        var reg = new ConnectorRegistry();
        reg.AddSource("cdcstub", connector);
        return reg;
    }

    private static DagNode CdcNode(NodeId? id = null)
    {
        var dataset = new DatasetDef("orders", new Dictionary<string, object?>(), null,
            new SyncModeDef(SyncMode.Cdc, null, "slot1"));
        var source = new ConnectionDef("src", "cdcstub", new Dictionary<string, object?>(), [dataset], "sources/src.yml");
        return new DagNode(id ?? new NodeId("aaaaaaaaaaaaaaaa"), NodeKind.SourceLoad, "src_src__orders",
            [], null, new SourceDatasetDef(source, dataset));
    }

    // The landing window: id=1 insert then update (last values survive), id=2 insert then delete
    // (nets to __deletes), id=3 delete only (in __deletes), id=4 insert (survives).
    private static readonly CdcRow[] LandingWindow =
    [
        new("insert", 1, 1, "a"),
        new("update", 1, 5, "a2"),
        new("insert", 2, 2, "b"),
        new("delete", 2, 6, "b"),
        new("delete", 3, 3, null),
        new("insert", 4, 4, "d"),
    ];

    [Fact]
    public async Task Landing_collapses_to_upserts_and_deletes()
    {
        var ctx = Context(new CdcStubConnector(new CdcStubSource(LandingWindow)));

        var result = await new SourceLoadExecutor().ExecuteAsync(CdcNode(), ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);

        // Raw rows all land in __changes.
        Assert.Equal(6L, await _duck.ScalarAsync<long>($"select count(*) from {Changes}", default));

        // Canonical holds ONLY the surviving upserts (last event per id, non-delete), _pz_ columns stripped.
        Assert.Equal(2L, await _duck.ScalarAsync<long>($"select count(*) from {Canonical}", default));
        Assert.Equal(0L, await _duck.ScalarAsync<long>(
            "select count(*) from (describe " + Canonical + ") where column_name like '\\_pz\\_%' escape '\\'", default));
        // id=1 survived with its LAST values (the update, not the insert).
        Assert.Equal("a2", await _duck.ScalarAsync<string>($"select name from {Canonical} where id = 1", default));
        // id=4 (insert-only) survives; id=2 and id=3 (net-deleted) are absent from canonical.
        Assert.Equal(1L, await _duck.ScalarAsync<long>($"select count(*) from {Canonical} where id = 4", default));
        Assert.Equal(0L, await _duck.ScalarAsync<long>($"select count(*) from {Canonical} where id in (2, 3)", default));

        // __deletes holds one row per net-deleted id (2 and 3), _pz_ columns stripped.
        Assert.Equal(2L, await _duck.ScalarAsync<long>($"select count(*) from {Deletes}", default));
        Assert.Equal(2L, await _duck.ScalarAsync<long>($"select count(*) from {Deletes} where id in (2, 3)", default));
        Assert.Equal(0L, await _duck.ScalarAsync<long>(
            "select count(*) from (describe " + Deletes + ") where column_name like '\\_pz\\_%' escape '\\'", default));
    }

    [Fact]
    public async Task Per_op_counts_are_raw_not_net()
    {
        // A window with 2 inserts, 1 update, 2 deletes -> CdcStats(2, 1, 2).
        CdcRow[] window =
        [
            new("insert", 1, 1, "a"),
            new("update", 1, 5, "a2"),
            new("insert", 2, 2, "b"),
            new("delete", 2, 6, "b"),
            new("delete", 3, 3, null),
        ];
        var ctx = Context(new CdcStubConnector(new CdcStubSource(window)));

        var result = await new SourceLoadExecutor().ExecuteAsync(CdcNode(), ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.NotNull(result.Cdc);
        Assert.Equal(new CdcStats(2, 1, 2), result.Cdc);
    }

    [Fact]
    public async Task Prior_token_is_replayed_and_candidate_captured()
    {
        var store = SyncStateStore.Local(Path.Combine(_dir, "state"));
        store.Set(SyncStateStore.Key("src", "orders"), new SyncState("prior-lsn", "prior-run"));

        var source = new CdcStubSource(LandingWindow, candidate: "next-lsn");
        var ctx = Context(new CdcStubConnector(source), syncState: store);

        var result = await new SourceLoadExecutor().ExecuteAsync(CdcNode(), ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal("prior-lsn", source.ObservedPriorSyncState);
        Assert.NotNull(result.SyncStateCandidate);
        Assert.Equal("next-lsn", result.SyncStateCandidate!.Token);
    }

    [Fact]
    public async Task Full_refresh_replays_null_token()
    {
        var store = SyncStateStore.Local(Path.Combine(_dir, "state"));
        store.Set(SyncStateStore.Key("src", "orders"), new SyncState("prior-lsn", "prior-run"));

        var source = new CdcStubSource(LandingWindow, candidate: "next-lsn");
        var ctx = Context(new CdcStubConnector(source), syncState: store, fullRefresh: true);

        var result = await new SourceLoadExecutor().ExecuteAsync(CdcNode(), ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Null(source.ObservedPriorSyncState);
    }

    [Fact]
    public async Task Multi_partition_cdc_read_fails_with_extended_PZ0316()
    {
        var ctx = Context(new CdcStubConnector(new CdcStubSource(LandingWindow, partitionCount: 2)));

        var result = await new SourceLoadExecutor().ExecuteAsync(CdcNode(), ctx, default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.NotNull(result.Error);
        Assert.Equal(PzErrorCode.SyncPartitionedReadConflict, result.Error!.Code);
        Assert.Contains("log position cannot span partitions", result.Error.Message);
    }

    [Fact]
    public async Task Unknown_keys_with_non_empty_window_fails_naming_connector_defect()
    {
        var ctx = Context(new CdcStubConnector(new CdcStubSource(LandingWindow, unknownKeys: true)));

        var result = await new SourceLoadExecutor().ExecuteAsync(CdcNode(), ctx, default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.NotNull(result.Error);
        Assert.Contains("change key columns", result.Error!.Message);
    }

    [Fact]
    public async Task Unknown_keys_with_empty_window_succeeds_with_empty_tables()
    {
        var ctx = Context(new CdcStubConnector(new CdcStubSource([], unknownKeys: true)));

        var result = await new SourceLoadExecutor().ExecuteAsync(CdcNode(), ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(new CdcStats(0, 0, 0), result.Cdc);
        Assert.Equal(0L, await _duck.ScalarAsync<long>($"select count(*) from {Canonical}", default));
        Assert.Equal(0L, await _duck.ScalarAsync<long>($"select count(*) from {Deletes}", default));
    }

    [Fact]
    public async Task Missing_pz_op_column_fails_naming_change_row_contract()
    {
        var ctx = Context(new CdcStubConnector(new CdcStubSource(LandingWindow, includeOpColumn: false)));

        var result = await new SourceLoadExecutor().ExecuteAsync(CdcNode(), ctx, default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.NotNull(result.Error);
        Assert.Contains("_pz_op", result.Error!.Message);
    }

    /// <summary>entry.Rows must be the POST-COLLAPSE
    /// canonical count (what <see cref="SourceLoadExecutor"/> reports as <see cref="NodeResult.RowsMoved"/>
    /// for a cdc SourceLoad), never the raw window total -- otherwise TryReuseAsync's count-verify (which
    /// compares against the COPIED CANONICAL table) fails on any window containing an update, a delete, or
    /// a repeated key. The prior run here is produced by actually EXECUTING the cdc collapse against
    /// <see cref="LandingWindow"/> (3 inserts, 1 update, 2 deletes over ids 1-4 -- raw window of 6 rows,
    /// only ids 1 (updated) and 4 (inserted) surviving into canonical), so <c>priorRows</c> is exactly the
    /// value the executor records, and the raw(6) != canonical(2) gap is genuinely exercised rather than
    /// hand-waved.</summary>
    [Fact]
    public async Task Retry_reuse_succeeds_when_raw_and_canonical_counts_differ()
    {
        var priorPaths = new RunPaths(_dir, "prior");
        Directory.CreateDirectory(priorPaths.RunDir);
        long priorRows;
        await using (var priorDuck = DuckSession.Open(priorPaths.StagingDbPath))
        {
            await priorDuck.ExecuteAsync("create schema if not exists staging");
            var priorCtx = new RunContext(priorDuck, Registry(new CdcStubConnector(new CdcStubSource(LandingWindow))),
                priorPaths, NullRunEvents.Instance);
            var priorResult = await new SourceLoadExecutor().ExecuteAsync(CdcNode(), priorCtx, default);
            Assert.Equal(NodeStatus.Success, priorResult.Status);
            priorRows = priorResult.RowsMoved;
        }

        // Sanity: proves raw (6) != canonical (2) is genuinely in play, not a coincidentally-equal fixture.
        Assert.Equal(2L, priorRows);

        var node = CdcNode(new NodeId("cdccdccdccdccdcc"));
        var reuse = new ReuseManifest(new Dictionary<NodeId, ReuseEntry>
        {
            [node.Id] = new(priorPaths.StagingDbPath, priorRows, null),
        });
        // Any OpenAsync would throw -> proves the reuse path (not re-extraction) copied both tables.
        var ctx = Context(new CdcStubConnector(new ThrowingSource()), reuse: reuse);

        var result = await new SourceLoadExecutor().ExecuteAsync(node, ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(NodeProvenance.Reused, result.Provenance);
        Assert.Equal(2L, result.RowsMoved);
        Assert.Equal(2L, await _duck.ScalarAsync<long>($"select count(*) from {Canonical}", default));
        Assert.Equal(2L, await _duck.ScalarAsync<long>($"select count(*) from {Deletes}", default));
    }

    /// <summary>The count-verify guard must still refuse a genuinely mismatched copy -- a tampered/stale
    /// manifest entry (claims 5 rows; the prior staging's canonical table actually has 4) falls back to
    /// normal re-extraction instead of trusting the copy, exactly like the non-cdc guard in
    /// <c>StagingReuseTests.Row_count_mismatch_falls_back_to_normal_extraction</c>.</summary>
    [Fact]
    public async Task Retry_reuse_falls_back_on_genuine_row_count_mismatch()
    {
        var priorPaths = new RunPaths(_dir, "prior");
        Directory.CreateDirectory(priorPaths.RunDir);
        await using (var priorDuck = DuckSession.Open(priorPaths.StagingDbPath))
        {
            await priorDuck.ExecuteAsync("create schema staging");
            await priorDuck.ExecuteAsync($"create table {Canonical} as select * from range(4) t(id)");
            await priorDuck.ExecuteAsync($"create table {Deletes} as select * from range(2) t(id)");
        }

        var node = CdcNode(new NodeId("cdccdccdccdccdcc"));
        var reuse = new ReuseManifest(new Dictionary<NodeId, ReuseEntry>
        {
            [node.Id] = new(priorPaths.StagingDbPath, 5, null),
        });
        var notices = new List<string>();
        var ctx = Context(new CdcStubConnector(new CdcStubSource(LandingWindow)), reuse: reuse, notice: notices.Add);

        var result = await new SourceLoadExecutor().ExecuteAsync(node, ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Null(result.Provenance);
        Assert.Contains(notices, n => n.Contains("re-extracting", StringComparison.Ordinal));
        // Re-extraction ran the real cdc collapse against LandingWindow -- canonical ends up with the 2
        // surviving keys, not the tampered manifest's stale prior data.
        Assert.Equal(2L, await _duck.ScalarAsync<long>($"select count(*) from {Canonical}", default));
    }

    // Test 7: advancement is commit-gated for cdc exactly as for feed datasets — CommitGatedAdvancement
    // keys on SyncStateCandidate with no dataset-kind filter, so a cdc SourceLoad advances only after
    // every structural descendant sink succeeds.
    [Fact]
    public void Cdc_token_persists_only_after_downstream_sink_success()
    {
        var store = SyncStateStore.Local(Path.Combine(_dir, "adv-state"));
        var sourceId = new NodeId("1111111111111111");
        var sinkId = new NodeId("2222222222222222");
        var key = SyncStateStore.Key("src", "orders");
        var dag = new CompiledDag([CdcNode(sourceId), CdcSinkNode(sinkId, sourceId)]);

        var sourceResult = new NodeResult(sourceId, NodeKind.SourceLoad, "src_src__orders",
            NodeStatus.Success, 6, TimeSpan.Zero, null, SyncStateCandidate: new SyncState("cdc-lsn", "test-run"));
        var okSink = new NodeResult(sinkId, NodeKind.SinkWrite, "cap.out", NodeStatus.Success, 6, TimeSpan.Zero, null);

        SyncStateAdvancement.Advance(dag, [sourceResult, okSink], store);
        Assert.Equal("cdc-lsn", store.Get(key)!.Token);
    }

    [Fact]
    public void Cdc_token_unchanged_when_downstream_sink_fails()
    {
        var store = SyncStateStore.Local(Path.Combine(_dir, "adv-state-fail"));
        var sourceId = new NodeId("3333333333333333");
        var sinkId = new NodeId("4444444444444444");
        var key = SyncStateStore.Key("src", "orders");
        var dag = new CompiledDag([CdcNode(sourceId), CdcSinkNode(sinkId, sourceId)]);

        var sourceResult = new NodeResult(sourceId, NodeKind.SourceLoad, "src_src__orders",
            NodeStatus.Success, 6, TimeSpan.Zero, null, SyncStateCandidate: new SyncState("cdc-lsn", "test-run"));
        var failSink = new NodeResult(sinkId, NodeKind.SinkWrite, "cap.out", NodeStatus.Failed, 0, TimeSpan.Zero,
            new PzError("PZ0001", "boom", null, null, null));

        SyncStateAdvancement.Advance(dag, [sourceResult, failSink], store);
        Assert.Null(store.Get(key));
    }

    private static DagNode CdcSinkNode(NodeId id, NodeId dependsOn)
    {
        var sink = new ConnectionDef("cap", "inmemory", new Dictionary<string, object?>(), [],
            "sinks/cap.yml") { Outputs = [new OutputDef("out", "src_src__orders", "merge", "fail_on_change", new Dictionary<string, object?>())] };
        return new DagNode(id, NodeKind.SinkWrite, "cap.out", [dependsOn], null, new SinkOutputDef(sink, sink.Outputs[0]));
    }
}

/// <summary>A single change row (op envelope + id/name payload) for the cdc stub.</summary>
internal sealed record CdcRow(string Op, long Id, long Lsn, string? Name);

/// <summary>Fake cdc <see cref="ISource"/>: records the replayed <see cref="DatasetSpec.PriorSyncState"/>
/// and returns <paramref name="partitionCount"/> partitions, each emitting <paramref name="rows"/> as one
/// change batch. Change keys default to <c>[id]</c>; <paramref name="unknownKeys"/> true => the partition
/// reports unknown change keys. <paramref name="includeOpColumn"/> false => the landed rows omit `_pz_op`
/// (change-row contract breach).</summary>
internal sealed class CdcStubSource(
    IReadOnlyList<CdcRow> rows, bool unknownKeys = false, int partitionCount = 1,
    bool includeOpColumn = true, string? candidate = null) : ISource, INaturalReadShapeSource
{
    public string? ObservedPriorSyncState { get; private set; }

    private IReadOnlyList<string>? Keys => unknownKeys ? null : ["id"];

    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct) =>
        new(new DatasetSchema(CdcSchema.For(includeOpColumn)));

    public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
    {
        scan = null;
        return false;
    }

    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct)
    {
        ObservedPriorSyncState = spec.PriorSyncState;
        IReadOnlyList<IDatasetPartition> partitions = Enumerable.Range(0, partitionCount)
            .Select(_ => (IDatasetPartition)new CdcStubPartition(rows, Keys, includeOpColumn, candidate))
            .ToList();
        return new(partitions);
    }

    public NaturalReadShape GetNaturalReadShape(DatasetSpec spec) => NaturalReadShape.Feed;

    public ValueTask DisposeAsync() => default;
}

/// <summary>One cdc partition: emits its change rows as a single batch (or none, for an empty window),
/// reports a sync-state candidate and — via <see cref="IChangeCapturePartition"/> — its change keys.</summary>
internal sealed class CdcStubPartition(
    IReadOnlyList<CdcRow> rows, IReadOnlyList<string>? keys, bool includeOpColumn, string? candidate)
    : IDatasetPartition, ISyncStatePartition, IChangeCapturePartition
{
    public async IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
    {
        await Task.Yield();
        if (rows.Count > 0)
        {
            yield return CdcSchema.BuildBatch(rows, includeOpColumn);
        }
    }

    public bool TryGetSyncStateCandidate(out string? candidate2)
    {
        candidate2 = candidate;
        return candidate is not null;
    }

    public bool TryGetChangeKeyColumns(out IReadOnlyList<string>? keyColumns)
    {
        keyColumns = keys;
        return keys is not null;
    }
}

/// <summary>Builds the change-row Arrow schema/batches: id, name, plus the `_pz_op`/`_pz_lsn`/
/// `_pz_changed_at` envelope (op column optional, to exercise the contract check).</summary>
internal static class CdcSchema
{
    public static Schema For(bool includeOp)
    {
        var fields = new List<Field>
        {
            new("id", Int64Type.Default, nullable: false),
            new("name", StringType.Default, nullable: true),
        };
        if (includeOp)
        {
            fields.Add(new Field("_pz_op", StringType.Default, nullable: false));
        }

        fields.Add(new Field("_pz_lsn", Int64Type.Default, nullable: false));
        fields.Add(new Field("_pz_changed_at", Int64Type.Default, nullable: true));
        return new Schema(fields, null);
    }

    public static RecordBatch BuildBatch(IReadOnlyList<CdcRow> rows, bool includeOp)
    {
        var id = new Int64Array.Builder();
        var name = new StringArray.Builder();
        var op = new StringArray.Builder();
        var lsn = new Int64Array.Builder();
        var changedAt = new Int64Array.Builder();
        foreach (var r in rows)
        {
            id.Append(r.Id);
            if (r.Name is null) { name.AppendNull(); } else { name.Append(r.Name); }
            op.Append(r.Op);
            lsn.Append(r.Lsn);
            changedAt.Append(r.Lsn);
        }

        var arrays = new List<IArrowArray> { id.Build(), name.Build() };
        if (includeOp)
        {
            arrays.Add(op.Build());
        }

        arrays.Add(lsn.Build());
        arrays.Add(changedAt.Build());
        return new RecordBatch(For(includeOp), arrays, rows.Count);
    }
}

/// <summary>Connector wrapping a pre-built source, advertising ChangeCapture (a real cdc connector) but
/// no PartitionedRead/StablePartitionIds — the multi-partition guard is exercised via the executor.</summary>
internal sealed class CdcStubConnector(ISource source) : ISourceConnector
{
    public ConnectorInfo Info => new("cdcstub", "0.1.0", ProtocolVersion.Major);
    public ConnectorCapabilities Capabilities => ConnectorCapabilities.ChangeCapture;
    public string ConnectionConfigSchema => "{}";
    public string DatasetConfigSchema => "{}";

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
        new(ValidationResult.Success);

    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new ConnectionCheck(true));

    public ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(source);
}

/// <summary>Source whose OpenAsync/read would throw — proves the reuse path never contacts it.</summary>
internal sealed class ThrowingSource : ISource
{
    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct) =>
        throw new InvalidOperationException("reuse path must not read the source");

    public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan) =>
        throw new InvalidOperationException("reuse path must not read the source");

    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
        throw new InvalidOperationException("reuse path must not read the source");

    public ValueTask DisposeAsync() => default;
}
