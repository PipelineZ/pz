using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit.Reference;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.DuckDb;
using Pz.Engine.Artifacts;
using Pz.Engine.Execution;
using Pz.Engine.State;

namespace Pz.Engine.Tests.Execution;

/// <summary>A manifested SourceLoad copies the failed run's staged
/// table instead of contacting the connector. Fixture mirrors <c>WatermarkFlowTests</c> (temp dir,
/// <see cref="DuckSession.Open"/>, <c>create schema if not exists staging</c>, <see cref="ConnectorRegistry"/>,
/// <see cref="RunContext"/> with <see cref="NullRunEvents.Instance"/>).</summary>
public sealed class StagingReuseTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-staging-reuse-tests", Guid.NewGuid().ToString("N"));
    private DuckSession _duck = null!;
    private ConnectorRegistry _registry = null!;

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        var currentPaths = new RunPaths(_dir, "current");
        Directory.CreateDirectory(currentPaths.RunDir);
        _duck = DuckSession.Open(currentPaths.StagingDbPath);
        await _duck.ExecuteAsync("create schema if not exists staging");
        _registry = new ConnectorRegistry();
    }

    public async Task DisposeAsync()
    {
        await _duck.DisposeAsync();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private static DagNode SourceLoadNode(NodeId id, string connectorName, string sourceName, string datasetName, long rows)
    {
        var dataset = new DatasetDef(datasetName, new Dictionary<string, object?> { ["rows"] = rows }, null,
            new SyncModeDef(SyncMode.Incremental, new IncrementalDef("id")));
        var source = new ConnectionDef(sourceName, connectorName, new Dictionary<string, object?>(), [dataset], $"sources/{sourceName}.yml");
        return new DagNode(id, NodeKind.SourceLoad, $"src_{sourceName}__{datasetName}", [], null, new SourceDatasetDef(source, dataset));
    }

    private RunContext Ctx(ReuseManifest reuse, Action<string>? notice = null) =>
        new(_duck, _registry, new RunPaths(_dir, "current"), NullRunEvents.Instance, Reuse: reuse, Notice: notice);

    /// <summary>Proves the reuse path never contacts the source: any OpenAsync throws.</summary>
    private sealed class ThrowingSourceConnector : ISourceConnector
    {
        public ConnectorInfo Info => new("throwing", "0.0.0", ProtocolVersion.Major);
        public ConnectorCapabilities Capabilities => ConnectorCapabilities.None;
        public string ConnectionConfigSchema => "{}";
        public string DatasetConfigSchema => "{}";
        public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
            new(ValidationResult.Success);
        public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
            new(new ConnectionCheck(true));
        public ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct) =>
            throw new InvalidOperationException("reuse path must not open the connector");
    }

    [Fact]
    public async Task Reuse_happy_path_copies_prior_staging_without_touching_connector()
    {
        var priorPaths = new RunPaths(_dir, "prior");
        Directory.CreateDirectory(priorPaths.RunDir);
        await using (var priorDuck = DuckSession.Open(priorPaths.StagingDbPath))
        {
            await priorDuck.ExecuteAsync("create schema staging");
            await priorDuck.ExecuteAsync("create table staging.src_crm__orders as select * from range(5) t(id)");
        }

        _registry.AddSource("throwing", new ThrowingSourceConnector());
        var node = SourceLoadNode(new NodeId("1111111111111111"), "throwing", "crm", "orders", 5);

        var reuse = new ReuseManifest(new Dictionary<NodeId, ReuseEntry>
        {
            [node.Id] = new(priorPaths.StagingDbPath, 5, new PriorWatermark("id", "bigint", "4")),
        });

        var result = await new SourceLoadExecutor().ExecuteAsync(node, Ctx(reuse), CancellationToken.None);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(5, result.RowsMoved);
        Assert.Equal(NodeProvenance.Reused, result.Provenance);
        Assert.NotNull(result.WatermarkCandidate);
        Assert.Equal("4", result.WatermarkCandidate!.Value);
        Assert.Equal("current", result.WatermarkCandidate.RunId);

        Assert.Equal(5L, await _duck.ScalarAsync<long>("select count(*) from staging.src_crm__orders", default));
    }

    [Fact]
    public async Task Row_count_mismatch_falls_back_to_normal_extraction()
    {
        var priorPaths = new RunPaths(_dir, "prior");
        Directory.CreateDirectory(priorPaths.RunDir);
        await using (var priorDuck = DuckSession.Open(priorPaths.StagingDbPath))
        {
            await priorDuck.ExecuteAsync("create schema staging");
            await priorDuck.ExecuteAsync("create table staging.src_crm__orders as select * from range(5) t(id)");
        }

        var mem = new InMemoryConnector();
        _registry.AddSource("inmemory", mem);
        var node = SourceLoadNode(new NodeId("2222222222222222"), "inmemory", "crm", "orders", 7);

        var reuse = new ReuseManifest(new Dictionary<NodeId, ReuseEntry>
        {
            [node.Id] = new(priorPaths.StagingDbPath, 99, new PriorWatermark("id", "bigint", "4")),
        });

        var notices = new List<string>();
        var result = await new SourceLoadExecutor().ExecuteAsync(node, Ctx(reuse, notices.Add), CancellationToken.None);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Null(result.Provenance);
        Assert.Equal(7, result.RowsMoved);
        Assert.Contains(notices, n => n.Contains("re-extracting", StringComparison.Ordinal));

        Assert.Equal(7L, await _duck.ScalarAsync<long>("select count(*) from staging.src_crm__orders", default));
    }

    [Fact]
    public async Task Missing_prior_table_falls_back_to_normal_extraction()
    {
        var priorPaths = new RunPaths(_dir, "prior");
        Directory.CreateDirectory(priorPaths.RunDir);
        await using (var priorDuck = DuckSession.Open(priorPaths.StagingDbPath))
        {
            await priorDuck.ExecuteAsync("create schema staging");
            // No staging.src_crm__orders table created.
        }

        var mem = new InMemoryConnector();
        _registry.AddSource("inmemory", mem);
        var node = SourceLoadNode(new NodeId("3333333333333333"), "inmemory", "crm", "orders", 6);

        var reuse = new ReuseManifest(new Dictionary<NodeId, ReuseEntry>
        {
            [node.Id] = new(priorPaths.StagingDbPath, 6, new PriorWatermark("id", "bigint", "5")),
        });

        var notices = new List<string>();
        var result = await new SourceLoadExecutor().ExecuteAsync(node, Ctx(reuse, notices.Add), CancellationToken.None);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Null(result.Provenance);
        Assert.Equal(6, result.RowsMoved);
        Assert.Contains(notices, n => n.Contains("re-extracting", StringComparison.Ordinal));
    }

    /// <summary>Two reused SourceLoads executed CONCURRENTLY against the same
    /// <see cref="RunContext"/> (same shared <c>DuckSession</c>, serialized per-STATEMENT not per-sequence
    /// — the gate covers one statement, never a whole sequence) would interleave on a single shared
    /// ATTACH alias; the loser's ATTACH would
    /// fail (duplicate alias already bound) and fall back to the connector, exactly what reuse exists to
    /// prevent. A per-node alias (derived from the node's own content-addressed ID) removes the collision.
    /// Both nodes' connectors throw on OpenAsync, so ANY fallback-to-connector would surface as an
    /// exception/failure here, not just a wrong Provenance.</summary>
    [Fact]
    public async Task Concurrent_reuse_of_two_datasets_does_not_race_on_shared_attach_alias()
    {
        var priorPaths = new RunPaths(_dir, "prior");
        Directory.CreateDirectory(priorPaths.RunDir);
        await using (var priorDuck = DuckSession.Open(priorPaths.StagingDbPath))
        {
            await priorDuck.ExecuteAsync("create schema staging");
            await priorDuck.ExecuteAsync("create table staging.src_crm__orders as select * from range(5) t(id)");
            await priorDuck.ExecuteAsync("create table staging.src_crm__customers as select * from range(3) t(id)");
        }

        _registry.AddSource("throwing", new ThrowingSourceConnector());
        var nodeA = SourceLoadNode(new NodeId("aaaaaaaaaaaaaaaa"), "throwing", "crm", "orders", 5);
        var nodeB = SourceLoadNode(new NodeId("bbbbbbbbbbbbbbbb"), "throwing", "crm", "customers", 3);

        var reuse = new ReuseManifest(new Dictionary<NodeId, ReuseEntry>
        {
            [nodeA.Id] = new(priorPaths.StagingDbPath, 5, new PriorWatermark("id", "bigint", "4")),
            [nodeB.Id] = new(priorPaths.StagingDbPath, 3, new PriorWatermark("id", "bigint", "2")),
        });

        var ctx = Ctx(reuse);
        var executor = new SourceLoadExecutor();

        var results = await Task.WhenAll(
            executor.ExecuteAsync(nodeA, ctx, CancellationToken.None),
            executor.ExecuteAsync(nodeB, ctx, CancellationToken.None));
        var resultA = results[0];
        var resultB = results[1];

        Assert.Equal(NodeStatus.Success, resultA.Status);
        Assert.Equal(NodeProvenance.Reused, resultA.Provenance);
        Assert.Equal(5, resultA.RowsMoved);

        Assert.Equal(NodeStatus.Success, resultB.Status);
        Assert.Equal(NodeProvenance.Reused, resultB.Provenance);
        Assert.Equal(3, resultB.RowsMoved);

        Assert.Equal(5L, await _duck.ScalarAsync<long>("select count(*) from staging.src_crm__orders", default));
        Assert.Equal(3L, await _duck.ScalarAsync<long>("select count(*) from staging.src_crm__customers", default));
    }

    [Fact]
    public async Task No_manifest_entry_uses_normal_path()
    {
        var mem = new InMemoryConnector();
        _registry.AddSource("inmemory", mem);
        var node = SourceLoadNode(new NodeId("4444444444444444"), "inmemory", "crm", "orders", 3);

        var result = await new SourceLoadExecutor().ExecuteAsync(node, Ctx(ReuseManifest.Empty), CancellationToken.None);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Null(result.Provenance);
        Assert.Equal(3, result.RowsMoved);
    }
}
