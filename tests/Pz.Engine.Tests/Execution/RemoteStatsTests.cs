using System.Runtime.CompilerServices;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.Engine.Resilience;

namespace Pz.Engine.Tests.Execution;

/// <summary>Connector-reported stats reach <see cref="NodeFacts.Remote"/>: the engine sets the ambient handle
/// per attempt, sums across retries, and tells the user once when a connector breaks the contract.</summary>
public sealed class RemoteStatsTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));
    private DuckSession _duck = null!;
    private readonly List<string> _notices = [];

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

    private RunContext Context(ISource source)
    {
        var reg = new ConnectorRegistry();
        reg.AddSource("liststub", new ListStubConnector(source, ConnectorCapabilities.PartitionedRead));
        return new RunContext(_duck, reg, new RunPaths(_dir, "test-run"), NullRunEvents.Instance,
            Notice: n => { lock (_notices) { _notices.Add(n); } });
    }

    private static DagNode Node(string id, string dataset)
    {
        var source = new ConnectionDef("mem", "liststub", new Dictionary<string, object?>(),
            [new DatasetDef(dataset, new Dictionary<string, object?>(), null, null)], "sources/mem.yml");
        return new DagNode(new NodeId(id), NodeKind.SourceLoad, $"src_mem__{dataset}",
            [], null, new SourceDatasetDef(source, source.Datasets[0]));
    }

    private static KindDispatchingExecutor Executor(int maxAttempts = 1) =>
        new(new RetryPolicy(maxAttempts, TimeSpan.Zero, TimeSpan.Zero), new FixedRandom(0.5),
            delay: (_, _) => Task.CompletedTask);

    private static ReportingPartition Reporting(int batches, Action<IConnectorStats> perBatch,
        Func<Exception?>? fault = null) => new(batches, perBatch, fault);

    private static void RequestAndCost(IConnectorStats s)
    {
        s.AddRequests(1);
        s.AddCost(0.5, "ru");
    }

    [Fact]
    public async Task Stats_reported_inside_a_partition_iterator_land_on_node_facts()
    {
        var ctx = Context(new ListStubSource([Reporting(3, RequestAndCost), Reporting(3, RequestAndCost)]));

        var result = await Executor().ExecuteAsync(Node("aaaaaaaaaaaaaaaa", "numbers"), ctx, CancellationToken.None);

        Assert.Equal(NodeStatus.Success, result.Status);
        var remote = Assert.IsType<RemoteStats>(result.Facts!.Remote);
        Assert.Equal(6, remote.Requests);
        Assert.Equal(3.0, remote.CostUnits);
        Assert.Equal("ru", remote.CostUnit);
    }

    [Fact]
    public async Task Stats_sum_across_retry_attempts()
    {
        var attempt = 0;
        var partition = new ReportingPartition(1, s =>
        {
            var n = Interlocked.Increment(ref attempt);
            s.AddRequests(n == 1 ? 2 : 3);
        }, () => attempt == 1 ? new PzConnectorException("blip", isTransient: true) : null);
        var ctx = Context(new ListStubSource([partition]));

        var result = await Executor(maxAttempts: 2).ExecuteAsync(Node("aaaaaaaaaaaaaaaa", "numbers"), ctx, CancellationToken.None);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(2, result.Facts!.Attempts);
        Assert.Equal(5, result.Facts.Remote!.Requests);
    }

    [Fact]
    public async Task A_connector_that_reports_nothing_has_no_remote()
    {
        var ctx = Context(new ListStubSource([Reporting(2, _ => { })]));

        var result = await Executor().ExecuteAsync(Node("aaaaaaaaaaaaaaaa", "numbers"), ctx, CancellationToken.None);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Null(result.Facts!.Remote);
    }

    [Fact]
    public async Task Invalid_stats_never_fail_the_run_and_note_once_per_rule()
    {
        var partition = new ReportingPartition(1, s =>
        {
            s.AddRequests(-1);
            s.AddCost(1, "Bad Unit");
            s.AddExtra("nope", 1);
            for (var i = 0; i < 9; i++)
            {
                s.AddExtra($"x.k{i}", 1);
            }

            s.AddCost(1, "ru");
            s.AddCost(1, "credits");
        }, null);
        var ctx = Context(new ListStubSource([partition]));

        var result = await Executor().ExecuteAsync(Node("aaaaaaaaaaaaaaaa", "numbers"), ctx, CancellationToken.None);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(5, _notices.Count);
        Assert.All(_notices, n => Assert.Contains("'liststub'", n, StringComparison.Ordinal));
        Assert.All(_notices, n => Assert.DoesNotContain("Bad Unit", n, StringComparison.Ordinal));
        foreach (var rule in new[]
        {
            "non-negative finite value", "cost unit format", "one cost unit per node",
            "extra name format", "at most 8 extras",
        })
        {
            Assert.Single(_notices, n => n.Contains($"'{rule}'", StringComparison.Ordinal));
        }

        var remote = result.Facts!.Remote!;
        Assert.Equal(8, remote.Extras.Count);
        Assert.Equal(1.0, remote.CostUnits);
        Assert.Equal("ru", remote.CostUnit);
    }

    [Fact]
    public async Task Two_nodes_with_the_same_drop_note_once_per_connector()
    {
        var ctx = Context(new ListStubSource([Reporting(1, s => s.AddRequests(-1))]));
        var executor = Executor();

        var first = await executor.ExecuteAsync(Node("aaaaaaaaaaaaaaaa", "one"), ctx, CancellationToken.None);
        var second = await executor.ExecuteAsync(Node("bbbbbbbbbbbbbbbb", "two"), ctx, CancellationToken.None);

        Assert.Equal(NodeStatus.Success, first.Status);
        Assert.Equal(NodeStatus.Success, second.Status);
        Assert.Single(_notices);
    }
}

/// <summary>Partition that reports through the ambient <see cref="ConnectorStats.Current"/> from inside its
/// iterator, once per batch, the way a real connector's read loop does.</summary>
internal sealed class ReportingPartition(int batches, Action<IConnectorStats> perBatch, Func<Exception?>? fault)
    : IDatasetPartition
{
    public async IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options,
        [EnumeratorCancellation] CancellationToken ct)
    {
        await Task.Yield();
        for (var i = 0; i < batches; i++)
        {
            perBatch(ConnectorStats.Current);
            yield return StubSchema.BuildBatch(i);
        }

        if (fault?.Invoke() is { } thrown)
        {
            throw thrown;
        }
    }
}
