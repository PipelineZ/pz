using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit.Reference;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Dispatch;
using Pz.Engine.Execution;

namespace Pz.Engine.Tests.Execution;

/// <summary>The shared DuckDB session gate is held per batch, not per source load: a channel-path source
/// waiting on its upstream between batches must not block other nodes' local work.</summary>
[Collection("partition-fault-timing")]
public sealed class ChannelIngestConcurrencyTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));
    private DuckSession _duck = null!;
    private RunContext _ctx = null!;

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _duck = DuckSession.Open(Path.Combine(_dir, "staging.duckdb"));
        await _duck.ExecuteAsync("create schema if not exists staging");
        var mem = new InMemoryConnector();
        var reg = new ConnectorRegistry();
        reg.AddSource("inmemory", mem);
        reg.AddSink("inmemory", mem);
        _ctx = new RunContext(_duck, reg, new RunPaths(_dir, "test-run"), NullRunEvents.Instance);
    }

    public async Task DisposeAsync()
    {
        await _duck.DisposeAsync();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private static DagNode SourceLoadNode(string nodeId, string dataset, Dictionary<string, object?> options)
    {
        var source = new ConnectionDef("mem", "inmemory", new Dictionary<string, object?>(),
            [new DatasetDef(dataset, options, null)], "sources/mem.yml");
        return new DagNode(new NodeId(nodeId), NodeKind.SourceLoad, $"src_mem__{dataset}",
            [], null, new SourceDatasetDef(source, source.Datasets[0]));
    }

    [Fact]
    public async Task Local_source_load_completes_while_a_channel_source_waits_between_batches()
    {
        var rowsA = 4L * BatchOptions.Default.MaxRowsPerBatch;
        var aParked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseA = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        Task Park(CancellationToken ct)
        {
            aParked.TrySetResult();
            return releaseA.Task.WaitAsync(ct);
        }

        var nodeA = SourceLoadNode("aaaaaaaaaaaaaaaa", "slow", new Dictionary<string, object?>
        {
            ["rows"] = rowsA,
            ["partitions"] = 1,
            ["before_batch_hook"] = (Func<int, CancellationToken, Task>)((i, ct) => i == 1 ? Park(ct) : Task.CompletedTask),
        });
        var nodeB = SourceLoadNode("bbbbbbbbbbbbbbbb", "fast", new Dictionary<string, object?> { ["rows"] = 1000L });

        var executor = new KindDispatchingExecutor();
        var execA = executor.ExecuteAsync(nodeA, _ctx, default);
        try
        {
            await aParked.Task.WaitAsync(TimeSpan.FromSeconds(30));

            var b = await executor.ExecuteAsync(nodeB, _ctx, default).WaitAsync(TimeSpan.FromSeconds(30));
            Assert.Equal(NodeStatus.Success, b.Status);
            Assert.Equal(1000L, await _duck.ScalarAsync<long>(
                "select count(*) from " + StagingNames.ForSourceLoad("mem", "fast")));
            Assert.False(execA.IsCompleted);
        }
        finally
        {
            releaseA.TrySetResult();

            // A must finish before DisposeAsync closes the connection under it. Its own outcome is
            // asserted below; an exception here must not replace a failure from the block above.
            try
            {
                await execA.WaitAsync(TimeSpan.FromSeconds(30));
            }
            catch
            {
            }
        }

        var a = await execA.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.Equal(NodeStatus.Success, a.Status);
        Assert.Equal(rowsA, a.RowsMoved);
    }
}
