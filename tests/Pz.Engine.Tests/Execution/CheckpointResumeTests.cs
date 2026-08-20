using System.Globalization;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Execution;

namespace Pz.Engine.Tests.Execution;

/// <summary>Intra-partition checkpoints. A checkpointing partition
/// (<see cref="ICheckpointingPartition"/>) stages into a segment table; each token it offers commits
/// atomically (segment → part table, checkpoint row upserted). On a later attempt the engine resumes
/// only when a checkpoint row exists, the part table's row count still matches it (an intact resume
/// prefix), AND the connector accepts the token via <see cref="ICheckpointingPartition.TryResumeFrom"/>
/// — anything less restarts the partition from scratch. Shares <see cref="ListStubSource"/>/
/// <see cref="ListStubConnector"/>/<see cref="StubSchema"/> (already internal in this assembly/namespace
/// from <see cref="PartitionModeTests"/>/<see cref="StreamingSourceDrainTests"/>).</summary>
public sealed class CheckpointResumeTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));
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

    private RunContext Context(ISourceConnector connector)
    {
        var reg = new ConnectorRegistry();
        reg.AddSource("liststub", connector);
        return new RunContext(_duck, reg, new RunPaths(_dir, "test-run"), NullRunEvents.Instance);
    }

    private static ListStubConnector Connector(CheckpointingStubPartition partition) => new(
        new ListStubSource([partition]),
        ConnectorCapabilities.PartitionedRead | ConnectorCapabilities.StablePartitionIds | ConnectorCapabilities.CheckpointableReads);

    private static DagNode Node()
    {
        var source = new ConnectionDef("mem", "liststub", new Dictionary<string, object?>(),
            [new DatasetDef("numbers", new Dictionary<string, object?>(), null)], "sources/mem.yml");
        return new DagNode(new NodeId("aaaaaaaaaaaaaaaa"), NodeKind.SourceLoad, "src_mem__numbers",
            [], null, new SourceDatasetDef(source, source.Datasets[0]));
    }

    [Fact]
    public async Task Checkpointed_prefix_survives_a_failed_attempt()
    {
        var partition = new CheckpointingStubPartition("p", [10, 20, 30, 40, 50], failAfterBatches: 2);
        var ctx = Context(Connector(partition));

        var thrown = await Assert.ThrowsAsync<PzConnectorException>(
            () => new SourceLoadExecutor().ExecuteAsync(Node(), ctx, CancellationToken.None));
        Assert.True(thrown.IsTransient);

        // Prefix (2 rows) is in the part table with its token; nothing reached main.
        Assert.Equal(0L, await _duck.ScalarAsync<long>("select count(*) from staging.src_mem__numbers", default));
        Assert.Equal("tok-2", await _duck.ScalarAsync<string>(
            "select checkpoint from pz_meta.partition_checkpoints where node_id = 'aaaaaaaaaaaaaaaa'", default));
        Assert.Equal(2L, await _duck.ScalarAsync<long>(
            "select rows from pz_meta.partition_checkpoints where node_id = 'aaaaaaaaaaaaaaaa'", default));
    }

    [Fact]
    public async Task Resume_continues_strictly_after_the_token()
    {
        var partition = new CheckpointingStubPartition("p", [10, 20, 30, 40, 50], failAfterBatches: 2);
        var ctx = Context(Connector(partition));
        await Assert.ThrowsAsync<PzConnectorException>(
            () => new SourceLoadExecutor().ExecuteAsync(Node(), ctx, CancellationToken.None));

        var resumed = new CheckpointingStubPartition("p", [10, 20, 30, 40, 50]);
        var result = await new SourceLoadExecutor().ExecuteAsync(Node(), Context(Connector(resumed)), CancellationToken.None);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(["tok-2"], resumed.ResumeCalls);
        Assert.Equal(new PartitionStats(1, 1, 0, 1), result.Partitions);
        Assert.Equal(5, result.RowsMoved);
        Assert.Equal(150L, await _duck.ScalarAsync<long>("select cast(sum(id) as bigint) from staging.src_mem__numbers", default));
        Assert.Equal(5L, await _duck.ScalarAsync<long>(
            "select count(distinct id) from staging.src_mem__numbers", default)); // no duplicates
        Assert.Equal(0L, await _duck.ScalarAsync<long>(
            "select count(*) from pz_meta.partition_checkpoints", default)); // cleared on completion
    }

    [Fact]
    public async Task Torn_part_table_restarts_the_partition_without_resume()
    {
        var partition = new CheckpointingStubPartition("p", [10, 20, 30], failAfterBatches: 2);
        var ctx = Context(Connector(partition));
        await Assert.ThrowsAsync<PzConnectorException>(
            () => new SourceLoadExecutor().ExecuteAsync(Node(), ctx, CancellationToken.None));

        // Tamper: ledger says 2 rows, table now has 1 — torn state.
        var partTable = PartitionLedger.PartTable(PartitionLedger.NodeKey(Node()), "p");
        await _duck.ExecuteAsync($"delete from {partTable} where id = 20");

        var resumed = new CheckpointingStubPartition("p", [10, 20, 30]);
        var result = await new SourceLoadExecutor().ExecuteAsync(Node(), Context(Connector(resumed)), CancellationToken.None);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Empty(resumed.ResumeCalls); // guard failed BEFORE consulting the connector
        Assert.Equal(new PartitionStats(1, 1, 0, 0), result.Partitions);
        Assert.Equal(60L, await _duck.ScalarAsync<long>("select cast(sum(id) as bigint) from staging.src_mem__numbers", default));
    }

    [Fact]
    public async Task Rejected_resume_restarts_from_scratch()
    {
        var partition = new CheckpointingStubPartition("p", [10, 20, 30], failAfterBatches: 2);
        await Assert.ThrowsAsync<PzConnectorException>(
            () => new SourceLoadExecutor().ExecuteAsync(Node(), Context(Connector(partition)), CancellationToken.None));

        var resumed = new CheckpointingStubPartition("p", [10, 20, 30]) { RejectResume = true };
        var result = await new SourceLoadExecutor().ExecuteAsync(Node(), Context(Connector(resumed)), CancellationToken.None);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(["tok-2"], resumed.ResumeCalls);
        Assert.Equal(new PartitionStats(1, 1, 0, 0), result.Partitions);
        Assert.Equal(60L, await _duck.ScalarAsync<long>("select cast(sum(id) as bigint) from staging.src_mem__numbers", default));
    }
}

/// <summary>Yields one 1-row batch per value; exposes a checkpoint token "tok-N" after the N-th
/// yielded batch; TryResumeFrom("tok-N") restarts the yield at index N. Records calls.
///
/// <c>_yielded++</c> must run BEFORE <c>yield return</c>, not after: a C# iterator's post-yield
/// statements only execute on the NEXT MoveNextAsync call, so incrementing afterwards makes
/// TryGetCheckpoint's read of <c>_yielded</c> lag the true count of delivered batches by one (after 2
/// batches are delivered it reports "tok-1", not "tok-2"). That breaks self-consistency with
/// TryResumeFrom's "resume strictly AT this index" contract — a resume from the under-counted token
/// would re-read an already-persisted row and duplicate it.</summary>
internal sealed class CheckpointingStubPartition(string id, long[] values, int? failAfterBatches = null)
    : ICheckpointingPartition
{
    private int _yielded;
    private int _start;
    public List<string> ResumeCalls { get; } = [];
    public bool RejectResume { get; set; }

    public string PartitionId => id;

    public bool TryResumeFrom(string checkpoint)
    {
        ResumeCalls.Add(checkpoint);
        if (RejectResume || !checkpoint.StartsWith("tok-", StringComparison.Ordinal))
        {
            return false;
        }

        _start = int.Parse(checkpoint["tok-".Length..], CultureInfo.InvariantCulture);
        return true;
    }

    public bool TryGetCheckpoint(out string? checkpoint)
    {
        checkpoint = _yielded > 0 ? $"tok-{_start + _yielded}" : null;
        return checkpoint is not null;
    }

    public async IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options,
        [EnumeratorCancellation] CancellationToken ct)
    {
        await Task.Yield();
        _yielded = 0;
        for (var i = _start; i < values.Length; i++)
        {
            if (failAfterBatches is { } cap && _yielded >= cap)
            {
                throw new PzConnectorException("mid-read failure", isTransient: true);
            }

            _yielded++;
            yield return StubSchema.BuildBatch(values[i]);
        }
    }
}
