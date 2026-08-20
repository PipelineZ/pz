using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Execution;

namespace Pz.Engine.Tests.Execution;

/// <summary>Partition-scoped extraction mode: a connector declaring both
/// <see cref="ConnectorCapabilities.PartitionedRead"/> and <see cref="ConnectorCapabilities.StablePartitionIds"/>
/// engages <see cref="PartitionModeLoader"/> instead of the legacy channel/ingest path -- every
/// partition lands in its own part table, then moves into the main staging table together with a
/// <c>pz_meta.partitions_done</c> row in one transaction, so a later attempt (same run or
/// <c>pz retry</c>) skips it. Mirrors <see cref="StreamingSourceDrainTests"/>'s fixture pattern: real
/// <see cref="DuckSession"/>, hand-built <see cref="DagNode"/>, <see cref="NullRunEvents.Instance"/>.</summary>
public sealed class PartitionModeTests : IAsyncLifetime
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

    private static DagNode Node()
    {
        var source = new ConnectionDef("mem", "liststub", new Dictionary<string, object?>(),
            [new DatasetDef("numbers", new Dictionary<string, object?>(), null)], "sources/mem.yml");
        return new DagNode(new NodeId("aaaaaaaaaaaaaaaa"), NodeKind.SourceLoad, "src_mem__numbers",
            [], null, new SourceDatasetDef(source, source.Datasets[0]));
    }

    [Fact]
    public async Task Partition_mode_stages_all_partitions_and_reports_stats()
    {
        var source = new ListStubSource(
        [
            new IdentifiedStubPartition("a", [1, 2]),
            new IdentifiedStubPartition("b", [3]),
            new IdentifiedStubPartition("c", [4, 5, 6]),
        ]);
        var ctx = Context(new ListStubConnector(source, ConnectorCapabilities.PartitionedRead | ConnectorCapabilities.StablePartitionIds));

        var result = await new SourceLoadExecutor().ExecuteAsync(Node(), ctx, CancellationToken.None);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(6, result.RowsMoved);
        Assert.Null(result.Timings);
        Assert.Equal(new PartitionStats(3, 3, 0, 0), result.Partitions);
        Assert.Equal(6L, await _duck.ScalarAsync<long>("select count(*) from staging.src_mem__numbers", default));
        Assert.Equal(3L, await _duck.ScalarAsync<long>(
            "select count(*) from pz_meta.partitions_done where node_id = 'aaaaaaaaaaaaaaaa'", default));
        Assert.Equal(0L, await _duck.ScalarAsync<long>(
            "select count(*) from information_schema.tables where table_schema = 'staging' and table_name like '\\_\\_pz\\_%' escape '\\'", default));
    }

    [Fact]
    public async Task Second_call_skips_done_partitions()
    {
        var aReads = 0;
        var failB = true;
        var source = new ListStubSource(
        [
            new IdentifiedStubPartition("a", [1, 2], onRead: () => aReads++),
            new IdentifiedStubPartition("b", [3],
                fault: () => failB
                    ? new PzConnectorException("blip", isTransient: true)
                    : null),
        ]);
        var connector = new ListStubConnector(source, ConnectorCapabilities.PartitionedRead | ConnectorCapabilities.StablePartitionIds);
        var ctx = Context(connector);

        var thrown = await Assert.ThrowsAsync<PzConnectorException>(
            () => new SourceLoadExecutor().ExecuteAsync(Node(), ctx, CancellationToken.None));
        Assert.True(thrown.IsTransient);

        failB = false;
        var result = await new SourceLoadExecutor().ExecuteAsync(Node(), ctx, CancellationToken.None);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(3, result.RowsMoved);
        Assert.Equal(new PartitionStats(2, 2, 0, 0), result.Partitions);
        Assert.Equal(1, aReads); // partition a extracted exactly once across both attempts
    }

    [Fact]
    public async Task Without_flag_the_legacy_path_runs_and_no_ledger_appears()
    {
        var source = new ListStubSource([new IdentifiedStubPartition("a", [1])]);
        var ctx = Context(new ListStubConnector(source, ConnectorCapabilities.PartitionedRead));

        var result = await new SourceLoadExecutor().ExecuteAsync(Node(), ctx, CancellationToken.None);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Null(result.Partitions);
        Assert.Equal(0L, await _duck.ScalarAsync<long>(
            "select count(*) from information_schema.schemata where schema_name = 'pz_meta'", default));
    }

    [Fact]
    public async Task Identity_violation_fails_with_PZ0319_and_no_raw_ids()
    {
        var source = new ListStubSource(
        [
            new IdentifiedStubPartition("s3://bucket/secret-path", [1]),
            new IdentifiedStubPartition("s3://bucket/secret-path", [2]), // duplicate id
            new StubPartition(3),                                       // not IIdentifiedPartition
        ]);
        var ctx = Context(new ListStubConnector(source, ConnectorCapabilities.PartitionedRead | ConnectorCapabilities.StablePartitionIds));

        var result = await new SourceLoadExecutor().ExecuteAsync(Node(), ctx, CancellationToken.None);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.Equal("PZ0319", result.Error!.Code);
        Assert.DoesNotContain("secret-path", result.Error.Message, StringComparison.Ordinal);
        Assert.Contains("duplicates", result.Error.Message, StringComparison.Ordinal);
    }
}

/// <summary>One identified partition yielding one batch per value in <paramref name="values"/>.
/// <paramref name="fault"/> is consulted per read (<c>Func&lt;Exception?&gt;</c>) -- a null return
/// falls through to yielding values normally, so a test can flip a captured flag between attempts and
/// have the SAME stub partition read cleanly on a later attempt (the resume-after-transient-failure
/// contract).</summary>
internal sealed class IdentifiedStubPartition(string id, long[] values,
    Action? onRead = null, Func<CancellationToken, Task>? gate = null, Func<Exception?>? fault = null)
    : IIdentifiedPartition
{
    public string PartitionId => id;

    public async IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options,
        [EnumeratorCancellation] CancellationToken ct)
    {
        await Task.Yield();
        onRead?.Invoke();
        if (gate is not null)
        {
            await gate(ct).ConfigureAwait(false);
        }

        var thrown = fault?.Invoke();
        if (thrown is not null)
        {
            throw thrown;
        }

        foreach (var v in values)
        {
            yield return StubSchema.BuildBatch(v);
        }
    }
}

/// <summary>Source that hands back a fixed, already-planned partition list -- the list path partition
/// mode runs against. <paramref name="feedShaped"/> makes this source implement
/// <see cref="INaturalReadShapeSource"/>
/// and resolve <see cref="NaturalReadShape.Feed"/> -- opt-in per test, since most callers of this
/// shared stub exercise ordinary (non-sync) partition-mode behavior and must keep resolving Full.</summary>
internal sealed class ListStubSource(IReadOnlyList<IDatasetPartition> partitions, bool feedShaped = false)
    : ISource, INaturalReadShapeSource
{
    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct) =>
        new(new DatasetSchema(StubSchema.IdSchema));

    public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
    {
        scan = null;
        return false;
    }

    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
        new(partitions);

    public NaturalReadShape GetNaturalReadShape(DatasetSpec spec) => feedShaped ? NaturalReadShape.Feed : NaturalReadShape.Full;

    public ValueTask DisposeAsync() => default;
}

/// <summary>Connector returning a pre-built <see cref="ListStubSource"/>, advertising a test-chosen
/// capability set -- lets a test flip <see cref="ConnectorCapabilities.StablePartitionIds"/> on or off.</summary>
internal sealed class ListStubConnector(ISource source, ConnectorCapabilities capabilities) : ISourceConnector
{
    public ConnectorInfo Info => new("liststub", "0.1.0", ProtocolVersion.Major);
    public ConnectorCapabilities Capabilities => capabilities;
    public string ConnectionConfigSchema => "{}";
    public string DatasetConfigSchema => "{}";

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
        new(ValidationResult.Success);

    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new ConnectionCheck(true));

    public ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(source);
}
