using Apache.Arrow;
using Apache.Arrow.Types;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Parquet;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;
using Pz.Engine.Resilience;
using Pz.TestSupport;

namespace Pz.Connector.AzureBlob.Tests;

/// <summary>Azure universal-write adoption of the engine's IOperationGate.
/// <see cref="AzureWriteSession"/>'s three discrete write-session ops
/// (open_write/commit_copy/delete_temp) are the wrap sites -- every fact here drives a full
/// <see cref="AzureSink"/>/<see cref="AzureWriteSession"/> write rather than poking a session's internals
/// directly, since the gate is threaded in via <see cref="IOperationGateAware.UseOperationGate"/> on the
/// sink, not the session. Azurite/docker-gated (mirrors <see cref="AzureUniversalSinkEndToEndTests"/>);
/// every test writes under a unique guid-suffixed prefix -- the shared <c>pz-e2e</c> container
/// (<see cref="AzuriteFixture"/>) is not cleaned between test runs/collections.</summary>
[Collection("azurite")]
public sealed class AzureGateTests(AzuriteFixture fixture)
{
    private static readonly Schema FixedSchema = new(
    [
        new Field("id", Int64Type.Default, nullable: true),
        new Field("name", StringType.Default, nullable: true),
    ], null);

    private static readonly Schema PartitionSchema = new(
    [
        new Field("id", Int64Type.Default, nullable: true),
        new Field("event_time", new TimestampType(TimeUnit.Microsecond, "UTC"), nullable: true),
    ], null);

    private static ConnectorConfig Config(AzuriteFixture fixture) => new(new Dictionary<string, object?>
    {
        ["auth"] = "connection_string",
        ["connection_string"] = fixture.ConnectionString,
    });

    private static RecordBatch BuildBatch(int startId, int rows)
    {
        var idBuilder = new Int64Array.Builder();
        var nameBuilder = new StringArray.Builder();
        for (var i = 0; i < rows; i++)
        {
            idBuilder.Append(startId + i);
            nameBuilder.Append($"row-{startId + i}");
        }

        return new RecordBatch(FixedSchema, [idBuilder.Build(), nameBuilder.Build()], rows);
    }

    private static RecordBatch BuildPartitionBatch((long Id, DateTimeOffset When)[] rows)
    {
        var idBuilder = new Int64Array.Builder();
        var timeBuilder = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"));
        foreach (var (id, when) in rows)
        {
            idBuilder.Append(id);
            timeBuilder.Append(when);
        }

        return new RecordBatch(PartitionSchema, [idBuilder.Build(), timeBuilder.Build()], rows.Length);
    }

    private static OutputSpec ParquetOutput(string prefix) => new(
        "sink", "orders", "replace", "fail_on_change",
        new Dictionary<string, object?>
        {
            ["container"] = AzuriteFixture.Container,
            ["path"] = prefix,
            ["format"] = "parquet",
        });

    private static OutputSpec PartitionedOutput(string prefix) => new(
        "sink", "orders", "replace", "fail_on_change",
        new Dictionary<string, object?>
        {
            ["container"] = AzuriteFixture.Container,
            ["path"] = $"{prefix}/{{yyyy}}/{{MM}}/{{dd}}/",
            ["format"] = "parquet",
            ["partition_by"] = "event_time",
        });

    private static async Task<long> ParquetRowCountAsync(BlobContainerClient container, string blobName)
    {
        var download = await container.GetBlobClient(blobName).OpenReadAsync();
        await using (download.ConfigureAwait(false))
        {
            await using var reader = await ParquetReader.CreateAsync(download, leaveStreamOpen: true);
            long total = 0;
            for (var rg = 0; rg < reader.RowGroupCount; rg++)
            {
                using var rowGroup = reader.OpenRowGroupReader(rg);
                total += rowGroup.RowCount;
            }

            return total;
        }
    }

    /// <summary>Local IOperationGate decorator: throws for exactly one opLabel, delegates every other op
    /// straight through -- proves cleanup-path suppression (<see cref="Cleanup_failure_still_suppressed"/>)
    /// without needing a real retry policy.</summary>
    private sealed class LabelFailingGate(string failLabel) : IOperationGate
    {
        public async Task<T> ExecuteAsync<T>(string opLabel, bool idempotent,
            Func<CancellationToken, Task<T>> op, CancellationToken ct)
        {
            if (opLabel == failLabel)
            {
                throw new PzConnectorException("inject", isTransient: true);
            }

            return await op(ct).ConfigureAwait(false);
        }

        public void ReportBudget(int remaining, DateTimeOffset resetAt)
        {
        }
    }

    /// <summary>Local IOperationGate decorator wrapping a REAL <see cref="OperationGate"/>: forwards every
    /// op to it unchanged, except the FIRST invocation of a chosen label's op delegate, which throws a
    /// transient <see cref="PzConnectorException"/> instead of running the real op -- proving the real
    /// gate's own retry loop (not some connector-side retry) recovers the failure. Counts every delegate
    /// invocation per label so a test can assert exactly how many times each op body actually ran.</summary>
    private sealed class FirstCallFailingGate(IOperationGate inner, string failLabel) : IOperationGate
    {
        private readonly Dictionary<string, int> _counts = new();

        public int CountOf(string opLabel) => _counts.GetValueOrDefault(opLabel);

        public Task<T> ExecuteAsync<T>(string opLabel, bool idempotent,
            Func<CancellationToken, Task<T>> op, CancellationToken ct)
            => inner.ExecuteAsync(opLabel, idempotent, innerCt =>
            {
                var count = _counts[opLabel] = _counts.GetValueOrDefault(opLabel) + 1;
                if (opLabel == failLabel && count == 1)
                {
                    throw new PzConnectorException("inject", isTransient: true);
                }

                return op(innerCt);
            }, ct);

        public void ReportBudget(int remaining, DateTimeOffset resetAt) => inner.ReportBudget(remaining, resetAt);
    }

    [SkippableFact]
    public async Task Gated_ops_observed_in_order()
    {
        DockerFacts.SkipUnlessDocker();

        var prefix = $"out-{Guid.NewGuid():N}";
        await using var sink = new AzureSink(Config(fixture));
        var gate = new CountingOperationGate();
        ((IOperationGateAware)sink).UseOperationGate(gate);

        var session = await sink.BeginWriteAsync(ParquetOutput(prefix), FixedSchema, CancellationToken.None);
        using var batch = BuildBatch(0, 10);
        await session.WriteBatchAsync(batch, CancellationToken.None);
        var result = await session.CommitAsync(CancellationToken.None);
        await session.DisposeAsync();

        Assert.Equal(10, result.RowsWritten);
        Assert.Equal(["azure.open_write", "azure.commit_copy", "azure.delete_temp"], gate.Labels);

        var container = new BlobServiceClient(fixture.ConnectionString).GetBlobContainerClient(AzuriteFixture.Container);
        var finalBlob = container.GetBlobClient($"{prefix}/orders.parquet");
        Assert.True((await finalBlob.ExistsAsync()).Value);
        Assert.Equal(10, await ParquetRowCountAsync(container, $"{prefix}/orders.parquet"));
    }

    [SkippableFact]
    public async Task Partitioned_write_gates_each_bucket()
    {
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();

        var prefix = $"out-{Guid.NewGuid():N}";
        await using var sink = new AzureSink(Config(fixture));
        var gate = new CountingOperationGate();
        ((IOperationGateAware)sink).UseOperationGate(gate);

        var day12 = new DateTimeOffset(2026, 7, 12, 8, 0, 0, TimeSpan.Zero);
        var day13 = new DateTimeOffset(2026, 7, 13, 21, 30, 0, TimeSpan.Zero);

        var session = await sink.BeginWriteAsync(PartitionedOutput(prefix), PartitionSchema, CancellationToken.None);
        using var batch = BuildPartitionBatch([(1, day12), (2, day13), (3, day12)]);
        await session.WriteBatchAsync(batch, CancellationToken.None);
        var result = await session.CommitAsync(CancellationToken.None);
        await session.DisposeAsync();

        Assert.Equal(3, result.RowsWritten);

        Assert.Equal(2, gate.Labels.Count(l => l == "azure.open_write"));
        Assert.Equal(2, gate.Labels.Count(l => l == "azure.commit_copy"));
        Assert.Equal(2, gate.Labels.Count(l => l == "azure.delete_temp"));

        var container = new BlobServiceClient(fixture.ConnectionString).GetBlobContainerClient(AzuriteFixture.Container);
        Assert.True((await container.GetBlobClient($"{prefix}/2026/07/12/orders.parquet").ExistsAsync()).Value);
        Assert.True((await container.GetBlobClient($"{prefix}/2026/07/13/orders.parquet").ExistsAsync()).Value);
    }

    [SkippableFact]
    public async Task Cleanup_failure_still_suppressed()
    {
        DockerFacts.SkipUnlessDocker();

        var prefix = $"out-{Guid.NewGuid():N}";
        await using var sink = new AzureSink(Config(fixture));
        var gate = new LabelFailingGate("azure.delete_temp");
        ((IOperationGateAware)sink).UseOperationGate(gate);

        var session = await sink.BeginWriteAsync(ParquetOutput(prefix), FixedSchema, CancellationToken.None);
        using var batch = BuildBatch(0, 5);
        await session.WriteBatchAsync(batch, CancellationToken.None);

        // Best-effort suppression preserved: a delete_temp op exhaustion must never turn an
        // already-landed commit (the copy-promote succeeded) into a failed one.
        var result = await session.CommitAsync(CancellationToken.None);
        await session.DisposeAsync();

        Assert.Equal(5, result.RowsWritten);

        var container = new BlobServiceClient(fixture.ConnectionString).GetBlobContainerClient(AzuriteFixture.Container);
        var finalBlob = container.GetBlobClient($"{prefix}/orders.parquet");
        Assert.True((await finalBlob.ExistsAsync()).Value);
        Assert.Equal(5, await ParquetRowCountAsync(container, $"{prefix}/orders.parquet"));

        // The temp blob is simply left behind -- delete_temp's failure was suppressed, not retried
        // to success by some other path.
        var remaining = new List<string>();
        await foreach (var item in container.GetBlobsAsync(traits: BlobTraits.None, states: BlobStates.None, prefix: prefix, cancellationToken: default))
        {
            remaining.Add(item.Name);
        }

        Assert.Contains(remaining, name => name.Contains(".pz-tmp-", StringComparison.Ordinal));
    }

    [SkippableFact]
    public async Task Commit_copy_retries_through_real_gate()
    {
        DockerFacts.SkipUnlessDocker();

        var prefix = $"out-{Guid.NewGuid():N}";
        await using var sink = new AzureSink(Config(fixture));

        var policy = new RetryPolicy(3, TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(30));
        var realGate = new OperationGate(policy, pacing: null, new FixedRandom(0.5), (_, _) => Task.CompletedTask);
        var gate = new FirstCallFailingGate(realGate, "azure.commit_copy");
        ((IOperationGateAware)sink).UseOperationGate(gate);

        var session = await sink.BeginWriteAsync(ParquetOutput(prefix), FixedSchema, CancellationToken.None);
        using var batch = BuildBatch(0, 7);
        await session.WriteBatchAsync(batch, CancellationToken.None);

        var result = await session.CommitAsync(CancellationToken.None);
        await session.DisposeAsync();

        Assert.Equal(7, result.RowsWritten);

        var container = new BlobServiceClient(fixture.ConnectionString).GetBlobContainerClient(AzuriteFixture.Container);
        var finalBlob = container.GetBlobClient($"{prefix}/orders.parquet");
        Assert.True((await finalBlob.ExistsAsync()).Value);
        Assert.Equal(7, await ParquetRowCountAsync(container, $"{prefix}/orders.parquet"));

        // The temp blob was NOT re-uploaded (open_write ran exactly once); the copy-promote was the
        // only op retried, through the real gate's own retry loop -- fail once, succeed on the second
        // attempt.
        Assert.Equal(1, gate.CountOf("azure.open_write"));
        Assert.Equal(2, gate.CountOf("azure.commit_copy"));
    }

    /// <summary>Local stand-in for <c>Pz.Engine.Tests.Execution.FixedRandom</c> -- that helper is internal
    /// to Pz.Engine.Tests and not referenceable here. Same contract: NextDouble() pinned at 0.5 maps to
    /// exactly zero jitter in RetryPolicy.ComputeDelay.</summary>
    private sealed class FixedRandom(double value) : Random
    {
        public override double NextDouble() => value;
    }
}
