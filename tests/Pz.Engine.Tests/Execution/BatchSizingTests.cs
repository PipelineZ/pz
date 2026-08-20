using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.Engine.Dispatch;

namespace Pz.Engine.Tests.Execution;

/// <summary>Proves <c>engine.batch_bytes</c> actually shapes emitted batch sizes (not
/// just row counts) for both wide (few rows/batch) and narrow (many rows/batch) schemas, and that the
/// configured value reaches both the universal-source read path (<see cref="SourceLoadExecutor"/>) and
/// the egress read path (<see cref="SinkWriteExecutor"/>'s <see cref="IDuckSession.QueryArrowAsync"/>
/// call) rather than only one of the two. All fixtures are synchronous/deterministic -- no sleeps, no
/// wall-clock dependence.</summary>
public sealed class BatchSizingTests
{
    private const int Target = 65536; // 64KB

    [Fact]
    public void Wide_rows_emit_small_row_count_batches()
    {
        // One large-string column: each row costs roughly 1000+ bytes, so the target is reached after a
        // small number of rows.
        var schema = new Schema([new Field("blob", StringType.Default, nullable: false)], null);
        var value = new string('x', 1000);

        var batches = CollectSteadyStateBatches(schema, Target, rowsToProduce: 400,
            i => [value]);

        Assert.True(batches.Count >= 3, $"expected several steady-state batches, got {batches.Count}");
        foreach (var batch in batches)
        {
            Assert.True(batch.Length < 200, $"expected a small row count for wide rows, got {batch.Length}");
            AssertWithinTolerance(Target, batch.ApproximateSize());
            batch.Dispose();
        }
    }

    [Fact]
    public void Narrow_rows_emit_large_row_count_batches()
    {
        // Single int64 column: each row costs ~8 bytes, so the target requires a large row count.
        var schema = new Schema([new Field("id", Int64Type.Default, nullable: false)], null);

        var batches = CollectSteadyStateBatches(schema, Target, rowsToProduce: 40_000,
            i => [i]);

        Assert.True(batches.Count >= 3, $"expected several steady-state batches, got {batches.Count}");
        foreach (var batch in batches)
        {
            Assert.True(batch.Length > 1000, $"expected a large row count for narrow rows, got {batch.Length}");
            AssertWithinTolerance(Target, batch.ApproximateSize());
            batch.Dispose();
        }
    }

    [Fact]
    public async Task Configured_batch_bytes_reaches_source_and_egress()
    {
        const int configuredBytes = 2_000_000;
        var dir = Path.Combine(Path.GetTempPath(), "pz-batch-sizing-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            await using var realDuck = DuckSession.Open(Path.Combine(dir, "staging.duckdb"));
            await realDuck.ExecuteAsync("create schema if not exists staging");
            var spyDuck = new SpyDuckSession(realDuck);

            var spySource = new SpySourceConnector();
            var registry = new ConnectorRegistry();
            registry.AddSource("spy", spySource);
            registry.AddSink("inmemory", new Pz.Connectors.TestKit.Reference.InMemoryConnector());

            var ctx = new RunContext(spyDuck, registry, new RunPaths(dir, "test-run"), NullRunEvents.Instance,
                Batch: new BatchOptions(configuredBytes));

            var sourceDef = new ConnectionDef("spy", "spy", new Dictionary<string, object?>(),
                [new DatasetDef("t", new Dictionary<string, object?>(), null)], "sources/spy.yml");
            var sourceNode = new DagNode(new NodeId("aaaaaaaaaaaaaaaa"), NodeKind.SourceLoad, "src_spy__t",
                [], null, new SourceDatasetDef(sourceDef, sourceDef.Datasets[0]));

            var loadResult = await new KindDispatchingExecutor().ExecuteAsync(sourceNode, ctx, default);
            Assert.Equal(NodeStatus.Success, loadResult.Status);
            Assert.NotNull(spySource.LastReadOptions);
            Assert.Equal(configuredBytes, spySource.LastReadOptions!.TargetBatchBytes);

            var sinkDef = new ConnectionDef("cap", "inmemory", new Dictionary<string, object?>(), [],
                "sinks/cap.yml") { Outputs = [new OutputDef("out", "src_spy__t", "replace", "fail_on_change", new Dictionary<string, object?>())] };
            var sinkNode = new DagNode(new NodeId("bbbbbbbbbbbbbbbb"), NodeKind.SinkWrite, "cap.out",
                [sourceNode.Id], null, new SinkOutputDef(sinkDef, sinkDef.Outputs[0]));

            var sinkResult = await new KindDispatchingExecutor().ExecuteAsync(sinkNode, ctx, default);
            Assert.Equal(NodeStatus.Success, sinkResult.Status);
            Assert.Equal(configuredBytes, spyDuck.LastEgressTargetBatchBytes);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { }
        }
    }

    private static void AssertWithinTolerance(long target, long actual)
    {
        var lower = target * 0.75;
        var upper = target * 1.25;
        Assert.True(actual >= lower && actual <= upper,
            $"expected batch bytes within +/-25% of {target}, got {actual}");
    }

    /// <summary>Drives <see cref="ArrowBatchBuilder"/> directly -- the shared seam every batch-producing
    /// site (universal source reads, egress) ultimately goes through -- with <paramref name="rowsToProduce"/>
    /// rows, returning every batch <see cref="ArrowBatchBuilder.TryTakeBatch"/> actually cut (i.e. excluding
    /// the final, necessarily-partial remainder, which <see cref="ArrowBatchBuilder.Flush"/> would return
    /// but this helper never calls).</summary>
    private static List<RecordBatch> CollectSteadyStateBatches(
        Schema schema, int targetBatchBytes, int rowsToProduce, Func<long, object?[]> row)
    {
        var builder = new ArrowBatchBuilder(schema, targetBatchBytes);
        var batches = new List<RecordBatch>();
        for (long i = 0; i < rowsToProduce; i++)
        {
            builder.AppendRow(row(i));
            if (builder.TryTakeBatch(out var batch))
            {
                batches.Add(batch!);
            }
        }

        return batches;
    }

    private sealed class SpySourceConnector : ISourceConnector, ISource
    {
        private static readonly Schema SpySchema = new([new Field("id", Int64Type.Default, nullable: false)], null);

        public BatchOptions? LastReadOptions { get; private set; }

        public ConnectorInfo Info => new("spy", "0.1.0", ProtocolVersion.Major);
        public ConnectorCapabilities Capabilities => ConnectorCapabilities.None;
        public string ConnectionConfigSchema => """{ "type": "object", "properties": {}, "additionalProperties": false }""";
        public string DatasetConfigSchema => """{ "type": "object", "properties": {}, "additionalProperties": true }""";

        public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
            new(ValidationResult.Success);

        public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
            new(new ConnectionCheck(true));

        public ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(this);

        public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct) =>
            new(new DatasetSchema(SpySchema));

        public bool TryGetNativeScan(DatasetSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeScan? scan)
        {
            scan = null;
            return false;
        }

        public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
            new(new IDatasetPartition[] { new SpyPartition(this) });

        public ValueTask DisposeAsync() => default;

        private sealed class SpyPartition(SpySourceConnector owner) : IDatasetPartition
        {
            public async IAsyncEnumerable<RecordBatch> ReadAsync(
                BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
            {
                owner.LastReadOptions = options;
                await Task.Yield();

                var builder = new ArrowBatchBuilder(SpySchema, options.TargetBatchBytes);
                builder.AppendRow([1L]);
                var batch = builder.Flush();
                if (batch is not null)
                {
                    yield return batch;
                }
            }
        }
    }

    /// <summary>Delegates every call straight through to <paramref name="inner"/>, recording only the
    /// <c>targetBatchBytes</c> the egress path (<see cref="SinkWriteExecutor"/>) actually passed to
    /// <see cref="QueryArrowAsync"/> -- the second of the two config-driven batch-producing call sites.</summary>
    private sealed class SpyDuckSession(IDuckSession inner) : IDuckSession
    {
        public int? LastEgressTargetBatchBytes { get; private set; }

        public Task ExecuteAsync(string sql, CancellationToken ct = default) => inner.ExecuteAsync(sql, ct);

        public Task<T> ScalarAsync<T>(string sql, CancellationToken ct = default) => inner.ScalarAsync<T>(sql, ct);

        public Task<long> IngestArrowAsync(string targetTable, Schema schema,
            IAsyncEnumerable<RecordBatch> batches, CancellationToken ct = default) =>
            inner.IngestArrowAsync(targetTable, schema, batches, ct);

        public IAsyncEnumerable<RecordBatch> QueryArrowAsync(
            string sql, int targetBatchBytes = 32 * 1024 * 1024, CancellationToken ct = default)
        {
            LastEgressTargetBatchBytes = targetBatchBytes;
            return inner.QueryArrowAsync(sql, targetBatchBytes, ct);
        }

        public Task<Schema> GetResultSchemaAsync(string sql, CancellationToken ct = default) =>
            inner.GetResultSchemaAsync(sql, ct);

        public Task CreateEmptyTableAsync(string targetTable, Schema schema, CancellationToken ct = default) =>
            inner.CreateEmptyTableAsync(targetTable, schema, ct);

        public Task<long> AppendArrowBatchAsync(string targetTable, RecordBatch batch, CancellationToken ct = default) =>
            inner.AppendArrowBatchAsync(targetTable, batch, ct);

        public Task ExecuteTransactionAsync(IReadOnlyList<string> statements, CancellationToken ct = default) =>
            inner.ExecuteTransactionAsync(statements, ct);

        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }
}
