using System.Runtime.CompilerServices;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;
using Pz.Connectors.TestKit.Reference;

/// <summary>The ColumnPruning contract says a batch carries exactly the hinted columns in the hint's
/// order, and the host binds batch columns to the staged table by position — so a connector that
/// narrows but keeps its own column order is as wrong as one that ignores the hint, and the
/// acceptance fact has to fail both. This drives the fact against all three shapes.</summary>
public sealed class ColumnPruningDetectionTests
{
    private enum PruningMode
    {
        /// <summary>Exactly the hinted columns, in hint order: the contract.</summary>
        HonorsHint,

        /// <summary>Declares the capability, yields the full declared schema.</summary>
        IgnoresHint,

        /// <summary>Only the hinted columns, but in declared-schema order rather than the hint's.</summary>
        DeclaredOrder,
    }

    /// <summary>InMemoryConnector plus a declared ColumnPruning capability, with reads projected
    /// according to <paramref name="mode"/>.</summary>
    private sealed class PruningConnector(PruningMode mode) : ISourceConnector
    {
        private readonly InMemoryConnector _inner = new();

        public ConnectorInfo Info => _inner.Info;

        public ConnectorCapabilities Capabilities => _inner.Capabilities | ConnectorCapabilities.ColumnPruning;

        public string ConnectionConfigSchema => _inner.ConnectionConfigSchema;

        public string DatasetConfigSchema => _inner.DatasetConfigSchema;

        public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
            _inner.ValidateAsync(config, ct);

        public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
            _inner.CheckConnectionAsync(config, ct);

        public async ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct) =>
            new PruningSource(await ((ISourceConnector)_inner).OpenAsync(config, ct), mode);
    }

    private sealed class PruningSource(ISource inner, PruningMode mode) : ISource
    {
        public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct) =>
            inner.GetSchemaAsync(spec, ct);

        public bool TryGetNativeScan(DatasetSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeScan? scan) =>
            inner.TryGetNativeScan(spec, out scan);

        public async ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct)
        {
            var partitions = await inner.PlanReadAsync(spec, hints, ct);
            if (mode == PruningMode.IgnoresHint || hints.Columns is not { Count: > 0 } hinted)
            {
                return partitions;
            }

            var declared = (await inner.GetSchemaAsync(spec, ct)).Schema;
            IReadOnlyList<string> columns = mode == PruningMode.DeclaredOrder
                ? declared.FieldsList.Select(f => f.Name)
                    .Where(n => hinted.Contains(n, StringComparer.OrdinalIgnoreCase)).ToList()
                : hinted;
            return partitions.Select(p => (IDatasetPartition)new ProjectingPartition(p, columns)).ToList();
        }

        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }

    /// <summary>Re-shapes every inner batch to <paramref name="columns"/>, in that order. The
    /// projected batch owns only the kept arrays; the dropped ones are released here, since nothing
    /// else references them once the inner batch's array list goes out of scope.</summary>
    private sealed class ProjectingPartition(IDatasetPartition inner, IReadOnlyList<string> columns) : IDatasetPartition
    {
        public async IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
        {
            await foreach (var batch in inner.ReadAsync(options, ct).WithCancellation(ct))
            {
                var fields = new List<Field>(columns.Count);
                var arrays = new List<IArrowArray>(columns.Count);
                var kept = new HashSet<int>();
                foreach (var name in columns)
                {
                    var index = batch.Schema.FieldsList.ToList()
                        .FindIndex(f => string.Equals(f.Name, name, StringComparison.OrdinalIgnoreCase));
                    if (index < 0)
                    {
                        throw new InvalidOperationException($"hinted column '{name}' is not in the batch");
                    }

                    fields.Add(batch.Schema.FieldsList[index]);
                    arrays.Add(batch.Column(index));
                    kept.Add(index);
                }

                for (var i = 0; i < batch.ColumnCount; i++)
                {
                    if (!kept.Contains(i))
                    {
                        batch.Column(i).Dispose();
                    }
                }

                yield return new RecordBatch(new Schema(fields, batch.Schema.Metadata), arrays, batch.Length);
            }
        }
    }

    private sealed class PruningAcceptance(PruningMode mode) : SourceConnectorAcceptanceTests
    {
        protected override ISourceConnector CreateSource() => new PruningConnector(mode);

        protected override ConnectorConfig ValidConfig => ConnectorConfig.Empty;

        protected override DatasetSpec SmallDataset => new("mem", "numbers",
            new Dictionary<string, object?> { ["rows"] = 500L, ["partitions"] = 2 });
    }

    [Fact]
    public async Task A_connector_that_honors_the_hint_passes()
    {
        await new PruningAcceptance(PruningMode.HonorsHint).ColumnPruning_yields_exactly_the_hinted_columns_in_hint_order();
    }

    [Fact]
    public async Task A_connector_that_declares_pruning_but_ignores_the_hint_fails()
    {
        var sut = new PruningAcceptance(PruningMode.IgnoresHint);

        var ex = await Assert.ThrowsAnyAsync<Exception>(
            () => sut.ColumnPruning_yields_exactly_the_hinted_columns_in_hint_order());

        // The field-count assertion: five declared columns arrived where two were hinted.
        Assert.IsType<Xunit.Sdk.EqualException>(ex);
    }

    [Fact]
    public async Task A_connector_that_prunes_in_declared_order_instead_of_hint_order_fails()
    {
        var sut = new PruningAcceptance(PruningMode.DeclaredOrder);

        var ex = await Assert.ThrowsAnyAsync<Exception>(
            () => sut.ColumnPruning_yields_exactly_the_hinted_columns_in_hint_order());

        // The per-position name assertion: the right two columns, transposed.
        Assert.IsType<Xunit.Sdk.EqualException>(ex);
    }

    [Fact]
    public async Task A_connector_that_does_not_declare_pruning_skips_the_fact()
    {
        var sut = new InMemorySourceAcceptance();

        var ex = await Assert.ThrowsAnyAsync<Exception>(
            () => sut.ColumnPruning_yields_exactly_the_hinted_columns_in_hint_order());

        Assert.IsType<Xunit.SkipException>(ex);
    }
}
