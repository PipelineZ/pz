using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.DuckDb;
using Pz.Engine.Execution;

namespace Pz.Engine.Tests.Execution;

/// <summary><c>variant:</c> on rows that arrive over Arrow (the universal path, as an Http page's
/// <c>payload</c> does): the column lands as VARCHAR, then becomes VARIANT in staging. The connector also
/// sees the columns on its spec.</summary>
public sealed class VariantLandingTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-variant-landing-tests", Guid.NewGuid().ToString("N"));
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

    private async Task<(NodeResult Result, JsonTextSource Source)> LoadAsync(IReadOnlyList<string> variant,
        params string?[] payloads)
    {
        var source = new JsonTextSource(payloads);
        var registry = new ConnectorRegistry();
        registry.AddSource("pages", source);
        var dataset = new DatasetDef("t", new Dictionary<string, object?>(), null) { Variant = variant };
        var def = new ConnectionDef("api", "pages", new Dictionary<string, object?>(), [dataset], "connections.yml");
        var node = new DagNode(new NodeId("aaaaaaaaaaaaaaaa"), NodeKind.SourceLoad, "src_api__t", [], null,
            new SourceDatasetDef(def, dataset));
        var ctx = new RunContext(_duck, registry, new RunPaths(_dir, "test-run"), NullRunEvents.Instance);
        return (await new SourceLoadExecutor().ExecuteAsync(node, ctx, default), source);
    }

    [Fact]
    public async Task An_arrow_landed_json_text_column_becomes_variant_and_reads_by_field()
    {
        var (result, source) = await LoadAsync(["payload"],
            """{"data":[{"id":1,"n":"a"},{"id":2,"n":"b"}],"next":"p2"}""", """{"data":[],"next":null}""", null);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(["payload"], source.SeenVariantColumns);
        Assert.Equal("VARIANT", await _duck.ScalarAsync<string>("select typeof(payload) from staging.src_api__t limit 1"));
        Assert.Equal("a,b", await _duck.ScalarAsync<string>(
            "select string_agg(r.n::VARCHAR, ',' order by r.id::INT) from " +
            "(select unnest(payload.data::VARIANT[]) as r from staging.src_api__t)"));
        Assert.Equal(1L, await _duck.ScalarAsync<long>("select count(*) from staging.src_api__t where payload is null"));
        Assert.Equal(0L, await _duck.ScalarAsync<long>("select count(*) from duckdb_tables() where table_name like '%__pz_variant'"));
    }

    [Fact]
    public async Task Malformed_rows_fail_PZ0547_and_leave_the_landed_text()
    {
        var (result, _) = await LoadAsync(["payload"], """{"ok":1}""", "<html>rate limited</html>");

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.Equal(PzErrorCode.VariantConversionFailed, result.Error!.Code);
        Assert.Contains("1 row(s) of 'payload' hold text that is not JSON", result.Error.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("rate limited", result.Error.Message, StringComparison.Ordinal);
        Assert.Equal("VARCHAR", await _duck.ScalarAsync<string>("select typeof(payload) from staging.src_api__t limit 1"));
    }

    [Fact]
    public async Task A_column_that_is_not_text_is_PZ0547()
    {
        var (result, _) = await LoadAsync(["page"], """{"ok":1}""");

        Assert.Equal(PzErrorCode.VariantConversionFailed, result.Error!.Code);
        Assert.Contains("'page', which landed as INTEGER, not JSON text", result.Error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Without_variant_nothing_changes()
    {
        var (result, source) = await LoadAsync([], """{"ok":1}""");

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Empty(source.SeenVariantColumns!);
        Assert.Equal("VARCHAR", await _duck.ScalarAsync<string>("select typeof(payload) from staging.src_api__t limit 1"));
    }

    /// <summary>One batch of (payload VARCHAR, page INTEGER), like an Http page envelope.</summary>
    private sealed class JsonTextSource(string?[] payloads) : ISourceConnector, ISource
    {
        private static readonly Schema PageSchema = new(
            [new Field("payload", StringType.Default, nullable: true), new Field("page", Int32Type.Default, nullable: false)], null);

        public IReadOnlyList<string>? SeenVariantColumns { get; private set; }

        public ConnectorInfo Info => new("pages", "0.1.0", ProtocolVersion.Major);
        public ConnectorCapabilities Capabilities => ConnectorCapabilities.None;
        public string ConnectionConfigSchema => """{ "type": "object" }""";
        public string DatasetConfigSchema => """{ "type": "object" }""";

        public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) => new(ValidationResult.Success);
        public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) => new(new ConnectionCheck(true));
        public ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(this);
        public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct) => new(new DatasetSchema(PageSchema));

        public bool TryGetNativeScan(DatasetSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeScan? scan)
        {
            scan = null;
            return false;
        }

        public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct)
        {
            SeenVariantColumns = spec.VariantColumns;
            return new(new IDatasetPartition[] { new Partition(payloads) });
        }

        public ValueTask DisposeAsync() => default;

        private sealed class Partition(string?[] payloads) : IDatasetPartition
        {
            public async IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
            {
                await Task.Yield();
                var builder = new ArrowBatchBuilder(PageSchema, options.TargetBatchBytes);
                for (var i = 0; i < payloads.Length; i++)
                {
                    builder.AppendRow([payloads[i], i + 1]);
                }

                if (builder.Flush() is { } batch)
                {
                    yield return batch;
                }
            }
        }
    }
}
