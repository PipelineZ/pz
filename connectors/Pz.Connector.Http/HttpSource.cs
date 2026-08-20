using System.Diagnostics.CodeAnalysis;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Formats;

namespace Pz.Connector.Http;

internal sealed class HttpSource : ISource, IOperationGateAware, INaturalReadShapeSource
{
    private readonly HttpConnectionConfig _connection;
    private readonly TimeProvider _time;
    private readonly HttpClient _client;
    private IOperationGate? _gate;

    public HttpSource(HttpConnectionConfig connection) : this(connection, TimeProvider.System)
    {
    }

    internal HttpSource(HttpConnectionConfig connection, TimeProvider time)
    {
        _connection = connection;
        _time = time;
        _client = CreateClient(connection);
    }

    /// <summary>Redirects are followed by <see cref="HttpPartition"/> itself, not by the handler.
    /// HttpClient's automatic redirect strips only the <c>Authorization</c> header when the target
    /// changes origin — a connection's configured <c>headers:</c> (commonly where an API key lives)
    /// and any api-key-header authenticator ride along to whatever host the endpoint names, which
    /// turns one hostile 302 into credential exfiltration. Following them by hand is what makes the
    /// same-origin check in <see cref="HttpConnectionConfig.IsAllowedTarget"/> reachable at all.</summary>
    internal static HttpClient CreateClient(HttpConnectionConfig connection)
    {
        var handler = new HttpClientHandler { AllowAutoRedirect = false };
        var client = new HttpClient(handler)
        {
            BaseAddress = connection.BaseUrl,
            Timeout = connection.Timeout,
            MaxResponseContentBufferSize = connection.MaxResponseBytes,
        };
        client.DefaultRequestHeaders.UserAgent.ParseAdd("pz-http/0.1");
        foreach (var (name, value) in connection.Headers)
        {
            client.DefaultRequestHeaders.TryAddWithoutValidation(name, value);
        }

        return client;
    }

    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct)
    {
        var config = HttpDatasetConfig.Parse(spec);
        var schema = config.IsContractMode
            ? ContractProjector.BuildSchema(config.Columns!)
            : ContractProjector.BuildSchema(RawEnvelope.Columns(config));
        return ValueTask.FromResult(new DatasetSchema(schema));
    }

    public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
    {
        scan = null;
        return false;
    }

    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints,
        CancellationToken ct)
    {
        var config = HttpDatasetConfig.Parse(spec);
        return ValueTask.FromResult<IReadOnlyList<IDatasetPartition>>(
            [new HttpPartition(_client, _connection, config, spec, _time, _gate)]);
    }

    /// <summary>The engine calls this exactly once per opened source, after OpenAsync returns and
    /// before any plan/read call — every <see cref="HttpPartition"/> this source hands out afterward
    /// routes its page fetches through the gate.</summary>
    public void UseOperationGate(IOperationGate gate) => _gate = gate;

    public NaturalReadShape GetNaturalReadShape(DatasetSpec spec) =>
        HttpDatasetConfig.Parse(spec).IsSyncMode ? NaturalReadShape.Feed : NaturalReadShape.Full;

    public ValueTask DisposeAsync()
    {
        _client.Dispose();
        return ValueTask.CompletedTask;
    }
}
