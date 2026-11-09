using System.Diagnostics.CodeAnalysis;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Formats;

namespace Pz.Connector.Http;

internal sealed class HttpSource : ISource, IOperationGateAware, INoticeAware, INaturalReadShapeSource
{
    private readonly HttpConnectionConfig _connection;
    private readonly TimeProvider _time;
    private readonly HttpClient _client;
    private readonly HttpClient _bareClient;
    private readonly bool _ownsAuthenticator;
    private IOperationGate? _gate;
    private Action<string>? _notice;

    public HttpSource(HttpConnectionConfig connection) : this(connection, TimeProvider.System)
    {
    }

    internal HttpSource(HttpConnectionConfig connection, TimeProvider time)
    {
        _connection = connection.Bind(time);
        _ownsAuthenticator = connection.AuthFactory is not null;
        _time = time;
        _client = CreateClient(connection);
        _bareClient = CreateBareClient(connection);
    }

    /// <summary>Redirects are followed by <see cref="HttpRequestSender"/> itself, not by the handler.
    /// HttpClient's automatic redirect strips only the <c>Authorization</c> header when the target
    /// changes origin — a connection's configured <c>headers:</c> (commonly where an API key lives)
    /// and any api-key-header authenticator ride along to whatever host the endpoint names, which
    /// turns one hostile 302 into credential exfiltration. Following them by hand is what makes the
    /// same-origin check in <see cref="HttpConnectionConfig.IsAllowedTarget"/> reachable at all.</summary>
    /// <remarks>Responses are requested compressed (gzip/deflate/brotli) and unpacked by the handler:
    /// JSON shrinks to 10–20% on the wire. The handler unpacks BEFORE HttpClient buffers the
    /// content, so <see cref="HttpClient.MaxResponseContentBufferSize"/> ('max_response_mb') caps the
    /// unpacked size — a small compressed response cannot expand past it.</remarks>
    internal static HttpClient CreateClient(HttpConnectionConfig connection)
    {
        var client = CreateBareClient(connection);
        client.BaseAddress = connection.BaseUrl;
        foreach (var (name, value) in connection.Headers)
        {
            client.DefaultRequestHeaders.TryAddWithoutValidation(name, value);
        }

        return client;
    }

    /// <summary>The client for requests to a host other than <c>base_url</c>'s origin: the same handler
    /// settings, timeout, buffer cap and user agent as <see cref="CreateClient"/>, but none of the
    /// connection's <c>headers:</c> (commonly where an API key lives) and no base address, so every
    /// request names its absolute target.</summary>
    internal static HttpClient CreateBareClient(HttpConnectionConfig connection)
    {
        var handler = new HttpClientHandler
        {
            AllowAutoRedirect = false,
            AutomaticDecompression = System.Net.DecompressionMethods.All,
        };
        var client = new HttpClient(handler)
        {
            Timeout = connection.Timeout,
            MaxResponseContentBufferSize = connection.MaxResponseBytes,
        };
        client.DefaultRequestHeaders.UserAgent.ParseAdd("pz-http/0.1");
        return client;
    }

    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct)
    {
        var config = HttpDatasetConfig.Parse(spec);
        var schema = config.IsFileMode
            ? FileOutput.Schema(config)
            : config.IsContractMode
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
        // for_each comes first: its items may themselves be file downloads.
        // One Retry-After pause per read, shared by every request the partition makes (pages, the
        // for_each list, items, downloads).
        var pause = new ThrottlePause(_time);
        IDatasetPartition partition = config.IsForEach
            ? new HttpForEachPartition(_client, _bareClient, _connection, config, spec, _time, _gate, pause)
                { Notice = _notice }
            : config.IsFileMode
                ? new HttpFilePartition(_client, _bareClient, _connection, config, spec, _time, _gate, pause)
                    { Notice = _notice }
                : new HttpPartition(_client, _bareClient, _connection, config, spec, _time, _gate, pause);
        return ValueTask.FromResult<IReadOnlyList<IDatasetPartition>>([partition]);
    }

    /// <summary>The engine calls this exactly once per opened source, after OpenAsync returns and
    /// before any plan/read call — every <see cref="HttpPartition"/> this source hands out afterward
    /// routes its page fetches through the gate.</summary>
    public void UseOperationGate(IOperationGate gate) => _gate = gate;

    /// <summary>Called at most once per open, before any plan/read call; partitions planned afterward
    /// report a file-mode zip with no matching member (`if_missing: empty`), a for_each cap that stopped
    /// the read, or for_each items skipped under `on_missing: skip` through it.</summary>
    public void UseNotice(Action<string> notice) => _notice = notice;

    public NaturalReadShape GetNaturalReadShape(DatasetSpec spec) =>
        HttpDatasetConfig.Parse(spec).IsSyncMode ? NaturalReadShape.Feed : NaturalReadShape.Full;

    public ValueTask DisposeAsync()
    {
        _client.Dispose();
        _bareClient.Dispose();
        if (_ownsAuthenticator)
        {
            (_connection.Authenticator as IDisposable)?.Dispose();
        }

        return ValueTask.CompletedTask;
    }
}
