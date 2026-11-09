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
        _client = CreateClient(_connection);
        _bareClient = CreateBareClient(_connection);
    }

    /// <summary>The bound connection: its authenticator and TLS material live as long as this source.</summary>
    internal HttpConnectionConfig Connection => _connection;

    /// <summary>Redirects are followed by <see cref="HttpRequestSender"/> itself, not by the handler.
    /// HttpClient's automatic redirect strips only the <c>Authorization</c> header when the target
    /// changes origin — a connection's configured <c>headers:</c> (commonly where an API key lives)
    /// and any api-key-header authenticator ride along to whatever host the endpoint names, which
    /// turns one hostile 302 into credential exfiltration. Following them by hand is what makes the
    /// same-origin check in <see cref="HttpConnectionConfig.IsAllowedTarget"/> reachable at all.</summary>
    /// <remarks>Responses are requested compressed (gzip/deflate/brotli) and unpacked by the handler:
    /// JSON shrinks to 10–20% on the wire. The handler unpacks BEFORE HttpClient buffers the
    /// content, so <see cref="HttpClient.MaxResponseContentBufferSize"/> ('max_response_mb') caps the
    /// unpacked size — a small compressed response cannot expand past it.
    /// <paramref name="connection"/> must be bound when it sets 'proxy' or 'tls': the handler takes
    /// the loaded material from <see cref="HttpConnectionConfig.Bound"/>. This client presents the client
    /// certificate.</remarks>
    internal static HttpClient CreateClient(HttpConnectionConfig connection)
    {
        var client = NewClient(connection, withClientCertificate: true);
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
    /// request names its absolute target. It never presents the client certificate: that identifies pz to
    /// the API, not to a host the endpoint chose.</summary>
    internal static HttpClient CreateBareClient(HttpConnectionConfig connection)
        => NewClient(connection, withClientCertificate: false);

    /// <summary>The User-Agent of every request the connector sends, the session login included.</summary>
    internal const string UserAgent = "pz-http/0.1";

    private static HttpClient NewClient(HttpConnectionConfig connection, bool withClientCertificate)
    {
        var handler = HttpHandlerFactory.Create(connection, connection.Bound, withClientCertificate);
        var client = new HttpClient(handler)
        {
            Timeout = connection.Timeout,
            MaxResponseContentBufferSize = connection.MaxResponseBytes,
        };
        client.DefaultRequestHeaders.UserAgent.ParseAdd(UserAgent);
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
        var label = $"http source '{spec.Source}.{spec.Dataset}'";
        if (PaginationClashes(_connection, config) is { Count: > 0 } clashes)
        {
            throw new PzConnectorException($"{label}: {string.Join("; ", clashes)}", isTransient: false);
        }

        NoticeIgnoredLocation(config, label, "");
        if (config.ForEach?.List is { } listRequest)
        {
            NoticeIgnoredLocation(listRequest, label, "for_each.");
        }

        // One Retry-After pause per read, shared by every request the partition makes (pages, the
        // for_each list, items, downloads), and only for a read that can have requests in flight
        // together: at concurrency 1 the gate's retry policy alone paces a throttled request, exactly as
        // before (a pause would override retry.max_delay).
        var pause = config.PageConcurrency > 1 || config.ForEach is { Concurrency: > 1 }
            ? new ThrottlePause(_time)
            : null;
        // for_each comes first: its items may themselves be file downloads.
        IDatasetPartition partition = config.IsForEach
            ? new HttpForEachPartition(_client, _bareClient, _connection, config, spec, _time, _gate, pause)
                { Notice = _notice }
            : config.IsFileMode
                ? new HttpFilePartition(_client, _bareClient, _connection, config, spec, _time, _gate, pause)
                    { Notice = _notice }
                : new HttpPartition(_client, _bareClient, _connection, config, spec, _time, _gate, pause)
                    { Notice = _notice };
        return ValueTask.FromResult<IReadOnlyList<IDatasetPartition>>([partition]);
    }

    /// <summary>Every way the dataset's pagination, or its `for_each` list's, clashes with what the connection
    /// sends: a page token header the connection also sets would be sent twice, and a session query token named
    /// like a pagination parameter would overwrite the page position (or be overwritten). Known from the
    /// configs alone, so the plan refuses them all at once and `pz validate` reports them
    /// (<see cref="HttpConnector.ValidateDatasetAsync"/>).</summary>
    internal static List<string> PaginationClashes(HttpConnectionConfig connection, HttpDatasetConfig config)
    {
        var clashes = new List<string>();
        AddClashes(connection, config, "", clashes);
        if (config.ForEach?.List is { } listRequest)
        {
            AddClashes(connection, listRequest, "for_each.", clashes);
        }

        return clashes;
    }

    private static void AddClashes(HttpConnectionConfig connection, HttpDatasetConfig request, string prefix,
        List<string> clashes)
    {
        if (request.HeaderPaging is { } paging && paging.CollisionWith(connection) is { } owner)
        {
            clashes.Add($"{prefix}pagination header '{paging.Header}' is also set by {owner}; the page token " +
                "header must be sent by pagination alone (rename one of them)");
        }

        // Compared as the query string compares them: decoded, in any case.
        if (connection.SessionInject?.Query is { } secret
            && request.QueryPagingParams.FirstOrDefault(p => string.Equals(HttpCheckpointToken.DecodedQueryName(secret), HttpCheckpointToken.DecodedQueryName(p),
                StringComparison.OrdinalIgnoreCase)) is { } clash)
        {
            clashes.Add($"{prefix}pagination query parameter '{clash}' is also the session sign-in's query token " +
                "('auth.inject.query'); each would overwrite the other on every page request (rename one of them, or " +
                "send the session token in a header)");
        }

        if (connection.Authenticator is { } auth and not SessionAuthenticator)
        {
            foreach (var keyParam in auth.SecretQueryParams)
            {
                if (request.QueryPagingParams.FirstOrDefault(p => string.Equals(HttpCheckpointToken.DecodedQueryName(keyParam),
                        HttpCheckpointToken.DecodedQueryName(p), StringComparison.OrdinalIgnoreCase)) is { } keyClash)
                {
                    clashes.Add($"{prefix}pagination query parameter '{keyClash}' is also the api_key query parameter " +
                        "('auth.param'); each would overwrite the other on every page request (rename one of them, or " +
                        "send the key in a header with 'auth.header')");
                }
            }
        }
    }

    /// <summary>An unknown `location` on a GET read is ignored, with a notice.</summary>
    private void NoticeIgnoredLocation(HttpDatasetConfig request, string label, string prefix)
    {
        if (request.IgnoredLocation is { } location)
        {
            _notice?.Invoke($"{label}: {prefix}pagination 'location' '{location}' is not one of query, body, header; " +
                "it is ignored and the token goes in the query string (fix it: `pz validate` reports it)");
        }
    }

    /// <summary>The engine calls this exactly once per opened source, after OpenAsync returns and
    /// before any plan/read call — every <see cref="HttpPartition"/> this source hands out afterward
    /// routes its page fetches through the gate.</summary>
    public void UseOperationGate(IOperationGate gate) => _gate = gate;

    /// <summary>Called at most once per open, before any plan/read call; partitions planned afterward
    /// report a file-mode zip with no matching member (`if_missing: empty`), a for_each cap that stopped
    /// the read, for_each items skipped under `on_missing: skip` or by `dedupe`, a read skipped by an `on_status`
    /// rule, or records `stop_at_watermark` dropped through it, and so does the sign-in (a refresh token rotated
    /// under `auth.rotation: ignore`).</summary>
    public void UseNotice(Action<string> notice)
    {
        _notice = notice;
        HttpAuth.UseNotice(_connection.Authenticator, notice);
    }

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

        _connection.Bound?.Dispose();
        return ValueTask.CompletedTask;
    }
}
