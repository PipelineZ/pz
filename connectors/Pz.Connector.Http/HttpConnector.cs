using Pz.Connectors.Abstractions;

[assembly: PzConnector("http", typeof(Pz.Connector.Http.HttpConnector))]

namespace Pz.Connector.Http;

/// <summary>Generic HTTP/REST API connector. Universal tier only; reads are GET only and declare
/// BoundedWindow ((lower, upper] window pushdown via the window_upper binding), SyncState (delta-link
/// mode: <see cref="HttpPartition"/> replays a stored token verbatim and captures the terminal page's
/// delta link via `delta_pointer`), GatedOperations (<see cref="HttpSource"/> routes every page fetch
/// through an engine-supplied <see cref="IOperationGate"/> when one is provided), and StablePartitionIds
/// + CheckpointableReads (the single partition's id is `source.dataset`, stable across plans;
/// <see cref="HttpPartition"/> implements <see cref="ICheckpointingPartition"/> and offers the
/// continuation link itself as the opaque checkpoint token, so an interrupted crawl resumes
/// mid-partition instead of restarting from the first page). On the sink side <see cref="HttpSink"/>
/// supports append (chunked JSON row-array/ndjson requests, ack-on-2xx) and merge (keyed per-row
/// PUT/PATCH); replace is refused earlier (PZ0324). CheckpointableWrites: <see cref="HttpWriteSession"/>
/// implements
/// <see cref="ICheckpointingSinkSession"/>, tracking cumulative 2xx-confirmed rows and accepting a
/// resume prefix so a retried delivery picks up strictly after the last acknowledged row. The sink
/// side is an explicit interface implementation because <c>ISourceConnector.OpenAsync</c> and
/// <c>ISinkConnector.OpenAsync</c> differ only by return type.</summary>
public sealed class HttpConnector : ISourceConnector, ISinkConnector, IOutputConfigSchema
{
    public ConnectorInfo Info { get; } = new("http", "0.1.0", ProtocolVersion.Major);

    public ConnectorCapabilities Capabilities =>
        ConnectorCapabilities.BoundedWindow | ConnectorCapabilities.SyncState |
        ConnectorCapabilities.GatedOperations | ConnectorCapabilities.StablePartitionIds |
        ConnectorCapabilities.CheckpointableReads |
        ConnectorCapabilities.Merge | ConnectorCapabilities.CheckpointableWrites;

    public string ConnectionConfigSchema => """
        { "type": "object", "required": ["base_url"], "properties": {
            "base_url": { "type": "string" },
            "check_path": { "type": "string" },
            "auth": { "type": "object", "properties": {
                "type": { "enum": ["api_key", "bearer", "basic", "azure_ad", "oauth2_client_credentials"] },
                "token": { "type": "string" }, "user": { "type": "string" },
                "password": { "type": "string" }, "key": { "type": "string" },
                "header": { "type": "string" }, "param": { "type": "string" },
                "method": { "enum": ["service_principal", "managed_identity", "credential_chain"] },
                "tenant_id": { "type": "string" }, "client_id": { "type": "string" },
                "client_secret": { "type": "string", "writeOnly": true },
                "certificate_path": { "type": "string" },
                "certificate_password": { "type": "string", "writeOnly": true },
                "scope": { "type": "string" }, "authority_host": { "type": "string" },
                "token_url": { "type": "string" },
                "client_auth": { "enum": ["body", "basic"] },
                "params": { "type": "object", "additionalProperties": { "type": ["string", "number", "boolean"] } } },
                "required": ["type"], "additionalProperties": false },
            "headers": { "type": "object", "additionalProperties": { "type": "string" } },
            "timeout_seconds": { "type": "number", "exclusiveMinimum": 0, "maximum": 3600 },
            "max_response_mb": { "type": "integer", "minimum": 1, "maximum": 2047 },
            "allow_hosts": { "type": "array", "items": { "type": "string" } } },
          "additionalProperties": false }
        """;

    public string DatasetConfigSchema => """
        { "type": "object", "required": ["path"], "properties": {
            "path": { "type": "string" },
            "query": { "type": "object", "additionalProperties": { "type": ["string", "number", "boolean"] } },
            "pagination": { "type": "object", "properties": {
                "strategy": { "enum": ["page", "offset", "link_header", "cursor", "next_url"] },
                "param": { "type": "string" }, "start": { "type": "integer" },
                "size_param": { "type": "string" }, "size": { "type": "integer" },
                "pointer": { "type": "string" }, "stop_on_short_page": { "type": "boolean" },
                "location": { "enum": ["query", "body"] } },
                "required": ["strategy"], "additionalProperties": false },
            "items": { "type": "string" },
            "columns": { "type": "object", "minProperties": 1, "additionalProperties": {
                "enum": ["int","bigint","double","decimal","varchar","boolean","date","timestamp"] } },
            "cursor": { "type": "string" }, "cursor_type": { "type": "string" },
            "cursor_pointer": { "type": "string" },
            "cursor_order": { "enum": ["asc", "desc"] },
            "delta_pointer": { "type": "string" },
            "method": { "enum": ["get", "post"] },
            "body": { "type": ["object", "string"] },
            "body_format": { "enum": ["json", "form"] },
            "content_type": { "type": "string" },
            "error_pointer": { "type": "string" },
            "max_pages": { "type": "integer", "minimum": 1 } },
          "additionalProperties": false }
        """;

    // Mirrors what HttpSinkOutputConfig.Parse actually reads: path (required, its own leading-'/'
    // check stays in Parse -- JSON Schema's "pattern" would duplicate that rule in a second syntax),
    // method (post/put/patch), body_format/rows_per_request (append-only; Parse still owns the
    // merge-vs-append cross-field refusal, which a property-level schema cannot express).
    public string OutputConfigSchema => """
        { "type": "object", "properties": {
            "path": { "type": "string" },
            "method": { "enum": ["post", "put", "patch"] },
            "body_format": { "enum": ["json_array", "ndjson"] },
            "rows_per_request": { "type": "integer", "minimum": 1 } },
          "additionalProperties": false }
        """;

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct)
    {
        var errors = new List<string>();
        var connection = HttpConnectionConfig.Parse(config, errors);
        return ValueTask.FromResult(new ValidationResult(errors)
        {
            Warnings = connection?.Warnings() ?? [],
        });
    }

    // Mirrors HttpPartition.MaxRedirects: the client itself never auto-follows (AllowAutoRedirect is
    // off, see HttpSource.CreateClient), so a bound here stops a redirect loop from hanging the check.
    private const int MaxCheckRedirects = 5;

    public async ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct)
    {
        var errors = new List<string>();
        var connection = HttpConnectionConfig.Parse(config, errors);
        if (connection is null || errors.Count > 0)
        {
            return new ConnectionCheck(false, string.Join("; ", errors));
        }

        // Binding builds the credential (and loads a certificate), so its failures are sign-in failures too.
        HttpConnectionConfig bound;
        try
        {
            bound = connection.Bind(TimeProvider.System);
        }
        catch (PzConnectorException ex)
        {
            return new ConnectionCheck(false, "sign-in failed: " + ex.Message);
        }

        try
        {
            using var client = HttpSource.CreateClient(connection);
            // Resolve check_path the same way dataset paths resolve (HttpPartition.BuildFirstUri):
            // a relative segment against the slash-terminated base, not root-relative to the host —
            // otherwise a base_url path prefix (e.g. '/api/v2') is silently dropped.
            var target = connection.CheckPath is { } checkPath
                ? new Uri(connection.BaseUrl, checkPath.TrimStart('/'))
                : connection.BaseUrl;

            // A real read follows a 3xx by hand (HttpPartition.SendFollowingRedirectsAsync) rather
            // than treating it as failure -- the check must accept the same shape of response a read
            // against this exact path would.
            for (var hop = 0; ; hop++)
            {
                using var request = new HttpRequestMessage(HttpMethod.Get, target);
                if (bound.Authenticator is { } authenticator)
                {
                    // Sign-in is its own failure: "the credentials don't work" and "the API said no"
                    // need different fixes, so the first request's sign-in is reported separately.
                    try
                    {
                        await authenticator.ApplyAsync(request, ct).ConfigureAwait(false);
                    }
                    catch (PzConnectorException ex)
                    {
                        return new ConnectionCheck(false, "sign-in failed: " + ex.Message);
                    }
                }

                using var response = await client.SendAsync(request, ct).ConfigureAwait(false);
                bound.Authenticator?.OnResponse(request, (int)response.StatusCode);

                if (response.IsSuccessStatusCode)
                {
                    return new ConnectionCheck(true);
                }

                var status = (int)response.StatusCode;
                if (status is not (301 or 302 or 303 or 307 or 308))
                {
                    return new ConnectionCheck(false, $"GET {target} returned HTTP {status}");
                }

                if (response.Headers.Location is not { } location)
                {
                    return new ConnectionCheck(false,
                        $"GET {target} returned HTTP {status} with no Location header");
                }

                if (hop >= MaxCheckRedirects)
                {
                    return new ConnectionCheck(false,
                        $"more than {MaxCheckRedirects} redirects starting at {target}");
                }

                target = new Uri(target, location);
                if (!connection.IsAllowedTarget(target))
                {
                    return new ConnectionCheck(false,
                        $"the redirect points at '{target.GetLeftPart(UriPartial.Authority)}', which is " +
                        $"not this connection's host '{connection.BaseUrl.GetLeftPart(UriPartial.Authority)}' " +
                        "(add it to 'allow_hosts' if this really is part of the same API)");
                }
            }
        }
        catch (HttpRequestException ex)
        {
            return new ConnectionCheck(false, $"could not reach '{connection.BaseUrl}': {ex.Message}");
        }
        catch (TaskCanceledException ex) when (!ct.IsCancellationRequested)
        {
            // A request timeout (HttpClient's own timeout) surfaces as TaskCanceledException too;
            // only swallow that case. Genuine caller cancellation must propagate, not report as a
            // connection failure.
            return new ConnectionCheck(false, $"could not reach '{connection.BaseUrl}': {ex.Message}");
        }
        finally
        {
            if (connection.AuthFactory is not null)
            {
                (bound.Authenticator as IDisposable)?.Dispose();
            }
        }
    }

    public ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct)
    {
        var errors = new List<string>();
        var connection = HttpConnectionConfig.Parse(config, errors);
        if (connection is null || errors.Count > 0)
        {
            throw new PzConnectorException(
                $"http source: invalid connection config: {string.Join("; ", errors)}", isTransient: false);
        }

        return ValueTask.FromResult<ISource>(new HttpSource(connection));
    }

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct)
    {
        var errors = new List<string>();
        var connection = HttpConnectionConfig.Parse(config, errors);
        if (connection is null || errors.Count > 0)
        {
            throw new PzConnectorException(
                $"http sink: invalid connection config: {string.Join("; ", errors)}", isTransient: false);
        }

        return ValueTask.FromResult<ISink>(new HttpSink(connection));
    }
}
