using Pz.Connectors.Abstractions;

[assembly: PzConnector("http", typeof(Pz.Connector.Http.HttpConnector))]

namespace Pz.Connector.Http;

/// <summary>Generic HTTP/REST API connector. Universal tier only; reads are GET or POST (`method:
/// post`, with a JSON or form `body`), in raw or contract (`columns:`) mode, and declare
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
                "type": { "enum": ["api_key", "bearer", "basic", "azure_ad", "oauth2_client_credentials", "oauth2_refresh_token", "session"] },
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
                "refresh_token": { "type": "string", "writeOnly": true },
                "rotation": { "enum": ["refuse", "ignore"] },
                "client_auth": { "enum": ["body", "basic"] },
                "params": { "type": "object", "additionalProperties": { "type": ["string", "number", "boolean"] } },
                "login": { "type": "object", "properties": {
                    "path": { "type": "string" }, "url": { "type": "string" },
                    "method": { "enum": ["post", "get"] },
                    "body": { "type": ["object", "string"], "writeOnly": true },
                    "body_format": { "enum": ["json", "form"] },
                    "content_type": { "type": "string" },
                    "headers": { "type": "object", "additionalProperties": { "type": ["string", "number", "boolean"] } } },
                  "additionalProperties": false },
                "token_pointer": { "type": "string" },
                "inject": { "oneOf": [
                    { "const": "bearer" },
                    { "type": "object", "properties": {
                        "header": { "type": "string" }, "prefix": { "type": "string" }, "query": { "type": "string" } },
                      "additionalProperties": false } ] },
                "ttl": { "type": "string", "pattern": "^[0-9]+(ms|s|m|h|d)$" },
                "expires_in_pointer": { "type": "string" },
                "reauth_status": { "type": "array", "minItems": 1,
                    "items": { "type": "integer", "minimum": 400, "maximum": 499 } } },
                "required": ["type"], "additionalProperties": false },
            "headers": { "type": "object", "additionalProperties": { "type": "string" } },
            "timeout_seconds": { "type": "number", "exclusiveMinimum": 0, "maximum": 3600 },
            "max_response_mb": { "type": "integer", "minimum": 1, "maximum": 2047 },
            "max_download_mb": { "type": "integer", "minimum": 1, "maximum": 1048576 },
            "allow_hosts": { "type": "array", "items": { "type": "string" } },
            "retry_after": { "type": "object", "properties": {
                "header": { "type": "string" }, "kind": { "enum": ["seconds", "ms", "epoch"] } },
                "required": ["header", "kind"], "additionalProperties": false },
            "proxy": { "oneOf": [
                { "const": "none" },
                { "type": "object", "properties": {
                    "url": { "type": "string" }, "user": { "type": "string" },
                    "password": { "type": "string", "writeOnly": true },
                    "bypass": { "type": "array", "items": { "type": "string" } } },
                  "required": ["url"], "additionalProperties": false } ] },
            "tls": { "type": "object", "properties": {
                "ca_file": { "type": "string" },
                "client_certificate": { "type": "object", "properties": {
                    "path": { "type": "string" }, "key_path": { "type": "string" },
                    "password": { "type": "string", "writeOnly": true } },
                  "required": ["path"], "additionalProperties": false } },
              "additionalProperties": false } },
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
                "pointer": { "type": "string" }, "header": { "type": "string" },
                "stop_on_short_page": { "type": "boolean" },
                "location": { "enum": ["query", "body", "header"] },
                "concurrency": { "type": "integer", "minimum": 1, "maximum": 16 } },
                "required": ["strategy"], "additionalProperties": false },
            "items": { "type": "string" },
            "columns": { "type": "object", "minProperties": 1, "additionalProperties": {
                "enum": ["int","bigint","double","decimal","varchar","boolean","date","timestamp"] } },
            "column_pointers": { "type": "object", "additionalProperties": { "type": "string" } },
            "epoch_unit": { "enum": ["s", "ms"] },
            "watermark_format": { "type": "string", "minLength": 1 },
            "watermark_time_zone": { "type": "string", "minLength": 1 },
            "cursor": { "type": "string" }, "cursor_type": { "type": "string" },
            "cursor_pointer": { "type": "string" },
            "cursor_order": { "enum": ["asc", "desc"] },
            "stop_at_watermark": { "type": "boolean" },
            "delta_pointer": { "type": "string" },
            "method": { "enum": ["get", "post"] },
            "body": { "type": ["object", "string"] },
            "body_format": { "enum": ["json", "form"] },
            "content_type": { "type": "string" },
            "error_pointer": { "type": "string" },
            "on_status": { "type": "array", "minItems": 1, "items": { "type": "object", "properties": {
                    "status": { "type": ["integer", "string", "array"] },
                    "body_contains": { "type": "string", "minLength": 1 },
                    "action": { "enum": ["transient", "permanent", "skip"] },
                    "retry_after": { "type": "string" } },
                    "required": ["status", "action"], "additionalProperties": false } },
            "max_pages": { "type": "integer", "minimum": 1 },
            "run_date": { "type": "object", "properties": {
                "offset_days": { "type": "integer", "minimum": -36500, "maximum": 36500 }, "timezone": { "type": "string" },
                "format": { "type": "string" } },
                "additionalProperties": false },
            "file": { "type": "object", "properties": {
                "format": { "enum": ["csv", "tsv", "json", "xml"] },
                "archive": { "enum": ["auto", "zip", "gzip", "none"] },
                "members": { "type": "string" },
                "if_missing": { "enum": ["error", "empty"] },
                "file_column": { "type": "string" },
                "encoding": { "enum": ["utf-8", "utf-16", "iso-8859-1"] },
                "delimiter": { "type": "string", "minLength": 1, "maxLength": 1 },
                "header": { "type": "boolean" },
                "skip_lines": { "type": "integer", "minimum": 0 },
                "layout": { "enum": ["lines", "document"] },
                "records": { "type": "string" } },
                "required": ["format"], "additionalProperties": false },
            "export": { "type": "object", "properties": {
                "job_id": { "type": "string" },
                "status_path": { "type": "string" }, "status_pointer": { "type": "string" },
                "ready": { "type": "array", "items": { "type": "string" }, "minItems": 1 },
                "failed": { "type": "array", "items": { "type": "string" } },
                "message_pointer": { "type": "string" },
                "download_pointer": { "type": "string" }, "download_path": { "type": "string" },
                "poll_interval_seconds": { "type": "integer", "minimum": 1, "maximum": 3600 },
                "timeout_minutes": { "type": "integer", "minimum": 1, "maximum": 1440 },
                "cancel_path": { "type": "string" },
                "cancel_method": { "type": "string",
                    "pattern": "^([Dd][Ee][Ll][Ee][Tt][Ee]|[Pp][Oo][Ss][Tt]|[Pp][Uu][Tt]|[Pp][Aa][Tt][Cc][Hh])$" } },
                "required": ["job_id", "status_path", "status_pointer", "ready"],
                "additionalProperties": false },
            "for_each": { "type": "object", "properties": {
                "path": { "type": "string" },
                "query": { "type": "object", "additionalProperties": { "type": ["string", "number", "boolean"] } },
                "method": { "enum": ["get", "post"] },
                "body": { "type": ["object", "string"] },
                "body_format": { "enum": ["json", "form"] },
                "content_type": { "type": "string" },
                "items": { "type": "string" },
                "pagination": { "type": "object", "properties": {
                    "strategy": { "enum": ["page", "offset", "link_header", "cursor", "next_url"] },
                    "param": { "type": "string" }, "start": { "type": "integer" },
                    "size_param": { "type": "string" }, "size": { "type": "integer" },
                    "pointer": { "type": "string" }, "header": { "type": "string" },
                    "stop_on_short_page": { "type": "boolean" },
                    "location": { "enum": ["query", "body", "header"] },
                "concurrency": { "type": "integer", "minimum": 1, "maximum": 16 } },
                    "required": ["strategy"], "additionalProperties": false },
                "error_pointer": { "type": "string" },
                "on_status": { "type": "array", "minItems": 1, "items": { "type": "object", "properties": {
                    "status": { "type": ["integer", "string", "array"] },
                    "body_contains": { "type": "string", "minLength": 1 },
                    "action": { "enum": ["transient", "permanent", "skip"] },
                    "retry_after": { "type": "string" } },
                    "required": ["status", "action"], "additionalProperties": false } },
                "max_pages": { "type": "integer", "minimum": 1 },
                "fields": { "type": "object", "minProperties": 1,
                    "additionalProperties": { "type": "string" } },
                "max_items": { "type": "integer", "minimum": 1 },
                "on_missing": { "enum": ["error", "skip"] },
                "concurrency": { "type": "integer", "minimum": 1, "maximum": 16 },
                "values": { "type": "array", "minItems": 1, "items": { "type": "object", "minProperties": 1,
                    "additionalProperties": { "type": ["string", "number", "boolean", "null"] } } },
                "dedupe": { "type": "boolean" } },
                "anyOf": [ { "required": ["path", "fields"] }, { "required": ["values"] } ],
                "additionalProperties": false } },
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

        // The 'tls:' files load first: their errors name the setting and are not sign-in failures.
        BoundTransport? transport;
        try
        {
            transport = BoundTransport.Load(connection.Transport);
        }
        catch (PzConnectorException ex)
        {
            return new ConnectionCheck(false, ex.Message);
        }

        // Binding builds the credential (and loads a certificate), so its failures are sign-in failures too.
        // A failed Bind disposes the transport itself, whatever it throws.
        HttpConnectionConfig bound;
        try
        {
            bound = connection.Bind(TimeProvider.System, transport);
        }
        catch (PzConnectorException ex)
        {
            return new ConnectionCheck(false, "sign-in failed: " + ex.Message);
        }

        // Resolve check_path the same way dataset paths resolve (HttpFirstRequest.Build):
        // a relative segment against the slash-terminated base, not root-relative to the host —
        // otherwise a base_url path prefix (e.g. '/api/v2') is silently dropped.
        var target = connection.CheckPath is { } checkPath
            ? new Uri(connection.BaseUrl, checkPath.TrimStart('/'))
            : connection.BaseUrl;
        try
        {
            using var client = HttpSource.CreateClient(bound);

            // A real read follows a 3xx by hand (HttpRequestSender.SendAsync) rather
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
                if (status == 407)
                {
                    return new ConnectionCheck(false, $"GET {target}: {HttpHandlerFactory.ProxyAuthenticationFailed}");
                }

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
            var detail = HttpHandlerFactory.Describe(ex, bound, target)?.Detail ?? ex.Message;
            return new ConnectionCheck(false, $"could not reach '{connection.BaseUrl}': {detail}");
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

            transport?.Dispose();
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
