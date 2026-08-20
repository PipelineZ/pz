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
public sealed class HttpConnector : ISourceConnector, ISinkConnector
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
                "type": { "enum": ["api_key", "bearer", "basic"] },
                "token": { "type": "string" }, "user": { "type": "string" },
                "password": { "type": "string" }, "key": { "type": "string" },
                "header": { "type": "string" }, "param": { "type": "string" } },
                "required": ["type"], "additionalProperties": false },
            "headers": { "type": "object", "additionalProperties": { "type": "string" } },
            "timeout_seconds": { "type": "number", "exclusiveMinimum": 0, "maximum": 3600 },
            "max_response_mb": { "type": "integer", "minimum": 1, "maximum": 4096 },
            "allow_hosts": { "type": "array", "items": { "type": "string" } } },
          "additionalProperties": false }
        """;

    public string DatasetConfigSchema => """
        { "type": "object", "required": ["path"], "properties": {
            "path": { "type": "string" },
            "query": { "type": "object", "additionalProperties": { "type": ["string", "number", "boolean"] } },
            "pagination": { "type": "object", "properties": {
                "strategy": { "enum": ["page", "link_header", "cursor"] },
                "param": { "type": "string" }, "start": { "type": "integer" },
                "size_param": { "type": "string" }, "size": { "type": "integer" },
                "pointer": { "type": "string" } },
                "required": ["strategy"], "additionalProperties": false },
            "items": { "type": "string" },
            "columns": { "type": "object", "minProperties": 1, "additionalProperties": {
                "enum": ["int","bigint","double","decimal","varchar","boolean","date","timestamp"] } },
            "cursor": { "type": "string" }, "cursor_type": { "type": "string" },
            "cursor_pointer": { "type": "string" },
            "cursor_order": { "enum": ["asc", "desc"] },
            "delta_pointer": { "type": "string" },
            "max_pages": { "type": "integer", "minimum": 1 } },
          "additionalProperties": false }
        """;

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct)
    {
        var errors = new List<string>();
        HttpConnectionConfig.Parse(config, errors);
        return ValueTask.FromResult(errors.Count == 0
            ? ValidationResult.Success
            : new ValidationResult(errors));
    }

    public async ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct)
    {
        var errors = new List<string>();
        var connection = HttpConnectionConfig.Parse(config, errors);
        if (connection is null || errors.Count > 0)
        {
            return new ConnectionCheck(false, string.Join("; ", errors));
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
            using var request = new HttpRequestMessage(HttpMethod.Get, target);
            connection.Authenticator?.Apply(request);
            using var response = await client.SendAsync(request, ct).ConfigureAwait(false);
            return response.IsSuccessStatusCode
                ? new ConnectionCheck(true)
                : new ConnectionCheck(false, $"GET {target} returned HTTP {(int)response.StatusCode}");
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
