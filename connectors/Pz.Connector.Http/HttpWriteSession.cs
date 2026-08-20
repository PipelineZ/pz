using System.Text;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Formats;
using Pz.Connectors.Toolkit.Http;

namespace Pz.Connector.Http;

/// <summary>One output's write session: serializes each engine-owned batch
/// to JSON rows (NdjsonWriteCodec — the batch is fully consumed before WriteBatchAsync
/// returns; nothing is retained), chunks them into requests, and counts a row as delivered
/// only when its request returns 2xx. Append chunks rows into requests; merge instead sends
/// one keyed PUT/PATCH per row (URL-escaped key substituted for the '{key}' path token).
/// Implements <see cref="ICheckpointingSinkSession"/>: acknowledged-row accounting folds in
/// any accepted resume prefix, so a retried attempt's commit total counts rows it never
/// re-sent. Never retries, never sleeps — classification into PzConnectorException(IsTransient,
/// RetryAfter) is this class's whole resilience duty.</summary>
internal sealed class HttpWriteSession(HttpClient client, HttpConnectionConfig connection,
    HttpSinkOutputConfig config, OutputSpec spec, TimeProvider time, IOperationGate? gate)
    : ICheckpointingSinkSession
{
    private long _rowsDelivered;
    private long _requests;
    private long _resumedRows;
    private long _lastReportedAck = -1;
    private bool _committed;
    private bool _aborted;

    private string Label => $"http sink '{spec.Sink}.{spec.Output}'";

    public async ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
    {
        ThrowIfFinished();

        var lines = await SerializeAsync(batch, ct).ConfigureAwait(false);

        if (spec.Mode == "merge")
        {
            foreach (var line in lines)
            {
                var row = System.Text.Json.Nodes.JsonNode.Parse(line)!;
                var keyValue = row[spec.Keys[0]]?.ToString();
                if (string.IsNullOrEmpty(keyValue))
                {
                    throw new PzConnectorException(
                        $"{Label}: merge key '{spec.Keys[0]}' is null/empty for a row -- keyed delivery " +
                        "requires a non-null key on every row", isTransient: false);
                }

                var uri = new Uri(connection.BaseUrl,
                    config.Path.TrimStart('/').Replace("{key}", Uri.EscapeDataString(keyValue), StringComparison.Ordinal));
                _rowsDelivered += await SendAsync(config.Method, uri, line, "application/json", 1, ct)
                    .ConfigureAwait(false);
                _requests++;
            }

            return;
        }

        for (var offset = 0; offset < lines.Count; offset += config.RowsPerRequest)
        {
            var chunk = lines.Skip(offset).Take(config.RowsPerRequest).ToList();
            var body = config.BodyFormat == "ndjson"
                ? string.Join("\n", chunk) + "\n"
                : "[" + string.Join(",", chunk) + "]";
            var contentType = config.BodyFormat == "ndjson" ? "application/x-ndjson" : "application/json";
            var uri = new Uri(connection.BaseUrl, config.Path.TrimStart('/'));

            _rowsDelivered += await SendAsync(config.Method, uri, body, contentType, chunk.Count, ct)
                .ConfigureAwait(false);
            _requests++;
        }
    }

    /// <summary>Stateless resume: the engine's validated prefix count is all the
    /// session needs — subsequent rows arrive already offset past it, and commit totals fold
    /// it back in.</summary>
    public bool TryResumeFrom(long acknowledgedRows)
    {
        _resumedRows = acknowledgedRows;
        return true;
    }

    public bool TryGetAcknowledgedRows(out long acknowledgedRows)
    {
        acknowledgedRows = _resumedRows + _rowsDelivered;
        if (acknowledgedRows == _lastReportedAck)
        {
            return false;
        }

        _lastReportedAck = acknowledgedRows;
        return true;
    }

    private static async Task<List<string>> SerializeAsync(RecordBatch batch, CancellationToken ct)
    {
        using var buffer = new MemoryStream();
        await NdjsonWriteCodec.WriteAsync(batch, buffer, ct).ConfigureAwait(false);
        buffer.Position = 0;
        var lines = new List<string>(batch.Length);
        using var reader = new StreamReader(buffer, Encoding.UTF8);
        while (await reader.ReadLineAsync(ct).ConfigureAwait(false) is { } line)
        {
            if (line.Length > 0)
            {
                lines.Add(line);
            }
        }

        return lines;
    }

    private Task<int> SendAsync(HttpMethod method, Uri uri, string body, string contentType, int rows, CancellationToken ct)
        => gate is null
            ? SendCoreAsync(method, uri, body, contentType, rows, ct)
            // idempotent: spec.Mode == "merge" is sound only because a merge body is always the
            // FULL row (a retried send converges to the same destination state) -- a future
            // partial-body PATCH for merge must NOT inherit this flag.
            : gate.ExecuteAsync("http.send", idempotent: spec.Mode == "merge",
                innerCt => SendCoreAsync(method, uri, body, contentType, rows, innerCt), ct);

    private async Task<int> SendCoreAsync(HttpMethod method, Uri uri, string body, string contentType,
        int rows, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(method, uri)
        {
            Content = new StringContent(body, Encoding.UTF8, contentType),
        };
        connection.Authenticator?.Apply(request);
        // Never echo request/response bodies or query strings (auth may live there): error
        // messages carry the path-only URI.
        var safeUri = (request.RequestUri ?? uri).GetLeftPart(UriPartial.Path);

        HttpResponseMessage response;
        try
        {
            response = await client.SendAsync(request, ct).ConfigureAwait(false);
        }
        catch (HttpRequestException ex)
        {
            throw new PzConnectorException($"{Label}: request to {safeUri} failed: {ex.Message}",
                TransientClassifier.IsTransientException(ex), innerException: ex);
        }
        catch (TaskCanceledException ex) when (!ct.IsCancellationRequested)
        {
            throw new PzConnectorException($"{Label}: request to {safeUri} timed out",
                isTransient: true, innerException: ex);
        }

        using (response)
        {
            var status = (int)response.StatusCode;
            if (response.IsSuccessStatusCode)
            {
                return rows;
            }

            if (TransientClassifier.IsTransientStatus(status))
            {
                var retryAfter = TransientClassifier.ParseRetryAfter(
                    response.Headers.TryGetValues("Retry-After", out var values) ? values.FirstOrDefault() : null,
                    time.GetUtcNow());
                throw new PzConnectorException($"{Label}: HTTP {status} from {safeUri}",
                    isTransient: true, retryAfter);
            }

            throw new PzConnectorException(
                $"{Label}: HTTP {status} from {safeUri} (check the endpoint path and auth config)",
                isTransient: false);
        }
    }

    public ValueTask<WriteResult> CommitAsync(CancellationToken ct)
    {
        ThrowIfFinished();
        _committed = true;
        // Everything already delivered request-by-request: commit is bookkeeping.
        // Resumed-prefix rows count toward the total even though this session never re-sent them.
        return ValueTask.FromResult(new WriteResult(_resumedRows + _rowsDelivered, _requests));
    }

    public ValueTask AbortAsync(CancellationToken ct)
    {
        if (_committed)
        {
            throw new InvalidOperationException("abort must not be called after commit was attempted");
        }

        // AbortSemantics.None, honestly: nothing to clean up — delivered rows stay delivered.
        _aborted = true;
        return ValueTask.CompletedTask;
    }

    private void ThrowIfFinished()
    {
        if (_committed || _aborted)
        {
            throw new InvalidOperationException("the write session is already committed or aborted");
        }
    }

    public ValueTask DisposeAsync() => default;
}
