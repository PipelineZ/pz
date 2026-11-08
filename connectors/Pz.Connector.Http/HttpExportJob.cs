using System.Text.Json;
using System.Text.Json.Nodes;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Bindings;
using Pz.Connectors.Toolkit.Http;
using Pz.Connectors.Toolkit.Json;

namespace Pz.Connector.Http;

/// <summary>Runs one export job: sends the start request, polls the status endpoint until the job is
/// ready, failed or past its deadline, and returns the request that downloads the result. The start
/// goes through the gate as non-idempotent (a retry could create a second job on the vendor); polls
/// are idempotent GETs. Waits are timer waits on <paramref name="time"/>, never holding a gate slot.
/// Path templates expand the read's <paramref name="bindings"/> (rendered once per partition, so
/// start, polls and download agree on <c>{{ run_date }}</c>), then <c>{job_id}</c>.</summary>
internal sealed class HttpExportJob(HttpExportConfig export, HttpRequestSender sender, HttpConnectionConfig connection,
    HttpDatasetConfig config, TimeProvider time, string label, IReadOnlyDictionary<string, BindingValue> bindings)
{
    /// <summary>The bounds a poll response's <c>Retry-After</c> is held to: at most 10 minutes, and at
    /// least the smallest `poll_interval_seconds`, so a `Retry-After: 0` cannot turn polling into a
    /// tight loop.</summary>
    private static readonly TimeSpan MaxRetryAfter = TimeSpan.FromSeconds(600);

    private static readonly TimeSpan MinRetryAfter = TimeSpan.FromSeconds(1);

    /// <summary>How much of a failed job's message is quoted.</summary>
    private const int MaxMessageChars = 200;

    public async Task<(HttpPageRequest Download, bool SameOrigin)> RunAsync(HttpPageRequest start, CancellationToken ct)
    {
        var jobId = await sender.RunAsync("http.export_start", idempotent: false,
            token => StartAsync(start, token), ct).ConfigureAwait(false);
        var deadline = time.GetUtcNow() + export.Timeout;
        var statusUrl = Resolve(export.StatusPath, "export.status_path", jobId);
        var poll = new HttpPageRequest(HttpMethod.Get, statusUrl, null, null);

        var wait = export.PollInterval;
        while (true)
        {
            var left = deadline - time.GetUtcNow();
            if (left <= TimeSpan.Zero)
            {
                throw NotReady(jobId);
            }

            // The last wait ends at the deadline, so the job gets one final poll there.
            await Task.Delay(wait < left ? wait : left, time, ct).ConfigureAwait(false);

            var (ready, body, retryAfter) = await sender.RunAsync("http.export_poll", idempotent: true,
                token => PollAsync(poll, jobId, token), ct).ConfigureAwait(false);
            if (ready)
            {
                var download = DownloadUrl(body, jobId);
                return (new HttpPageRequest(HttpMethod.Get, download, null, null), IsBaseOrigin(download));
            }

            if (time.GetUtcNow() >= deadline)
            {
                throw NotReady(jobId);
            }

            wait = retryAfter is { } hint ? TimeSpan.FromTicks(Math.Clamp(hint.Ticks, MinRetryAfter.Ticks,
                MaxRetryAfter.Ticks)) : export.PollInterval;
        }
    }

    private PzConnectorException NotReady(string jobId)
        => new($"{label}: export job {jobId} not ready after {(int)export.Timeout.TotalMinutes} minutes",
            isTransient: true);

    private async Task<string> StartAsync(HttpPageRequest start, CancellationToken ct)
    {
        var (body, target, _) = await GetJsonAsync(start, ct).ConfigureAwait(false);
        if (config.ErrorPointer is { } errorPointer)
        {
            HttpPartition.ThrowIfErrorReported(body, errorPointer, target, label, stripUrlQueries: false,
                connection.RedactSecretParams);
        }

        if (JsonPointer.TryResolve(body, export.JobIdPointer, out var node) && node is JsonValue value)
        {
            var id = value.GetValueKind() switch
            {
                JsonValueKind.String => value.GetValue<string>(),
                // The number's own text, so 123 stays 123 (never 123.0).
                JsonValueKind.Number => value.ToJsonString(),
                _ => null,
            };
            if (!string.IsNullOrEmpty(id))
            {
                return id;
            }
        }

        throw new PzConnectorException(
            $"{label}: export.job_id '{export.JobIdPointer}' does not resolve to a string or number in the " +
            $"response from {target} (check the pointer against the start response)", isTransient: false);
    }

    /// <summary>One poll: whether the job is ready, the response body (where a download pointer is
    /// read) and the response's <c>Retry-After</c>. A failed status throws; any other status, or a
    /// JSON null, means still running.</summary>
    private async Task<(bool Ready, JsonNode? Body, TimeSpan? RetryAfter)> PollAsync(
        HttpPageRequest poll, string jobId, CancellationToken ct)
    {
        var (body, target, retryAfter) = await GetJsonAsync(poll, ct).ConfigureAwait(false);
        if (!JsonPointer.TryResolve(body, export.StatusPointer, out var node))
        {
            throw new PzConnectorException(
                $"{label}: export.status_pointer '{export.StatusPointer}' not found in the response from {target}",
                isTransient: false);
        }

        var status = node switch
        {
            null => null,
            JsonValue v when v.GetValueKind() == JsonValueKind.String => v.GetValue<string>(),
            JsonValue v when v.GetValueKind() == JsonValueKind.Number => v.ToJsonString(),
            _ => throw new PzConnectorException(
                $"{label}: export.status_pointer '{export.StatusPointer}' resolves to neither a string nor a " +
                $"number in the response from {target}", isTransient: false),
        };

        if (status is not null && Matches(export.Failed, status))
        {
            throw new PzConnectorException(
                $"{label}: export job {jobId} failed with status '{status}'{FailureMessage(body)}", isTransient: false);
        }

        return (status is not null && Matches(export.Ready, status), body, retryAfter);
    }

    private static bool Matches(IReadOnlyList<string> values, string status)
        => values.Any(v => string.Equals(v, status, StringComparison.OrdinalIgnoreCase));

    private string FailureMessage(JsonNode? body)
    {
        if (export.MessagePointer is not { } pointer || !JsonPointer.TryResolve(body, pointer, out var node)
            || node is null)
        {
            return "";
        }

        var text = node is JsonValue v && v.GetValueKind() == JsonValueKind.String
            ? v.GetValue<string>()
            : node.ToJsonString();
        // Redact before cutting, so a cut can never leave half a secret that no longer matches.
        var flat = connection.RedactSecretParams(text.ReplaceLineEndings(" "));
        return flat.Length == 0 ? "" : $": {(flat.Length <= MaxMessageChars ? flat : flat[..MaxMessageChars])}";
    }

    /// <summary>Sends a buffered request on the file-mode credential rules, refuses a non-2xx
    /// response, and parses the body as JSON. Returns the body, the request as messages name it, and
    /// a 2xx response's <c>Retry-After</c>.</summary>
    private async Task<(JsonNode? Body, string Target, TimeSpan? RetryAfter)> GetJsonAsync(HttpPageRequest request,
        CancellationToken ct)
    {
        using var sent = await sender.SendAsync(request, streamBody: false, credentialsOnlyToOrigin: true, ct)
            .ConfigureAwait(false);
        var text = await sent.Response.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
        if (!sent.Response.IsSuccessStatusCode)
        {
            throw sender.StatusError(sent, text);
        }

        var target = sender.Target(sent.Method, sent.SendUri);
        JsonNode? body;
        try
        {
            body = JsonNode.Parse(text);
        }
        catch (JsonException ex)
        {
            throw new PzConnectorException($"{label}: response from {target} is not valid JSON", isTransient: false,
                innerException: ex);
        }

        var retryAfter = TransientClassifier.ParseRetryAfter(
            sent.Response.Headers.TryGetValues("Retry-After", out var values) ? values.FirstOrDefault() : null,
            time.GetUtcNow());
        return (body, target, retryAfter);
    }

    private Uri DownloadUrl(JsonNode? ready, string jobId)
    {
        if (export.DownloadPath is { } path)
        {
            return Resolve(path, "export.download_path", jobId);
        }

        var pointer = export.DownloadPointer!;
        if (!JsonPointer.TryResolve(ready, pointer, out var node)
            || node is not JsonValue value || value.GetValueKind() != JsonValueKind.String
            || value.GetValue<string>() is not { Length: > 0 } text)
        {
            throw new PzConnectorException(
                $"{label}: export.download_pointer '{pointer}' does not resolve to a URL in export job {jobId}'s " +
                "ready response", isTransient: false);
        }

        if (!Uri.TryCreate(connection.BaseUrl, text, out var url))
        {
            throw new PzConnectorException(
                $"{label}: export job {jobId}'s download URL at '{pointer}' is not a valid URL", isTransient: false);
        }

        sender.EnsureAllowedTarget(url, $"export job {jobId}'s download URL");
        return url;
    }

    /// <summary>A path template resolved against <c>base_url</c> like <c>path</c>: bindings expanded and
    /// escaped first, then <c>{job_id}</c> replaced by the escaped job id (so a job id can never
    /// introduce a binding or a path segment).</summary>
    private Uri Resolve(string template, string option, string jobId)
    {
        var expanded = HttpFirstRequest.ExpandPath(template, bindings, label, option)
            .Replace(HttpExportConfig.JobIdToken, Uri.EscapeDataString(jobId), StringComparison.Ordinal);
        return new Uri(connection.BaseUrl, expanded[1..]);
    }

    private bool IsBaseOrigin(Uri uri)
        => string.Equals(uri.Scheme, connection.BaseUrl.Scheme, StringComparison.OrdinalIgnoreCase)
            && string.Equals(uri.Host, connection.BaseUrl.Host, StringComparison.OrdinalIgnoreCase)
            && uri.Port == connection.BaseUrl.Port;
}
