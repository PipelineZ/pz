using System.Text.Json;
using System.Text.Json.Nodes;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Bindings;
using Pz.Connectors.Toolkit.Http;
using Pz.Connectors.Toolkit.Json;

namespace Pz.Connector.Http;

/// <summary>Runs one export job: sends the start request, polls the status endpoint until the job is
/// ready, failed or past its deadline, and returns the requests that download the result (one per
/// result file). The start goes through the gate as non-idempotent (a retry could create a second job on
/// the vendor); polls are idempotent GETs. Waits are timer waits on <paramref name="time"/>, never holding
/// a gate slot. Path templates expand the read's <paramref name="bindings"/> (rendered once per partition,
/// so start, polls, download and cancel agree on <c>{{ run_date }}</c>), then <c>{job_id}</c>.
///
/// With `cancel_path`, a job the read abandons is cancelled: <see cref="RunAsync"/> cancels it itself
/// when the job times out, fails, a poll fails or the read is cancelled, and the partition calls
/// <see cref="CancelAsync"/> when it stops before reading every result file. Either way the call is made
/// at most once, never retried, and its outcome is a notice, never an error.</summary>
internal sealed class HttpExportJob(HttpExportConfig export, HttpRequestSender sender, HttpConnectionConfig connection,
    HttpDatasetConfig config, TimeProvider time, string label, IReadOnlyDictionary<string, BindingValue> bindings,
    Action<string>? notice = null)
{
    /// <summary>The bounds a poll response's <c>Retry-After</c> is held to: at most 10 minutes, and at
    /// least the smallest `poll_interval_seconds`, so a `Retry-After: 0` cannot turn polling into a
    /// tight loop.</summary>
    private static readonly TimeSpan MaxRetryAfter = TimeSpan.FromSeconds(600);

    private static readonly TimeSpan MinRetryAfter = TimeSpan.FromSeconds(1);

    /// <summary>How much of a failed job's message is quoted.</summary>
    private const int MaxMessageChars = 200;

    /// <summary>How much of a job id a message shows.</summary>
    private const int MaxJobIdChars = 100;

    private const string StartStage = "export start";
    private const string PollStage = "export poll";
    private const string CancelStage = "export cancel";

    /// <summary>The reasons a cancel notice names.</summary>
    public const string TimedOut = "timed out";
    public const string ReportedFailure = "reported failure";
    public const string PollFailed = "poll failed";
    public const string ReadCancelled = "read was cancelled";
    public const string ReadUnfinished = "read did not finish";

    /// <summary>The started job's id; null until the start response named one.</summary>
    private string? _jobId;

    /// <summary>Why the poll loop is about to throw, when the job itself said so (timed out, failed).</summary>
    private string? _stopReason;

    /// <summary>Set by the first <see cref="CancelAsync"/> call: the cancel is attempted at most once.</summary>
    private int _cancelAttempted;

    /// <summary>Every message about the start request names this stage after the label.</summary>
    private string StartLabel => $"{label}: {StartStage}";

    /// <summary>Every message about a poll, or the job state it reports, names this stage after the
    /// label.</summary>
    private string PollLabel => $"{label}: {PollStage}";

    private string CancelLabel => $"{label}: {CancelStage}";

    public async Task<IReadOnlyList<HttpPageRequest>> RunAsync(HttpPageRequest start, CancellationToken ct)
    {
        var jobId = await sender.RunAsync("http.export_start", idempotent: false,
            token => StartAsync(start, token), ct).ConfigureAwait(false);
        _jobId = jobId;
        try
        {
            return await PollAsync(jobId, ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            var reason = ex is OperationCanceledException && ct.IsCancellationRequested
                ? ReadCancelled
                : _stopReason ?? PollFailed;
            await CancelAsync(reason).ConfigureAwait(false);
            throw;
        }
    }

    private async Task<IReadOnlyList<HttpPageRequest>> PollAsync(string jobId, CancellationToken ct)
    {
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
                token => PollOnceAsync(poll, jobId, token), ct).ConfigureAwait(false);
            if (ready)
            {
                return Downloads(body, jobId);
            }

            if (time.GetUtcNow() >= deadline)
            {
                throw NotReady(jobId);
            }

            wait = retryAfter is { } hint ? TimeSpan.FromTicks(Math.Clamp(hint.Ticks, MinRetryAfter.Ticks,
                MaxRetryAfter.Ticks)) : export.PollInterval;
        }
    }

    /// <summary>Cancels the started job through `cancel_path`, best effort: at most once per job, through the
    /// gate as non-idempotent (paced, never retried), on a token of its own bounded by the connection's
    /// timeout (so it still runs after the read was cancelled, and never for long). Never throws: success
    /// and failure are both notices. Does nothing without `cancel_path` or before a job id is known.</summary>
    public async Task CancelAsync(string reason)
    {
        if (export.CancelPath is not { } path || _jobId is not { } jobId
            || Interlocked.Exchange(ref _cancelAttempted, 1) == 1)
        {
            return;
        }

        using var limit = new CancellationTokenSource(connection.Timeout, time);
        try
        {
            var url = Resolve(path, "export.cancel_path", jobId);
            var request = new HttpPageRequest(export.CancelMethod, url, null, null);
            await sender.RunAsync("http.export_cancel", idempotent: false, async token =>
            {
                using var sent = await sender.SendAsync(request, streamBody: false, credentialsOnlyToOrigin: true,
                    CancelStage, token).ConfigureAwait(false);
                if (!sent.Response.IsSuccessStatusCode)
                {
                    var text = await sent.Response.Content.ReadAsStringAsync(token).ConfigureAwait(false);
                    throw sender.StatusError(sent, text, CancelStage);
                }

                return true;
            }, limit.Token).ConfigureAwait(false);
            notice?.Invoke($"{CancelLabel}: cancelled export job {Shown(jobId)} ({reason})");
        }
        catch (OperationCanceledException) when (limit.IsCancellationRequested)
        {
            notice?.Invoke($"{CancelLabel}: could not cancel export job {Shown(jobId)} ({reason}): no response " +
                $"within timeout_seconds ({connection.Timeout.TotalSeconds:0.###} s)");
        }
        catch (Exception ex)
        {
            // Sender messages are already redacted and start with this stage's own label; anything else
            // is masked here, since a notice is shown as is.
            var detail = ex is PzConnectorException && ex.Message.StartsWith(CancelLabel + ": ", StringComparison.Ordinal)
                ? ex.Message[(CancelLabel.Length + 2)..]
                : connection.RedactSecretParams(ex.Message);
            notice?.Invoke($"{CancelLabel}: could not cancel export job {Shown(jobId)} ({reason}): {detail}");
        }
    }

    private PzConnectorException NotReady(string jobId)
    {
        _stopReason = TimedOut;
        return new($"{PollLabel}: export job {Shown(jobId)} not ready after {(int)export.Timeout.TotalMinutes} minutes",
            isTransient: true);
    }

    private async Task<string> StartAsync(HttpPageRequest start, CancellationToken ct)
    {
        var (body, target, _) = await GetJsonAsync(start, StartStage, ct).ConfigureAwait(false);
        if (config.ErrorPointer is { } errorPointer)
        {
            HttpPartition.ThrowIfErrorReported(body, errorPointer, target, StartLabel, stripUrlQueries: false,
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
            $"{StartLabel}: export.job_id '{export.JobIdPointer}' does not resolve to a string or number in the " +
            $"response from {target} (check the pointer against the start response)", isTransient: false);
    }

    /// <summary>One poll: whether the job is ready, the response body (where a download pointer is
    /// read) and the response's <c>Retry-After</c>. A failed status throws; any other status, or a
    /// JSON null, means still running.</summary>
    private async Task<(bool Ready, JsonNode? Body, TimeSpan? RetryAfter)> PollOnceAsync(
        HttpPageRequest poll, string jobId, CancellationToken ct)
    {
        var (body, target, retryAfter) = await GetJsonAsync(poll, PollStage, ct).ConfigureAwait(false);
        if (!JsonPointer.TryResolve(body, export.StatusPointer, out var node))
        {
            throw new PzConnectorException(
                $"{PollLabel}: export.status_pointer '{export.StatusPointer}' not found in the response from {target}",
                isTransient: false);
        }

        var status = node switch
        {
            null => null,
            JsonValue v when v.GetValueKind() == JsonValueKind.String => v.GetValue<string>(),
            JsonValue v when v.GetValueKind() == JsonValueKind.Number => v.ToJsonString(),
            _ => throw new PzConnectorException(
                $"{PollLabel}: export.status_pointer '{export.StatusPointer}' resolves to neither a string nor a " +
                $"number in the response from {target}", isTransient: false),
        };

        if (status is not null && Matches(export.Failed, status))
        {
            _stopReason = ReportedFailure;
            throw new PzConnectorException(
                $"{PollLabel}: export job {Shown(jobId)} failed with status '{status}'{FailureMessage(body)}", isTransient: false);
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
    /// a 2xx response's <c>Retry-After</c>. Every message names <paramref name="stage"/>.</summary>
    private async Task<(JsonNode? Body, string Target, TimeSpan? RetryAfter)> GetJsonAsync(HttpPageRequest request,
        string stage, CancellationToken ct)
    {
        using var sent = await sender.SendAsync(request, streamBody: false, credentialsOnlyToOrigin: true, stage, ct)
            .ConfigureAwait(false);
        var text = await sent.Response.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
        if (!sent.Response.IsSuccessStatusCode)
        {
            var rule = sender.MatchRule(sent, text);
            if (stage == StartStage && rule is { Action: StatusAction.Transient })
            {
                // A transient failure is retried by the node loop, which re-runs the read and POSTs the start
                // again: if the vendor queued a job before refusing, every attempt would leave one more. A
                // rule must not widen that to statuses the built-in classification treats as final.
                throw new PzConnectorException(
                    $"{label}: {stage}: HTTP {(int)sent.Response.StatusCode} from {sender.Target(sent)}: " +
                    $"{sender.RedactSnippet(text, sent.ServerChosen)} matched {rule.Label} (transient), but the " +
                    "export start is never retried by a rule: a retry sends the start again and can create a " +
                    "second export job; re-run once the API accepts exports again (the rule still applies to " +
                    "the polls and the download)", isTransient: false);
            }

            throw sender.StatusError(sent, text, stage, rule);
        }

        var target = sender.Target(sent);
        JsonNode? body;
        try
        {
            body = JsonNode.Parse(text);
        }
        catch (JsonException ex)
        {
            throw new PzConnectorException($"{label}: {stage}: response from {target} is not valid JSON",
                isTransient: false,
                innerException: ex);
        }

        var retryAfter = TransientClassifier.ParseRetryAfter(
            sent.Response.Headers.TryGetValues("Retry-After", out var values) ? values.FirstOrDefault() : null,
            time.GetUtcNow());
        return (body, target, retryAfter);
    }

    /// <summary>The download requests of a ready job: `download_path`, or what `download_pointer` names in
    /// the ready response: one URL, or a list of URLs read in order. Every URL is checked before the first
    /// download, so one bad entry fails the read before any rows land. An empty list is a notice and no
    /// files.</summary>
    private IReadOnlyList<HttpPageRequest> Downloads(JsonNode? ready, string jobId)
    {
        if (export.DownloadPath is { } path)
        {
            return [Get(Resolve(path, "export.download_path", jobId))];
        }

        var pointer = export.DownloadPointer!;
        JsonPointer.TryResolve(ready, pointer, out var node);
        if (node is JsonArray list)
        {
            if (list.Count == 0)
            {
                notice?.Invoke($"{PollLabel}: export job {Shown(jobId)}'s ready response lists no result files at " +
                    $"'{pointer}'; read as empty");
                return [];
            }

            var downloads = new List<HttpPageRequest>(list.Count);
            for (var i = 0; i < list.Count; i++)
            {
                if (UrlText(list[i]) is not { } entry)
                {
                    throw new PzConnectorException(
                        $"{PollLabel}: export.download_pointer '{pointer}' entry [{i}] is not a URL string in export " +
                        $"job {Shown(jobId)}'s ready response", isTransient: false);
                }

                downloads.Add(Get(DownloadUrl(entry, $"download URL [{i}] at '{pointer}'", jobId)));
            }

            return downloads;
        }

        if (UrlText(node) is not { } text)
        {
            throw new PzConnectorException(
                $"{PollLabel}: export.download_pointer '{pointer}' does not resolve to a URL or a list of URLs in " +
                $"export job {Shown(jobId)}'s ready response", isTransient: false);
        }

        return [Get(DownloadUrl(text, $"download URL at '{pointer}'", jobId))];

        static HttpPageRequest Get(Uri url) => new(HttpMethod.Get, url, null, null);
    }

    private static string? UrlText(JsonNode? node)
        => node is JsonValue value && value.GetValueKind() == JsonValueKind.String
            && value.GetValue<string>() is { Length: > 0 } text ? text : null;

    /// <summary>One result URL, resolved against <c>base_url</c> and checked against <c>allow_hosts</c>;
    /// <paramref name="what"/> names it in messages (never its query).</summary>
    private Uri DownloadUrl(string text, string what, string jobId)
    {
        if (!Uri.TryCreate(connection.BaseUrl, text, out var url))
        {
            throw new PzConnectorException(
                $"{PollLabel}: export job {Shown(jobId)}'s {what} is not a valid URL",
                isTransient: false);
        }

        sender.EnsureAllowedTarget(url, $"export job {Shown(jobId)}'s {what}", PollStage);
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

    /// <summary>A job id as messages show it: on one line, and at most <see cref="MaxJobIdChars"/>
    /// characters (a server can return anything as an id).</summary>
    private static string Shown(string jobId)
    {
        var flat = jobId.Replace("\r\n", " ", StringComparison.Ordinal).Replace('\r', ' ').Replace('\n', ' ');
        return flat.Length <= MaxJobIdChars ? flat : flat[..MaxJobIdChars] + "…";
    }
}
