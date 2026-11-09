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
/// With `cancel_path`, a job that never became ready is cancelled once when it times out, reports
/// failure, a poll fails or the read is cancelled while polling. The call is never retried, and its
/// outcome is a notice, never an error. A job that reached its ready state is never cancelled.</summary>
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

    /// <summary>The most result files one job's ready response may list.</summary>
    private const int MaxResultFiles = 1000;

    /// <summary>How much of a job id a message shows.</summary>
    private const int MaxJobIdChars = 100;

    private const string StartStage = "export start";
    private const string PollStage = "export poll";
    private const string CancelStage = "export cancel";

    /// <summary>The reasons a cancel notice names.</summary>
    private const string TimedOut = "timed out";
    private const string ReportedFailure = "reported failure";
    private const string PollFailed = "poll failed";
    private const string ReadCancelled = "read was cancelled";

    /// <summary>The longest a cancel call may take, and the shorter bound once the read itself was
    /// cancelled: both well inside the engine's stop grace for a cancelled node.</summary>
    private static readonly TimeSpan MaxCancelWait = TimeSpan.FromSeconds(10);

    private static readonly TimeSpan MaxCancelWaitCancelled = TimeSpan.FromSeconds(5);

    /// <summary>Why the poll loop is about to throw, when the job itself said so (timed out, failed).</summary>
    private string? _stopReason;

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
        JsonNode? ready;
        try
        {
            ready = await PollAsync(jobId, ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Only a job that never became ready is cancelled: a ready job is left alone whatever happens to
            // its download, since a vendor may hand the same finished job to the next attempt.
            var cancelled = ex is OperationCanceledException && ct.IsCancellationRequested;
            await CancelAsync(jobId, cancelled ? ReadCancelled : _stopReason ?? PollFailed, cancelled)
                .ConfigureAwait(false);
            throw;
        }

        return Downloads(ready, jobId);
    }

    /// <summary>Polls until the job is ready and returns the ready response; throws when it fails, times
    /// out or a poll fails.</summary>
    private async Task<JsonNode?> PollAsync(string jobId, CancellationToken ct)
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
                return body;
            }

            if (time.GetUtcNow() >= deadline)
            {
                throw NotReady(jobId);
            }

            wait = retryAfter is { } hint ? TimeSpan.FromTicks(Math.Clamp(hint.Ticks, MinRetryAfter.Ticks,
                MaxRetryAfter.Ticks)) : export.PollInterval;
        }
    }

    /// <summary>Cancels a job that never became ready through `cancel_path`, best effort: once, through the
    /// gate as non-idempotent (paced, never retried), on a token of its own so it still runs after the read
    /// was cancelled. That token is capped at <see cref="MaxCancelWait"/>, or <see cref="MaxCancelWaitCancelled"/>
    /// once the read itself is cancelled (a node timeout or Ctrl-C), and never above <c>timeout_seconds</c>, so
    /// the call always ends well inside the engine's stop grace. Never throws: success and failure are both
    /// notices. The response body is never read: only the status counts.</summary>
    private async Task CancelAsync(string jobId, string reason, bool readCancelled)
    {
        if (export.CancelPath is not { } path)
        {
            return;
        }

        var cap = readCancelled ? MaxCancelWaitCancelled : MaxCancelWait;
        var bound = connection.Timeout < cap ? connection.Timeout : cap;
        using var limit = new CancellationTokenSource(bound, time);
        try
        {
            var url = Resolve(path, "export.cancel_path", jobId);
            var request = new HttpPageRequest(export.CancelMethod, url, null, null);
            await sender.RunAsync("http.export_cancel", idempotent: false, async token =>
            {
                using var sent = await sender.SendAsync(request, streamBody: true, credentialsOnlyToOrigin: true,
                    CancelStage, token).ConfigureAwait(false);
                if (!sent.Response.IsSuccessStatusCode)
                {
                    throw new PzConnectorException(
                        $"{CancelLabel}: HTTP {(int)sent.Response.StatusCode} from {sender.Target(sent)}",
                        isTransient: false);
                }

                return true;
            }, limit.Token).ConfigureAwait(false);
            notice?.Invoke($"{CancelLabel}: cancelled export job {Shown(jobId)} ({reason})");
        }
        catch (OperationCanceledException) when (limit.IsCancellationRequested)
        {
            notice?.Invoke($"{CancelLabel}: could not cancel export job {Shown(jobId)} ({reason}): no response " +
                $"within {bound.TotalSeconds:0.###} s");
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
            if (id is not null)
            {
                // Escaping keeps a job id inside one path segment, but '.' and '..' stay dot segments that URL
                // resolution removes, so a cancel or poll would reach another resource. Such an id, and one no
                // path segment can carry faithfully, is refused before any request uses it.
                if (!UsableInPath(id))
                {
                    throw new PzConnectorException(
                        $"{StartLabel}: export job id '{Shown(id)}' from {target} cannot be used in a URL path (it is " +
                        "empty, '.' or '..', or contains '/', '\\' or a control character)", isTransient: false);
                }

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

            if (list.Count > MaxResultFiles)
            {
                throw new PzConnectorException(
                    $"{PollLabel}: export job {Shown(jobId)}'s ready response lists {list.Count} result files at " +
                    $"'{pointer}'; at most {MaxResultFiles} are read per job (narrow the export, e.g. a shorter " +
                    "date range)", isTransient: false);
            }

            var downloads = new List<HttpPageRequest>(list.Count);
            // Compared after resolution, query included: a repeated URL is one file listed twice, and reading
            // it twice would land every row twice. Different signatures make different URLs.
            var seen = new HashSet<string>(StringComparer.Ordinal);
            var duplicates = 0;
            for (var i = 0; i < list.Count; i++)
            {
                if (UrlText(list[i]) is not { } entry)
                {
                    throw new PzConnectorException(
                        $"{PollLabel}: export.download_pointer '{pointer}' entry [{i}] is not a URL string in export " +
                        $"job {Shown(jobId)}'s ready response", isTransient: false);
                }

                var url = DownloadUrl(entry, $"download URL [{i}] at '{pointer}'", jobId);
                if (seen.Add(url.AbsoluteUri))
                {
                    downloads.Add(Get(url));
                }
                else
                {
                    duplicates++;
                }
            }

            if (duplicates > 0)
            {
                notice?.Invoke($"{PollLabel}: export job {Shown(jobId)}'s ready response lists {duplicates} duplicate " +
                    $"result file URLs at '{pointer}'; each file is read once");
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

    private static bool UsableInPath(string id)
        => id.Length > 0 && id is not ("." or "..")
            && !id.Any(c => c is '/' or '\\' || char.IsControl(c));

    /// <summary>A job id as messages show it: on one line, and at most <see cref="MaxJobIdChars"/>
    /// characters (a server can return anything as an id).</summary>
    private static string Shown(string jobId)
    {
        var flat = string.Concat(jobId.Replace("\r\n", " ", StringComparison.Ordinal)
            .Select(c => char.IsControl(c) ? ' ' : c));
        return flat.Length <= MaxJobIdChars ? flat : flat[..MaxJobIdChars] + "…";
    }
}
