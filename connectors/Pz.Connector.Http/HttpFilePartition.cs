using System.Runtime.CompilerServices;
using System.Text;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Bindings;

namespace Pz.Connector.Http;

/// <summary>One file-mode dataset read: download the response body to a per-partition temp
/// directory, open it as zip, gzip or plain, and parse each selected file with the dataset's format
/// reader. The whole download is one gated, idempotent operation (`http.download`); a retry starts
/// it over. Never retries, and never sleeps except to wait out the read's shared <c>Retry-After</c>
/// pause when it can run requests concurrently. Emits no checkpoint: a file has no resumable position.</summary>
internal sealed class HttpFilePartition(HttpClient client, HttpClient bareClient, HttpConnectionConfig connection,
    HttpDatasetConfig config, DatasetSpec spec, TimeProvider time, IOperationGate? gate,
    ThrottlePause? pause = null)
    : ICheckpointingPartition
{
    /// <summary>The most decompressed bytes one partition may read, as a multiple of
    /// `max_download_mb`.</summary>
    private const int DecompressionFactor = 20;

    /// <summary>How much of a refused response body is quoted (before redaction and trimming).</summary>
    private const int SnippetBytes = 512;

    /// <summary>How much of a refused response body an <c>on_status</c> <c>body_contains</c> is matched
    /// against.</summary>
    private const int RuleMatchBytes = 64 * 1024;

    // The notice for a download skipped by an on_status rule; set inside the gated download.
    private string? _skipNotice;

    private string Label => $"http source '{spec.Source}.{spec.Dataset}'";

    private const string DownloadStage = "download";

    /// <summary>Every message about the download names its stage after the label.</summary>
    private string DownloadLabel => $"{Label}: {DownloadStage}";

    // A plain download, and an export's start and polls: the dataset's own query is user config, so
    // messages mask only the authenticator's secret params, as for JSON pages. A redirect off
    // base_url's origin is server-chosen and loses its whole query (SentResponse.ServerChosen).
    private readonly HttpRequestSender _sender = new(client, bareClient, connection, config.Body, gate, time,
        $"http source '{spec.Source}.{spec.Dataset}'", pause: pause, rules: config.OnStatus);

    // An export's download URL is chosen by the API and its query can itself be a credential (a signed
    // link), so messages drop that query whole.
    private readonly HttpRequestSender _exportDownloadSender = new(client, bareClient, connection, config.Body,
        gate, time, $"http source '{spec.Source}.{spec.Dataset}'", redactWholeQuery: true, pause, config.OnStatus);

    /// <summary>Where a non-fatal notice goes (a zip with no member matching under
    /// `if_missing: empty`); null when the engine subscribed none.</summary>
    public Action<string>? Notice { get; init; }

    /// <summary>The parent of the per-read scratch directory; null means `Path.GetTempPath()`. Tests
    /// point it at a directory of their own.</summary>
    internal string? ScratchRoot { get; set; }

    /// <summary>The for_each item this download is for: its fields expand into the request and land on
    /// every row. Null for a dataset without for_each.</summary>
    internal ItemValues? Item { get; init; }

    /// <summary>The engine bindings the request expands, when a for_each read already built them for
    /// every item to share. Null means this partition builds its own from its config.</summary>
    internal IReadOnlyDictionary<string, BindingValue>? Bindings { get; init; }

    /// <summary>The <c>pz_fetched_at</c> of every row, when a for_each read read the clock once for all
    /// items. Null means this partition reads the clock itself.</summary>
    internal DateTimeOffset? FetchedAt { get; init; }

    /// <summary>Where <see cref="Item"/> came from (the list page and record index), appended to an
    /// error building the download request.</summary>
    internal string? ItemOrigin { get; init; }

    /// <summary>When set, a 404 or 410 answering the download ends the read with no rows and sets
    /// <see cref="Missing"/> instead of failing.</summary>
    internal bool SkipMissing { get; init; }

    /// <summary>True once the download was answered 404/410 under <see cref="SkipMissing"/>.</summary>
    internal bool Missing { get; private set; }

    public string PartitionId => $"{spec.Source}.{spec.Dataset}";

    public bool TryResumeFrom(string checkpoint) => false;

    public bool TryGetCheckpoint(out string? checkpoint)
    {
        checkpoint = null;
        return false;
    }

    public async IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var file = config.File!;
        // Read once: every row of this read shares one fetch time and one {{ run_date }}.
        var now = FetchedAt ?? time.GetUtcNow();
        var bindings = Bindings ?? HttpFirstRequest.Bindings(spec, config, config.RunDate.Render(now), Label);
        var first = BuildFirstRequest(bindings);

        var download = first;
        var sender = _sender;
        if (config.Export is { } export)
        {
            // Same-origin needs no handling here: the send strips credentials from any request to
            // another origin.
            download = await new HttpExportJob(export, _sender, connection, config, time, Label, bindings)
                .RunAsync(first, ct).ConfigureAwait(false);
            sender = _exportDownloadSender;
        }

        var reader = ReaderFor(file.Format);
        var budget = new DecompressionBudget(DecompressionFactor * connection.MaxDownloadBytes, Label);
        using var scratch = new DownloadScratch(ScratchRoot);
        var (path, fallbackName) = await sender.RunAsync("http.download", idempotent: true,
            token => DownloadAsync(sender, download, scratch, token), ct).ConfigureAwait(false);
        // DownloadAsync returns a null path only for a 404/410 under SkipMissing or an on_status skip.
        if (path is null)
        {
            Missing = true;
            if (Item is null && _skipNotice is not null)
            {
                Notice?.Invoke(_skipNotice);
            }

            yield break;
        }

        // Open is lazy: a bad, encrypted or non-matching zip throws on the first MoveNext, inside the
        // scratch's using. Each file's stream is opened by the enumerator and disposed here once its
        // reader is done, on success, failure and cancellation alike (readers leave it open).
        var any = false;
        foreach (var source in ArchiveReader.Open(path, file, fallbackName, budget, Label))
        {
            any = true;
            using (source)
            {
                await foreach (var batch in reader.ReadAsync(source, config, now, options, Label, ct)
                                   .ConfigureAwait(false))
                {
                    yield return batch;
                }
            }

            ct.ThrowIfCancellationRequested();
        }

        if (!any)
        {
            Notice?.Invoke($"{Label}: no zip entry matches file.members '{file.Members}'; " +
                "read as empty (if_missing: empty)");
        }
    }

    private HttpPageRequest BuildFirstRequest(IReadOnlyDictionary<string, BindingValue> bindings)
    {
        try
        {
            return HttpFirstRequest.Build(connection, config, strategy: null, bindings, Label, Item);
        }
        catch (PzConnectorException ex) when (ItemOrigin is not null)
        {
            throw new PzConnectorException($"{ex.Message} ({ItemOrigin})", ex.IsTransient, ex.RetryAfter,
                innerException: ex, code: ex.Code, hint: ex.Hint);
        }
    }

    private IFileFormatReader ReaderFor(HttpFileFormat format) => format switch
    {
        HttpFileFormat.Csv or HttpFileFormat.Tsv => new HttpCsvFileReader { Item = Item },
        HttpFileFormat.Json => new HttpJsonFileReader(connection.MaxResponseBytes) { Item = Item },
        HttpFileFormat.Xml => new HttpXmlFileReader { Item = Item },
        _ => throw new ArgumentOutOfRangeException(nameof(format), format, null),
    };

    /// <summary>Sends the download, refuses a non-2xx or error-page response, and saves the body
    /// within `max_download_mb`. Returns the saved file and the name a non-zip file goes by, or a null
    /// path for a 404/410 under <see cref="SkipMissing"/>.</summary>
    private async Task<(string? Path, string FallbackName)> DownloadAsync(HttpRequestSender sender,
        HttpPageRequest request, DownloadScratch scratch, CancellationToken ct)
    {
        using var sent = await sender.SendAsync(request, streamBody: true, credentialsOnlyToOrigin: true,
            DownloadStage, ct).ConfigureAwait(false);
        var content = sent.Response.Content;
        var target = sender.Target(sent);
        var idle = new IdleRead(connection.Timeout, time,
            () => $"{DownloadLabel}: download from {target} {IdleRead.Tail(connection.Timeout)}");
        if (!sent.Response.IsSuccessStatusCode)
        {
            var status = (int)sent.Response.StatusCode;
            if (!sender.RulesCover(status))
            {
                // No rule can match this status: exactly the order this read always had, so an item skipped
                // under on_missing never waits for its error body.
                if (SkipMissing && status is 404 or 410)
                {
                    return (null, "");
                }

                throw sender.StatusError(sent, await BodyHeadAsync(content, idle, ct).ConfigureAwait(false),
                    DownloadStage, rule: null);
            }

            // Rules match the raw head; only the message text has URL queries stripped.
            var head = await HeadAsync(content, idle, sender.RulesReadBody ? RuleMatchBytes : SnippetBytes, ct)
                .ConfigureAwait(false);
            var rule = sender.MatchRule(sent, head);
            // The direct download is the read's only request; an export's download is not (skip is refused
            // with export, so this is defence in depth).
            if (rule is { Action: StatusAction.Skip } && config.Export is null)
            {
                _skipNotice = $"{DownloadLabel}: skipped the read: HTTP {status} from {target} matched " +
                    $"{rule.Label} (skip); 0 rows landed";
                return (null, "");
            }

            if (rule is null && SkipMissing && status is 404 or 410)
            {
                return (null, "");
            }

            throw sender.StatusError(sent, HttpRequestSender.StripUrlQueries(head), DownloadStage, rule);
        }

        var mediaType = content.Headers.ContentType?.MediaType?.ToLowerInvariant();
        if (IsErrorPageType(config.File!.Format, mediaType))
        {
            var snippet = sender.RedactSnippet(await BodyHeadAsync(content, idle, ct).ConfigureAwait(false));
            throw new PzConnectorException(
                $"{DownloadLabel}: expected a {FormatName(config.File.Format)} file but " +
                $"{target} answered {mediaType}: {snippet}", isTransient: false);
        }

        string path;
        try
        {
            await using var body = await content.ReadAsStreamAsync(ct).ConfigureAwait(false);
            path = await scratch.SaveAsync(body, connection.MaxDownloadBytes, DownloadLabel, idle, ct)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is IOException or HttpRequestException && !ct.IsCancellationRequested)
        {
            throw new PzConnectorException(
                $"{DownloadLabel}: download from {target} failed: {ex.Message}",
                isTransient: true, innerException: ex);
        }

        return (path, FallbackName(sent.SendUri));
    }

    /// <summary>A media type (lower-cased, without parameters) that only an error page would carry
    /// for this format. These are usually a login or error page served with 200; reading one as data would
    /// land garbage rows. Missing and generic types are accepted.</summary>
    private static bool IsErrorPageType(HttpFileFormat format, string? mediaType)
    {
        if (mediaType is null)
        {
            return false;
        }

        return mediaType == "text/html"
            || (format != HttpFileFormat.Json && mediaType is "application/json" or "application/problem+json");
    }

    private static string FormatName(HttpFileFormat format) => format.ToString().ToLowerInvariant();

    /// <summary>The final URL path's last segment, unescaped, or `download` when it is empty.</summary>
    private static string FallbackName(Uri uri)
    {
        var path = uri.AbsolutePath;
        var last = Uri.UnescapeDataString(path[(path.LastIndexOf('/') + 1)..]);
        return last.Length == 0 ? "download" : last;
    }

    /// <summary>The head of a refused body with the whole query of every URL in it dropped: an error
    /// page or a storage error can echo a signed link, whose query is the credential.</summary>
    private static async Task<string> BodyHeadAsync(HttpContent content, IdleRead idle, CancellationToken ct)
        => HttpRequestSender.StripUrlQueries(await HeadAsync(content, idle, SnippetBytes, ct).ConfigureAwait(false));

    /// <summary>At most the first <paramref name="limit"/> bytes of the body, as UTF-8 text, each read
    /// bounded by <paramref name="idle"/>.</summary>
    private static async Task<string> HeadAsync(HttpContent content, IdleRead idle, int limit, CancellationToken ct)
    {
        await using var stream = await content.ReadAsStreamAsync(ct).ConfigureAwait(false);
        var buffer = new byte[limit];
        var n = 0;
        int read;
        while (n < buffer.Length
               && (read = await idle.ReadAsync(stream, buffer.AsMemory(n), ct).ConfigureAwait(false)) > 0)
        {
            n += read;
        }

        return Encoding.UTF8.GetString(buffer, 0, n);
    }
}
