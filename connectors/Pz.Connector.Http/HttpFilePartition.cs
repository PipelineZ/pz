using System.Runtime.CompilerServices;
using System.Text;
using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Http;

/// <summary>One file-mode dataset read: download the response body to a per-partition temp
/// directory, open it as zip, gzip or plain, and parse each selected file with the dataset's format
/// reader. The whole download is one gated, idempotent operation (`http.download`); a retry starts
/// it over. Never retries, never sleeps. Emits no checkpoint: a file has no resumable position.</summary>
internal sealed class HttpFilePartition(HttpClient client, HttpClient bareClient, HttpConnectionConfig connection,
    HttpDatasetConfig config, DatasetSpec spec, TimeProvider time, IOperationGate? gate)
    : ICheckpointingPartition
{
    /// <summary>The most decompressed bytes one partition may read, as a multiple of
    /// `max_download_mb`.</summary>
    private const int DecompressionFactor = 20;

    /// <summary>How much of a refused response body is quoted (before redaction and trimming).</summary>
    private const int SnippetBytes = 512;

    private string Label => $"http source '{spec.Source}.{spec.Dataset}'";

    // A plain download, and an export's start and polls: the dataset's own query is user config, so
    // messages mask only the authenticator's secret params, as for JSON pages.
    private readonly HttpRequestSender _sender = new(client, bareClient, connection, config.Body, gate, time,
        $"http source '{spec.Source}.{spec.Dataset}'");

    // An export's download URL is chosen by the API and its query can itself be a credential (a signed
    // link), so messages drop that query whole.
    private readonly HttpRequestSender _exportDownloadSender = new(client, bareClient, connection, config.Body,
        gate, time, $"http source '{spec.Source}.{spec.Dataset}'", redactWholeQuery: true);

    /// <summary>Where a non-fatal notice goes (a zip with no member matching under
    /// `if_missing: empty`); null when the engine subscribed none.</summary>
    public Action<string>? Notice { get; init; }

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
        var now = time.GetUtcNow();
        var bindings = HttpFirstRequest.Bindings(spec, config, config.RunDate.Render(now), Label);
        var first = HttpFirstRequest.Build(connection, config, strategy: null, bindings, Label);

        var download = first;
        var sender = _sender;
        if (config.Export is { } export)
        {
            // Same-origin needs no handling here: the send strips credentials from any request to
            // another origin.
            (download, _) = await new HttpExportJob(export, _sender, connection, config, time, Label, bindings)
                .RunAsync(first, ct).ConfigureAwait(false);
            sender = _exportDownloadSender;
        }

        var reader = ReaderFor(file.Format);
        var budget = new DecompressionBudget(DecompressionFactor * connection.MaxDownloadBytes, Label);
        using var scratch = new DownloadScratch();
        var (path, fallbackName) = await sender.RunAsync("http.download", idempotent: true,
            token => DownloadAsync(sender, download, scratch, token), ct).ConfigureAwait(false);

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

    private IFileFormatReader ReaderFor(HttpFileFormat format) => format switch
    {
        HttpFileFormat.Csv or HttpFileFormat.Tsv => new HttpCsvFileReader(),
        HttpFileFormat.Json => new HttpJsonFileReader(connection.MaxResponseBytes),
        HttpFileFormat.Xml => new HttpXmlFileReader(),
        _ => throw new ArgumentOutOfRangeException(nameof(format), format, null),
    };

    /// <summary>Sends the download, refuses a non-2xx or error-page response, and saves the body
    /// within `max_download_mb`. Returns the saved file and the name a non-zip file goes by.</summary>
    private async Task<(string Path, string FallbackName)> DownloadAsync(HttpRequestSender sender,
        HttpPageRequest request, DownloadScratch scratch, CancellationToken ct)
    {
        using var sent = await sender.SendAsync(request, streamBody: true, credentialsOnlyToOrigin: true, ct)
            .ConfigureAwait(false);
        var content = sent.Response.Content;
        if (!sent.Response.IsSuccessStatusCode)
        {
            throw sender.StatusError(sent, await BodyHeadAsync(content, ct).ConfigureAwait(false));
        }

        var mediaType = content.Headers.ContentType?.MediaType?.ToLowerInvariant();
        if (IsErrorPageType(config.File!.Format, mediaType))
        {
            var snippet = sender.RedactSnippet(await BodyHeadAsync(content, ct).ConfigureAwait(false));
            throw new PzConnectorException(
                $"{Label}: expected a {FormatName(config.File.Format)} file but " +
                $"{sender.Target(sent.Method, sent.SendUri)} answered {mediaType}: {snippet}", isTransient: false);
        }

        string path;
        try
        {
            await using var body = await content.ReadAsStreamAsync(ct).ConfigureAwait(false);
            path = await scratch.SaveAsync(body, connection.MaxDownloadBytes, Label, ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is IOException or HttpRequestException && !ct.IsCancellationRequested)
        {
            throw new PzConnectorException(
                $"{Label}: download from {sender.Target(sent.Method, sent.SendUri)} failed: {ex.Message}",
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
    private static async Task<string> BodyHeadAsync(HttpContent content, CancellationToken ct)
        => HttpRequestSender.StripUrlQueries(await HeadAsync(content, ct).ConfigureAwait(false));

    /// <summary>At most the first <see cref="SnippetBytes"/> bytes of the body, as UTF-8 text.</summary>
    private static async Task<string> HeadAsync(HttpContent content, CancellationToken ct)
    {
        await using var stream = await content.ReadAsStreamAsync(ct).ConfigureAwait(false);
        var buffer = new byte[SnippetBytes];
        var n = await stream.ReadAtLeastAsync(buffer, buffer.Length, throwOnEndOfStream: false, ct)
            .ConfigureAwait(false);
        return Encoding.UTF8.GetString(buffer, 0, n);
    }
}
