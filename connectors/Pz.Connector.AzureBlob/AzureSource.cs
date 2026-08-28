using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using Apache.Arrow;
using Azure;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Files.DataLake.Models;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Paths;
using Sylvan.Data.Csv;

namespace Pz.Connector.AzureBlob;

/// <summary>Azure source: native scan over the DuckDB azure extension is the one read path. The Azure
/// SDK listing (<see cref="AzureAuth.CreateBlobContainerClient"/> for <c>az</c>, <see
/// cref="AzureAuth.CreateDataLakeFileSystemClient"/> for <c>abfss</c>), narrowed server-side by the glob's
/// static (pre-wildcard) prefix and filtered client-side by regex, serves ONLY <see
/// cref="GetSchemaAsync"/>'s first-match peek -- it stops as soon as one name arrives instead of draining
/// the rest of the listing. <see cref="PlanReadAsync"/> is a refusal stub; reads execute on the native
/// tier.</summary>
internal sealed class AzureSource(ConnectorConfig config) : ISource
{
    /// <summary>Mirrors <c>Pz.Connector.LocalFiles.CsvSource.GetSchemaAsync</c>'s cross-connector-replicated
    /// pattern: for csv, peek the first matched blob's actual header and report only the declared `columns:`
    /// contract entries also present there (the contract-pruning rule). For parquet,
    /// download the first matched blob and read its footer via <see cref="AzureParquetReader.ReadSchema"/>.
    /// For json, there is no header row to peek and no footer to read, so the declared `columns:` contract
    /// IS the schema -- no blob is downloaded. Finds the first match by breaking out of <see
    /// cref="EnumerateMatchingBlobsAsync"/> as soon as one name arrives -- disposing the enumerator early so
    /// listing stops there instead of walking every match; no `List&lt;string&gt;` of all matched names is
    /// ever materialized just to peek. No match is a clean named permanent error.</summary>
    public async ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct)
    {
        var format = GetFormat(spec);
        var loc = AzureUrl.ParseDataset(spec.Options, $"dataset '{spec.Dataset}'");

        string? firstMatch = null;
        await foreach (var name in EnumerateMatchingBlobsAsync(loc, spec, ct).ConfigureAwait(false))
        {
            firstMatch = name;
            break;
        }

        if (firstMatch is null)
        {
            throw NoMatch(spec, loc);
        }

        if (format == "json")
        {
            // No header row to peek (unlike csv) and no footer to read (unlike parquet) -- the declared
            // columns: contract IS the schema, so no blob download is needed here at all.
            var jsonColumns = GetColumnsContract(spec);
            var jsonFields = jsonColumns.Select(kv => AzureTypeNameMap.ToArrowField(kv.Key, kv.Value)).ToArray();
            return new DatasetSchema(new Schema(jsonFields, null));
        }

        var stream = await OpenBlobStreamAsync(config, loc.Scheme, loc.Container, firstMatch, ct).ConfigureAwait(false);
        try
        {
            if (format == "csv")
            {
                var columns = GetColumnsContract(spec);
                var header = await ReadCsvHeaderAsync(stream, ct).ConfigureAwait(false);
                var fields = columns
                    .Where(kv => header.Contains(kv.Key))
                    .Select(kv => AzureTypeNameMap.ToArrowField(kv.Key, kv.Value))
                    .ToArray();
                return new DatasetSchema(new Schema(fields, null));
            }

            return new DatasetSchema(AzureParquetReader.ReadSchema(stream));
        }
        finally
        {
            await stream.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Emits <c>read_parquet('&lt;url&gt;')</c> / <c>read_csv('&lt;url&gt;', header = true,
    /// auto_detect = true | auto_detect = false, columns = {…})</c> / <c>read_json('&lt;url&gt;',
    /// auto_detect = true | columns = {…}, format = 'newline_delimited')</c> wrapped by
    /// <see cref="AzureWindowSql.Wrap"/> for windowed datasets. When `path` is date-templated and the
    /// dataset is windowed (both watermark bounds present), scans the watermark-window's minimal cover
    /// instead of the single literal path, emitting a DuckDB list literal (<c>read_parquet(['url1',
    /// 'url2'])</c>) so a query never re-reads out-of-window partitions; a single-element cover (or a
    /// non-templated path) keeps the single-URL form (<see cref="CoverKeys"/>).
    ///
    /// Csv and json follow the same two-state contract model and never decline: a fully contract-less
    /// dataset passes `auto_detect = true` with no `columns=` map at all for either format, letting DuckDB
    /// infer the schema as part of the real read. A declared `columns:` contract -- partial or full, this
    /// method has no way to tell them apart without reading the file, which it deliberately never does --
    /// renders the strict `columns = {…}` map for BOTH formats: DuckDB reads only the named columns,
    /// `auto_detect` implicitly off. Combining `types = {…}` with `auto_detect = true` is deliberately NOT
    /// used for csv: it works, but it KEEPS every column in the file rather than confining the read to
    /// what's declared. json has no such option anyway -- the bundled DuckDB's `read_json` has no `types=`
    /// named parameter at all (see <c>AzureSqlGenTests.ReadJson_has_no_types_named_parameter</c>). Native
    /// scan is the one tier AzureBlob reads through (it is <see cref="PlanReadAsync"/>-refusal
    /// native-only), so schema inference needs no separate connector-level capability flag (mirrors
    /// LocalFiles' <c>CsvSource.TryGetNativeScan</c>).</summary>
    public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
    {
        var format = spec.Options.TryGetValue("format", out var f) ? f?.ToString() : null;
        var loc = AzureUrl.ParseDataset(spec.Options, $"dataset '{spec.Dataset}'");
        var keyPatterns = CoverKeys(loc.Key, spec);
        var urlList = string.Join(", ", keyPatterns.Select(k =>
            $"'{AzureUrl.Escape(AzureUrl.Render(loc with { Key = k }))}'"));
        var urlArg = keyPatterns.Count == 1 ? urlList : $"[{urlList}]";
        var secret = AzureAuth.CreateSecretSql(config, AzureAuth.SecretName(spec.Source));
        string fragment;
        string mechanism;
        var schemaInferred = false;
        string? sniffFragment = null;

        if (format == "csv")
        {
            // Two-state model, not three -- mirrors LocalFiles' CsvSource.TryGetNativeScan exactly. A
            // declared columns: contract, partial or full (native scan has no way to tell them apart
            // without reading the file, which it deliberately never does), always renders the strict
            // fragment (auto_detect = false, columns = {...}): DuckDB reads only the named columns.
            // The types=+auto_detect=true combination works, but it KEEPS every column in the file
            // instead of confining the read to the declared ones, so csv deliberately does not offer
            // "declare some columns, infer the rest".
            var declared = ExtractColumns(spec); // null (contract-less) or declared (partial or full)
            if (declared is { Count: > 0 })
            {
                var duckColumns = string.Join(", ", declared.Select(c =>
                    $"'{AzureUrl.Escape(c.Key)}': '{AzureTypeNameMap.ToDuckDbName(c.Value, c.Key)}'"));
                fragment = $"read_csv({urlArg}, header = true, auto_detect = false, columns = {{{duckColumns}}})";
            }
            else
            {
                fragment = $"read_csv({urlArg}, header = true, auto_detect = true)";
                schemaInferred = true;
                if (keyPatterns.Count == 1)
                {
                    // Single-key reads only: a multi-key window cover would sniff just one member
                    // file and claim a verdict for the set.
                    sniffFragment = $"sniff_csv({urlList})";
                }
            }

            mechanism = "read_csv";
        }
        else if (format == "json")
        {
            // Two-state model for its own, unrelated reason: DuckDB's read_json in the bundled version
            // has no `types=` named parameter at all (Binder Error: "Invalid named parameter 'types'
            // for function read_json" -- confirmed against the real bundled DuckDB, see
            // AzureSqlGenTests.ReadJson_has_no_types_named_parameter), so read_csv's
            // types=+auto_detect=true combination has no json counterpart. Any declared columns:
            // (partial or full) render as the `columns = {...}` map (auto_detect implicitly off), and
            // only a fully contract-less dataset gets `auto_detect = true` with no map at all.
            var declared = ExtractColumns(spec);
            if (declared is { Count: > 0 })
            {
                var duckColumns = string.Join(", ", declared.Select(c =>
                    $"'{AzureUrl.Escape(c.Key)}': '{AzureTypeNameMap.ToDuckDbName(c.Value, c.Key)}'"));
                fragment = $"read_json({urlArg}, columns = {{{duckColumns}}}, format = 'newline_delimited')";
            }
            else
            {
                fragment = $"read_json({urlArg}, auto_detect = true, format = 'newline_delimited')";
                schemaInferred = true;
            }

            mechanism = "read_json";
        }
        else
        {
            fragment = $"read_parquet({urlArg})";
            mechanism = "read_parquet";
        }

        scan = new NativeScan(AzureWindowSql.Wrap(fragment, spec), SetupStatements: ["install azure", "load azure", secret])
        {
            Mechanism = mechanism,
            SchemaInferred = schemaInferred,
            SniffFragment = sniffFragment,
        };
        return true;
    }

    /// <summary>The container-relative key pattern(s) to scan: the watermark-window minimal cover when the
    /// path is date-templated and both bounds are present, else the single literal key. Pure — no I/O.</summary>
    private static IReadOnlyList<string> CoverKeys(string key, DatasetSpec spec)
    {
        if (!PathTemplate.HasDateTokens(key) || spec.WatermarkValue is null || spec.WatermarkUpperBound is null)
        {
            return [key];
        }

        var lo = PathTemplate.ParseCanonical(spec.WatermarkValue);
        var hi = PathTemplate.ParseCanonical(spec.WatermarkUpperBound);
        return PathTemplate.WindowCover(key, lo, hi);
    }

    /// <summary>Test-only pure projection of <see cref="CoverKeys"/> onto each element's server-side listing
    /// prefix -- the real (networked) listing narrows by these same prefixes; see <see
    /// cref="EnumerateMatchingBlobsAsync"/>. Exposed because the listing itself needs a live Azure/Azurite
    /// endpoint and is exercised by the e2e suite instead.</summary>
    internal static IReadOnlyList<string> CoverPrefixesForTest(string key, DatasetSpec spec) =>
        CoverKeys(key, spec).Select(PathTemplate.StaticPrefix).ToArray();

    /// <summary>Azure reads are native-only, so this is a refusal stub. It preserves two error messages:
    /// csv/json without a declared `columns:` contract gets the clear contract error, everything else gets
    /// the native-only refusal -- the ParquetSource precedent. <see cref="TryGetNativeScan"/> never
    /// declines for a contract-less csv/json dataset, and the planner only ever calls `PlanReadAsync` when
    /// `engine.force_universal`/`files_per_partition` forces the universal tier -- which it refuses
    /// outright for any <see cref="INativeOnlySource"/> at plan time (this connector's contract). So this
    /// method is normally unreachable in practice; the pre-check inside it stays only as defense in depth
    /// if that plan-time refusal were ever bypassed.</summary>
    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct)
    {
        if (GetFormat(spec) is "csv" or "json")
        {
            GetColumnsContract(spec); // throws the named contract error when the columns: contract is absent
        }

        throw new PzConnectorException(
            $"PZ0312: dataset '{spec.Dataset}': azure reads are native-scan only; they cannot run on the " +
            "universal tier (remove engine.force_universal / files_per_partition)",
            isTransient: false);
    }

    public ValueTask DisposeAsync() => default;

    /// <summary>Opens a fresh, independent download stream for one blob/file. With reads native-only,
    /// <see cref="GetSchemaAsync"/>'s peek is the only caller.</summary>
    internal static async Task<Stream> OpenBlobStreamAsync(
        ConnectorConfig config, string scheme, string container, string blobName, CancellationToken ct)
    {
        try
        {
            return scheme == "abfss"
                ? await AzureAuth.CreateDataLakeFileSystemClient(config, container).GetFileClient(blobName)
                    .OpenReadAsync(cancellationToken: ct).ConfigureAwait(false)
                : await AzureAuth.CreateBlobContainerClient(config, container).GetBlobClient(blobName)
                    .OpenReadAsync(cancellationToken: ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is RequestFailedException or IOException)
        {
            throw new PzConnectorException(
                $"azure blob '{container}/{blobName}': download failed: {ex.Message}",
                AzureTransient.IsTransient(ex), innerException: ex);
        }
    }

    /// <summary>Narrows listing to the watermark-window cover (<see cref="CoverKeys"/>): for each cover
    /// element, streams just that element's server-side prefix listing (<see cref="EnumeratePrefixAsync"/>)
    /// and yields the names matching that same element's wildcard tail as SDK pages arrive -- never one
    /// element's glob against another element's listing. <c>WindowCover</c>/<c>CoverKeys</c> elements are
    /// disjoint and a single prefix listing yields no duplicate names, so this deliberately does NOT retain
    /// a cross-element `seen` set -- that would reintroduce the O(N)-names memory this streaming form
    /// exists to avoid.</summary>
    private async IAsyncEnumerable<string> EnumerateMatchingBlobsAsync(
        AzureLocation loc, DatasetSpec spec, [EnumeratorCancellation] CancellationToken ct)
    {
        foreach (var key in CoverKeys(loc.Key, spec))
        {
            var regex = new Regex(GlobToRegexPattern(key), RegexOptions.None);
            await foreach (var name in EnumeratePrefixAsync(loc, PathTemplate.StaticPrefix(key), ct).ConfigureAwait(false))
            {
                if (regex.IsMatch(name))
                {
                    yield return name;
                }
            }
        }
    }

    /// <summary>Server-side-narrows via the SDK's own prefix listing (blob container listing for <c>az</c>,
    /// directory listing for <c>abfss</c>) for one cover element's static prefix, streaming names as pages
    /// arrive. Called once per <see cref="CoverKeys"/> element by <see cref="EnumerateMatchingBlobsAsync"/>.</summary>
    private IAsyncEnumerable<string> EnumeratePrefixAsync(AzureLocation loc, string prefix, CancellationToken ct) =>
        loc.Scheme == "abfss"
            ? StreamNamesAsync(
                AzureAuth.CreateDataLakeFileSystemClient(config, loc.Container)
                    .GetPathsAsync(path: DirectoryOf(prefix), recursive: true, cancellationToken: ct),
                static (PathItem item) => item.IsDirectory != true ? item.Name : null,
                loc.Container)
            : StreamNamesAsync(
                AzureAuth.CreateBlobContainerClient(config, loc.Container).GetBlobsAsync(
                    traits: BlobTraits.None, states: BlobStates.None, prefix: prefix, cancellationToken: ct),
                static (BlobItem item) => item.Name,
                loc.Container);

    /// <summary>Drains one Azure SDK <see cref="IAsyncEnumerable{T}"/> page listing, applying
    /// <paramref name="selector"/> per item (returning <c>null</c> to skip, e.g. DataLake directory
    /// entries) and wrapping any SDK fault as <see cref="PzConnectorException"/> per <c>MoveNextAsync</c>,
    /// so a fault can surface mid-stream without buffering names first. The
    /// <c>yield break</c>/<c>yield return</c> statements sit outside the try block (C# forbids a `yield`
    /// inside a try with a catch clause), so only <c>MoveNextAsync</c> itself -- never the yield -- is
    /// covered by the catch.</summary>
    private static async IAsyncEnumerable<string> StreamNamesAsync<T>(
        IAsyncEnumerable<T> pages, Func<T, string?> selector, string container)
    {
        await using var enumerator = pages.GetAsyncEnumerator();
        while (true)
        {
            bool hasNext;
            T current;
            try
            {
                hasNext = await enumerator.MoveNextAsync().ConfigureAwait(false);
                current = hasNext ? enumerator.Current : default!;
            }
            catch (Exception ex) when (ex is RequestFailedException or IOException)
            {
                throw new PzConnectorException(
                    $"azure container '{container}': list failed: {ex.Message}", AzureTransient.IsTransient(ex), innerException: ex);
            }

            if (!hasNext)
            {
                yield break;
            }

            if (selector(current) is { } name)
            {
                yield return name;
            }
        }
    }

    /// <summary>Translates a glob pattern (`*` = any run of non-`/` characters, `**` = any run including
    /// `/`, `?` = one non-`/` character) to a full-match regex and returns the subset of
    /// <paramref name="blobNames"/> that match. Pure and offline: the SDK narrows candidates server-side via
    /// <see cref="PathTemplate.StaticPrefix"/>; this applies the wildcard portion.</summary>
    internal static IReadOnlyList<string> MatchGlob(IEnumerable<string> blobNames, string pattern)
    {
        var regex = new Regex(GlobToRegexPattern(pattern), RegexOptions.None);
        return blobNames.Where(n => regex.IsMatch(n)).ToArray();
    }

    private static string GlobToRegexPattern(string pattern)
    {
        var sb = new StringBuilder("^");
        for (var i = 0; i < pattern.Length; i++)
        {
            var c = pattern[i];
            if (c == '*')
            {
                if (i + 1 < pattern.Length && pattern[i + 1] == '*')
                {
                    sb.Append(".*");
                    i++;
                }
                else
                {
                    sb.Append("[^/]*");
                }
            }
            else if (c == '?')
            {
                sb.Append("[^/]");
            }
            else
            {
                sb.Append(Regex.Escape(c.ToString()));
            }
        }

        sb.Append('$');
        return sb.ToString();
    }

    /// <summary>The directory portion of a listing prefix, for the DataLake (<c>abfss</c>) directory-scoped
    /// <c>GetPathsAsync</c> listing -- unlike blob container listing, DataLake lists by directory, not by an
    /// arbitrary name prefix, so a prefix mid-segment (e.g. <c>in/a</c>) is trimmed back to its containing
    /// directory (<c>in</c>); <see cref="MatchGlob"/> still applies the full original pattern afterward, so
    /// listing a broader directory than strictly necessary never affects correctness.</summary>
    private static string DirectoryOf(string prefix)
    {
        var slash = prefix.LastIndexOf('/');
        return slash < 0 ? "" : prefix[..slash];
    }

    private static PzConnectorException NoMatch(DatasetSpec spec, AzureLocation loc) =>
        new($"dataset '{spec.Dataset}': no blobs matched '{AzureUrl.Render(loc)}'", isTransient: false);

    private static string? GetFormat(DatasetSpec spec) =>
        spec.Options.TryGetValue("format", out var f) ? f?.ToString() : null;

    /// <summary>Peeks a csv blob's actual header row for <see cref="GetSchemaAsync"/> (mirrors
    /// <c>Pz.Connector.LocalFiles.CsvSource.ReadHeaderAsync</c>, replicated per this connector's
    /// no-cross-connector-reference rule). Leaves no data rows consumed beyond the header.</summary>
    private static async Task<HashSet<string>> ReadCsvHeaderAsync(Stream blob, CancellationToken ct)
    {
        using var textReader = new StreamReader(blob, leaveOpen: true);
        using var csv = await CsvDataReader.CreateAsync(
            textReader, new CsvDataReaderOptions { HasHeaders = true }, ct).ConfigureAwait(false);

        var names = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < csv.FieldCount; i++)
        {
            names.Add(csv.GetName(i));
        }

        return names;
    }

    /// <summary>The declared `columns:` contract, injected by <c>SourceLoadExecutor</c> into
    /// <see cref="DatasetSpec.Options"/>["columns"]. Shared by csv and json (both require a contract; v0
    /// does no type inference). Missing contract is a permanent failure.</summary>
    private static IReadOnlyDictionary<string, string> GetColumnsContract(DatasetSpec spec) =>
        ExtractColumns(spec) ?? throw new PzConnectorException(
            $"dataset '{spec.Dataset}': azure csv/json requires a declared columns: contract", isTransient: false);

    private static IReadOnlyDictionary<string, string>? ExtractColumns(DatasetSpec spec) =>
        spec.Options.TryGetValue("columns", out var value) && value is IReadOnlyDictionary<string, string> { Count: > 0 } columns
            ? columns
            : null;
}
