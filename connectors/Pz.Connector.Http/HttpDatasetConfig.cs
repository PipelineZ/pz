using System.Globalization;
using System.Net.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Bindings;
using Pz.Connectors.Toolkit.Json;
using Pz.Connectors.Toolkit.Paging;

namespace Pz.Connector.Http;

internal sealed record HttpDatasetConfig(string Path, IReadOnlyDictionary<string, string> Query,
    Func<IPageStrategy>? PageStrategyFactory, string ItemsPointer,
    IReadOnlyDictionary<string, string>? Columns, string? Cursor, string? CursorType,
    string CursorPointer, string? CursorOrder, int? MaxPages, string? DeltaLinkPointer,
    bool StopOnShortPage = false, int? PageSize = null,
    HttpMethod? Method = null, HttpBody? Body = null, string? ErrorPointer = null,
    BodyPaging? BodyPaging = null)
{
    private static readonly HashSet<string> ContractTypes =
        ["int", "bigint", "double", "decimal", "varchar", "boolean", "date", "timestamp"];

    /// <summary>True for a POST read; a config constructed without a method is a GET.</summary>
    public bool IsPost => Method == HttpMethod.Post;

    public bool IsContractMode => Columns is { Count: > 0 };

    /// <summary>The `file:` option; non-null switches the dataset to file mode.</summary>
    public HttpFileConfig? File { get; init; }

    /// <summary>The `export:` option (an asynchronous export job whose result is the file); only
    /// valid together with <see cref="File"/>.</summary>
    public HttpExportConfig? Export { get; init; }

    public bool IsFileMode => File is not null;

    /// <summary>What `{{ run_date }}` renders to; <see cref="RunDateConfig.Default"/> when the
    /// dataset declares no `run_date` option.</summary>
    public RunDateConfig RunDate { get; init; } = RunDateConfig.Default;

    /// <summary>True when `delta_pointer` is configured: the dataset is a change-feed source
    /// (Graph-style `@odata.deltaLink`) — <see cref="HttpPartition"/> replays a stored sync token
    /// verbatim as the first request and captures the terminal page's delta link.</summary>
    public bool IsSyncMode => DeltaLinkPointer is not null;

    /// <summary>The `for_each:` option; non-null makes this config the item request, run once per
    /// record of the list request it describes.</summary>
    public HttpForEachConfig? ForEach { get; init; }

    public bool IsForEach => ForEach is not null;

    /// <summary>`pagination.concurrency`: how many page requests may be in flight (1 is strictly
    /// sequential); above 1 only for the plain `page` strategy.</summary>
    public int PageConcurrency { get; init; } = 1;

    /// <summary>`strategy: page` in the query string: what page read-ahead stamps on each request it
    /// computes. Null for every other strategy and for body paging.</summary>
    internal PageNumbering? PageNumbering { get; init; }

    internal const int MaxConcurrency = 16;

    /// <summary>Reads a `concurrency` value (an integer from 1 to <see cref="MaxConcurrency"/>, default 1);
    /// a bad value is reported under <paramref name="label"/> and read as 1.</summary>
    internal static int ParseConcurrency(IReadOnlyDictionary<string, object?> block, string label,
        List<string> errors)
    {
        if (!block.TryGetValue("concurrency", out var raw) || raw is null)
        {
            return 1;
        }

        if (YamlScalarText.NonScalarKind(raw) is null && long.TryParse(YamlScalarText.Of(raw), NumberStyles.Integer,
                CultureInfo.InvariantCulture, out var n)
            && n is >= 1 and <= MaxConcurrency)
        {
            return (int)n;
        }

        errors.Add($"{label} must be an integer from 1 to {MaxConcurrency}, got '{YamlScalarText.Of(raw)}'");
        return 1;
    }

    public static HttpDatasetConfig Parse(DatasetSpec spec)
    {
        var errors = new List<string>();
        var config = ParseCore(spec.Options, spec, errors, listRequest: false);
        if (errors.Count > 0)
        {
            throw new PzConnectorException(
                $"http dataset '{spec.Source}.{spec.Dataset}': {string.Join("; ", errors)} " +
                "(fix the dataset options and re-run)", isTransient: false);
        }

        return config;
    }

    /// <summary>Parses one request's options, appending every problem to <paramref name="errors"/>
    /// (the result is meaningful only when none were added). With <paramref name="listRequest"/> the
    /// options are a `for_each` block: the keys only the item request or the dataset may have are
    /// refused, its other errors are prefixed `for_each: `, and item references are not allowed.
    /// Bindings come from <paramref name="spec"/> in both modes, so the list request formats the
    /// watermark by the dataset's cursor.</summary>
    internal static HttpDatasetConfig ParseCore(IReadOnlyDictionary<string, object?> options, DatasetSpec spec,
        List<string> errors, bool listRequest)
    {
        var reported = errors;
        if (listRequest)
        {
            var refused = HttpForEachConfig.ItemOnlyKeys.Where(options.ContainsKey).ToList();
            foreach (var key in refused)
            {
                reported.Add($"for_each.{key} is not allowed in the list request");
            }

            options = options.Where(o => !refused.Contains(o.Key)).ToDictionary();
            errors = [];
        }

        // The engine's bindings, then the connector's own.
        string[] knownBindings = [.. BindingExpander.FromSpec(spec).Keys, "run_date"];

        HttpForEachConfig? forEach = null;
        IReadOnlyCollection<string>? itemFields = null;
        if (!listRequest && options.TryGetValue("for_each", out var rawForEach) && rawForEach is not null)
        {
            forEach = HttpForEachConfig.Parse(rawForEach, spec, errors, out itemFields);
        }

        var path = Get(options, "path");
        if (string.IsNullOrEmpty(path) || path[0] != '/')
        {
            errors.Add("option 'path' is required and must start with '/'");
        }
        else
        {
            ValidateBindingTemplate(path, "option 'path'", knownBindings, errors, itemFields: itemFields);

            // Item values are substituted into the path; a literal '?' or '#' could leave an empty or dot
            // segment after it that the URL builder no longer sees as a path segment.
            if (forEach is not null && path.AsSpan().IndexOfAny('?', '#') >= 0)
            {
                errors.Add("path cannot contain '?' or '#' in a for_each dataset; put query parameters in query:");
            }
        }

        var runDate = RunDateConfig.Parse(options.TryGetValue("run_date", out var rd) ? rd : null, errors);

        var query = new Dictionary<string, string>();
        if (options.TryGetValue("query", out var q) && q is IReadOnlyDictionary<string, object?> qmap)
        {
            foreach (var (name, value) in qmap)
            {
                var template = YamlScalarText.Of(value) ?? "";
                query[name] = template;
                ValidateBindingTemplate(template, $"query option '{name}'", knownBindings, errors,
                    itemFields: itemFields);
            }
        }

        var method = ParseMethod(options, errors);
        var body = ParseBody(options, method, knownBindings, itemFields, errors);
        var errorPointer = Get(options, "error_pointer");
        if (errorPointer is not null)
        {
            ValidatePointer(errorPointer, "error_pointer", errors);
        }

        var items = ValidatePointer(Get(options, "items") ?? "", "items", errors);
        var factory = ParsePagination(options, items, errors, out var stopOnShortPage, out var pageSize,
            out var pageNumbering);
        BodyPaging? bodyPaging = null;
        if (method == HttpMethod.Post && options.TryGetValue("pagination", out var pg)
            && pg is IReadOnlyDictionary<string, object?> pgBlock)
        {
            // A non-post method with `location: body` is already refused by ParseBody.
            bodyPaging = BodyPaging.Parse(pgBlock,
                pgBlock.TryGetValue("strategy", out var st) ? YamlScalarText.Of(st) ?? "" : "",
                body?.Template, items, errors);
        }

        var pageConcurrency = 1;
        if (options.TryGetValue("pagination", out var pgc) && pgc is IReadOnlyDictionary<string, object?> pgcBlock)
        {
            pageConcurrency = ParseConcurrency(pgcBlock, "pagination.concurrency", errors);
            if (pageConcurrency > 1)
            {
                var strategy = pgcBlock.TryGetValue("strategy", out var stc) ? YamlScalarText.Of(stc) : null;
                if (forEach is not null || listRequest)
                {
                    errors.Add("pagination.concurrency is not supported with for_each; use for_each.concurrency");
                }
                else if (strategy != "page")
                {
                    errors.Add("pagination.concurrency above 1 needs strategy: page");
                }

                if (Get(options, "delta_pointer") is not null)
                {
                    errors.Add("pagination.concurrency cannot be combined with delta_pointer");
                }
            }
        }

        var columns = options.TryGetValue("columns", out var c)
            ? c as IReadOnlyDictionary<string, string>
            : null;

        var cursor = Get(options, "cursor");
        var cursorType = Get(options, "cursor_type");
        var cursorPointer = Get(options, "cursor_pointer");
        if (columns is { Count: > 0 })
        {
            if (cursorType is not null || cursorPointer is not null)
            {
                errors.Add("'cursor_type'/'cursor_pointer' are raw-mode options; contract mode types " +
                    "the cursor in 'columns'");
            }

            if (cursor is not null && !columns.ContainsKey(cursor))
            {
                errors.Add($"'cursor' names '{cursor}', which is not declared in 'columns'");
            }
        }
        else
        {
            if ((cursor is null) != (cursorType is null))
            {
                errors.Add("raw-mode incremental requires 'cursor' and 'cursor_type' together");
            }

            if (cursorType is not null && !ContractTypes.Contains(cursorType))
            {
                errors.Add($"'cursor_type' must be one of: {string.Join(", ", ContractTypes.Order())}");
            }

            if (cursorPointer is not null)
            {
                ValidatePointer(cursorPointer, "cursor_pointer", errors);
                if (cursor is null)
                {
                    errors.Add("'cursor_pointer' requires 'cursor'");
                }
            }
        }

        // How the API serves records relative to the cursor. Load-bearing only under truncation
        // (HttpPartition guard) and at compile (PZ0229).
        var cursorOrder = Get(options, "cursor_order");
        if (cursorOrder is not (null or "asc" or "desc"))
        {
            errors.Add($"'cursor_order' must be 'asc' or 'desc', got '{cursorOrder}'");
        }

        if (cursorOrder is not null && cursor is null && columns is null)
        {
            errors.Add("'cursor_order' requires a cursor — declare the raw-mode 'cursor' option " +
                "or a 'columns' contract");
        }

        if (cursor is not null && cursor.StartsWith("pz_", StringComparison.Ordinal))
        {
            errors.Add("'cursor' must not use the reserved 'pz_' prefix");
        }

        if (cursor is not null && cursor == "payload" && columns is null)
        {
            errors.Add("'incremental.cursor' (or cursor option): 'payload' collides with the raw envelope " +
                "column 'payload' — choose the JSON field's real name; it is added as its own column");
        }

        var deltaPointer = Get(options, "delta_pointer");
        if (deltaPointer is not null)
        {
            ValidatePointer(deltaPointer, "delta_pointer", errors);
        }

        if (method == HttpMethod.Post && deltaPointer is not null)
        {
            errors.Add("'delta_pointer' is not supported with 'method: post' (a delta link is replayed " +
                "as a GET)");
        }

        int? maxPages = null;
        if (options.TryGetValue("max_pages", out var mp) && mp is not null)
        {
            if (long.TryParse(mp.ToString(), out var pages) && pages > 0)
            {
                maxPages = (int)Math.Min(pages, int.MaxValue);
            }
            else
            {
                errors.Add($"'max_pages' must be a positive integer, got '{mp}'");
            }
        }

        options.TryGetValue("file", out var rawFile);
        options.TryGetValue("export", out var rawExport);
        var file = HttpFileConfig.Parse(rawFile, errors);
        var export = HttpExportConfig.Parse(rawExport, errors);
        if (export is not null)
        {
            // A rejected status_path (already reported) leaves it null.
            if (export.StatusPath is { } statusPath)
            {
                ValidateBindingTemplate(statusPath, "export.status_path", knownBindings, errors);
            }

            if (export.DownloadPath is { } downloadPath)
            {
                ValidateBindingTemplate(downloadPath, "export.download_path", knownBindings, errors);
            }
        }

        ValidateFileMode(options, rawFile is not null, rawExport is not null, file, columns, errors);

        if (options.TryGetValue("for_each", out var fe) && fe is not null)
        {
            ValidateForEach(options, spec, rawExport is not null, deltaPointer is not null, errors);
        }

        if (listRequest)
        {
            reported.AddRange(errors.Select(e => "for_each: " + e));
        }

        // Body paging replaces the URL strategy (which was parsed only for validation and the
        // stop_on_short_page/size semantics); leaving it set would stamp the query string too.
        return new HttpDatasetConfig(path!, query, bodyPaging is null ? factory : null, items, columns, cursor, cursorType,
            cursorPointer ?? (cursor is null ? "" : "/" + cursor), cursorOrder, maxPages, deltaPointer,
            stopOnShortPage, pageSize, method, body, errorPointer, bodyPaging)
        {
            RunDate = runDate,
            File = file,
            Export = export,
            ForEach = forEach,
            PageConcurrency = pageConcurrency,
            PageNumbering = bodyPaging is null ? pageNumbering : null,
        };
    }

    private static void ValidateForEach(IReadOnlyDictionary<string, object?> options, DatasetSpec spec,
        bool hasExport, bool hasDelta, List<string> errors)
    {
        if (hasDelta)
        {
            errors.Add("'for_each' cannot be combined with 'delta_pointer' (a change feed has no list of items)");
        }

        if (hasExport)
        {
            errors.Add("'for_each' cannot be combined with 'export'");
        }

        // A SQL-declared incremental has no WatermarkCursor on its first run or a full refresh, so a raw
        // cursor or a watermark reference in either request marks the dataset incremental too.
        var block = options["for_each"] as IReadOnlyDictionary<string, object?>;
        var incremental = spec.WatermarkCursor is not null
            || options.GetValueOrDefault("cursor") is not null
            || ReferencesWatermark(options)
            || (block is not null && ReferencesWatermark(block));
        if (!incremental)
        {
            return;
        }

        foreach (var (present, key) in new[]
        {
            (block?.GetValueOrDefault("max_items") is not null, "max_items"),
            (block?.GetValueOrDefault("max_pages") is not null, "for_each.max_pages"),
            (options.GetValueOrDefault("max_pages") is not null, "max_pages"),
        })
        {
            if (present)
            {
                errors.Add($"for_each on an incremental dataset cannot use {key}: a cut-short item would be " +
                    "skipped for good once the watermark passes it");
            }
        }
    }

    /// <summary>Whether a request's path, query or body references {{ watermark }} or {{ window_upper }}.</summary>
    private static bool ReferencesWatermark(IReadOnlyDictionary<string, object?> request)
        => new[] { "path", "query", "body" }
            .SelectMany(key => Strings(request.GetValueOrDefault(key)))
            .SelectMany(HttpTemplate.EngineReferences)
            .Any(name => name is "watermark" or "window_upper");

    private static IEnumerable<string> Strings(object? value) => value switch
    {
        string text => [text],
        IReadOnlyDictionary<string, object?> map => map.Values.SelectMany(Strings),
        System.Collections.IEnumerable list => list.Cast<object?>().SelectMany(Strings),
        _ => [],
    };

    private static void ValidateFileMode(IReadOnlyDictionary<string, object?> options, bool hasFile,
        bool hasExport, HttpFileConfig? file, IReadOnlyDictionary<string, string>? columns, List<string> errors)
    {
        if (hasExport && !hasFile)
        {
            errors.Add("'export' requires 'file' (the export's result is read as a file)");
        }

        if (!hasFile)
        {
            return;
        }

        foreach (var option in new[] { "pagination", "delta_pointer", "max_pages" })
        {
            if (options.TryGetValue(option, out var value) && value is not null)
            {
                errors.Add($"'{option}' cannot be combined with 'file' (a file is read in one request)");
            }
        }

        if (file is null)
        {
            return;
        }

        // `items` only locates records in a JSON document; the dataset default must not trip this,
        // so it is checked only when the key is written.
        if (file is { Format: HttpFileFormat.Json, JsonLayout: HttpJsonLayout.Lines }
            && options.TryGetValue("items", out var items) && items is not null)
        {
            errors.Add("'items' cannot be combined with file.layout lines (it applies to layout document)");
        }

        if (file.FileColumn is not null)
        {
            if (columns is not { Count: > 0 })
            {
                errors.Add("file.file_column is only for contract mode (raw mode always has 'pz_file')");
            }
            else if (columns.ContainsKey(file.FileColumn))
            {
                errors.Add($"file.file_column '{file.FileColumn}' collides with a column in 'columns'");
            }
        }
    }

    private static HttpMethod ParseMethod(IReadOnlyDictionary<string, object?> options, List<string> errors)
    {
        var method = Get(options, "method");
        if (method is null or "get")
        {
            return HttpMethod.Get;
        }

        if (method == "post")
        {
            return HttpMethod.Post;
        }

        errors.Add($"'method' must be 'get' or 'post', got '{method}'");
        return HttpMethod.Get;
    }

    private static HttpBody? ParseBody(IReadOnlyDictionary<string, object?> options, HttpMethod method,
        IReadOnlyCollection<string> knownBindings, IReadOnlyCollection<string>? itemFields, List<string> errors)
    {
        options.TryGetValue("body", out var raw);
        var bodyFormat = Get(options, "body_format");
        var contentType = Get(options, "content_type");
        var locationBody = options.TryGetValue("pagination", out var p)
            && p is IReadOnlyDictionary<string, object?> block
            && block.TryGetValue("location", out var loc) && YamlScalarText.Of(loc) == "body";

        if (method != HttpMethod.Post)
        {
            foreach (var (present, option) in new[]
            {
                (raw is not null, "body"), (bodyFormat is not null, "body_format"),
                (contentType is not null, "content_type"), (locationBody, "pagination.location: body"),
            })
            {
                if (present)
                {
                    errors.Add($"'{option}' requires 'method: post'");
                }
            }

            return null;
        }

        if (raw is null)
        {
            if (bodyFormat is not null || contentType is not null)
            {
                errors.Add("'body_format'/'content_type' require a 'body'");
            }

            return null;
        }

        return HttpBody.Parse(raw, bodyFormat, contentType, knownBindings, errors, itemFields);
    }

    private static string? Get(IReadOnlyDictionary<string, object?> options, string key)
        => options.TryGetValue(key, out var value) ? YamlScalarText.Of(value) : null;

    /// <summary>Offline validation (no live request needed) of a `path`, `query:` or `body` template
    /// against the binding vocabulary (listed in the order given) — first line of defense;
    /// <see cref="HttpFirstRequest"/>'s TryExpand calls are the runtime-side defense in depth for the
    /// same two failure modes.</summary>
    internal static void ValidateBindingTemplate(string template, string label,
        IReadOnlyCollection<string> knownBindings, List<string> errors, bool echoTemplate = true,
        IReadOnlyCollection<string>? itemFields = null)
    {
        // An item reference is not a malformed engine binding: it is checked against the declared
        // item fields (none here unless the caller passes them, which refuses it).
        if (HttpTemplate.HasItemReferences(template))
        {
            if (!HttpTemplate.TryValidate(template, knownBindings, itemFields, out var error))
            {
                errors.Add($"{label}: {error}");
            }

            return;
        }

        IReadOnlyList<string> referenced;
        try
        {
            referenced = BindingExpander.ReferencedBindings(template);
        }
        catch (FormatException)
        {
            errors.Add($"{label}: malformed binding template{(echoTemplate ? $" '{template}'" : "")} " +
                "(expected '{{ name }}' with a lowercase name, e.g. '{{ watermark }}')");
            return;
        }

        foreach (var name in referenced)
        {
            if (!knownBindings.Contains(name))
            {
                errors.Add($"{label}: unknown binding '{name}' — accepted: " +
                    string.Join(", ", knownBindings));
            }
        }
    }

    internal static string ValidatePointer(string pointer, string option, List<string> errors)
    {
        try
        {
            JsonPointer.TryResolve(System.Text.Json.Nodes.JsonNode.Parse("{}"), pointer, out _);
        }
        catch (ArgumentException)
        {
            errors.Add($"'{option}' is not a valid JSON pointer: '{pointer}'");
        }

        return pointer;
    }

    private static Func<IPageStrategy>? ParsePagination(
        IReadOnlyDictionary<string, object?> options, string itemsPointer, List<string> errors,
        out bool stopOnShortPage, out int? pageSize, out PageNumbering? pageNumbering)
    {
        stopOnShortPage = false;
        pageSize = null;
        pageNumbering = null;

        if (!options.TryGetValue("pagination", out var p) || p is null)
        {
            return null;
        }

        if (p is not IReadOnlyDictionary<string, object?> block)
        {
            errors.Add("'pagination' must be a map with a 'strategy' key");
            return null;
        }

        string? Get(string key) => block.TryGetValue(key, out var v) ? YamlScalarText.Of(v) : null;
        var strategy = Get("strategy");

        if (block.TryGetValue("stop_on_short_page", out var stopRaw) && stopRaw is not null)
        {
            if (strategy is not ("page" or "offset"))
            {
                errors.Add("pagination 'stop_on_short_page' is only valid with strategy 'page' or " +
                    "'offset' (a numbered or offset page can fall short of the requested size; got " +
                    $"strategy '{strategy}')");
            }
            else if (stopRaw is not bool)
            {
                errors.Add($"pagination 'stop_on_short_page' must be a boolean, got '{stopRaw}'");
            }
            else
            {
                stopOnShortPage = (bool)stopRaw;
            }
        }

        var stopsOnShort = stopOnShortPage;
        switch (strategy)
        {
            case "link_header":
                return () => new LinkHeaderStrategy();
            case "page":
            {
                var param = Get("param") ?? "page";
                // Page numbers are zero-or-positive (some APIs page from 0); negative or non-numeric
                // values are rejected rather than silently coerced to the default.
                var start = ParseStart(defaultStart: 1);
                var size = pageSize = ParseSize();
                var sizeParam = Get("size_param");
                pageNumbering = new PageNumbering(param, (int)start, sizeParam, size);
                return () => new PageParamsStrategy(param, (int)start, sizeParam, size);
            }
            case "offset":
            {
                var param = Get("param") ?? "offset";
                var start = ParseStart(defaultStart: 0);
                var size = pageSize = ParseSize();
                var sizeParam = Get("size_param") ?? "limit";
                return () => new OffsetStrategy(param, start, sizeParam, size, itemsPointer);
            }
            case "cursor" when Get("pointer") is { } pointer && Get("param") is { } cursorParam:
                ValidatePointer(pointer, "pagination.pointer", errors);
                return () => new CursorTokenStrategy(pointer, cursorParam);
            case "cursor":
                errors.Add("pagination strategy 'cursor' requires 'pointer' and 'param'");
                return null;
            case "next_url" when Get("pointer") is { } linkPointer:
                ValidatePointer(linkPointer, "pagination.pointer", errors);
                return () => new NextUrlStrategy(linkPointer);
            case "next_url":
                errors.Add("pagination strategy 'next_url' requires 'pointer' (the JSON pointer to the " +
                    "next-page link in the response body, e.g. '/@odata.nextLink' or '/links/next')");
                return null;
            case var unknown:
                errors.Add($"unknown pagination strategy '{unknown}' " +
                    "(accepted: page, offset, link_header, cursor, next_url)");
                return null;
        }

        long ParseStart(long defaultStart)
        {
            if (Get("start") is not { } startRaw)
            {
                return defaultStart;
            }

            if (!int.TryParse(startRaw, out var start) || start < 0)
            {
                errors.Add($"pagination 'start' must be a non-negative integer, got '{startRaw}'");
                return defaultStart;
            }

            return start;
        }

        int? ParseSize()
        {
            int? size = null;
            if (Get("size") is { } sizeRaw)
            {
                if (!int.TryParse(sizeRaw, out var sz) || sz <= 0)
                {
                    errors.Add($"pagination 'size' must be a positive integer, got '{sizeRaw}'");
                }
                else
                {
                    size = sz;
                }
            }

            if (stopsOnShort && size is null)
            {
                errors.Add("pagination 'stop_on_short_page' requires 'size' -- 'short' is relative " +
                    "to the requested page size, and there is none declared to compare against");
            }

            return size;
        }
    }
}

/// <summary>Query-string page numbering (<c>strategy: page</c>): the page param, its first value, and the
/// size param and size stamped beside it.</summary>
internal sealed record PageNumbering(string Param, int Start, string? SizeParam, int? Size);
