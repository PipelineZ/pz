using System.Globalization;
using System.Net.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Bindings;
using Pz.Connectors.Toolkit.Formats;
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

    /// <summary>The `watermark_format` option; null when the dataset declares none, which keeps today's
    /// rendering (<see cref="HttpFirstRequest.FormatBinding"/>) untouched.</summary>
    public WatermarkFormat? WatermarkFormat { get; init; }

    /// <summary>The `watermark_time_zone` option: the zone a `date` or custom `watermark_format` renders
    /// its wall time in, and floors or rounds up in; null for UTC.</summary>
    public TimeZoneInfo? WatermarkTimeZone { get; init; }

    internal const string TimeZoneOption = "watermark_time_zone";

    internal const string EpochUnitOption = "epoch_unit";

    /// <summary>How contract columns and the raw cursor are read from JSON: the `column_pointers` the
    /// columns' long-form `pointer` entries became, and the `epoch_unit` of epoch-number timestamps.</summary>
    public ContractReadOptions ReadOptions { get; init; } = new() { EpochUnitOption = EpochUnitOption };

    /// <summary>True when `delta_pointer` is configured: the dataset is a change-feed source
    /// (Graph-style `@odata.deltaLink`) — <see cref="HttpPartition"/> replays a stored sync token
    /// verbatim as the first request and captures the terminal page's delta link.</summary>
    public bool IsSyncMode => DeltaLinkPointer is not null;

    /// <summary>The `for_each:` option; non-null makes this config the item request, run once per
    /// record of the list request it describes.</summary>
    public HttpForEachConfig? ForEach { get; init; }

    public bool IsForEach => ForEach is not null;

    /// <summary>The request's `on_status` rules, tried in order before the built-in classification of a
    /// non-2xx response. Null when the request declares none.</summary>
    public IReadOnlyList<HttpStatusRule>? OnStatus { get; init; }

    /// <summary>True when some `on_status` rule of this request skips.</summary>
    public bool HasSkipRule => OnStatus?.Any(r => r.Action == StatusAction.Skip) == true;

    /// <summary>`pagination.concurrency`: how many page requests may be in flight (1 is strictly
    /// sequential); above 1 only for the plain `page` strategy.</summary>
    public int PageConcurrency { get; init; } = 1;

    /// <summary>`strategy: page` in the query string: what page read-ahead stamps on each request it
    /// computes. Null for every other strategy and for body paging.</summary>
    internal PageNumbering? PageNumbering { get; init; }

    /// <summary>`stop_at_watermark`: a newest-first feed (`cursor_order: desc`) stops at the first page holding a
    /// record at or below the watermark, and those records are dropped (<see cref="HttpPartition"/>).</summary>
    public bool StopAtWatermark { get; init; }

    /// <summary>A `pagination.location` that is none of query, body, header: ignored, as GET reads always did,
    /// with a notice at plan time. Null when the location is known or absent.</summary>
    internal string? IgnoredLocation { get; init; }

    /// <summary>`pagination.location: header`: the cursor token goes in a request header. Null otherwise.</summary>
    internal HeaderTokenPaging? HeaderPaging { get; init; }

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
        WatermarkFormat? watermarkFormat = null;
        if (Get(options, "watermark_format") is { } formatText)
        {
            watermarkFormat = WatermarkFormat.Parse(formatText, out var formatError);
            if (formatError is not null)
            {
                errors.Add(formatError);
            }
        }

        TimeZoneInfo? watermarkZone = null;
        if (Get(options, TimeZoneOption) is { } zoneId)
        {
            watermarkZone = RunDateConfig.FindZone(zoneId, TimeZoneOption, errors);
            // An unparseable format is reported on its own; only an absent one is "missing".
            if (watermarkFormat is null && Get(options, "watermark_format") is null)
            {
                errors.Add($"'{TimeZoneOption}' needs a 'watermark_format' of 'date' or a custom format " +
                    "(the default iso8601 is always UTC)");
            }
            else if (watermarkFormat is { IsAbsolute: true })
            {
                errors.Add($"'{TimeZoneOption}' cannot apply to 'watermark_format: {watermarkFormat.Name}', which " +
                    $"names an instant on its own; remove '{TimeZoneOption}'");
            }
            else if (watermarkFormat is { HasUtcMarker: true })
            {
                errors.Add($"'watermark_format' '{watermarkFormat.Name}' writes a UTC marker ('Z' or 'UTC'), so " +
                    $"'{TimeZoneOption}' would make it lie; drop the marker or '{TimeZoneOption}'");
            }
        }

        var epochUnit = Get(options, EpochUnitOption) switch
        {
            null => (EpochUnit?)null,
            "s" => EpochUnit.Seconds,
            "ms" => EpochUnit.Milliseconds,
            var other => Refuse($"'{EpochUnitOption}' must be 's' or 'ms', got '{other}'"),
        };

        EpochUnit? Refuse(string error)
        {
            errors.Add(error);
            return null;
        }

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

        var onStatus = HttpStatusRule.Parse(options.GetValueOrDefault("on_status"),
            listRequest ? "for_each." : "", errors);

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

        string? ignoredLocation = null;
        if (options.TryGetValue("pagination", out var lp) && lp is IReadOnlyDictionary<string, object?> lpBlock
            && YamlScalarText.Of(lpBlock.GetValueOrDefault("location")) is { } loc
            && loc is not ("query" or "body" or "header"))
        {
            ignoredLocation = loc;
        }

        HeaderTokenPaging? headerPaging = null;
        if (options.TryGetValue("pagination", out var hp) && hp is IReadOnlyDictionary<string, object?> hpBlock
            && YamlScalarText.Of(hpBlock.GetValueOrDefault("location")) == "header"
            && YamlScalarText.Of(hpBlock.GetValueOrDefault("strategy")) == "cursor"
            && YamlScalarText.Of(hpBlock.GetValueOrDefault("param")) is { } tokenHeader)
        {
            headerPaging = new HeaderTokenPaging(tokenHeader, new PageTokenSource(
                YamlScalarText.Of(hpBlock.GetValueOrDefault("pointer")),
                YamlScalarText.Of(hpBlock.GetValueOrDefault("header"))));
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
                // Column names are matched exactly; a near miss in case is named, since it reads as declared.
                var nearMiss = columns.Keys.FirstOrDefault(k => string.Equals(k, cursor, StringComparison.OrdinalIgnoreCase));
                errors.Add($"'cursor' names '{cursor}', which is not declared in 'columns'" +
                    (nearMiss is null ? "" : $" (column '{nearMiss}' differs only in case; names are case-sensitive)"));
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

        var stopAtWatermark = false;
        if (options.TryGetValue("stop_at_watermark", out var rawStop) && rawStop is not null)
        {
            if (rawStop is bool stop)
            {
                stopAtWatermark = stop;
            }
            else
            {
                errors.Add($"'stop_at_watermark' must be a boolean, got '{YamlScalarText.Of(rawStop)}'");
            }
        }

        if (stopAtWatermark)
        {
            if (cursorOrder != "desc")
            {
                errors.Add("'stop_at_watermark' requires 'cursor_order: desc' (it stops a newest-first crawl at the " +
                    "first record the watermark already covers)");
            }

            // The cursor must be known statically: the order check runs on every crawl, including a SQL-declared
            // incremental's first run, which has no watermark cursor yet.
            if (cursor is null)
            {
                errors.Add("'stop_at_watermark' requires 'cursor' (the incremental cursor: a JSON field with " +
                    "'cursor_type' in raw mode, a 'columns' name in contract mode)");
            }

            // Each of these ends or splits a crawl some other way, so "stopped on data" could not be proven.
            foreach (var (present, other) in new[]
            {
                (options.GetValueOrDefault("for_each") is not null, "for_each"),
                (options.GetValueOrDefault("file") is not null, "file"),
                (deltaPointer is not null, "delta_pointer"),
                (options.GetValueOrDefault("max_pages") is not null, "max_pages"),
                (pageConcurrency > 1, "pagination.concurrency above 1"),
            })
            {
                if (present)
                {
                    errors.Add($"'stop_at_watermark' cannot be combined with {other}");
                }
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
        var pointers = ParseColumnPointers(options, columns, file, itemFields, errors);
        ValidateWatermarkFormat(options, watermarkFormat, watermarkZone, columns, cursor ?? spec.WatermarkCursor,
            cursorType, errors);
        if (options.ContainsKey(EpochUnitOption))
        {
            ValidateEpochUnit(columns, cursorType, file, errors);
        }

        if (options.TryGetValue("for_each", out var fe) && fe is not null)
        {
            ValidateForEach(options, spec, rawExport is not null, deltaPointer is not null, errors);
        }

        if (!listRequest)
        {
            ValidateSkipRules(options, spec, rawExport is not null, errors);
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
            WatermarkFormat = watermarkFormat,
            WatermarkTimeZone = watermarkZone,
            ReadOptions = new ContractReadOptions
            {
                EpochUnitOption = EpochUnitOption, EpochUnit = epochUnit, Pointers = pointers,
            },
            File = file,
            Export = export,
            ForEach = forEach,
            PageConcurrency = pageConcurrency,
            PageNumbering = bodyPaging is null ? pageNumbering : null,
            OnStatus = onStatus is { Count: > 0 } ? onStatus : null,
            HeaderPaging = headerPaging,
            IgnoredLocation = ignoredLocation,
            StopAtWatermark = stopAtWatermark,
        };
    }

    /// <summary>Whether <paramref name="format"/> can render a cursor of <paramref name="cursorType"/>;
    /// null when it can, else the reason. Numeric and text cursors are sent as stored; a date cursor has no
    /// time of day for a time-rendering format to invent.</summary>
    internal static string? CheckFormatForCursor(WatermarkFormat format, TimeZoneInfo? zone, string? cursorType)
        => cursorType switch
        {
            null or "timestamp" => null,
            "date" when !format.IsDateOnly => $"'watermark_format: {format.Name}' renders a time, but the cursor is " +
                "a date; use 'date' or a custom format of years, months and days only (e.g. 'yyyyMMdd')",
            "date" when zone is not null => $"'{TimeZoneOption}' cannot apply to a date cursor, which has no time " +
                $"of day to convert; remove '{TimeZoneOption}'",
            "date" => null,
            _ => $"'watermark_format' applies to timestamp and date cursors, but the cursor is '{cursorType}', " +
                "which is sent as stored; remove 'watermark_format'",
        };

    private static void ValidateWatermarkFormat(IReadOnlyDictionary<string, object?> options, WatermarkFormat? format,
        TimeZoneInfo? zone, IReadOnlyDictionary<string, string>? columns, string? cursor, string? cursorType, List<string> errors)
    {
        if (format is null)
        {
            return;
        }

        // The type is known offline for a raw cursor or a contract column; a SQL-declared incremental
        // learns it at run time, where HttpFirstRequest.Bindings checks again.
        var type = columns is { Count: > 0 }
            ? cursor is not null && columns.TryGetValue(cursor, out var t) ? t : null
            : cursorType;
        if (CheckFormatForCursor(format, zone, type) is { } error)
        {
            errors.Add(error);
        }

        var forEach = options.GetValueOrDefault("for_each") as IReadOnlyDictionary<string, object?>;
        var referenced = ReferencesWatermark(options) || (forEach is not null && ReferencesWatermark(forEach))
            || Strings(options.GetValueOrDefault("export")).SelectMany(HttpTemplate.EngineReferences)
                .Any(name => name is "watermark" or "window_upper");
        if (!referenced)
        {
            errors.Add("'watermark_format' has no effect: no path, query, body or export path references " +
                "'{{ watermark }}' or '{{ window_upper }}'; reference one, or remove 'watermark_format'");
        }
    }

    private static void ValidateEpochUnit(IReadOnlyDictionary<string, string>? columns, string? cursorType,
        HttpFileConfig? file, List<string> errors)
    {
        if (file is { Format: not HttpFileFormat.Json })
        {
            errors.Add($"'{EpochUnitOption}' reads JSON numbers, and file.format {file.Format.ToString().ToLowerInvariant()} " +
                $"has only text values; remove '{EpochUnitOption}'");
        }
        else if (!(columns is { Count: > 0 } ? columns.Values.Contains("timestamp") : cursorType == "timestamp"))
        {
            errors.Add($"'{EpochUnitOption}' has no effect: no 'timestamp' column (or raw-mode cursor_type " +
                $"timestamp) to read epoch numbers into; remove '{EpochUnitOption}'");
        }
    }

    /// <summary>The `column_pointers` option (pz writes it from the columns' long-form `pointer`), checked
    /// against the contract, the file format and the for_each fields; null when absent.</summary>
    private static Dictionary<string, string>? ParseColumnPointers(IReadOnlyDictionary<string, object?> options,
        IReadOnlyDictionary<string, string>? columns, HttpFileConfig? file, IReadOnlyCollection<string>? itemFields,
        List<string> errors)
    {
        if (!options.TryGetValue("column_pointers", out var raw) || raw is null)
        {
            return null;
        }

        if (raw is not IReadOnlyDictionary<string, object?> map)
        {
            errors.Add("'column_pointers' must be a map of column name to JSON pointer");
            return null;
        }

        var pointers = new Dictionary<string, string>();
        foreach (var (name, value) in map)
        {
            if (columns is null || !columns.ContainsKey(name))
            {
                errors.Add($"column '{name}' has a 'pointer' but is not declared in 'columns'");
                continue;
            }

            if (value is not string { Length: > 0 } pointer || pointer[0] != '/' || HasBadEscape(pointer))
            {
                errors.Add($"column '{name}': 'pointer' must be a JSON pointer starting with '/' (with '~0'/'~1' escapes)");
                continue;
            }

            if (itemFields?.Contains(name) == true)
            {
                errors.Add($"column '{name}' is filled from the for_each field '{name}', so it cannot also have a " +
                    "'pointer'; rename the column or the field");
                continue;
            }

            pointers[name] = pointer;
        }

        if (pointers.Count > 0 && file is { Format: not HttpFileFormat.Json })
        {
            errors.Add($"a column 'pointer' reads a JSON record, and file.format " +
                $"{file.Format.ToString().ToLowerInvariant()} has none; remove 'pointer' from " +
                string.Join(", ", pointers.Keys.Select(k => $"'{k}'")));
        }

        return pointers.Count > 0 ? pointers : null;
    }

    /// <summary>RFC 6901: every '~' is followed by '0' or '1'.</summary>
    private static bool HasBadEscape(string pointer)
    {
        for (var i = 0; i < pointer.Length; i++)
        {
            if (pointer[i] == '~' && (i + 1 >= pointer.Length || pointer[i + 1] is not ('0' or '1')))
            {
                return true;
            }
        }

        return false;
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

        var block = options["for_each"] as IReadOnlyDictionary<string, object?>;
        if (!IsIncremental(options, spec))
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

    /// <summary>Whether the dataset reads incrementally. A SQL-declared incremental has no WatermarkCursor
    /// on its first run or a full refresh, so a raw cursor or a watermark reference in either request
    /// (the dataset's own or its for_each list) marks the dataset incremental too.</summary>
    private static bool IsIncremental(IReadOnlyDictionary<string, object?> options, DatasetSpec spec)
    {
        var block = options.GetValueOrDefault("for_each") as IReadOnlyDictionary<string, object?>;
        return spec.WatermarkCursor is not null
            || options.GetValueOrDefault("cursor") is not null
            || ReferencesWatermark(options)
            || (block is not null && ReferencesWatermark(block));
    }

    /// <summary>Refuses `skip` rules where a skipped response could cost data for good: on a dataset that is
    /// or could be incremental (a skipped window lands nothing and the engine advances the watermark past
    /// it; a skipped item lets its siblings move the watermark past it), and with `export` (a job has no
    /// single first request to skip). A SQL-declared incremental reaches the connector with no
    /// WatermarkCursor on its first run and under --full-refresh, so a `columns:` contract, whose columns
    /// can be that cursor, counts as possibly incremental. Skip rules are found in the raw maps, so a rule
    /// with other errors still has this refusal reported in the same pass; the for_each list's rules are
    /// covered too.</summary>
    private static void ValidateSkipRules(IReadOnlyDictionary<string, object?> options, DatasetSpec spec,
        bool hasExport, List<string> errors)
    {
        var own = HttpStatusRule.SkipLabels(options.GetValueOrDefault("on_status"), "");
        var list = HttpStatusRule.SkipLabels(
            (options.GetValueOrDefault("for_each") as IReadOnlyDictionary<string, object?>)?.GetValueOrDefault("on_status"),
            "for_each.");
        var skips = own.Concat(list).ToList();
        if (skips.Count == 0)
        {
            return;
        }

        var hasContract = options.GetValueOrDefault("columns") is IReadOnlyDictionary<string, string> { Count: > 0 };
        if (IsIncremental(options, spec) || hasContract)
        {
            errors.Add($"{string.Join(", ", skips)}: action 'skip' is not allowed on a dataset that is or could be " +
                "incremental — a skipped response would let the watermark move past rows never read (a raw " +
                "'cursor', a {{ watermark }}/{{ window_upper }} reference, an incremental: block, or a 'columns:' " +
                "contract, since a 'columns:' contract can carry the cursor of a SQL-declared incremental, which " +
                "the connector cannot see on a first run or under --full-refresh); use action: permanent (fail) " +
                "or transient (retry)");
        }

        if (hasExport && own.Count > 0)
        {
            errors.Add($"{string.Join(", ", own)}: action 'skip' cannot be combined with 'export' (an export job has " +
                "no single first request to skip); use action: permanent or transient");
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
        // An unknown location is not an error here: GET reads ignored the key before header paging existed, and
        // configs that ran then must keep running. The dataset schema reports it at `pz validate`, and the read
        // gives a notice (IgnoredLocation).
        var location = Get("location");

        if (location == "header")
        {
            if (strategy != "cursor")
            {
                errors.Add("pagination 'location: header' is only valid with strategy 'cursor' (got strategy " +
                    $"'{strategy}')");
            }
            else if (Get("param") is { } headerParam)
            {
                if (!HeaderTokenPaging.IsHeaderName(headerParam))
                {
                    errors.Add($"pagination 'param' '{headerParam}' is not a valid HTTP header name");
                }
                else if (HeaderTokenPaging.ReservedHeaders.Contains(headerParam))
                {
                    errors.Add($"pagination 'param' '{headerParam}' cannot carry the page token (pz or the HTTP " +
                        "stack sets that header); name the header the API reads the token from");
                }
                else if (!HeaderTokenPaging.IsRequestHeader(headerParam))
                {
                    errors.Add($"pagination 'param' '{headerParam}' cannot carry the page token (HTTP treats it as a " +
                        "content header, which a request without a body never sends); name the header the API reads " +
                        "the token from");
                }
            }
        }

        if (Get("header") is { } responseHeader)
        {
            if (strategy != "cursor")
            {
                errors.Add($"pagination 'header' is only valid with strategy 'cursor' (got strategy '{strategy}')");
            }
            else if (!HeaderTokenPaging.IsHeaderName(responseHeader))
            {
                errors.Add($"pagination 'header' '{responseHeader}' is not a valid HTTP header name");
            }
        }

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
            case "cursor":
            {
                var pointer = Get("pointer");
                var header = Get("header");
                var cursorParam = Get("param");
                if ((pointer is null) == (header is null))
                {
                    errors.Add("pagination strategy 'cursor' needs exactly one of 'pointer' or 'header' (where " +
                        "the next-page token is read: a JSON pointer into the body, or a response header name)");
                }

                if (cursorParam is null)
                {
                    errors.Add("pagination strategy 'cursor' requires 'param' (the query parameter, body pointer " +
                        "or request header the token is sent in)");
                }

                if (pointer is not null)
                {
                    ValidatePointer(pointer, "pagination.pointer", errors);
                }

                // A header-sent token is HeaderTokenPaging's, not a URL strategy's.
                if (cursorParam is null || (pointer is null) == (header is null) || location == "header")
                {
                    return null;
                }

                return pointer is not null
                    ? () => new CursorTokenStrategy(pointer, cursorParam)
                    : () => new ResponseHeaderCursorStrategy(header!, cursorParam);
            }
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
