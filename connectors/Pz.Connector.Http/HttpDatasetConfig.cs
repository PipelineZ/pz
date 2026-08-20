using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Bindings;
using Pz.Connectors.Toolkit.Json;
using Pz.Connectors.Toolkit.Paging;

namespace Pz.Connector.Http;

internal sealed record HttpDatasetConfig(string Path, IReadOnlyDictionary<string, string> Query,
    Func<IPageStrategy>? PageStrategyFactory, string ItemsPointer,
    IReadOnlyDictionary<string, string>? Columns, string? Cursor, string? CursorType,
    string CursorPointer, string? CursorOrder, int? MaxPages, string? DeltaLinkPointer)
{
    private static readonly HashSet<string> ContractTypes =
        ["int", "bigint", "double", "decimal", "varchar", "boolean", "date", "timestamp"];

    public bool IsContractMode => Columns is { Count: > 0 };

    /// <summary>True when `delta_pointer` is configured: the dataset is a change-feed source
    /// (Graph-style `@odata.deltaLink`) — <see cref="HttpPartition"/> replays a stored sync token
    /// verbatim as the first request and captures the terminal page's delta link.</summary>
    public bool IsSyncMode => DeltaLinkPointer is not null;

    public static HttpDatasetConfig Parse(DatasetSpec spec)
    {
        var errors = new List<string>();
        var options = spec.Options;

        var path = Get(options, "path");
        if (string.IsNullOrEmpty(path) || path[0] != '/')
        {
            errors.Add("option 'path' is required and must start with '/'");
        }

        var query = new Dictionary<string, string>();
        if (options.TryGetValue("query", out var q) && q is IReadOnlyDictionary<string, object?> qmap)
        {
            var knownBindings = BindingExpander.FromSpec(spec).Keys.ToArray();
            foreach (var (name, value) in qmap)
            {
                var template = value?.ToString() ?? "";
                query[name] = template;
                ValidateBindingTemplate(template, name, knownBindings, errors);
            }
        }

        var factory = ParsePagination(options, errors);
        var items = ValidatePointer(Get(options, "items") ?? "", "items", errors);
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

        if (errors.Count > 0)
        {
            throw new PzConnectorException(
                $"http dataset '{spec.Source}.{spec.Dataset}': {string.Join("; ", errors)} " +
                "(fix the dataset options and re-run)", isTransient: false);
        }

        return new HttpDatasetConfig(path!, query, factory, items, columns, cursor, cursorType,
            cursorPointer ?? (cursor is null ? "" : "/" + cursor), cursorOrder, maxPages, deltaPointer);
    }

    private static string? Get(IReadOnlyDictionary<string, object?> options, string key)
        => options.TryGetValue(key, out var value) ? value?.ToString() : null;

    /// <summary>Offline validation (no live request needed) of a `query:` value template
    /// against the engine-binding vocabulary — first line of defense; <see cref="HttpPartition"/>'s
    /// TryExpand call is the runtime-side defense in depth for the same two failure modes.</summary>
    private static void ValidateBindingTemplate(string template, string option,
        IReadOnlyCollection<string> knownBindings, List<string> errors)
    {
        IReadOnlyList<string> referenced;
        try
        {
            referenced = BindingExpander.ReferencedBindings(template);
        }
        catch (FormatException)
        {
            errors.Add($"query option '{option}': malformed binding template '{template}' " +
                "(expected '{{ name }}' with a lowercase name, e.g. '{{ watermark }}')");
            return;
        }

        foreach (var name in referenced)
        {
            if (!knownBindings.Contains(name))
            {
                errors.Add($"query option '{option}': unknown binding '{name}' — accepted: " +
                    string.Join(", ", knownBindings.Order()));
            }
        }
    }

    private static string ValidatePointer(string pointer, string option, List<string> errors)
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
        IReadOnlyDictionary<string, object?> options, List<string> errors)
    {
        if (!options.TryGetValue("pagination", out var p) || p is null)
        {
            return null;
        }

        if (p is not IReadOnlyDictionary<string, object?> block)
        {
            errors.Add("'pagination' must be a map with a 'strategy' key");
            return null;
        }

        string? Get(string key) => block.TryGetValue(key, out var v) ? v?.ToString() : null;
        switch (Get("strategy"))
        {
            case "link_header":
                return () => new LinkHeaderStrategy();
            case "page":
                var param = Get("param") ?? "page";

                var start = 1;
                if (Get("start") is { } startRaw)
                {
                    // Page numbers are zero-or-positive offsets (some APIs page from 0); negative or
                    // non-numeric values are rejected rather than silently coerced to the default.
                    if (!int.TryParse(startRaw, out start) || start < 0)
                    {
                        errors.Add($"pagination 'start' must be a non-negative integer, got '{startRaw}'");
                        start = 1;
                    }
                }

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

                return () => new PageParamsStrategy(param, start, Get("size_param"), size);
            case "cursor" when Get("pointer") is { } pointer && Get("param") is { } cursorParam:
                ValidatePointer(pointer, "pagination.pointer", errors);
                return () => new CursorTokenStrategy(pointer, cursorParam);
            case "cursor":
                errors.Add("pagination strategy 'cursor' requires 'pointer' and 'param'");
                return null;
            case var unknown:
                errors.Add($"unknown pagination strategy '{unknown}' " +
                    "(accepted: page, link_header, cursor)");
                return null;
        }
    }
}
