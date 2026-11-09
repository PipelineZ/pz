using System.Globalization;
using System.Text.Json.Nodes;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Bindings;
using Pz.Connectors.Toolkit.Paging;

namespace Pz.Connector.Http;

/// <summary>Builds a dataset's first request from its options: the engine bindings, then the path,
/// the query string and (for a POST) the body, all expanded from the same bindings with the same
/// formatting, so a watermark appears identically wherever it is referenced.</summary>
internal static partial class HttpFirstRequest
{
    public static Dictionary<string, BindingValue> Bindings(DatasetSpec spec, HttpDatasetConfig config,
        string runDate, string label)
    {
        if (spec.WatermarkCursor is { } wmCursor)
        {
            if (config.Cursor is null && !config.IsContractMode)
            {
                throw new PzConnectorException(
                    $"{label}: incremental dataset needs the raw-mode 'cursor' + 'cursor_type' options " +
                    "(or a 'columns' contract declaring the cursor)", isTransient: false, code: PzConnectorErrorCodes.Config);
            }

            if (config.Cursor is { } optCursor && optCursor != wmCursor)
            {
                throw new PzConnectorException(
                    $"{label}: incremental.cursor is '{wmCursor}' but options.cursor is '{optCursor}'; " +
                    "align the two", isTransient: false, code: PzConnectorErrorCodes.Config);
            }
        }

        string? cursorType;
        if (config.IsContractMode && spec.WatermarkCursor is not null)
        {
            if (!config.Columns!.TryGetValue(spec.WatermarkCursor, out cursorType))
            {
                throw new PzConnectorException(
                    $"{label}: incremental cursor '{spec.WatermarkCursor}' is not declared in 'columns:' — " +
                    "add it to the contract so its values land and its type is known", isTransient: false, code: PzConnectorErrorCodes.Config);
            }
        }
        else
        {
            cursorType = config.CursorType;
        }
        var bindings = new Dictionary<string, BindingValue>
        {
            ["watermark"] = Bound(spec.WatermarkValue, cursorType, config, upper: false, label),
            // Upper bound of the engine-resolved (lower, upper] window; null when the dataset is
            // not windowed, in which case TryExpand omits the param.
            ["window_upper"] = Bound(spec.WatermarkUpperBound, cursorType, config, upper: true, label),
            // Never null: already rendered text, so it passes through FormatBinding untouched.
            ["run_date"] = new(runDate, null),
        };
        return bindings;
    }

    /// <summary>One window bound. Without `watermark_format` it stays typed and is rendered by
    /// <see cref="FormatBinding"/> where it is used, exactly as before the option existed. With one it is
    /// rendered here, once, where the bound's direction is known; the result is typed `bigint` for an epoch
    /// format (so a whole-value JSON body binding sends a number) and untyped text otherwise, which
    /// FormatBinding passes through.</summary>
    private static BindingValue Bound(string? value, string? cursorType, HttpDatasetConfig config, bool upper,
        string label)
    {
        if (config.WatermarkFormat is not { } format)
        {
            return new BindingValue(value, cursorType);
        }

        // The offline check cannot know a SQL-declared incremental's type; this is where it is known.
        if (HttpDatasetConfig.CheckFormatForCursor(format, config.WatermarkTimeZone, cursorType) is { } error)
        {
            throw new PzConnectorException($"{label}: {error}", isTransient: false);
        }

        if (value is null)
        {
            return new BindingValue(null, null);
        }

        // The stored value is never echoed: rendered binding values stay out of messages.
        if (!TryParseBound(value, out var ts))
        {
            throw new PzConnectorException(
                $"{label}: 'watermark_format: {format.Name}' cannot render the stored {(upper ? "window upper bound" : "watermark")}: " +
                "it is not a timestamp or date; re-run with --full-refresh if the cursor's type changed",
                isTransient: false);
        }

        try
        {
            return new BindingValue(format.Render(ts, upper, config.WatermarkTimeZone), format.IsNumeric ? "bigint" : null);
        }
        catch (PzConnectorException ex)
        {
            throw new PzConnectorException($"{label}: {ex.Message}", isTransient: false, innerException: ex);
        }
    }

    [System.Text.RegularExpressions.GeneratedRegex(@"(?<=\.\d{7})\d+")]
    private static partial System.Text.RegularExpressions.Regex SubTickDigits();

    /// <summary>Parses a stored bound as UTC. Digits past the 100 ns tick (a nanosecond cursor) are cut off
    /// first: the parser would round them, and a lower bound rounded up past the stored value can floor
    /// into the next unit and skip rows.</summary>
    internal static bool TryParseBound(string value, out DateTimeOffset ts)
        => DateTimeOffset.TryParse(SubTickDigits().Replace(value, ""), CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out ts);

    public static HttpPageRequest Build(HttpConnectionConfig connection, HttpDatasetConfig config,
        IPageStrategy? strategy, IReadOnlyDictionary<string, BindingValue> bindings, string label,
        ItemValues? item = null)
    {
        var url = BuildFirstUri(connection, config, strategy, bindings, label, item);
        if (!config.IsPost)
        {
            return new HttpPageRequest(HttpMethod.Get, url, null, null);
        }

        // The query string and the body expand the same bindings with the same formatting, so a
        // watermark appears identically in both.
        JsonNode? json = null;
        string? text = null;
        if (config.Body is { } body)
        {
            if (body.IsJson)
            {
                json = body.Render(bindings, FormatBinding, out var error, item);
                if (error is not null)
                {
                    throw new PzConnectorException($"{label}: body: {error}", isTransient: false);
                }

                if (config.BodyPaging is { } paging && json is not null)
                {
                    json = paging.First(json);
                }
            }
            else
            {
                try
                {
                    text = body.RenderText(bindings, FormatBinding, item);
                }
                catch (PzConnectorException ex)
                {
                    throw new PzConnectorException($"{label}: {ex.Message}", isTransient: false,
                        innerException: ex, code: ex.Code, hint: ex.Hint);
                }
            }
        }

        return new HttpPageRequest(HttpMethod.Post, url, json, text);
    }

    private static Uri BuildFirstUri(HttpConnectionConfig connection, HttpDatasetConfig config,
        IPageStrategy? strategy, IReadOnlyDictionary<string, BindingValue> bindings, string label,
        ItemValues? item)
    {
        var pairs = new List<string>();
        foreach (var (name, template) in config.Query)
        {
            bool expanded;
            string? value, error;
            try
            {
                // Defense in depth: HttpDatasetConfig.Parse already validates every query template
                // offline, but a malformed/unknown binding must never surface as an uncoded
                // FormatException from a live read.
                // Raw text either way: the value is escaped once, below, when the pair is joined.
                expanded = HttpTemplate.HasItemReferences(template)
                    ? HttpTemplate.TryExpand(template, bindings, item, FormatBinding,
                        (_, node) => HttpTemplate.ItemText(node), out value, out error)
                    : BindingExpander.TryExpand(template, bindings, FormatBinding, out value, out error);
            }
            catch (FormatException ex)
            {
                throw new PzConnectorException(
                    $"{label}: query parameter '{name}': malformed binding template '{template}': " +
                    $"{ex.Message}", isTransient: false, code: PzConnectorErrorCodes.Config);
            }

            if (!expanded)
            {
                throw new PzConnectorException($"{label}: query parameter '{name}': {error}",
                    isTransient: false);
            }

            if (value is null)
            {
                continue; // referenced binding is null (first run / --full-refresh): omit the param
            }

            pairs.Add($"{Uri.EscapeDataString(name)}={Uri.EscapeDataString(value)}");
        }

        // config.Path always starts with '/' (HttpDatasetConfig.Parse validated); stripping it makes
        // the segment relative so it resolves against the FULL slash-terminated base_url (including
        // any path prefix) instead of root-relative to the host.
        var first = new UriBuilder(new Uri(connection.BaseUrl, ExpandPath(config.Path, bindings, label, item: item)[1..]))
        {
            Query = string.Join('&', pairs),
        }.Uri;

        // The strategy's own params (page number, size) go on the first request too — an API's
        // default page size is otherwise in effect for page one, skipping or re-delivering rows
        // the moment page two jumps ahead. A sync-token replay never comes through here; it stays verbatim.
        return strategy is null ? first : strategy.FirstRequestUri(first);
    }

    // Each substituted value is escaped as one path segment, so a '/' in a value cannot add
    // segments, while the template's own literal characters stay as written. A query parameter
    // with a null binding can be dropped; a path cannot drop anything, so a null is an error.
    internal static string ExpandPath(string template, IReadOnlyDictionary<string, BindingValue> bindings,
        string label, string option = "path", ItemValues? item = null)
    {
        var hasItemReferences = HttpTemplate.HasItemReferences(template);
        bool expanded;
        string? path, error;
        try
        {
            expanded = hasItemReferences
                ? HttpTemplate.TryExpand(template, bindings, item,
                    (name, value) => Uri.EscapeDataString(FormatBinding(name, value)),
                    (name, node) => Uri.EscapeDataString(ItemPathSegment(name, node, label, option)),
                    out path, out error)
                : BindingExpander.TryExpand(template, bindings,
                    (name, value) => Uri.EscapeDataString(FormatBinding(name, value)), out path, out error);
        }
        catch (FormatException ex)
        {
            throw new PzConnectorException(
                $"{label}: option '{option}': malformed binding template '{template}': {ex.Message}",
                isTransient: false, code: PzConnectorErrorCodes.Config);
        }

        if (!expanded)
        {
            throw new PzConnectorException($"{label}: option '{option}': {error}", isTransient: false, code: PzConnectorErrorCodes.Config);
        }

        if (path is null)
        {
            var name = (hasItemReferences
                ? HttpTemplate.EngineReferences(template)
                : BindingExpander.ReferencedBindings(template)).First(n => bindings[n].IsNull);
            throw new PzConnectorException(
                $"{label}: option '{option}' references '{{{{ {name} }}}}', which has no value on this run " +
                "(a first run or --full-refresh has no watermark) — declare 'initial:' so the first run " +
                "has a lower bound, or use '{{ run_date }}'", isTransient: false, code: PzConnectorErrorCodes.Config);
        }

        if (hasItemReferences)
        {
            EnsureItemSegments(template, path, label, option);
        }

        return path;
    }

    // Substituted values are escaped, so a '/' never comes from one and the template's segments line up
    // with the expanded path's. A segment holding an item reference may not expand to '.' or '..' (a dot
    // segment moves the request to another resource) nor to nothing (an empty segment): together with
    // the template's literal text, values that pass one by one can still make either.
    private static void EnsureItemSegments(string template, string path, string label, string option)
    {
        var templateSegments = template.Split('/');
        var pathSegments = path.Split('/');
        for (var i = 0; i < templateSegments.Length && i < pathSegments.Length; i++)
        {
            if (!HttpTemplate.HasItemReferences(templateSegments[i]) || pathSegments[i] is not ("" or "." or ".."))
            {
                continue;
            }

            var fields = string.Join(", ", HttpTemplate.ItemReferences(templateSegments[i]).Select(f => $"'{f}'"));
            var made = pathSegments[i].Length == 0 ? "an empty path segment" : $"the path segment '{pathSegments[i]}'";
            throw new PzConnectorException(
                $"{label}: option '{option}': item field(s) {fields} make {made}, which cannot be a path segment",
                isTransient: false);
        }
    }

    // Escaping keeps an item value inside its segment, but '.' or '..' as the whole value would still
    // be resolved as a dot segment and move the request to another resource.
    private static string ItemPathSegment(string name, JsonNode node, string label,
        string option)
    {
        var text = HttpTemplate.ItemText(node);
        if (text is "." or "..")
        {
            throw new PzConnectorException(
                $"{label}: option '{option}': item field '{name}' is '{text}', which cannot be a path segment",
                isTransient: false);
        }

        return text;
    }

    // Formats a bound value for a query param. Serves BOTH the lower bound (watermark) and the
    // upper bound (window_upper). A timestamp cursor renders as ISO-8601 UTC to whole seconds
    // (many APIs reject sub-second precision). Rounding direction differs by bound so the
    // (lower, upper] window is never under-covered:
    //   - lower (watermark, exclusive '>'): truncate DOWN — re-reads at most the boundary second.
    //   - upper (window_upper, inclusive '<='): round UP — a sub-second upper like ...00.5 must
    //     still include rows at ...00.5, so we query '<= ...01'. Rows in (upper, ceil] are
    //     over-extracted this window, but the engine's window-scoped MAX probe caps the watermark
    //     candidate at 'upper', so they re-deliver in the next window: re-delivery, never loss,
    //     even for an empty (lower, floor_sec(upper)] slice (windowed->append requires
    //     accept_duplicates, PZ0214). Truncating the upper DOWN would instead drop the fractional
    //     tail: an empty truncated slice advances the watermark to the full untruncated upper
    //     (empty-slice advancement), silently skipping those rows.
    internal static string FormatBinding(string name, BindingValue value)
    {
        if (value.TypeName != "timestamp" || value.Value is null || !TryParseBound(value.Value, out var ts))
        {
            return value.Value!;
        }

        return WatermarkFormat.Iso8601.Render(ts, upper: name == "window_upper");
    }
}
