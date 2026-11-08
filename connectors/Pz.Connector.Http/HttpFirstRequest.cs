using System.Globalization;
using System.Text.Json.Nodes;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Bindings;
using Pz.Connectors.Toolkit.Paging;

namespace Pz.Connector.Http;

/// <summary>Builds a dataset's first request from its options: the engine bindings, then the path,
/// the query string and (for a POST) the body, all expanded from the same bindings with the same
/// formatting, so a watermark appears identically wherever it is referenced.</summary>
internal static class HttpFirstRequest
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
                    "(or a 'columns' contract declaring the cursor)", isTransient: false);
            }

            if (config.Cursor is { } optCursor && optCursor != wmCursor)
            {
                throw new PzConnectorException(
                    $"{label}: incremental.cursor is '{wmCursor}' but options.cursor is '{optCursor}'; " +
                    "align the two", isTransient: false);
            }
        }

        string? cursorType;
        if (config.IsContractMode && spec.WatermarkCursor is not null)
        {
            if (!config.Columns!.TryGetValue(spec.WatermarkCursor, out cursorType))
            {
                throw new PzConnectorException(
                    $"{label}: incremental cursor '{spec.WatermarkCursor}' is not declared in 'columns:' — " +
                    "add it to the contract so its values land and its type is known", isTransient: false);
            }
        }
        else
        {
            cursorType = config.CursorType;
        }
        var bindings = new Dictionary<string, BindingValue>
        {
            ["watermark"] = new(spec.WatermarkValue, cursorType),
            // Upper bound of the engine-resolved (lower, upper] window; null when the dataset is
            // not windowed, in which case TryExpand omits the param.
            ["window_upper"] = new(spec.WatermarkUpperBound, cursorType),
            // Never null: already rendered text, so it passes through FormatBinding untouched.
            ["run_date"] = new(runDate, null),
        };
        return bindings;
    }

    public static HttpPageRequest Build(HttpConnectionConfig connection, HttpDatasetConfig config,
        IPageStrategy? strategy, IReadOnlyDictionary<string, BindingValue> bindings, string label)
    {
        var url = BuildFirstUri(connection, config, strategy, bindings, label);
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
                json = body.Render(bindings, FormatBinding, out var error);
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
                    text = body.RenderText(bindings, FormatBinding);
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
        IPageStrategy? strategy, IReadOnlyDictionary<string, BindingValue> bindings, string label)
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
                expanded = BindingExpander.TryExpand(template, bindings, FormatBinding, out value, out error);
            }
            catch (FormatException ex)
            {
                throw new PzConnectorException(
                    $"{label}: query parameter '{name}': malformed binding template '{template}': " +
                    $"{ex.Message}", isTransient: false);
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
        var first = new UriBuilder(new Uri(connection.BaseUrl, ExpandPath(config.Path, bindings, label)[1..]))
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
    private static string ExpandPath(string template, IReadOnlyDictionary<string, BindingValue> bindings,
        string label)
    {
        bool expanded;
        string? path, error;
        try
        {
            expanded = BindingExpander.TryExpand(template, bindings,
                (name, value) => Uri.EscapeDataString(FormatBinding(name, value)), out path, out error);
        }
        catch (FormatException ex)
        {
            throw new PzConnectorException(
                $"{label}: option 'path': malformed binding template '{template}': {ex.Message}",
                isTransient: false);
        }

        if (!expanded)
        {
            throw new PzConnectorException($"{label}: option 'path': {error}", isTransient: false);
        }

        if (path is null)
        {
            var name = BindingExpander.ReferencedBindings(template).First(n => bindings[n].IsNull);
            throw new PzConnectorException(
                $"{label}: option 'path' references '{{{{ {name} }}}}', which has no value on this run " +
                "(a first run or --full-refresh has no watermark) — declare 'initial:' so the first run " +
                "has a lower bound, or use '{{ run_date }}'", isTransient: false);
        }

        return path;
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
        if (value.TypeName != "timestamp"
            || !DateTimeOffset.TryParse(value.Value, CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var ts))
        {
            return value.Value!;
        }

        var floored = new DateTimeOffset(ts.Year, ts.Month, ts.Day, ts.Hour, ts.Minute, ts.Second, TimeSpan.Zero);
        var rendered = name == "window_upper" && ts > floored ? floored.AddSeconds(1) : floored;
        return rendered.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
    }
}
