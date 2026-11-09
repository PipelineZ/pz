using Pz.Connectors.Abstractions;

namespace Pz.Connector.Http;

/// <summary>One attempt's `stop_at_watermark` bookkeeping. Every row's cursor must be at or below the one
/// before it (nulls aside): a feed that is not newest-first fails, because stopping early on it would skip
/// rows for good. With a watermark, a row at or below it is dropped and marks its page as the last one.</summary>
internal sealed class StopAtWatermarkState
{
    private readonly int _ordinal;
    private readonly object? _watermark;
    private readonly string _label;
    private object? _previous;
    private int _stopPage;
    private int _dropped;

    private StopAtWatermarkState(int ordinal, object? watermark, string label)
    {
        _ordinal = ordinal;
        _watermark = watermark;
        _label = label;
    }

    /// <summary>True once a page held a record at or below the watermark: the crawl ends with that page.</summary>
    public bool Reached => _stopPage > 0;

    /// <summary>The state for a read with `stop_at_watermark`, or null when it does not apply: the option is
    /// off, or the read has no cursor column (a SQL-declared incremental on its first run has none, and no
    /// watermark either). The stored watermark is parsed the way a text cursor value is, so it compares with
    /// the projected cursors.</summary>
    public static StopAtWatermarkState? For(HttpDatasetConfig config, DatasetSpec spec, int cursorOrdinal, string label)
    {
        if (!config.StopAtWatermark || cursorOrdinal < 0)
        {
            return null;
        }

        var type = config.IsContractMode ? config.Columns![spec.WatermarkCursor!] : config.CursorType!;
        var watermark = spec.WatermarkValue is { } stored
            ? TextValueParser.Parse(stored, type, $"{label}: the stored watermark")
            : null;
        return new StopAtWatermarkState(cursorOrdinal, watermark, label);
    }

    /// <summary>The rows of page <paramref name="page"/> (1-based) to land, in order: all but those at or below
    /// the watermark. Throws when a cursor is greater than the one before it.</summary>
    public List<object?[]> Filter(IEnumerable<object?[]> rows, int page)
    {
        var kept = new List<object?[]>();
        foreach (var row in rows)
        {
            var value = row[_ordinal];
            if (value is not null)
            {
                if (_previous is not null && Compare(value, _previous) > 0)
                {
                    throw new PzConnectorException(
                        $"{_label}: stop_at_watermark needs a newest-first feed, but on page {page} a cursor value " +
                        "is greater than the one before it; stopping at the watermark would skip rows for good — " +
                        "sort the feed newest-first, or remove stop_at_watermark and cursor_order: desc",
                        isTransient: false);
                }

                _previous = value;
                if (_watermark is not null && Compare(value, _watermark) <= 0)
                {
                    _dropped++;
                    _stopPage = page;
                    continue;
                }
            }

            kept.Add(row);
        }

        return kept;
    }

    /// <summary>Called once the crawl is over: a crawl cut short (<paramref name="truncated"/>) before it reached
    /// the watermark would skip the rows between, so it fails; a stop reports the dropped records.</summary>
    public void Finish(bool truncated, Action<string>? notice)
    {
        if (!Reached && _watermark is not null && truncated)
        {
            throw new PzConnectorException(
                $"{_label}: stop_at_watermark: the crawl stopped before it reached the end of the feed and did not " +
                "reach the watermark, so the rows between would be skipped for good; remove max_pages",
                isTransient: false);
        }

        if (Reached)
        {
            notice?.Invoke($"{_label}: stop_at_watermark stopped the crawl at page {_stopPage} of this attempt, the " +
                $"first holding records at or below the watermark; dropped {_dropped} records at or below the watermark");
        }
    }

    /// <summary>Cursor order: text compares ordinally (as DuckDB orders varchar), everything else by its own
    /// comparison; both sides come from the same contract type.</summary>
    private static int Compare(object a, object b)
        => a is string x && b is string y ? string.CompareOrdinal(x, y) : Comparer<object>.Default.Compare(a, b);
}
