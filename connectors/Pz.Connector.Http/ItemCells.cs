namespace Pz.Connector.Http;

/// <summary>What one item contributes to every row of its read, converted once.
/// Contract mode: a column named like a declared field is filled from the item and never read from the
/// record; <see cref="RecordColumns"/> are the rest, in declared order, and <see cref="Place"/> puts both
/// back in declared order. Raw mode: <see cref="Json"/> is the `pz_item` value, and a cursor named like
/// a field takes <see cref="Cursor"/>.</summary>
internal sealed class ItemCells
{
    private readonly int _columnCount;

    private ItemCells(string json, bool cursorFromItem, object? cursor,
        IReadOnlyDictionary<string, string> recordColumns, IReadOnlyList<(int Index, object? Value)> columns,
        int columnCount)
    {
        Json = json;
        CursorFromItem = cursorFromItem;
        Cursor = cursor;
        RecordColumns = recordColumns;
        Columns = columns;
        _columnCount = columnCount;
    }

    public string Json { get; }

    public bool CursorFromItem { get; }

    public object? Cursor { get; }

    public IReadOnlyDictionary<string, string> RecordColumns { get; }

    /// <summary>The item-sourced contract columns: index in the declared order, and value. Ascending.</summary>
    public IReadOnlyList<(int Index, object? Value)> Columns { get; }

    /// <summary>Null without an item, so a read without one takes its usual path unchanged.</summary>
    public static ItemCells? For(HttpDatasetConfig config, ItemValues? item, string label)
    {
        if (item is null)
        {
            return null;
        }

        var context = $"{label}: {item.Identity()}";
        var record = new Dictionary<string, string>();
        var columns = new List<(int Index, object? Value)>();
        if (config.IsContractMode)
        {
            var i = 0;
            foreach (var (name, typeName) in config.Columns!)
            {
                if (item.Declares(name))
                {
                    columns.Add((i, item.ValueAs(name, typeName, context)));
                }
                else
                {
                    record[name] = typeName;
                }

                i++;
            }
        }

        var cursorFromItem = !config.IsContractMode && config.Cursor is { } cursor && item.Declares(cursor);
        var cursorValue = cursorFromItem ? item.ValueAs(config.Cursor!, config.CursorType!, context) : null;
        return new ItemCells(item.ToJsonString(), cursorFromItem, cursorValue, record, columns,
            config.Columns?.Count ?? 0);
    }

    /// <summary>A row of every contract column in declared order, from <paramref name="recordValues"/>
    /// (one per <see cref="RecordColumns"/>) and the item's columns, with <paramref name="extra"/> trailing
    /// slots left for the caller.</summary>
    public object?[] Place(object?[] recordValues, int extra)
    {
        var row = new object?[_columnCount + extra];
        var fromRecord = 0;
        var fromItem = 0;
        for (var i = 0; i < _columnCount; i++)
        {
            row[i] = fromItem < Columns.Count && Columns[fromItem].Index == i
                ? Columns[fromItem++].Value
                : recordValues[fromRecord++];
        }

        return row;
    }
}
