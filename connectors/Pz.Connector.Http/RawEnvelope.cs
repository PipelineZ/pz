namespace Pz.Connector.Http;

/// <summary>The raw landing envelope: schema is always exact regardless of API drift;
/// shaping happens downstream in DuckDB SQL. The cursor column joins the envelope so the engine's
/// post-land MAX(cursor) watermark capture works unchanged.</summary>
internal static class RawEnvelope
{
    public static IReadOnlyDictionary<string, string> Columns(HttpDatasetConfig config)
    {
        var columns = new Dictionary<string, string>
        {
            ["payload"] = "varchar",
            ["pz_page"] = "int",
            ["pz_fetched_at"] = "timestamp",
        };
        if (config.Cursor is { } cursor)
        {
            columns[cursor] = config.CursorType!;
        }

        return columns;
    }
}
