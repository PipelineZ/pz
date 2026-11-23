using Pz.Core.Validation;
using Pz.DuckDb;

namespace Pz.Engine.Execution;

/// <summary>A staged relation's columns as DuckDB names and types them, read with <c>describe</c> and every
/// value cast to VARCHAR, so it works for every column type, including those the Arrow path does not carry
/// (STRUCT, LIST, VARIANT, SMALLINT…). Use it wherever only names or DuckDB types are needed.</summary>
public static class StagedColumns
{
    public static async Task<IReadOnlyList<(string Name, string Type)>> DescribeAsync(IDuckSession duck, string from,
        CancellationToken ct)
    {
        var columns = new List<(string, string)>();
        await foreach (var batch in duck.QueryArrowAsync(
            $"select column_name::VARCHAR, column_type::VARCHAR from (describe select * from {from})", ct: ct)
            .ConfigureAwait(false))
        {
            using (batch)
            {
                var names = (Apache.Arrow.StringArray)batch.Column(0);
                var types = (Apache.Arrow.StringArray)batch.Column(1);
                for (var r = 0; r < batch.Length; r++)
                {
                    columns.Add((names.GetString(r), types.GetString(r)));
                }
            }
        }

        return columns;
    }

    /// <summary>PZ0546 for an output whose staged input has columns the connector's Arrow path cannot carry:
    /// each column with its DuckDB type, and a cast per column that turns it into one it can.</summary>
    public static async Task<PzError> UnsupportedForArrowAsync(IDuckSession duck, string from,
        UnsupportedArrowTypeException ex, string output, string connector, string? file, CancellationToken ct)
    {
        var names = ex.Columns.Select(c => c.Name).ToHashSet(StringComparer.Ordinal);
        var described = await DescribeAsync(duck, from, ct).ConfigureAwait(false);
        var offending = described.Where(c => names.Contains(c.Name)).ToList();
        return new PzError(PzErrorCode.SinkColumnTypeUnsupported,
            $"output '{output}': connector '{connector}' receives rows over pz's Arrow path, which does not carry " +
            $"column(s) {string.Join(", ", offending.Select(c => $"{c.Name} ({c.Type})"))}; nothing was written",
            file, null,
            $"cast in the pipeline: {string.Join(", ", offending.Select(c => CastFor(c.Name, c.Type)))}");
    }

    /// <summary>The cast that turns a column of DuckDB type <paramref name="type"/> into one the Arrow path
    /// carries: nested and semi-structured values to JSON text, narrow or unsigned integers to INTEGER/BIGINT/
    /// DECIMAL, FLOAT to DOUBLE, other-precision timestamps to TIMESTAMP, binary to base64 text, the rest to
    /// VARCHAR.</summary>
    public static string CastFor(string name, string type)
    {
        var column = IsPlainIdentifier(name) ? name : ArrowInterop.QuoteIdentifier(name);
        var upper = type.ToUpperInvariant();
        if (upper.EndsWith(']') || upper.StartsWith("STRUCT", StringComparison.Ordinal)
            || upper.StartsWith("MAP", StringComparison.Ordinal) || upper.StartsWith("UNION", StringComparison.Ordinal)
            || upper == "VARIANT")
        {
            return $"{column}::JSON::VARCHAR";
        }

        return upper switch
        {
            "TINYINT" or "SMALLINT" or "UTINYINT" or "USMALLINT" => $"{column}::INTEGER",
            "UINTEGER" => $"{column}::BIGINT",
            "UBIGINT" or "UHUGEINT" => $"{column}::DECIMAL(38,0)",
            "FLOAT" => $"{column}::DOUBLE",
            "TIMESTAMP_NS" or "TIMESTAMP_MS" or "TIMESTAMP_S" => $"{column}::TIMESTAMP",
            "BLOB" => $"base64({column})",
            _ => $"{column}::VARCHAR",
        };
    }

    private static bool IsPlainIdentifier(string name) =>
        name.Length > 0 && (char.IsAsciiLetter(name[0]) || name[0] == '_')
        && name.All(c => char.IsAsciiLetterOrDigit(c) || c == '_');
}
