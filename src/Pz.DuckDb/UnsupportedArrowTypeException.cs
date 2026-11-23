namespace Pz.DuckDb;

/// <summary>A result has columns whose types pz's Arrow path does not carry (the v0 type matrix: BOOLEAN,
/// INTEGER, BIGINT, DOUBLE, DECIMAL, VARCHAR, DATE, TIMESTAMP). <see cref="Columns"/> names every such column
/// with the Arrow type DuckDB exported it as, so a caller can report them all at once and suggest a cast.</summary>
public sealed class UnsupportedArrowTypeException(IReadOnlyList<(string Name, string ArrowType)> columns, string context)
    : NotSupportedException(
        $"{context} does not support column(s) " +
        string.Join(", ", columns.Select(c => $"'{c.Name}' (Arrow type '{c.ArrowType}')")) +
        "; the v0 type matrix carries BOOLEAN, INTEGER, BIGINT, DOUBLE, DECIMAL, VARCHAR, DATE and TIMESTAMP")
{
    public IReadOnlyList<(string Name, string ArrowType)> Columns { get; } = columns;
}
