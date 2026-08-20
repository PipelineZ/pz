namespace Pz.Engine.State;

/// <summary>Renders a canonical cursor value (WindowMath forms) as a typed DuckDB/SQL literal for
/// substitution into pipeline SQL and bound-evaluation probes. Numerics are canonical digits and
/// render bare; date/timestamp render as typed quoted literals (quote-doubled defensively).</summary>
public static class CursorLiterals
{
    public static string Typed(string cursorType, string canonicalValue) => cursorType switch
    {
        "timestamp" => $"TIMESTAMP '{canonicalValue.Replace("'", "''")}'",
        "date" => $"DATE '{canonicalValue.Replace("'", "''")}'",
        _ => canonicalValue,
    };
}
