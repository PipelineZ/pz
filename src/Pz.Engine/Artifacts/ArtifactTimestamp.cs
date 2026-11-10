using System.Globalization;

namespace Pz.Engine.Artifacts;

/// <summary>The one spelling of a persisted timestamp: UTC, millisecond precision, a literal <c>Z</c>. Used
/// wherever a run artifact or ledger records a moment, so the local and SQL Server backends render
/// identically. Public because the SQL Server backend is a separate assembly.</summary>
public static class ArtifactTimestamp
{
    public const string Format = "yyyy-MM-ddTHH:mm:ss.fffZ";

    public static string Render(DateTime utc) => utc.ToString(Format, CultureInfo.InvariantCulture);
}
