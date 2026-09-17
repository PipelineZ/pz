using System.Globalization;

namespace Pz.Connector.Http;

/// <summary>Option values arrive from YAML already typed, and this connector puts them on the wire as
/// text — query parameters, headers. They must read the way the author wrote them: an unquoted
/// <c>archived: true</c> is <c>true</c>, not .NET's <c>True</c>, and <c>1.5</c> keeps its `.` on a host
/// whose culture writes a comma.</summary>
internal static class YamlScalarText
{
    public static string? Of(object? value) => value switch
    {
        null => null,
        string s => s,
        bool b => b ? "true" : "false",
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString(),
    };
}
