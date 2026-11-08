using System.Globalization;

namespace Pz.Connector.Http;

/// <summary>Option values arrive from YAML already typed, and this connector puts them on the wire as
/// text — query parameters, headers. They must read the way the author wrote them: an unquoted
/// <c>archived: true</c> is <c>true</c>, not .NET's <c>True</c>, and <c>1.5</c> keeps its `.` on a host
/// whose culture writes a comma.</summary>
internal static class YamlScalarText
{
    /// <summary>"map" or "list" when <paramref name="value"/> is a YAML collection, else null.</summary>
    public static string? NonScalarKind(object? value) => value switch
    {
        System.Collections.IDictionary or IReadOnlyDictionary<string, object?> => "map",
        System.Collections.IEnumerable and not string => "list",
        _ => null,
    };

    public static string? Of(object? value) => value switch
    {
        null => null,
        string s => s,
        bool b => b ? "true" : "false",
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString(),
    };
}
