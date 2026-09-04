namespace Pz.Connectors.Toolkit.Formats;

/// <summary>One file format a file-place connector accepts as <c>format:</c>. <see cref="Extension"/> is
/// the suffix used for a default read path (<c>&lt;entity&gt;.&lt;ext&gt;</c>) and for sink object names.
/// <see cref="DuckDbExtensions"/> are the DuckDB extensions a native scan/copy must install and load
/// first. <see cref="OptionKeys"/> are the format-scoped options this format admits; a format-scoped
/// option present on an entity of another format is PZ0362.</summary>
public sealed record FileFormat(
    string Name,
    string Extension,
    bool NativeRead,
    bool NativeWrite,
    IReadOnlyList<string> DuckDbExtensions,
    IReadOnlySet<string> OptionKeys);

/// <summary>What a native read fragment needs from the connector: <see cref="UrlArg"/> is already a
/// SQL string literal (<c>'…'</c>) or list literal (<c>['…', '…']</c>) with the connector's own escaping;
/// <see cref="FileCount"/> says how many files it names; <see cref="DeclaredColumns"/> is the
/// <c>columns:</c> contract or null; <see cref="DuckDbTypeName"/> maps (typeName, columnName) to the
/// DuckDB type name and throws the connector's own permanent error for an unknown type.</summary>
public sealed record FormatReadRequest(
    string UrlArg,
    int FileCount,
    IReadOnlyDictionary<string, string>? DeclaredColumns,
    Func<string, string, string> DuckDbTypeName);
