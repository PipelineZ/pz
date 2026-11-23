namespace Pz.Connectors.Toolkit.Formats;

/// <summary>One file format a file-place connector accepts as <c>format:</c>. <see cref="Extension"/> is
/// the suffix used for a default read path (<c>&lt;entity&gt;.&lt;ext&gt;</c>) and for sink object names.
/// <see cref="UniversalTier"/> is true when the managed readers/writers (Sylvan csv, Parquet.Net, the
/// NDJSON codec) implement the format, so a connector without a native tier, or an output forced onto
/// the universal tier, can still carry it. <see cref="DuckDbExtensions"/> are the DuckDB extensions a
/// native scan/copy must install and load first. <see cref="OptionKeys"/> are the format-scoped options
/// this format admits; a format-scoped option present on an entity of another format is PZ0362.
/// <see cref="LocalWriteOnly"/> is true when the format's DuckDB writer must only target the local
/// filesystem: a remote write failure aborts the host process instead of failing the node.</summary>
public sealed record FileFormat(
    string Name,
    string Extension,
    bool NativeRead,
    bool NativeWrite,
    bool UniversalTier,
    bool LocalWriteOnly,
    IReadOnlyList<string> DuckDbExtensions,
    IReadOnlySet<string> OptionKeys);

/// <summary>What a native read fragment needs from the connector: <see cref="UrlArg"/> is already a
/// SQL string literal (<c>'…'</c>) or list literal (<c>['…', '…']</c>) with the connector's own escaping;
/// <see cref="FileCount"/> says how many files it names -- formats that read one file per entity (a
/// footer-scoped or header-scoped peek) refuse a multi-file read with the file count in the message;
/// <see cref="DeclaredColumns"/> is the <c>columns:</c> contract or null. A declared column's DuckDB
/// type name comes from <see cref="ColumnTypeCatalog.ToDuckDbName"/> -- the one shared `columns:`
/// contract type matrix every file-place connector's contract uses, so there is nothing
/// connector-specific left to plug in here.</summary>
public sealed record FormatReadRequest(
    string UrlArg,
    int FileCount,
    IReadOnlyDictionary<string, string>? DeclaredColumns)
{
    /// <summary>A JSON read lands each object whole as one VARIANT column named <c>record</c>
    /// (<c>variant: [record]</c>) instead of auto-detected columns. Use <see cref="For"/> to set it from a
    /// spec.</summary>
    public bool RecordAsVariant { get; init; }

    /// <summary>The request for <paramref name="spec"/>, with <see cref="RecordAsVariant"/> set when the
    /// read's <c>variant:</c> names <c>record</c>.</summary>
    public static FormatReadRequest For(Pz.Connectors.Abstractions.DatasetSpec spec, string urlArg, int fileCount,
        IReadOnlyDictionary<string, string>? declaredColumns) =>
        new(urlArg, fileCount, declaredColumns)
        {
            RecordAsVariant = spec.VariantColumns.Contains(VariantRecordColumn, StringComparer.OrdinalIgnoreCase),
        };

    /// <summary>The column a JSON read with <c>variant: [record]</c> lands each object in.</summary>
    public const string VariantRecordColumn = "record";
}
