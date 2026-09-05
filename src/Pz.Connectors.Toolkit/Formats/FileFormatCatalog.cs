using System.Diagnostics;
using Pz.Connectors.Abstractions;

namespace Pz.Connectors.Toolkit.Formats;

/// <summary>The one table of file formats shared by the file-place connectors (localfiles, s3, gcs,
/// azureblob, sftp): resolution and option validation, DuckDB <c>read_*</c> fragments, <c>COPY</c>
/// option clauses, the extensions a format needs, and the JSON-schema members every connector's
/// <c>DatasetConfigSchema</c> embeds. Pure and offline: no I/O, nothing place-specific, no secrets.
/// Fragments are byte-stable goldens (<c>FileFormatCatalogTests</c>): they appear in plan.json.</summary>
public static class FileFormatCatalog
{
    private static readonly IReadOnlySet<string> NoOptions = new HashSet<string>(StringComparer.Ordinal);

    private static readonly IReadOnlySet<string> CsvOptions = new HashSet<string>(StringComparer.Ordinal) { "delimiter" };
    private static readonly IReadOnlySet<string> JsonOptions = new HashSet<string>(StringComparer.Ordinal) { "layout" };
    private static readonly IReadOnlySet<string> XlsxOptions = new HashSet<string>(StringComparer.Ordinal) { "sheet", "header" };

    private static readonly Dictionary<string, FileFormat> Formats = new(StringComparer.OrdinalIgnoreCase)
    {
        ["csv"] = new("csv", "csv", NativeRead: true, NativeWrite: true, UniversalTier: true, [], CsvOptions),
        ["tsv"] = new("tsv", "tsv", NativeRead: true, NativeWrite: true, UniversalTier: true, [], NoOptions),
        ["json"] = new("json", "json", NativeRead: true, NativeWrite: true, UniversalTier: true, [], JsonOptions),
        ["parquet"] = new("parquet", "parquet", NativeRead: true, NativeWrite: true, UniversalTier: true, [], NoOptions),
        ["xlsx"] = new("xlsx", "xlsx", NativeRead: true, NativeWrite: true, UniversalTier: false, ["excel"], XlsxOptions),
        ["avro"] = new("avro", "avro", NativeRead: true, NativeWrite: false, UniversalTier: false, ["avro"], NoOptions),
    };

    /// <summary>Every format-scoped option key any format owns. A key here that the resolved format's
    /// <see cref="FileFormat.OptionKeys"/> does not contain is PZ0362 -- an option on the wrong format is
    /// a mistake, never silently ignored. Grows with the catalog.</summary>
    private static readonly HashSet<string> AllFormatOptionKeys = new(StringComparer.Ordinal) { "delimiter", "layout", "sheet", "header" };

    /// <summary>Canonical names, sorted ordinal -- the order every "supported:" message and the schema
    /// enum use.</summary>
    public static IReadOnlyList<string> Names { get; } = Formats.Keys.Order(StringComparer.Ordinal).ToArray();

    /// <summary>JSON-schema object members (no surrounding braces) for <c>format</c> and every
    /// format-scoped option, to splice into a connector's <c>DatasetConfigSchema</c> "properties".</summary>
    public static string SchemaProperties { get; } =
        "\"format\": { \"enum\": [" + string.Join(", ", Names.Select(n => "\"" + n + "\"")) + "] }, " +
        "\"delimiter\": { \"type\": \"string\", \"minLength\": 1, \"maxLength\": 1 }, " +
        "\"layout\": { \"enum\": [\"ndjson\", \"array\"] }, " +
        "\"sheet\": { \"type\": \"string\", \"minLength\": 1 }, \"header\": { \"type\": \"boolean\" }";

    /// <summary>Resolves <c>format:</c> (falling back to <paramref name="defaultFormat"/>; null means the
    /// connector requires it) and validates the format-scoped options. <paramref name="context"/> is
    /// "dataset 'x'" or "output 'y'".</summary>
    public static FileFormat Resolve(
        IReadOnlyDictionary<string, object?> options, string? defaultFormat, string connector, string context)
    {
        ArgumentNullException.ThrowIfNull(options);
        var name = options.TryGetValue("format", out var v) && v?.ToString() is { Length: > 0 } given ? given : defaultFormat;
        if (name is null)
        {
            throw Permanent($"PZ0361: {context}: {connector} requires 'format' (supported: {Supported()})");
        }

        if (!Formats.TryGetValue(name, out var format))
        {
            throw Permanent($"PZ0361: {context}: {connector} does not support format '{name}' (supported: {Supported()})");
        }

        foreach (var key in AllFormatOptionKeys)
        {
            if (options.ContainsKey(key) && !format.OptionKeys.Contains(key))
            {
                throw Permanent($"PZ0362: {context}: '{key}' is not an option of format '{format.Name}' -- remove it or change the format");
            }
        }

        ValidateOptions(format, options, context);
        return format;
    }

    /// <summary>The FROM-usable DuckDB fragment for a native read. csv and json follow the two-state
    /// contract model: a declared contract (partial or full) renders the strict <c>columns = {…}</c> map
    /// and reads only those columns; no contract auto-detects.</summary>
    /// <param name="options">Format-scoped options: <c>delimiter</c> (csv) and <c>layout</c> (json)
    /// change the fragment; other formats ignore it.</param>
    /// <param name="context">Names the dataset in an option error, e.g. "dataset 'x'".</param>
    public static string ReadFragment(
        FileFormat format, IReadOnlyDictionary<string, object?> options, FormatReadRequest read, string context)
    {
        ArgumentNullException.ThrowIfNull(format);
        ArgumentNullException.ThrowIfNull(read);
        var declared = read.DeclaredColumns is { Count: > 0 } d ? d : null;
        return format.Name switch
        {
            "csv" or "tsv" => declared is null
                ? $"read_csv({read.UrlArg}, header = true, auto_detect = true{DelimiterSuffix(format, options, context, "delim")})"
                : $"read_csv({read.UrlArg}, header = true, auto_detect = false, columns = {{{ColumnsMap(declared, read.DuckDbTypeName)}}}{DelimiterSuffix(format, options, context, "delim")})",
            "json" => declared is null
                ? $"read_json({read.UrlArg}, auto_detect = true, format = '{JsonReadFormat(format, options)}')"
                : $"read_json({read.UrlArg}, columns = {{{ColumnsMap(declared, read.DuckDbTypeName)}}}, format = '{JsonReadFormat(format, options)}')",
            "parquet" => $"read_parquet({read.UrlArg})",
            "xlsx" => XlsxRead(options, read, context, declared),
            "avro" => declared is null ? $"read_avro({read.UrlArg})" : CastProjection($"read_avro({read.UrlArg})", declared, read.DuckDbTypeName),
            _ => throw new UnreachableException($"format '{format.Name}' has no native read fragment"),
        };
    }

    /// <summary>read_xlsx reads exactly one workbook -- unlike csv/json/parquet, it takes no list literal,
    /// so a multi-file match (e.g. a wildcarded path:) is refused rather than silently reading one file
    /// of several.</summary>
    private static string XlsxRead(IReadOnlyDictionary<string, object?> options, FormatReadRequest read, string context, IReadOnlyDictionary<string, string>? declared)
    {
        if (read.FileCount != 1)
        {
            throw Permanent($"PZ0361: {context}: xlsx reads one workbook per entity; this read matches {read.FileCount} files -- name a single file in path:");
        }

        var inner = $"read_xlsx({read.UrlArg}, {XlsxReadArgs(options, context)})";
        return declared is null ? inner : CastProjection(inner, declared, read.DuckDbTypeName);
    }

    /// <summary>read_xlsx/read_avro take no columns= map: a declared contract is applied as a projecting
    /// cast, which also prunes to the declared columns. A cast failure is DuckDB's own loud error at
    /// scan time, the same posture as read_csv(auto_detect = false).</summary>
    private static string CastProjection(string inner, IReadOnlyDictionary<string, string> declared, Func<string, string, string> duckType) =>
        "(select " + string.Join(", ", declared.Select(c => $"{Ident(c.Key)}::{duckType(c.Value, c.Key)} as {Ident(c.Key)}")) + $" from {inner})";

    private static string Ident(string name) => "\"" + name.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";

    private static string JsonReadFormat(FileFormat format, IReadOnlyDictionary<string, object?> options) =>
        JsonLayout(format, options) == "array" ? "array" : "newline_delimited";

    /// <summary>True when DuckDB invents the schema (contract-less csv/json auto_detect) rather than
    /// reading one the file or the contract declares -- drives the engine's inference lints.</summary>
    public static bool SchemaInferred(FileFormat format, IReadOnlyDictionary<string, string>? declared)
    {
        ArgumentNullException.ThrowIfNull(format);
        return format.Name is "csv" or "tsv" or "json" or "xlsx" && declared is not { Count: > 0 };
    }

    /// <summary>DuckDB's own sniffer verdict for a SINGLE csv file, or null for every other format --
    /// callers pass it only for inferred, single-file reads.</summary>
    /// <param name="options"><c>delimiter</c> (csv) is passed to sniff_csv so the sniffer sees the same
    /// field separator as the read.</param>
    /// <param name="context">Names the dataset in a delimiter option error, e.g. "dataset 'x'".</param>
    public static string? SniffFragment(
        FileFormat format, IReadOnlyDictionary<string, object?> options, string singleUrlLiteral, string context)
    {
        ArgumentNullException.ThrowIfNull(format);
        return format.Name is "csv" or "tsv" ? $"sniff_csv({singleUrlLiteral}{DelimiterSuffix(format, options, context, "delim")})" : null;
    }

    /// <summary>The parenthesised COPY options for a native write, e.g. <c>format csv, header</c>.</summary>
    /// <param name="options"><c>delimiter</c> (csv/tsv) and <c>layout</c> (json) change the COPY
    /// options.</param>
    /// <param name="context">Names the output in an option error, e.g. "output 'y'".</param>
    public static string CopyClause(FileFormat format, IReadOnlyDictionary<string, object?> options, string context)
    {
        ArgumentNullException.ThrowIfNull(format);
        return format.Name switch
        {
            "parquet" => "format parquet",
            "csv" or "tsv" => "format csv, header" + CopyDelimiterSuffix(format, options, context),
            "json" => JsonLayout(format, options) == "array" ? "format json, array true" : "format json",
            "xlsx" => "format xlsx, " + XlsxCopyArgs(options, context),
            _ => throw new UnreachableException($"format '{format.Name}' has no COPY clause"),
        };
    }

    /// <summary>The COPY-clause delimiter suffix -- COPY's <c>delimiter</c> keyword takes no <c>=</c>,
    /// unlike <c>read_csv</c>'s <c>delim =</c>, so this cannot reuse <see cref="DelimiterSuffix"/>.</summary>
    private static string CopyDelimiterSuffix(FileFormat format, IReadOnlyDictionary<string, object?> options, string context)
    {
        var delimiter = Delimiter(format, options, context);
        return delimiter == ',' ? "" : $", delimiter {DelimiterLiteral(delimiter)}";
    }

    /// <summary><c>install X</c>/<c>load X</c> for each DuckDB extension the format needs; the engine's
    /// setup ledger runs each distinct statement once per run.</summary>
    public static IReadOnlyList<string> SetupStatements(FileFormat format)
    {
        ArgumentNullException.ThrowIfNull(format);
        var statements = new List<string>(format.DuckDbExtensions.Count * 2);
        foreach (var ext in format.DuckDbExtensions)
        {
            statements.Add($"install {ext}");
            statements.Add($"load {ext}");
        }

        return statements;
    }

    /// <summary>The planner-facing mechanism name of a native read: the DuckDB function used.</summary>
    public static string ReadMechanism(FileFormat format)
    {
        ArgumentNullException.ThrowIfNull(format);
        return format.Name switch
        {
            "csv" or "tsv" => "read_csv",
            "json" => "read_json",
            "parquet" => "read_parquet",
            "xlsx" => "read_xlsx",
            "avro" => "read_avro",
            _ => throw new UnreachableException($"format '{format.Name}' has no read mechanism"),
        };
    }

    /// <summary>PZ0361 when the format is read-only.</summary>
    public static void EnsureWritable(FileFormat format, string connector, string context)
    {
        ArgumentNullException.ThrowIfNull(format);
        if (!format.NativeWrite)
        {
            throw Permanent($"PZ0361: {context}: format '{format.Name}' is read-only on {connector} -- write parquet, csv or json instead");
        }
    }

    /// <summary>PZ0361 when the resolved format has no <see cref="FileFormat.UniversalTier"/> reader/writer
    /// for a caller that has no native tier to fall back to -- sftp (no native tier at all) and any output
    /// or read forced onto the universal tier by an otherwise-native connector.</summary>
    /// <param name="options">Option-level refusals: json <c>layout: array</c> is native-only regardless
    /// of the format's tier.</param>
    public static void EnsureUniversalTierSupported(
        FileFormat format, IReadOnlyDictionary<string, object?> options, string connector, string context)
    {
        ArgumentNullException.ThrowIfNull(format);
        if (format.Name == "json" && JsonLayout(format, options) == "array")
        {
            throw Permanent($"PZ0361: {context}: json 'layout: array' is native-only; {connector}'s managed json writer/reader handles newline-delimited json only -- " +
                "use 'layout: ndjson', remove engine.force_universal / partition_by so the native tier can carry it, " +
                "or use a connector with a native tier (localfiles, s3, gcs, azureblob)");
        }

        if (!format.UniversalTier)
        {
            var choices = string.Join(", ", Formats.Values.Where(f => f.UniversalTier).Select(f => f.Name).Order(StringComparer.Ordinal));
            throw Permanent($"PZ0361: {context}: format '{format.Name}' is native-only (DuckDB reads and writes it); {connector} has no native tier here -- " +
                $"choose one of {choices}, or use a connector whose native tier carries it (localfiles, s3, gcs, azureblob)");
        }
    }

    /// <summary>The csv/tsv field delimiter: tab for tsv, the validated <c>delimiter:</c> for csv, comma
    /// by default. ASCII only, because the managed writer emits it as one byte; a quote, newline or
    /// carriage return is refused because it collides with csv's own quoting and row-terminator
    /// characters and would yield unparseable output.</summary>
    public static char Delimiter(FileFormat format, IReadOnlyDictionary<string, object?> options, string context)
    {
        ArgumentNullException.ThrowIfNull(format);
        ArgumentNullException.ThrowIfNull(options);
        if (format.Name == "tsv")
        {
            return '\t';
        }

        if (format.Name != "csv" || !options.TryGetValue("delimiter", out var raw))
        {
            return ',';
        }

        var text = raw?.ToString() ?? "";
        if (text.Length != 1 || !char.IsAscii(text[0]) || text[0] is '"' or '\n' or '\r')
        {
            throw Permanent(
                $"PZ0362: {context}: 'delimiter' must be one ASCII character other than a quote, newline or carriage return (got '{text}')");
        }

        return text[0];
    }

    /// <summary><c>layout:</c> of a json entity -- <c>ndjson</c> (default, newline-delimited) or
    /// <c>array</c> (one top-level JSON array of objects).</summary>
    public static string JsonLayout(FileFormat format, IReadOnlyDictionary<string, object?> options)
    {
        ArgumentNullException.ThrowIfNull(format);
        ArgumentNullException.ThrowIfNull(options);
        return format.Name == "json" && options.TryGetValue("layout", out var v) && v?.ToString() is { Length: > 0 } layout
            ? layout
            : "ndjson";
    }

    private static void ValidateOptions(FileFormat format, IReadOnlyDictionary<string, object?> options, string context)
    {
        _ = Delimiter(format, options, context);
        var layout = JsonLayout(format, options);
        if (layout is not ("ndjson" or "array"))
        {
            throw Permanent($"PZ0362: {context}: 'layout' must be 'ndjson' or 'array' (got '{layout}')");
        }

        if (format.Name == "xlsx")
        {
            _ = Sheet(options, context);
            _ = Header(options, context);
        }
    }

    /// <summary><c>sheet:</c> of an xlsx entity, or null for the workbook's first sheet.</summary>
    private static string? Sheet(IReadOnlyDictionary<string, object?> options, string context)
    {
        if (!options.TryGetValue("sheet", out var raw))
        {
            return null;
        }

        var sheet = raw?.ToString();
        return string.IsNullOrEmpty(sheet) ? throw Permanent($"PZ0362: {context}: 'sheet' must be a non-empty sheet name") : sheet;
    }

    /// <summary><c>header:</c> of an xlsx entity -- true (default) treats row 1 as column names,
    /// false yields DuckDB's generated <c>A1</c>/<c>B1</c>… names.</summary>
    private static bool Header(IReadOnlyDictionary<string, object?> options, string context)
    {
        if (!options.TryGetValue("header", out var raw))
        {
            return true;
        }

        return raw switch
        {
            bool b => b,
            string s when bool.TryParse(s, out var b) => b,
            _ => throw Permanent($"PZ0362: {context}: 'header' must be true or false (got '{raw}')"),
        };
    }

    /// <summary>read_xlsx's keyword arguments, e.g. <c>header = true, sheet = 'Q1'</c>.</summary>
    private static string XlsxReadArgs(IReadOnlyDictionary<string, object?> options, string context)
    {
        var header = Header(options, context) ? "true" : "false";
        var sheet = Sheet(options, context);
        return sheet is null ? $"header = {header}" : $"header = {header}, sheet = '{Esc(sheet)}'";
    }

    /// <summary>COPY's xlsx keyword arguments -- unlike <c>read_xlsx</c>, COPY's option keywords take no
    /// <c>=</c>, e.g. <c>header true, sheet 'Q1'</c>.</summary>
    private static string XlsxCopyArgs(IReadOnlyDictionary<string, object?> options, string context)
    {
        var header = Header(options, context) ? "true" : "false";
        var sheet = Sheet(options, context);
        return sheet is null ? $"header {header}" : $"header {header}, sheet '{Esc(sheet)}'";
    }

    /// <summary>The DuckDB string literal for a delimiter: tab as the two characters <c>\t</c>, which
    /// read_csv/COPY interpret; anything else as the character itself, quote-doubled.</summary>
    private static string DelimiterLiteral(char delimiter) =>
        delimiter == '\t' ? "'\\t'" : $"'{Esc(delimiter.ToString())}'";

    private static string DelimiterSuffix(FileFormat format, IReadOnlyDictionary<string, object?> options, string context, string keyword)
    {
        var delimiter = Delimiter(format, options, context);
        return delimiter == ',' ? "" : $", {keyword} = {DelimiterLiteral(delimiter)}";
    }

    private static string ColumnsMap(IReadOnlyDictionary<string, string> declared, Func<string, string, string> duckType) =>
        string.Join(", ", declared.Select(c => $"'{Esc(c.Key)}': '{duckType(c.Value, c.Key)}'"));

    private static string Esc(string value) => value.Replace("'", "''", StringComparison.Ordinal);

    private static string Supported() => string.Join(", ", Names);

    private static PzConnectorException Permanent(string message) => new(message, isTransient: false);
}
