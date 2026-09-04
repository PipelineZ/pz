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

    private static readonly Dictionary<string, FileFormat> Formats = new(StringComparer.OrdinalIgnoreCase)
    {
        ["csv"] = new("csv", "csv", NativeRead: true, NativeWrite: true, UniversalTier: true, [], NoOptions),
        ["json"] = new("json", "json", NativeRead: true, NativeWrite: true, UniversalTier: true, [], NoOptions),
        ["parquet"] = new("parquet", "parquet", NativeRead: true, NativeWrite: true, UniversalTier: true, [], NoOptions),
    };

    /// <summary>Every format-scoped option key any format owns. A key here that the resolved format's
    /// <see cref="FileFormat.OptionKeys"/> does not contain is PZ0362 -- an option on the wrong format is
    /// a mistake, never silently ignored. Grows with the catalog.</summary>
    private static readonly HashSet<string> AllFormatOptionKeys = new(StringComparer.Ordinal);

    /// <summary>Canonical names, sorted ordinal -- the order every "supported:" message and the schema
    /// enum use.</summary>
    public static IReadOnlyList<string> Names { get; } = Formats.Keys.Order(StringComparer.Ordinal).ToArray();

    /// <summary>JSON-schema object members (no surrounding braces) for <c>format</c> and every
    /// format-scoped option, to splice into a connector's <c>DatasetConfigSchema</c> "properties".</summary>
    public static string SchemaProperties { get; } =
        "\"format\": { \"enum\": [" + string.Join(", ", Names.Select(n => "\"" + n + "\"")) + "] }";

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
    /// <param name="options">The dataset's format-scoped options -- a delimiter, a json layout, a sheet
    /// -- once a format admits one; unused until then.</param>
    /// <param name="context">Names the dataset in an option error, e.g. "dataset 'x'".</param>
    public static string ReadFragment(
        FileFormat format, IReadOnlyDictionary<string, object?> options, FormatReadRequest read, string context)
    {
        ArgumentNullException.ThrowIfNull(format);
        ArgumentNullException.ThrowIfNull(read);
        var declared = read.DeclaredColumns is { Count: > 0 } d ? d : null;
        return format.Name switch
        {
            "csv" => declared is null
                ? $"read_csv({read.UrlArg}, header = true, auto_detect = true)"
                : $"read_csv({read.UrlArg}, header = true, auto_detect = false, columns = {{{ColumnsMap(declared, read.DuckDbTypeName)}}})",
            "json" => declared is null
                ? $"read_json({read.UrlArg}, auto_detect = true, format = 'newline_delimited')"
                : $"read_json({read.UrlArg}, columns = {{{ColumnsMap(declared, read.DuckDbTypeName)}}}, format = 'newline_delimited')",
            "parquet" => $"read_parquet({read.UrlArg})",
            _ => throw new UnreachableException($"format '{format.Name}' has no native read fragment"),
        };
    }

    /// <summary>True when DuckDB invents the schema (contract-less csv/json auto_detect) rather than
    /// reading one the file or the contract declares -- drives the engine's inference lints.</summary>
    public static bool SchemaInferred(FileFormat format, IReadOnlyDictionary<string, string>? declared)
    {
        ArgumentNullException.ThrowIfNull(format);
        return format.Name is "csv" or "json" && declared is not { Count: > 0 };
    }

    /// <summary>DuckDB's own sniffer verdict for a SINGLE csv file, or null for every other format --
    /// callers pass it only for inferred, single-file reads.</summary>
    /// <param name="options">The dataset's format-scoped options -- a delimiter, a json layout, a sheet
    /// -- once a format admits one; unused until then.</param>
    public static string? SniffFragment(FileFormat format, IReadOnlyDictionary<string, object?> options, string singleUrlLiteral)
    {
        ArgumentNullException.ThrowIfNull(format);
        return format.Name == "csv" ? $"sniff_csv({singleUrlLiteral})" : null;
    }

    /// <summary>The parenthesised COPY options for a native write, e.g. <c>format csv, header</c>.</summary>
    /// <param name="options">The output's format-scoped options -- a delimiter, a json layout, a sheet
    /// -- once a format admits one; unused until then.</param>
    /// <param name="context">Names the output in an option error, e.g. "output 'y'".</param>
    public static string CopyClause(FileFormat format, IReadOnlyDictionary<string, object?> options, string context)
    {
        ArgumentNullException.ThrowIfNull(format);
        return format.Name switch
        {
            "parquet" => "format parquet",
            "csv" => "format csv, header",
            "json" => "format json",
            _ => throw new UnreachableException($"format '{format.Name}' has no COPY clause"),
        };
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
            "csv" => "read_csv",
            "json" => "read_json",
            "parquet" => "read_parquet",
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
    /// <param name="options">Unused today; a later change refuses option-level cases (a native-only option
    /// on an otherwise universal-tier format) through it.</param>
    public static void EnsureUniversalTierSupported(
        FileFormat format, IReadOnlyDictionary<string, object?> options, string connector, string context)
    {
        ArgumentNullException.ThrowIfNull(format);
        if (!format.UniversalTier)
        {
            var choices = string.Join(", ", Formats.Values.Where(f => f.UniversalTier).Select(f => f.Name).Order(StringComparer.Ordinal));
            throw Permanent($"PZ0361: {context}: format '{format.Name}' is native-only (DuckDB reads and writes it); {connector} has no native tier here -- " +
                $"choose one of {choices}, or use a connector whose native tier carries it (localfiles, s3, gcs, azureblob)");
        }
    }

    private static void ValidateOptions(FileFormat format, IReadOnlyDictionary<string, object?> options, string context)
    {
        // Every format-scoped option key must appear on its format's OptionKeys; no format admits one yet, so there is nothing to validate.
    }

    private static string ColumnsMap(IReadOnlyDictionary<string, string> declared, Func<string, string, string> duckType) =>
        string.Join(", ", declared.Select(c => $"'{Esc(c.Key)}': '{duckType(c.Value, c.Key)}'"));

    private static string Esc(string value) => value.Replace("'", "''", StringComparison.Ordinal);

    private static string Supported() => string.Join(", ", Names);

    private static PzConnectorException Permanent(string message) => new(message, isTransient: false);
}
