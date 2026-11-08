using System.Globalization;
using System.Text;
using System.Xml;

namespace Pz.Connector.Http;

internal enum HttpFileFormat { Csv, Tsv, Json, Xml }

internal enum HttpArchive { Auto, Zip, Gzip, None }

internal enum HttpJsonLayout { Lines, Document }

/// <summary>The `file:` dataset option: the response body is a file (or an archive of files) read in
/// the given format instead of a JSON page. Its presence switches the dataset to file mode.
/// Options that do not apply to the chosen format are refused rather than ignored.</summary>
internal sealed record HttpFileConfig(HttpFileFormat Format)
{
    public HttpArchive Archive { get; init; } = HttpArchive.Auto;

    public string? Members { get; init; }

    public bool IfMissingEmpty { get; init; }

    public string? FileColumn { get; init; }

    /// <summary>Null = UTF-8, or the XML declaration for xml.</summary>
    public Encoding? Encoding { get; init; }

    public char Delimiter { get; init; } = ',';

    public bool Header { get; init; } = true;

    public int SkipLines { get; init; }

    public HttpJsonLayout JsonLayout { get; init; } = HttpJsonLayout.Lines;

    /// <summary>Xml element local names from the root down to the record element.</summary>
    public IReadOnlyList<string>? RecordPath { get; init; }

    public static HttpFileConfig? Parse(object? raw, List<string> errors)
    {
        if (raw is null)
        {
            return null;
        }

        if (raw is not IReadOnlyDictionary<string, object?> block)
        {
            errors.Add("'file' must be a map with at least 'format'");
            return null;
        }

        string? Get(string key)
        {
            if (!block.TryGetValue(key, out var v) || v is null)
            {
                return null;
            }

            if (YamlScalarText.NonScalarKind(v) is { } kind)
            {
                errors.Add($"file.{key} must be a string, got a {kind}");
                return null;
            }

            return YamlScalarText.Of(v);
        }

        bool Has(string key) => block.TryGetValue(key, out var v) && v is not null;


        HttpFileFormat? format = null;
        switch (Get("format"))
        {
            case null:
                errors.Add("file.format is required (csv, tsv, json or xml)");
                break;
            case "csv": format = HttpFileFormat.Csv; break;
            case "tsv": format = HttpFileFormat.Tsv; break;
            case "json": format = HttpFileFormat.Json; break;
            case "xml": format = HttpFileFormat.Xml; break;
            case var other:
                errors.Add($"file.format must be csv, tsv, json or xml, got '{other}'");
                break;
        }

        var archive = HttpArchive.Auto;
        switch (Get("archive"))
        {
            case null or "auto": break;
            case "zip": archive = HttpArchive.Zip; break;
            case "gzip": archive = HttpArchive.Gzip; break;
            case "none": archive = HttpArchive.None; break;
            case var other:
                errors.Add($"file.archive must be auto, zip, gzip or none, got '{other}'");
                break;
        }

        var members = Get("members");
        if (Has("members") && string.IsNullOrWhiteSpace(members))
        {
            errors.Add("file.members must be a non-empty glob");
        }

        var ifMissingEmpty = false;
        switch (Get("if_missing"))
        {
            case null or "error": break;
            case "empty": ifMissingEmpty = true; break;
            case var other:
                errors.Add($"file.if_missing must be error or empty, got '{other}'");
                break;
        }

        if (archive is HttpArchive.None or HttpArchive.Gzip)
        {
            if (Has("members"))
            {
                errors.Add("file.members needs archive zip or auto");
            }

            if (Has("if_missing"))
            {
                errors.Add("file.if_missing needs archive zip or auto");
            }
        }

        var fileColumn = Get("file_column");
        if (Has("file_column") && string.IsNullOrWhiteSpace(fileColumn))
        {
            errors.Add("file.file_column must be a non-empty column name");
        }

        var encoding = ParseEncoding(Get("encoding"), errors);

        var delimiter = format == HttpFileFormat.Tsv ? '\t' : ',';
        if (Has("delimiter"))
        {
            var text = Get("delimiter")!;
            if (format == HttpFileFormat.Tsv)
            {
                errors.Add("file.delimiter cannot be set for tsv (the delimiter is a tab)");
            }
            else if (format is not null and not HttpFileFormat.Csv)
            {
                errors.Add($"file.delimiter only applies to csv, not {block["format"]}");
            }
            else if (text.Length != 1)
            {
                errors.Add($"file.delimiter must be exactly one character, got '{text}'");
            }
            else
            {
                delimiter = text[0];
            }
        }

        var header = true;
        if (Has("header"))
        {
            if (block["header"] is bool h)
            {
                header = h;
            }
            else
            {
                errors.Add($"file.header must be true or false, got '{Get("header")}'");
            }

            RequireFormat("header", format, errors, HttpFileFormat.Csv, HttpFileFormat.Tsv);
        }

        var skipLines = 0;
        if (Has("skip_lines"))
        {
            if (!int.TryParse(Get("skip_lines"), NumberStyles.None, CultureInfo.InvariantCulture, out skipLines))
            {
                errors.Add($"file.skip_lines must be a non-negative integer, got '{Get("skip_lines")}'");
            }

            RequireFormat("skip_lines", format, errors, HttpFileFormat.Csv, HttpFileFormat.Tsv);
        }

        var layout = HttpJsonLayout.Lines;
        if (Has("layout"))
        {
            switch (Get("layout"))
            {
                case "lines": break;
                case "document": layout = HttpJsonLayout.Document; break;
                case var other:
                    errors.Add($"file.layout must be lines or document, got '{other}'");
                    break;
            }

            RequireFormat("layout", format, errors, HttpFileFormat.Json);
        }

        IReadOnlyList<string>? recordPath = null;
        if (Has("records"))
        {
            recordPath = ParseRecords(Get("records")!, errors);
            RequireFormat("records", format, errors, HttpFileFormat.Xml);
        }
        else if (format == HttpFileFormat.Xml)
        {
            errors.Add("file.records is required for xml (for example /report/items/item)");
        }

        // A config with other errors is still returned (errors fail the dataset) so cross-option rules
        // in HttpDatasetConfig report in the same pass.
        if (format is null)
        {
            return null;
        }

        return new HttpFileConfig(format.Value)
        {
            Archive = archive,
            Members = members,
            IfMissingEmpty = ifMissingEmpty,
            FileColumn = fileColumn,
            Encoding = encoding,
            Delimiter = delimiter,
            Header = header,
            SkipLines = skipLines,
            JsonLayout = format == HttpFileFormat.Json ? layout : HttpJsonLayout.Lines,
            RecordPath = recordPath,
        };
    }

    private static void RequireFormat(string option, HttpFileFormat? format, List<string> errors,
        params HttpFileFormat[] allowed)
    {
        if (format is { } f && !allowed.Contains(f))
        {
            errors.Add($"file.{option} only applies to {string.Join(" and ", allowed.Select(a => a.ToString().ToLowerInvariant()))}, " +
                $"not {f.ToString().ToLowerInvariant()}");
        }
    }

    private static Encoding? ParseEncoding(string? name, List<string> errors)
    {
        switch (name?.ToLowerInvariant())
        {
            case null: return null;
            case "utf-8": return new UTF8Encoding(false);
            case "utf-16": return Encoding.Unicode;
            case "iso-8859-1": return Encoding.Latin1;
            default:
                errors.Add($"file.encoding must be utf-8, utf-16 or iso-8859-1, got '{name}'");
                return null;
        }
    }

    private static IReadOnlyList<string>? ParseRecords(string text, List<string> errors)
    {
        if (text.Length < 2 || text[0] != '/')
        {
            errors.Add($"file.records must be an absolute element path like /a/b/c, got '{text}'");
            return null;
        }

        var segments = text[1..].Split('/');
        foreach (var segment in segments)
        {
            if (segment.Length == 0 || segment.IndexOfAny(['[', '@', '*', ':']) >= 0 || !IsLocalName(segment))
            {
                errors.Add($"file.records must be plain element local names (no empty segments, " +
                    $"predicates, attributes, wildcards or prefixes), got '{text}'");
                return null;
            }
        }

        return segments;
    }

    private static bool IsLocalName(string name)
    {
        try
        {
            XmlConvert.VerifyNCName(name);
            return true;
        }
        catch (XmlException)
        {
            return false;
        }
    }
}
