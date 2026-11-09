using System.Text.Json;
using Json.Schema;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Http.Tests;

public class HttpFileConfigTests
{
    private static Dictionary<string, object?> Map(params (string Key, object? Value)[] pairs)
        => pairs.ToDictionary(p => p.Key, p => p.Value);

    private static HttpDatasetConfig Parse(Dictionary<string, object?> file,
        Dictionary<string, object?>? extra = null, Dictionary<string, object?>? export = null)
    {
        var options = new Dictionary<string, object?> { ["path"] = "/f", ["file"] = file };
        if (export is not null)
        {
            options["export"] = export;
        }

        foreach (var (k, v) in extra ?? [])
        {
            options[k] = v;
        }

        return HttpDatasetConfig.Parse(new DatasetSpec("s", "d", options));
    }

    private static string Fail(Dictionary<string, object?> options)
        => Assert.Throws<PzConnectorException>(
            () => HttpDatasetConfig.Parse(new DatasetSpec("s", "d", options))).Message;

    private static string FailFile(Dictionary<string, object?> file, Dictionary<string, object?>? extra = null)
    {
        var options = new Dictionary<string, object?> { ["path"] = "/f", ["file"] = file };
        foreach (var (k, v) in extra ?? [])
        {
            options[k] = v;
        }

        return Fail(options);
    }

    private static Dictionary<string, object?> ValidExport(params (string Key, object? Value)[] overrides)
    {
        var export = Map(("job_id", "/id"), ("status_path", "/reports/{job_id}"), ("status_pointer", "/state"),
            ("ready", new List<object?> { "done" }), ("download_pointer", "/url"));
        foreach (var (k, v) in overrides)
        {
            if (v is null)
            {
                export.Remove(k);
            }
            else
            {
                export[k] = v;
            }
        }

        return export;
    }

    private static readonly Dictionary<string, object?> Contract = new()
    {
        ["columns"] = new Dictionary<string, string> { ["day"] = "date", ["n"] = "bigint" },
    };

    [Fact]
    public void Minimal_csv_file_parses()
    {
        var config = Parse(Map(("format", "csv")));

        Assert.True(config.IsFileMode);
        Assert.Null(config.Export);
        var file = config.File!;
        Assert.Equal(HttpFileFormat.Csv, file.Format);
        Assert.Equal(HttpArchive.Auto, file.Archive);
        Assert.Null(file.Members);
        Assert.False(file.IfMissingEmpty);
        Assert.Null(file.FileColumn);
        Assert.Null(file.Encoding);
        Assert.Equal(',', file.Delimiter);
        Assert.True(file.Header);
        Assert.Equal(0, file.SkipLines);
        Assert.Equal(HttpJsonLayout.Lines, file.JsonLayout);
        Assert.Null(file.RecordPath);
    }

    [Fact]
    public void A_dataset_without_file_is_not_file_mode()
        => Assert.False(HttpDatasetConfig.Parse(new DatasetSpec("s", "d", Map(("path", "/p")))).IsFileMode);

    [Fact]
    public void Format_is_required()
        => Assert.Contains("file.format is required", FailFile(Map(("archive", "none"))));

    [Fact]
    public void Encodings_map_to_their_text_encodings()
    {
        Assert.Equal(System.Text.Encoding.Unicode, Parse(Map(("format", "csv"), ("encoding", "utf-16"))).File!.Encoding);
        Assert.Equal(System.Text.Encoding.Latin1, Parse(Map(("format", "csv"), ("encoding", "ISO-8859-1"))).File!.Encoding);
        var utf8 = Parse(Map(("format", "csv"), ("encoding", "utf-8"))).File!.Encoding!;
        Assert.Equal(65001, utf8.CodePage);
        Assert.Empty(utf8.GetPreamble());
        Assert.Contains("file.encoding", FailFile(Map(("format", "csv"), ("encoding", "ebcdic"))));
    }

    [Fact]
    public void Tsv_fixes_the_delimiter()
    {
        Assert.Equal('\t', Parse(Map(("format", "tsv"))).File!.Delimiter);
        Assert.Contains("file.delimiter cannot be set for tsv",
            FailFile(Map(("format", "tsv"), ("delimiter", ";"))));
    }

    [Fact]
    public void Delimiter_must_be_one_character()
    {
        Assert.Equal(';', Parse(Map(("format", "csv"), ("delimiter", ";"))).File!.Delimiter);
        Assert.Contains("file.delimiter must be exactly one character",
            FailFile(Map(("format", "csv"), ("delimiter", ";;"))));
    }

    [Fact]
    public void Members_needs_a_zip()
    {
        Assert.Contains("file.members needs archive zip or auto",
            FailFile(Map(("format", "csv"), ("archive", "none"), ("members", "a.csv"))));
        Assert.Contains("file.members needs archive zip or auto",
            FailFile(Map(("format", "csv"), ("archive", "gzip"), ("members", "a.csv"))));
        Assert.Contains("file.if_missing needs archive zip or auto",
            FailFile(Map(("format", "csv"), ("archive", "none"), ("if_missing", "empty"))));

        var ok = Parse(Map(("format", "csv"), ("archive", "zip"), ("members", "a_*.csv"), ("if_missing", "empty"))).File!;
        Assert.Equal(HttpArchive.Zip, ok.Archive);
        Assert.Equal("a_*.csv", ok.Members);
        Assert.True(ok.IfMissingEmpty);
        Assert.Equal("m", Parse(Map(("format", "csv"), ("members", "m"))).File!.Members);
    }

    [Fact]
    public void Options_must_match_the_format()
    {
        var message = Fail(new Dictionary<string, object?>
        {
            ["path"] = "/f",
            ["items"] = "/rows",
            ["file"] = Map(("format", "json"), ("delimiter", ";"), ("records", "/a/b"), ("layout", "lines")),
        });
        Assert.Contains("file.delimiter only applies to csv", message);
        Assert.Contains("file.records only applies to xml", message);
        Assert.Contains("'items' cannot be combined with file.layout lines", message);

        Assert.Contains("file.layout only applies to json",
            FailFile(Map(("format", "xml"), ("records", "/a/b"), ("layout", "lines"))));
        Assert.Contains("file.records only applies to xml",
            FailFile(Map(("format", "csv"), ("records", "/a/b"))));
        Assert.Contains("file.header only applies to csv and tsv",
            FailFile(Map(("format", "json"), ("header", false))));
        Assert.Contains("file.skip_lines only applies to csv and tsv",
            FailFile(Map(("format", "xml"), ("records", "/a"), ("skip_lines", 2L))));
    }

    [Fact]
    public void Items_is_fine_for_a_json_document_and_ignored_when_absent()
    {
        var doc = Parse(Map(("format", "json"), ("layout", "document")),
            new() { ["items"] = "/rows" });
        Assert.Equal(HttpJsonLayout.Document, doc.File!.JsonLayout);
        Assert.Equal("/rows", doc.ItemsPointer);

        Assert.Equal(HttpJsonLayout.Lines, Parse(Map(("format", "json"))).File!.JsonLayout);
    }

    [Fact]
    public void Csv_header_and_skip_lines_parse()
    {
        var file = Parse(Map(("format", "csv"), ("header", false), ("skip_lines", 3L))).File!;
        Assert.False(file.Header);
        Assert.Equal(3, file.SkipLines);
        Assert.Contains("file.skip_lines must be a non-negative integer",
            FailFile(Map(("format", "csv"), ("skip_lines", -1L))));
    }

    [Fact]
    public void Xml_needs_records()
    {
        Assert.Contains("file.records is required for xml", FailFile(Map(("format", "xml"))));
        Assert.Equal(["a", "b"], Parse(Map(("format", "xml"), ("records", "/a/b"))).File!.RecordPath);
        Assert.Equal(["a"], Parse(Map(("format", "xml"), ("records", "/a"))).File!.RecordPath);

        foreach (var bad in new[] { "a/b", "/a[1]", "/", "/a//b", "/a/@id", "/a/*", "/a/b/", "/1a", "/x:y" })
        {
            Assert.Contains("file.records", FailFile(Map(("format", "xml"), ("records", bad))));
        }
    }

    [Fact]
    public void File_column_rules()
    {
        Assert.Contains("file.file_column is only for contract mode",
            FailFile(Map(("format", "csv"), ("file_column", "src"))));
        Assert.Contains("collides with a column",
            FailFile(Map(("format", "csv"), ("file_column", "day")), Contract));

        var extra = new Dictionary<string, object?>(Contract);
        Assert.Equal("src", Parse(Map(("format", "csv"), ("file_column", "src")), extra).File!.FileColumn);
    }

    [Fact]
    public void File_mode_conflicts()
    {
        var message = FailFile(Map(("format", "csv")), new()
        {
            ["pagination"] = Map(("strategy", "link_header")),
            ["delta_pointer"] = "/d",
            ["max_pages"] = 3L,
        });
        Assert.Contains("'pagination' cannot be combined with 'file'", message);
        Assert.Contains("'delta_pointer' cannot be combined with 'file'", message);
        Assert.Contains("'max_pages' cannot be combined with 'file'", message);
    }

    [Fact]
    public void Export_needs_file()
        => Assert.Contains("'export' requires 'file'", Fail(new Dictionary<string, object?>
        {
            ["path"] = "/f",
            ["export"] = ValidExport(),
        }));

    [Fact]
    public void Export_download_exactly_one()
    {
        var file = Map(("format", "csv"));
        Assert.Contains("exactly one of download_pointer and download_path", FailFile(file,
            new() { ["export"] = ValidExport(("download_path", "/dl/{job_id}")) }));
        Assert.Contains("needs one of download_pointer or download_path", FailFile(file,
            new() { ["export"] = ValidExport(("download_pointer", null)) }));

        var viaPath = Parse(file, export: ValidExport(("download_pointer", null),
            ("download_path", "/dl/{job_id}/x/{job_id}"))).Export!;
        Assert.Null(viaPath.DownloadPointer);
        Assert.Equal("/dl/{job_id}/x/{job_id}", viaPath.DownloadPath);
    }

    [Fact]
    public void Export_paths_and_ranges()
    {
        var file = Map(("format", "csv"));
        string Err(params (string, object?)[] o) => FailFile(file, new() { ["export"] = ValidExport(o) });

        Assert.Contains("export.status_path must start with '/'", Err(("status_path", "reports/{job_id}")));
        Assert.Contains("at most once", Err(("status_path", "/r/{job_id}/{job_id}")));
        Assert.Contains("export.download_path must start with '/'",
            Err(("download_pointer", null), ("download_path", "dl")));
        Assert.Contains("export.poll_interval_seconds", Err(("poll_interval_seconds", 0L)));
        Assert.Contains("export.poll_interval_seconds", Err(("poll_interval_seconds", 3601L)));
        Assert.Contains("export.timeout_minutes", Err(("timeout_minutes", 0L)));
        Assert.Contains("export.timeout_minutes", Err(("timeout_minutes", 1441L)));
        Assert.Contains("export.job_id is required", Err(("job_id", null)));
        Assert.Contains("export.ready is required", Err(("ready", null)));
        Assert.Contains("export.ready must not be empty", Err(("ready", new List<object?>())));
        Assert.Contains("export.ready must be a list", Err(("ready", "done")));
        Assert.Contains("not a valid JSON pointer", Err(("status_pointer", "state")));

        // A status endpoint with no {job_id} is allowed.
        var plain = Parse(file, export: ValidExport(("status_path", "/reports/status"),
            ("poll_interval_seconds", 3600L), ("timeout_minutes", 1440L))).Export!;
        Assert.Equal("/reports/status", plain.StatusPath);
        Assert.Equal(TimeSpan.FromSeconds(3600), plain.PollInterval);
        Assert.Equal(TimeSpan.FromMinutes(1440), plain.Timeout);
    }

    [Fact]
    public void Export_cancel_path_rules()
    {
        var file = Map(("format", "csv"));
        string Err(params (string, object?)[] o) => FailFile(file, new() { ["export"] = ValidExport(o) });

        Assert.Contains("export.cancel_path must start with '/'", Err(("cancel_path", "reports/{job_id}")));
        Assert.Contains("export.cancel_path must contain {job_id} exactly once",
            Err(("cancel_path", "/reports/cancel")));
        Assert.Contains("export.cancel_path must contain {job_id} exactly once",
            Err(("cancel_path", "/r/{job_id}/{job_id}")));
        Assert.Contains("export.cancel_method must be one of delete, post, put, patch, got 'get'",
            Err(("cancel_path", "/r/{job_id}"), ("cancel_method", "get")));
        Assert.Contains("export.cancel_method requires export.cancel_path", Err(("cancel_method", "post")));
        Assert.Contains("export.cancel_path must be a string, got a list",
            Err(("cancel_path", new List<object?> { "/a" })));

        // Every error at once.
        var both = Err(("cancel_path", "r"), ("timeout_minutes", 0L));
        Assert.Contains("export.cancel_path", both);
        Assert.Contains("export.timeout_minutes", both);

        var none = Parse(file, export: ValidExport()).Export!;
        Assert.Null(none.CancelPath);

        var byDefault = Parse(file, export: ValidExport(("cancel_path", "/r/{job_id}"))).Export!;
        Assert.Equal("/r/{job_id}", byDefault.CancelPath);
        Assert.Equal(HttpMethod.Delete, byDefault.CancelMethod);

        foreach (var (text, method) in new[] { ("POST", HttpMethod.Post), ("put", HttpMethod.Put),
                     ("Patch", HttpMethod.Patch), ("delete", HttpMethod.Delete) })
        {
            Assert.Equal(method, Parse(file, export: ValidExport(("cancel_path", "/r/{job_id}"),
                ("cancel_method", text))).Export!.CancelMethod);
        }
    }

    [Fact]
    public void Empty_status_path_is_a_config_error_not_a_crash()
        => Assert.Contains("export.status_path must start with '/'", FailFile(Map(("format", "csv")),
            new() { ["export"] = ValidExport(("status_path", "")) }));

    [Fact]
    public void Collections_for_scalar_keys_are_refused()
    {
        var file = Map(("format", "csv"));
        Assert.Contains("export.status_pointer must be a string, got a list", FailFile(file,
            new() { ["export"] = ValidExport(("status_pointer", new List<object?> { "/a" })) }));
        Assert.Contains("file.members must be a string, got a map", FailFile(
            Map(("format", "csv"), ("members", Map(("a", "b"))))));
    }

    [Fact]
    public void Collections_never_throw_and_report_once()
    {
        var list = new List<object?> { "x" };
        var delimiter = FailFile(Map(("format", "csv"), ("delimiter", list)));
        Assert.Contains("file.delimiter must be a string, got a list", delimiter);
        Assert.Equal(1, Count(delimiter, "file.delimiter"));

        var records = FailFile(Map(("format", "xml"), ("records", Map(("a", "b")))));
        Assert.Contains("file.records must be a string, got a map", records);
        Assert.DoesNotContain("file.records is required", records);

        Assert.Equal(1, Count(FailFile(Map(("format", list))), "file.format"));
        Assert.Equal(1, Count(FailFile(Map(("format", "csv"), ("skip_lines", list))), "file.skip_lines"));
        Assert.Equal(1, Count(FailFile(Map(("format", "csv"), ("members", list))), "file.members"));
        Assert.Equal(1, Count(FailFile(Map(("format", "json"), ("layout", list))), "file.layout"));

        var export = FailFile(Map(("format", "csv")),
            new() { ["export"] = ValidExport(("status_pointer", list), ("status_path", list), ("download_pointer", list)) });
        Assert.Equal(1, Count(export, "export.status_pointer"));
        Assert.Equal(1, Count(export, "export.status_path"));
        Assert.Equal(1, Count(export, "export.download_pointer"));
        Assert.DoesNotContain("download_path", export);
    }

    private static int Count(string text, string needle)
        => text.Split(needle).Length - 1;

    [Fact]
    public void Export_defaults()
    {
        var export = Parse(Map(("format", "csv")), export: ValidExport(("failed", null),
            ("message_pointer", "/error/message"))).Export!;

        Assert.Equal("/id", export.JobIdPointer);
        Assert.Equal("/state", export.StatusPointer);
        Assert.Equal(["done"], export.Ready);
        Assert.Empty(export.Failed);
        Assert.Equal("/error/message", export.MessagePointer);
        Assert.Equal(TimeSpan.FromSeconds(10), export.PollInterval);
        Assert.Equal(TimeSpan.FromMinutes(30), export.Timeout);
    }

    [Fact]
    public void Export_failed_list_is_read()
    {
        var export = Parse(Map(("format", "csv")),
            export: ValidExport(("failed", new List<object?> { "failed", "cancelled" }))).Export!;
        Assert.Equal(["failed", "cancelled"], export.Failed);
    }

    private static HttpConnectionConfig? ParseConnection(object? maxDownloadMb, List<string> errors)
    {
        var values = new Dictionary<string, object?> { ["base_url"] = "https://api.example.com" };
        if (maxDownloadMb is not null)
        {
            values["max_download_mb"] = maxDownloadMb;
        }

        return HttpConnectionConfig.Parse(new ConnectorConfig(values), errors);
    }

    [Fact]
    public void Max_download_mb_parses_and_bounds()
    {
        var errors = new List<string>();
        Assert.Equal(4096L * 1024 * 1024, ParseConnection(null, errors)!.MaxDownloadBytes);
        Assert.Equal(1024L * 1024, ParseConnection(1L, errors)!.MaxDownloadBytes);
        Assert.Equal(1048576L * 1024 * 1024, ParseConnection(1048576L, errors)!.MaxDownloadBytes);
        Assert.Empty(errors);

        foreach (var bad in new object[] { 0L, 1048577L, "x" })
        {
            var e = new List<string>();
            ParseConnection(bad, e);
            Assert.Contains(e, m => m.Contains("max_download_mb"));
        }
    }

    [Fact]
    public void Schemas_accept_the_new_options()
    {
        var dataset = new HttpConnector().DatasetConfigSchema;
        var connection = new HttpConnector().ConnectionConfigSchema;
        foreach (var name in new[] { "file", "export", "run_date" })
        {
            Assert.Contains($"\"{name}\"", dataset);
        }

        Assert.Contains("\"max_download_mb\"", connection);

        var schema = JsonSchema.FromText(dataset);
        bool Valid(string json) => schema.Evaluate(JsonSerializer.Deserialize<JsonElement>(json)).IsValid;

        Assert.True(Valid("""
            {"path":"/p","file":{"format":"xml","archive":"zip","members":"*.xml","if_missing":"empty",
              "file_column":"f","encoding":"utf-16","records":"/a/b"},
             "export":{"job_id":"/id","status_path":"/s/{job_id}","status_pointer":"/state","ready":["done"],
              "failed":["x"],"message_pointer":"/m","download_pointer":"/u","poll_interval_seconds":15,"timeout_minutes":60}}
            """));
        Assert.True(Valid("""{"path":"/p","file":{"format":"csv","delimiter":";","header":false,"skip_lines":2}}"""));
        Assert.True(Valid("""{"path":"/p","file":{"format":"json","layout":"document"}}"""));
        Assert.False(Valid("""{"path":"/p","file":{"format":"yaml"}}"""));
        Assert.False(Valid("""{"path":"/p","file":{"format":"csv","bogus":1}}"""));
        Assert.False(Valid("""{"path":"/p","file":{}}"""));
        Assert.False(Valid("""{"path":"/p","export":{"job_id":"/id"}}"""));
        Assert.True(Valid("""
            {"path":"/p","file":{"format":"csv"},"export":{"job_id":"/id","status_path":"/s/{job_id}",
              "status_pointer":"/state","ready":["done"],"download_pointer":"/u","cancel_path":"/s/{job_id}",
              "cancel_method":"post"}}
            """));
        Assert.False(Valid("""
            {"path":"/p","file":{"format":"csv"},"export":{"job_id":"/id","status_path":"/s/{job_id}",
              "status_pointer":"/state","ready":["done"],"download_pointer":"/u","cancel_path":"/s/{job_id}",
              "cancel_method":"get"}}
            """));
        Assert.False(Valid("""{"path":"/p","file":{"format":"csv"},"export":{"job_id":"/id","status_path":"/s","status_pointer":"/s","ready":["d"],"timeout_minutes":1441}}"""));

        var connectionSchema = JsonSchema.FromText(connection);
        bool ConnValid(string json) => connectionSchema.Evaluate(JsonSerializer.Deserialize<JsonElement>(json)).IsValid;
        Assert.True(ConnValid("""{"base_url":"https://a","max_download_mb":1048576}"""));
        Assert.False(ConnValid("""{"base_url":"https://a","max_download_mb":1048577}"""));
        Assert.False(ConnValid("""{"base_url":"https://a","max_download_mb":0}"""));
    }
}
