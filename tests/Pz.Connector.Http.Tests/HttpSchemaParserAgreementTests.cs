using System.Text.Json;
using Json.Schema;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Http.Tests;

/// <summary>`pz validate` checks the published schemas and `pz run` the parsers; a value one accepts and the other
/// refuses passes validation and fails the run, or the other way round. For every key added to the http
/// connector's schemas these feed the same candidate values to both and require the same verdict. Quoted numbers
/// are the one deliberate gap, as for every older numeric key: the parsers read YAML scalars as text, the schema
/// types them.</summary>
public sealed class HttpSchemaParserAgreementTests
{
    private static readonly JsonSchema ConnectionSchema = JsonSchema.FromText(new HttpConnector().ConnectionConfigSchema);
    private static readonly JsonSchema DatasetSchema = JsonSchema.FromText(new HttpConnector().DatasetConfigSchema);

    private static Dictionary<string, object?> Map(params (string Key, object? Value)[] entries)
        => entries.ToDictionary(e => e.Key, e => e.Value);

    private static List<object?> List(params object?[] items) => [.. items];

    private static bool SchemaAccepts(JsonSchema schema, Dictionary<string, object?> config)
        => schema.Evaluate(JsonSerializer.SerializeToElement(config)).IsValid;

    // ---- connection: session sign-in ----

    private static Dictionary<string, object?> SessionConnection(Action<Dictionary<string, object?>> edit)
    {
        var auth = Map(("type", "session"), ("login", Map(("path", "/login"), ("body", Map(("user", "me"))))),
            ("token_pointer", "/token"), ("ttl", "30m"));
        edit(auth);
        return Map(("base_url", "https://api.example.com/"), ("auth", auth));
    }

    private static void AssertConnectionAgrees(Dictionary<string, object?> connection)
    {
        var errors = new List<string>();
        HttpConnectionConfig.Parse(new ConnectorConfig(connection), errors);
        Assert.True(errors.Count == 0 == SchemaAccepts(ConnectionSchema, connection),
            $"parser errors [{string.Join("; ", errors)}] but schema valid = {SchemaAccepts(ConnectionSchema, connection)}");
    }

    [Theory]
    [InlineData("post")] [InlineData("get")] [InlineData("POST")] [InlineData("Get")] [InlineData("put")]
    [InlineData("posts")] [InlineData("delete")]
    public void Login_method(string method)
        => AssertConnectionAgrees(SessionConnection(a =>
        {
            a["login"] = Map(("path", "/login"), ("method", method));
        }));

    [Theory]
    [InlineData("1s")] [InlineData("1000ms")] [InlineData("999ms")] [InlineData("0s")] [InlineData("0ms")]
    [InlineData("30d")] [InlineData("31d")] [InlineData("720h")] [InlineData("721h")] [InlineData("43200m")]
    [InlineData("43201m")] [InlineData("2592000s")] [InlineData("2592001s")] [InlineData("2592000000ms")]
    [InlineData("2592000001ms")] [InlineData("030m")] [InlineData("00s")] [InlineData("90s")] [InlineData("8h")]
    [InlineData("1d")] [InlineData("1.5h")] [InlineData("30M")] [InlineData("-1s")] [InlineData("30")]
    [InlineData("1h30m")] [InlineData(" 30m")] [InlineData("99999999999999999999s")] [InlineData("2591999s")]
    [InlineData("1999999999ms")] [InlineData("2499999999ms")] [InlineData("43199m")] [InlineData("719h")]
    public void Session_ttl(string ttl)
        => AssertConnectionAgrees(SessionConnection(a => a["ttl"] = ttl));

    [Theory]
    [InlineData(new long[] { 419 })] [InlineData(new long[] { 401, 440 })] [InlineData(new long[] { 400, 499 })]
    [InlineData(new long[] { 407 })] [InlineData(new long[] { 429 })] [InlineData(new long[] { 403, 429 })]
    [InlineData(new long[] { 399 })] [InlineData(new long[] { 500 })] [InlineData(new long[0])]
    public void Session_reauth_status(long[] statuses)
        => AssertConnectionAgrees(SessionConnection(a => a["reauth_status"] = statuses.Cast<object?>().ToList()));

    public static TheoryData<object?> Injects => new()
    {
        "bearer", "Bearer", "cookie",
        Map(("header", "X-Session")), Map(("header", "X-Session"), ("prefix", "Token ")), Map(("query", "sid")),
        Map(("header", "X-S"), ("query", "sid")), Map(("prefix", "Token ")), Map(("query", "sid"), ("prefix", "x")),
        Map(("header", "Content-Type")), Map(("header", "content-md5")), Map(("header", "Content-Foo")),
        Map(("header", "Expires")), Map(("header", "last-modified")), Map(("header", "Allow")),
        Map(("header", "X Session")), Map(("header", "X-Session:")), Map(("header", "Authorization")),
        Map(("cookie", "c")), Map(),
    };

    [Theory]
    [MemberData(nameof(Injects))]
    public void Session_inject(object? inject)
        => AssertConnectionAgrees(SessionConnection(a => a["inject"] = inject));

    [Theory]
    [InlineData("X-RateLimit-Reset", "seconds")] [InlineData("x-reset", "ms")] [InlineData("Reset", "epoch")]
    [InlineData("X Reset", "seconds")] [InlineData("X-Reset:", "seconds")] [InlineData("", "seconds")]
    [InlineData("X-Reset", "Seconds")] [InlineData("X-Reset", "minutes")]
    public void Retry_after_header(string header, string kind)
        => AssertConnectionAgrees(Map(("base_url", "https://api.example.com/"),
            ("retry_after", Map(("header", header), ("kind", kind)))));

    [Theory]
    [InlineData("refuse")] [InlineData("ignore")] [InlineData("Ignore")] [InlineData("keep")]
    public void Refresh_token_rotation(string rotation)
        => AssertConnectionAgrees(Map(("base_url", "https://api.example.com/"), ("auth", Map(
            ("type", "oauth2_refresh_token"), ("token_url", "https://login.example.com/token"), ("client_id", "c"),
            ("refresh_token", "r"), ("rotation", rotation)))));

    // ---- dataset ----

    private static void AssertDatasetAgrees(Dictionary<string, object?> options)
    {
        string? error = null;
        try
        {
            HttpDatasetConfig.Parse(new DatasetSpec("s", "d", options));
        }
        catch (PzConnectorException ex)
        {
            error = ex.Message;
        }

        Assert.True(error is null == SchemaAccepts(DatasetSchema, options),
            $"parser error [{error}] but schema valid = {SchemaAccepts(DatasetSchema, options)}");
    }

    private static Dictionary<string, object?> WithRule(Dictionary<string, object?> rule)
        => Map(("path", "/x"), ("on_status", List(rule)));

    public static TheoryData<object?> Statuses => new()
    {
        503L, 400L, 599L, 404L, 399L, 600L, 200L, 302L, 407L, "4xx", "5xx", "3xx", "4XX", "xx",
        List(500L, 503L), List(500L, "4xx"), List(407L), List(200L), List(), List(List(500L)), Map(("a", 1L)),
    };

    [Theory]
    [MemberData(nameof(Statuses))]
    public void On_status_status(object? status)
    {
        AssertDatasetAgrees(WithRule(Map(("status", status), ("action", "permanent"))));
        AssertDatasetAgrees(Map(("path", "/r/{{ item.id }}"), ("for_each", Map(("path", "/l"),
            ("fields", Map(("id", "/id"))), ("on_status", List(Map(("status", status), ("action", "transient"))))))));
    }

    public static TheoryData<object?, string?> SkipStatuses => new()
    {
        { 404L, null }, { List(404L, 410L), null }, { 401L, null }, { 403L, null }, { 408L, null }, { 429L, null },
        { List(404L, 429L), null }, { "4xx", null }, { List(404L, "5xx"), null },
        { 401L, "expired" }, { "4xx", "gone" }, { 429L, "quota" },
    };

    [Theory]
    [MemberData(nameof(SkipStatuses))]
    public void On_status_skip_without_body_contains_names_exact_narrow_codes(object? status, string? bodyContains)
    {
        var rule = Map(("status", status), ("action", "skip"));
        if (bodyContains is not null)
        {
            rule["body_contains"] = bodyContains;
        }

        AssertDatasetAgrees(WithRule(rule));
    }

    [Theory]
    [InlineData("transient", "30s")] [InlineData("transient", "600s")] [InlineData("transient", "601s")]
    [InlineData("transient", "10m")] [InlineData("transient", "11m")] [InlineData("transient", "1h")]
    [InlineData("transient", "600000ms")] [InlineData("transient", "600001ms")] [InlineData("transient", "1ms")]
    [InlineData("transient", "0s")] [InlineData("transient", "0ms")] [InlineData("transient", "0600s")]
    [InlineData("transient", "30")] [InlineData("transient", "1d")] [InlineData("transient", "599999ms")]
    [InlineData("transient", "99s")] [InlineData("transient", "100s")] [InlineData("transient", "5S")]
    [InlineData("permanent", "30s")] [InlineData("skip", "30s")]
    public void On_status_retry_after(string action, string retryAfter)
        => AssertDatasetAgrees(WithRule(Map(("status", 404L), ("action", action), ("retry_after", retryAfter))));

    [Theory]
    [InlineData("X-Next-Cursor")] [InlineData("link")] [InlineData("X Next")] [InlineData("X-Next:")]
    [InlineData("")]
    public void Pagination_response_header(string header)
        => AssertDatasetAgrees(Map(("path", "/x"),
            ("pagination", Map(("strategy", "cursor"), ("header", header), ("param", "c")))));

    [Theory]
    [InlineData("/jobs/{job_id}")] [InlineData("/jobs/{job_id}/cancel")] [InlineData("jobs/{job_id}")]
    [InlineData("/jobs")] [InlineData("/jobs/{job_id}/{job_id}")] [InlineData("")]
    public void Export_cancel_path(string cancelPath)
        => AssertDatasetAgrees(Export(Map(("cancel_path", cancelPath))));

    [Theory]
    [InlineData("delete", true)] [InlineData("POST", true)] [InlineData("Patch", true)] [InlineData("get", true)]
    [InlineData("delete", false)]
    public void Export_cancel_method(string method, bool withPath)
        => AssertDatasetAgrees(Export(withPath
            ? Map(("cancel_path", "/jobs/{job_id}"), ("cancel_method", method))
            : Map(("cancel_method", method))));

    private static Dictionary<string, object?> Export(Dictionary<string, object?> extra)
    {
        var export = Map(("job_id", "/id"), ("status_path", "/jobs/{job_id}"), ("status_pointer", "/state"),
            ("ready", List("done")), ("download_pointer", "/url"));
        foreach (var (k, v) in extra)
        {
            export[k] = v;
        }

        return Map(("path", "/jobs"), ("method", "post"), ("file", Map(("format", "csv"))), ("export", export));
    }

    [Theory]
    [InlineData("region")] [InlineData("_r2")] [InlineData("Region")] [InlineData("bad-name")] [InlineData("2x")]
    [InlineData("pz_x")] [InlineData("pzx")]
    public void For_each_value_keys(string key)
        => AssertDatasetAgrees(Map(("path", "/r"), ("for_each", Map(("values", List(Map((key, "eu"))))))));

    [Theory]
    [InlineData("/a")] [InlineData("/a/b~1c")] [InlineData("/")] [InlineData("//x")] [InlineData("a")]
    [InlineData("")] [InlineData("/a~2")] [InlineData("/a~")]
    public void Column_pointers(string pointer)
        => AssertDatasetAgrees(Map(("path", "/x"), ("columns", new Dictionary<string, string> { ["c"] = "varchar" }),
            ("column_pointers", Map(("c", pointer)))));

    [Theory]
    [InlineData("s")] [InlineData("ms")] [InlineData("S")] [InlineData("us")]
    public void Epoch_unit(string unit)
        => AssertDatasetAgrees(Map(("path", "/x"), ("epoch_unit", unit),
            ("columns", new Dictionary<string, string> { ["at"] = "timestamp" })));

    [Theory]
    [InlineData(true)] [InlineData(false)]
    public void Dedupe_and_stop_at_watermark_take_booleans(bool flag)
    {
        AssertDatasetAgrees(Map(("path", "/r"), ("for_each", Map(("values", List(Map(("k", "v")))), ("dedupe", flag)))));
        AssertDatasetAgrees(Map(("path", "/r"), ("for_each", Map(("values", List(Map(("k", "v")))), ("dedupe", "yes")))));
    }
}
