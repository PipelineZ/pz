using System.Net;
using System.Text.Json;
using Json.Schema;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Http.Tests;

/// <summary>Parsing and validation of `on_status` rules and the connection's `retry_after`.</summary>
public class HttpStatusRulesConfigTests
{
    private static Dictionary<string, object?> Rule(params (string Key, object? Value)[] entries)
        => entries.ToDictionary(e => e.Key, e => e.Value);

    private static Dictionary<string, object?> Options(object? onStatus, params (string Key, object? Value)[] extra)
    {
        var options = new Dictionary<string, object?> { ["path"] = "/r", ["on_status"] = onStatus };
        foreach (var (k, v) in extra)
        {
            options[k] = v;
        }

        return options;
    }

    private static HttpDatasetConfig Parse(Dictionary<string, object?> options, string? cursor = null)
        => HttpDatasetConfig.Parse(new DatasetSpec("s", "d", options) { WatermarkCursor = cursor });

    private static string Message(Dictionary<string, object?> options, string? cursor = null)
        => Assert.Throws<PzConnectorException>(() => Parse(options, cursor)).Message;

    [Fact]
    public void No_rules_leaves_OnStatus_null()
    {
        Assert.Null(HttpDatasetConfig.Parse(new DatasetSpec("s", "d", new Dictionary<string, object?> { ["path"] = "/r" })).OnStatus);
    }

    [Fact]
    public void Rules_parse_in_order_with_every_status_form()
    {
        var config = Parse(Options(new List<object?>
        {
            Rule(("status", 403), ("body_contains", "secondary rate limit"), ("action", "transient"),
                ("retry_after", "60s")),
            Rule(("status", new List<object?> { 400, "422", "5xx" }), ("body_contains", "no data"), ("action", "skip")),
            Rule(("status", "4xx"), ("action", "permanent")),
        }));

        var rules = config.OnStatus!;
        Assert.Equal(3, rules.Count);
        Assert.Equal("on_status[0]", rules[0].Label);
        Assert.Equal(StatusAction.Transient, rules[0].Action);
        Assert.Equal(TimeSpan.FromSeconds(60), rules[0].RetryAfter);
        Assert.Equal("secondary rate limit", rules[0].BodyContains);
        Assert.True(rules[1].Matches(422, "no data"));
        Assert.True(rules[1].Matches(504, "no data"));
        Assert.False(rules[1].Matches(404, "no data"));
        Assert.True(rules[2].Matches(418, ""));
        Assert.True(config.HasSkipRule);
    }

    [Fact]
    public void Body_contains_matches_ignoring_case()
    {
        var rule = Parse(Options(new List<object?>
        {
            Rule(("status", 403), ("body_contains", "Rate Limit"), ("action", "transient")),
        })).OnStatus![0];

        Assert.True(rule.Matches(403, """{"message":"You have exceeded a secondary RATE LIMIT."}"""));
        Assert.False(rule.Matches(403, """{"message":"Resource not accessible"}"""));
        Assert.False(rule.Matches(429, "rate limit"));
    }

    [Fact]
    public void First_matching_rule_wins()
    {
        var rules = Parse(Options(new List<object?>
        {
            Rule(("status", 400), ("body_contains", "try later"), ("action", "transient")),
            Rule(("status", "4xx"), ("action", "permanent")),
        })).OnStatus;

        Assert.Equal(StatusAction.Transient, HttpStatusRule.FirstMatch(rules, 400, "please TRY LATER")!.Action);
        Assert.Equal(StatusAction.Permanent, HttpStatusRule.FirstMatch(rules, 400, "bad request")!.Action);
        Assert.Null(HttpStatusRule.FirstMatch(rules, 500, "try later"));
    }

    [Fact]
    public void Every_rule_error_is_reported_at_once()
    {
        var message = Message(Options(new List<object?>
        {
            Rule(("status", 200), ("action", "transient")),
            Rule(("status", 407), ("action", "permanent")),
            Rule(("status", "6xx"), ("action", "retry")),
            Rule(("action", "skip"), ("body_contains", "")),
            Rule(("status", 429), ("action", "permanent"), ("retry_after", "5s")),
            Rule(("status", 429), ("action", "transient"), ("retry_after", "11m")),
            Rule(("status", 429), ("action", "transient"), ("retry_after", "0s")),
            Rule(("status", 429), ("action", "transient"), ("when", "always")),
            Rule(("status", new List<object?>()), ("action", "transient")),
            "not a map",
        }));

        Assert.Contains("on_status[0].status '200' is not a status from 400 to 599", message);
        Assert.Contains("on_status[1].status 407 cannot be overridden", message);
        Assert.Contains("on_status[2].status '6xx' is not a status", message);
        Assert.Contains("on_status[2].action must be transient, permanent or skip, got 'retry'", message);
        Assert.Contains("on_status[3].status is required", message);
        Assert.Contains("on_status[3].body_contains must be a non-empty string", message);
        Assert.Contains("on_status[4].retry_after only applies to action: transient", message);
        Assert.Contains("on_status[5].retry_after must be a duration above 0 and at most 600s", message);
        Assert.Contains("on_status[6].retry_after must be a duration", message);
        Assert.Contains("on_status[7]: unknown key 'when'", message);
        Assert.Contains("on_status[8].status must not be an empty list", message);
        Assert.Contains("on_status[9] must be a map", message);
    }

    [Fact]
    public void On_status_must_be_a_non_empty_list()
    {
        Assert.Contains("'on_status' must be a list of rules", Message(Options(Rule(("status", 400)))));
        Assert.Contains("'on_status' must be a list of rules", Message(Options("skip")));
        Assert.Contains("'on_status' must list at least one rule", Message(Options(new List<object?>())));
    }

    [Fact]
    public void Missing_action_is_named()
    {
        Assert.Contains("on_status[0].action is required (transient, permanent or skip)",
            Message(Options(new List<object?> { Rule(("status", 400)) })));
    }

    [Theory]
    [InlineData("500ms", 500)]
    [InlineData("30s", 30_000)]
    [InlineData("10m", 600_000)]
    public void Retry_after_takes_the_pz_duration_grammar(string text, int ms)
    {
        var rule = Parse(Options(new List<object?>
        {
            Rule(("status", 403), ("action", "transient"), ("retry_after", text)),
        })).OnStatus![0];

        Assert.Equal(TimeSpan.FromMilliseconds(ms), rule.RetryAfter);
    }

    [Theory]
    [InlineData("60")]
    [InlineData("1.5s")]
    [InlineData("-1s")]
    [InlineData("1m30s")]
    [InlineData("30S")]
    public void Retry_after_refuses_other_forms(string text)
    {
        Assert.Contains("on_status[0].retry_after must be a duration", Message(Options(new List<object?>
        {
            Rule(("status", 403), ("action", "transient"), ("retry_after", text)),
        })));
    }

    [Fact]
    public void Skip_is_refused_on_incremental_datasets()
    {
        var skip = new List<object?> { Rule(("status", 400), ("action", "skip")) };
        const string refused = "on_status[0]: action 'skip' is not allowed on a dataset that is or could be incremental";

        // An engine cursor, a raw cursor option, a watermark reference, a window_upper reference.
        Assert.Contains(refused, Message(Options(skip), cursor: "updated_at"));
        Assert.Contains(refused, Message(Options(skip, ("cursor", "id"), ("cursor_type", "bigint"))));
        Assert.Contains(refused, Message(Options(skip,
            ("query", new Dictionary<string, object?> { ["since"] = "{{ watermark }}" }))));
        Assert.Contains(refused, Message(Options(skip,
            ("query", new Dictionary<string, object?> { ["until"] = "{{ window_upper }}" }))));

        // Transient and permanent rules stay allowed there.
        Assert.NotNull(Parse(Options(new List<object?> { Rule(("status", 400), ("action", "permanent")) }),
            cursor: "updated_at").OnStatus);
    }

    [Fact]
    public void Skip_on_a_non_incremental_dataset_is_allowed()
    {
        var config = Parse(Options(new List<object?> { Rule(("status", 400), ("action", "skip")) },
            ("query", new Dictionary<string, object?> { ["day"] = "{{ run_date }}" })));

        Assert.True(config.HasSkipRule);
    }

    [Fact]
    public void Skip_is_refused_with_export()
    {
        var message = Message(Options(new List<object?> { Rule(("status", 400), ("action", "skip")) },
            ("file", new Dictionary<string, object?> { ["format"] = "csv" }),
            ("export", new Dictionary<string, object?>
            {
                ["job_id"] = "/id", ["status_path"] = "/jobs/{job_id}", ["status_pointer"] = "/state",
                ["ready"] = new List<object?> { "done" }, ["download_pointer"] = "/url",
            })));

        Assert.Contains("on_status[0]: action 'skip' cannot be combined with 'export'", message);
    }

    [Fact]
    public void For_each_list_gets_its_own_rules_without_inheritance()
    {
        var config = Parse(new Dictionary<string, object?>
        {
            ["path"] = "/orders/{{ item.id }}",
            ["on_status"] = new List<object?> { Rule(("status", 404), ("action", "skip")) },
            ["for_each"] = new Dictionary<string, object?>
            {
                ["path"] = "/orders",
                ["fields"] = new Dictionary<string, object?> { ["id"] = "/id" },
                ["on_status"] = new List<object?> { Rule(("status", 403), ("action", "transient")) },
            },
        });

        Assert.Equal("on_status[0]", Assert.Single(config.OnStatus!).Label);
        var listRule = Assert.Single(config.ForEach!.List.OnStatus!);
        Assert.Equal("for_each.on_status[0]", listRule.Label);
        Assert.Equal(StatusAction.Transient, listRule.Action);
    }

    [Fact]
    public void For_each_list_rule_errors_are_prefixed_and_list_skip_is_refused_on_incrementals()
    {
        var message = Message(new Dictionary<string, object?>
        {
            ["path"] = "/orders/{{ item.id }}",
            ["for_each"] = new Dictionary<string, object?>
            {
                ["path"] = "/orders",
                ["fields"] = new Dictionary<string, object?> { ["id"] = "/id" },
                ["on_status"] = new List<object?>
                {
                    Rule(("status", 404), ("action", "skip")),
                    Rule(("status", 99), ("action", "permanent")),
                },
            },
        }, cursor: "updated_at");

        Assert.Contains("for_each: on_status[1].status '99' is not a status", message);
        Assert.Contains("for_each.on_status[0]: action 'skip' is not allowed on a dataset that is or could be incremental", message);
    }

    // --- connection retry_after ---

    private static (HttpConnectionConfig? Config, List<string> Errors) Connection(object? retryAfter)
    {
        var errors = new List<string>();
        var config = HttpConnectionConfig.Parse(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["base_url"] = "https://api.example.com",
            ["retry_after"] = retryAfter,
        }), errors);
        return (config, errors);
    }

    [Fact]
    public void Connection_retry_after_parses()
    {
        var (config, errors) = Connection(new Dictionary<string, object?>
            { ["header"] = "X-RateLimit-Reset", ["kind"] = "epoch" });

        Assert.Empty(errors);
        Assert.Equal(new HttpRetryAfterHeader("X-RateLimit-Reset", RetryAfterKind.Epoch), config!.RetryAfter);
        Assert.Null(Connection(null).Config!.RetryAfter);
    }

    [Fact]
    public void Connection_retry_after_errors_are_all_reported()
    {
        var (_, errors) = Connection(new Dictionary<string, object?>
            { ["header"] = "X Reset", ["kind"] = "minutes", ["cap"] = 5 });

        Assert.Contains("retry_after: unknown key 'cap' (accepted: header, kind)", errors);
        Assert.Contains("retry_after.header must be a header name, got 'X Reset'", errors);
        Assert.Contains("retry_after.kind must be seconds, ms or epoch, got 'minutes'", errors);
        Assert.Contains("retry_after.kind is required (seconds, ms or epoch)",
            Connection(new Dictionary<string, object?> { ["header"] = "X" }).Errors);
        Assert.Contains("'retry_after' must be a map { header, kind } (kind: seconds, ms or epoch)",
            Connection("X-Reset").Errors);
    }

    private static HttpResponseMessage WithHeader(string name, string value)
    {
        var response = new HttpResponseMessage(HttpStatusCode.TooManyRequests);
        response.Headers.TryAddWithoutValidation(name, value);
        return response;
    }

    [Theory]
    [InlineData("seconds", "12", 12_000)]
    [InlineData("seconds", "1.5", 1_500)]
    [InlineData("ms", "2500", 2_500)]
    [InlineData("epoch", "1000045", 45_000)]
    [InlineData("epoch", "999990", 0)]
    [InlineData("seconds", "0", 0)]
    [InlineData("seconds", "86400", 600_000)]
    [InlineData("ms", "900000", 600_000)]
    [InlineData("epoch", "99999999999", 600_000)]
    public void Configured_header_reads_each_kind_capped_at_600s(string kindText, string value, int ms)
    {
        var kind = kindText switch { "seconds" => RetryAfterKind.Seconds, "ms" => RetryAfterKind.Milliseconds, _ => RetryAfterKind.Epoch };
        var now = DateTimeOffset.FromUnixTimeSeconds(1_000_000);
        using var response = WithHeader("X-Wait", value);

        var wait = new HttpRetryAfterHeader("X-Wait", kind).Read(response, now, out var note);

        Assert.Equal(TimeSpan.FromMilliseconds(ms), wait);
        Assert.Equal("", note);
    }

    [Theory]
    [InlineData("soon")]
    [InlineData("-5")]
    [InlineData("1e3")]
    public void Unreadable_configured_header_is_ignored_with_a_note(string value)
    {
        using var response = WithHeader("X-Wait", value);

        var wait = new HttpRetryAfterHeader("X-Wait", RetryAfterKind.Seconds).Read(response, DateTimeOffset.UnixEpoch,
            out var note);

        Assert.Null(wait);
        Assert.Equal(" (retry_after header 'X-Wait' value is not a non-negative number; the engine's backoff applies)",
            note);
    }

    [Fact]
    public void Absent_configured_header_says_nothing()
    {
        using var response = new HttpResponseMessage(HttpStatusCode.TooManyRequests);

        Assert.Null(new HttpRetryAfterHeader("X-Wait", RetryAfterKind.Seconds).Read(response, DateTimeOffset.UnixEpoch,
            out var note));
        Assert.Equal("", note);
    }

    [Fact]
    public void Schemas_carry_on_status_and_retry_after()
    {
        var connector = new HttpConnector();
        var dataset = JsonSchema.FromText(connector.DatasetConfigSchema);
        var connection = JsonSchema.FromText(connector.ConnectionConfigSchema);

        Assert.True(dataset.Evaluate(JsonSerializer.Deserialize<JsonElement>("""
            {"path":"/r","on_status":[{"status":403,"body_contains":"limit","action":"transient","retry_after":"60s"},
             {"status":[400,"5xx"],"action":"skip"}],
             "for_each":{"path":"/o","fields":{"id":"/id"},"on_status":[{"status":"4xx","action":"permanent"}]}}
            """)).IsValid);
        Assert.False(dataset.Evaluate(JsonSerializer.Deserialize<JsonElement>(
            """{"path":"/r","on_status":[{"status":403,"action":"ignore"}]}""")).IsValid);
        Assert.False(dataset.Evaluate(JsonSerializer.Deserialize<JsonElement>(
            """{"path":"/r","on_status":[{"action":"skip"}]}""")).IsValid);
        Assert.True(connection.Evaluate(JsonSerializer.Deserialize<JsonElement>(
            """{"base_url":"https://x","retry_after":{"header":"X-RateLimit-Reset","kind":"epoch"}}""")).IsValid);
        Assert.False(connection.Evaluate(JsonSerializer.Deserialize<JsonElement>(
            """{"base_url":"https://x","retry_after":{"header":"X-RateLimit-Reset"}}""")).IsValid);
    }

    // --- fix round: validation gaps ---

    [Fact]
    public void Skip_is_refused_with_a_columns_contract_because_a_SQL_declared_incremental_is_invisible()
    {
        // A SQL-declared incremental reaches the connector with no WatermarkCursor on its first run and
        // under --full-refresh, so a contract (whose columns can be its cursor) counts as possibly incremental.
        var message = Message(Options(new List<object?> { Rule(("status", 400), ("action", "skip")) },
            ("columns", new Dictionary<string, string> { ["id"] = "bigint", ["updated_at"] = "timestamp" })));

        Assert.Contains("on_status[0]: action 'skip' is not allowed on a dataset that is or could be incremental",
            message);
        Assert.Contains("a 'columns:' contract can carry the cursor of a SQL-declared incremental", message);
    }

    [Fact]
    public void Skip_refusal_is_reported_together_with_the_rules_other_errors()
    {
        var message = Message(Options(new List<object?>
        {
            Rule(("status", 400), ("action", "skip"), ("retry_after", "5s")),
        }), cursor: "updated_at");

        Assert.Contains("on_status[0].retry_after only applies to action: transient", message);
        Assert.Contains("on_status[0]: action 'skip' is not allowed on a dataset that is or could be incremental",
            message);
    }

    [Fact]
    public void List_skip_refusal_is_reported_together_with_a_sibling_error()
    {
        var message = Message(new Dictionary<string, object?>
        {
            ["path"] = "/orders/{{ item.id }}",
            ["for_each"] = new Dictionary<string, object?>
            {
                ["path"] = "/orders",
                ["fields"] = new Dictionary<string, object?> { ["id"] = "/id" },
                ["on_status"] = new List<object?>
                {
                    Rule(("status", 404), ("action", "skip"), ("body_contains", "")),
                    Rule(("status", 99), ("action", "permanent")),
                },
            },
        }, cursor: "updated_at");

        Assert.Contains("for_each: on_status[0].body_contains must be a non-empty string", message);
        Assert.Contains("for_each: on_status[1].status '99' is not a status", message);
        Assert.Contains("for_each.on_status[0]: action 'skip' is not allowed on a dataset that is or could be " +
            "incremental", message);
    }

    [Theory]
    [InlineData("4xx")]
    [InlineData("5xx")]
    [InlineData(401)]
    [InlineData(403)]
    [InlineData(408)]
    [InlineData(429)]
    public void Broad_skip_without_body_contains_is_refused(object status)
    {
        var message = Message(Options(new List<object?>
        {
            Rule(("status", new List<object?> { 400, status }), ("action", "skip")),
        }));

        Assert.Contains("on_status[0]: action 'skip' without body_contains must name exact status codes, not " +
            "'4xx'/'5xx', and none of 401, 403, 408, 429", message);
    }

    [Theory]
    [InlineData("4xx")]
    [InlineData(403)]
    [InlineData(429)]
    public void Broad_skip_with_body_contains_is_allowed(object status)
    {
        var config = Parse(Options(new List<object?>
        {
            Rule(("status", status), ("body_contains", "no data for period"), ("action", "skip")),
        }));

        Assert.True(config.HasSkipRule);
    }

    [Fact]
    public void Broad_transient_and_permanent_rules_stay_allowed()
    {
        Assert.Equal(2, Parse(Options(new List<object?>
        {
            Rule(("status", "4xx"), ("action", "permanent")),
            Rule(("status", 403), ("action", "transient")),
        })).OnStatus!.Count);
    }
}
