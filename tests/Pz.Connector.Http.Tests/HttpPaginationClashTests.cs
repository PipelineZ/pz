using Pz.Connector.Http;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Http.Tests;

/// <summary>Pagination that clashes with what the connection's sign-in or headers send: a page header the
/// connection also sets, and a session query token named like a pagination parameter. `pz validate` reports
/// them with the dataset (<see cref="IDatasetConfigValidator"/>), and the plan refuses them, every one at
/// once.</summary>
public sealed class HttpPaginationClashTests
{
    private static Dictionary<string, object?> Map(params (string Key, object? Value)[] entries)
        => entries.ToDictionary(e => e.Key, e => e.Value);

    private static Dictionary<string, object?> Session(Dictionary<string, object?> inject) => new()
    {
        ["type"] = "session",
        ["login"] = Map(("path", "/auth/login"), ("body", Map(("user", "me")))),
        ["token_pointer"] = "/token",
        ["ttl"] = "30m",
        ["inject"] = inject,
    };

    private static ConnectorConfig Connection(params (string Key, object? Value)[] extra)
    {
        var values = new Dictionary<string, object?> { ["base_url"] = "https://api.example.com/v1/" };
        foreach (var (k, v) in extra)
        {
            values[k] = v;
        }

        return new ConnectorConfig(values);
    }

    /// <summary>A dataset whose own pagination sends its token in <c>X-Page</c> and whose list pages by the
    /// query parameter <c>page</c>.</summary>
    private static Dictionary<string, object?> BothPaged() => Map(
        ("path", "/orders/{{ item.id }}"),
        ("pagination", Map(("strategy", "cursor"), ("pointer", "/next"), ("location", "header"), ("param", "X-Page"))),
        ("for_each", Map(("path", "/orders"), ("fields", Map(("id", "/id"))),
            ("pagination", Map(("strategy", "page"))))));

    private static async Task<ValidationResult> ValidateAsync(ConnectorConfig connection,
        Dictionary<string, object?> options)
        => await ((IDatasetConfigValidator)new HttpConnector()).ValidateDatasetAsync(connection,
            new DatasetSpec("s", "d", options), CancellationToken.None);

    [Fact]
    public async Task Validate_reports_a_page_header_clash_and_a_session_query_clash_together()
    {
        var connection = Connection(("headers", Map(("x-page", "fixed"))), ("auth", Session(Map(("query", "PAGE")))));

        var result = await ValidateAsync(connection, BothPaged());

        Assert.False(result.IsValid);
        Assert.Equal(2, result.Errors.Count);
        Assert.Contains(result.Errors, e => e.StartsWith("pagination header 'X-Page' is also set by the connection's " +
            "'headers:'", StringComparison.Ordinal));
        Assert.Contains(result.Errors, e => e.StartsWith("for_each.pagination query parameter 'page' is also the " +
            "session sign-in's query token", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Validate_reports_a_page_header_the_session_sign_in_injects()
    {
        var result = await ValidateAsync(Connection(("auth", Session(Map(("header", "X-Page"))))), BothPaged());

        var error = Assert.Single(result.Errors);
        Assert.Contains("the connection's session sign-in ('auth.inject.header')", error);
    }

    [Fact]
    public async Task Validate_passes_a_dataset_without_clashes()
    {
        var result = await ValidateAsync(Connection(("auth", Session(Map(("query", "token"))))), BothPaged());

        Assert.True(result.IsValid);
    }

    [Fact]
    public async Task Validate_leaves_an_unparseable_connection_to_the_connection_check()
    {
        // The connection's own check already reports it; the clash check adds nothing.
        Assert.True((await ValidateAsync(new ConnectorConfig(new Dictionary<string, object?>()), BothPaged())).IsValid);
    }

    [Fact]
    public async Task Validate_reports_every_refusal_of_the_dataset_parse_naming_the_option()
    {
        // The engine asks only about a schema-clean dataset, so these are the cross-field rules no schema states:
        // the plan would refuse them, and `pz validate` must too.
        var result = await ValidateAsync(Connection(),
            Map(("path", "/x"), ("stop_at_watermark", true), ("watermark_time_zone", "UTC")));

        Assert.Equal(3, result.Errors.Count);
        Assert.Contains(result.Errors, e => e.StartsWith("'stop_at_watermark' requires 'cursor_order: desc'", StringComparison.Ordinal));
        Assert.Contains(result.Errors, e => e.StartsWith("'stop_at_watermark' requires 'cursor'", StringComparison.Ordinal));
        Assert.Contains(result.Errors, e => e.StartsWith("'watermark_time_zone' needs a 'watermark_format'", StringComparison.Ordinal));
        Assert.All(result.Errors, e => Assert.DoesNotContain("http dataset", e));
    }

    [Fact]
    public async Task The_plan_refuses_every_clash_in_one_error()
    {
        var connector = new HttpConnector();
        await using var source = await connector.OpenAsync(
            Connection(("headers", Map(("X-Page", "fixed"))), ("auth", Session(Map(("query", "page"))))),
            CancellationToken.None);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await source.PlanReadAsync(
            new DatasetSpec("s", "d", BothPaged()), ReadHints.None, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.StartsWith("http source 's.d': pagination header 'X-Page' is also set by", ex.Message);
        Assert.Contains("; for_each.pagination query parameter 'page' is also the session sign-in's query token",
            ex.Message);
    }

    [Fact]
    public async Task A_connection_that_does_not_parse_is_refused_at_open_coded_config()
    {
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await new HttpConnector().OpenAsync(
            new ConnectorConfig(new Dictionary<string, object?> { ["base_url"] = "not-a-url" }), CancellationToken.None));
        Assert.Equal(PzConnectorErrorCodes.Config, ex.Code);
        Assert.False(ex.IsTransient);
    }
}
