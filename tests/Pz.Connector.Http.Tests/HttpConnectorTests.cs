using System.Text.Json;
using Json.Schema;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Http.Tests;

public class HttpConnectorTests
{
    private static ConnectorConfig Config(string baseUrl) => new(new Dictionary<string, object?>
    {
        ["base_url"] = baseUrl,
    });

    // Every dataset option must also be declared in DatasetConfigSchema (tier 3 of `pz validate`, the
    // JSON Schema every dataset's options are checked against): HttpDatasetConfig.cs parses and
    // validates 'cursor_order' directly, bypassing tier 3, so an option missing from the schema fails
    // `pz validate` with a schema-rejection error before ever reaching that friendlier validation.
    [Fact]
    public void Dataset_schema_accepts_a_valid_cursor_order_and_rejects_an_invalid_one()
    {
        var schema = JsonSchema.FromText(new HttpConnector().DatasetConfigSchema);

        var valid = JsonSerializer.Deserialize<JsonElement>(
            """{"path":"/items","cursor":"id","cursor_type":"bigint","cursor_order":"asc"}""");
        Assert.True(schema.Evaluate(valid).IsValid);

        var invalid = JsonSerializer.Deserialize<JsonElement>(
            """{"path":"/items","cursor":"id","cursor_type":"bigint","cursor_order":"sideways"}""");
        Assert.False(schema.Evaluate(invalid).IsValid);
    }

    [Fact]
    public async Task Validate_aggregates_all_errors()
    {
        var connector = new HttpConnector();
        var result = await connector.ValidateAsync(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["base_url"] = "not-a-url",
            ["auth"] = new Dictionary<string, object?> { ["type"] = "bearer" }, // missing token
        }), CancellationToken.None);

        Assert.False(result.IsValid);
        Assert.Equal(2, result.Errors.Count);
    }

    [Fact]
    public async Task Validate_accepts_minimal_config()
    {
        var result = await new HttpConnector().ValidateAsync(Config("https://api.example.com"),
            CancellationToken.None);
        Assert.True(result.IsValid);
    }

    [Fact]
    public async Task CheckConnection_reports_ok_and_failure_without_throwing()
    {
        await using var server = new StubHttpServer();
        server.Map("/", _ => new StubResponse(200, "{}"));
        var ok = await new HttpConnector().CheckConnectionAsync(Config(server.BaseUrl.ToString()),
            CancellationToken.None);
        Assert.True(ok.Ok);

        var refused = await new HttpConnector().CheckConnectionAsync(Config("http://127.0.0.1:1"),
            CancellationToken.None);
        Assert.False(refused.Ok);
        Assert.NotNull(refused.Message);
    }

    [Fact]
    public async Task CheckConnection_applies_configured_authenticator()
    {
        await using var server = new StubHttpServer();
        server.Map("/health", _ => new StubResponse(200, "{}"));

        var config = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["base_url"] = server.BaseUrl.ToString(),
            ["check_path"] = "/health",
            ["auth"] = new Dictionary<string, object?> { ["type"] = "bearer", ["token"] = "t-123" }
        });

        var result = await new HttpConnector().CheckConnectionAsync(config, CancellationToken.None);
        Assert.True(result.Ok);

        Assert.Single(server.Requests);
        var request = server.Requests[0];
        Assert.True(request.Headers.TryGetValue("Authorization", out var authHeader),
            "Authorization header not found in request");
        Assert.Equal("Bearer t-123", authHeader);
    }

    [Fact]
    public async Task CheckConnection_resolves_check_path_against_pathed_base_url()
    {
        // base_url carries a path prefix (no trailing slash) — check_path must resolve relative to
        // the FULL base, not root-relative to the host (which would drop 'api/v2').
        await using var server = new StubHttpServer();
        server.Map("/api/v2/health", _ => new StubResponse(200, "{}"));

        var config = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["base_url"] = server.BaseUrl + "api/v2",
            ["check_path"] = "/health",
        });

        var result = await new HttpConnector().CheckConnectionAsync(config, CancellationToken.None);

        Assert.True(result.Ok);
        Assert.Single(server.Requests);
        Assert.Equal("/api/v2/health", server.Requests[0].Url.AbsolutePath);
    }
}
