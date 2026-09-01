using Pz.Connectors.Abstractions;

namespace Pz.Connector.Gcs.Tests;

/// <summary>Contract-level shapes: every doomed configuration fails at the earliest
/// surface that can name its actual cause, and no third-party failure crosses a session boundary
/// unwrapped or with credential material in its message.</summary>
public sealed class GcsReviewFixTests
{
    private static ConnectorConfig Hmac() => new(new Dictionary<string, object?>
    {
        ["auth"] = "hmac",
        ["key_id"] = "k",
        ["secret"] = "s",
        ["root"] = "my-bucket/out",
    });

    [Fact]
    public void Hmac_with_partition_by_fails_at_plan_time_naming_partition_by()
    {
        // Without this, the planner would route hmac+partition_by to the universal tier and the
        // run would die at execute time blaming engine.force_universal — a cause the user never set.
        var spec = new OutputSpec("lake", "daily", "replace", "strict", new Dictionary<string, object?>
        {
            ["format"] = "csv",
            ["path"] = "d={yyyy}",
            ["partition_by"] = (IReadOnlyList<string>)["ts"],
        });

        var ex = Assert.Throws<PzConnectorException>(() => new GcsSink(Hmac()).TryGetNativeCopy(spec, out _));
        Assert.False(ex.IsTransient);
        Assert.Contains("'daily'", ex.Message, StringComparison.Ordinal);
        Assert.Contains("partition_by", ex.Message, StringComparison.Ordinal);
        Assert.Contains("service_account", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Adc_resolution_failure_is_a_named_permanent_error()
    {
        // GOOGLE_APPLICATION_CREDENTIALS pointing nowhere makes ADC resolution fail
        // deterministically regardless of the machine's real gcloud state.
        var original = Environment.GetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS");
        Environment.SetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", "/nonexistent/pz-adc-test.json");
        try
        {
            var ex = Assert.Throws<PzConnectorException>(() =>
                GcsAuth.CreateStorageClient(new ConnectorConfig(new Dictionary<string, object?> { ["auth"] = "adc" })));
            Assert.False(ex.IsTransient);
            Assert.Contains("'adc'", ex.Message, StringComparison.Ordinal);
            Assert.Contains("Application Default Credentials", ex.Message, StringComparison.Ordinal);
        }
        finally
        {
            Environment.SetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", original);
        }
    }

    [Fact]
    public void Key_json_parse_failure_never_echoes_the_key_material()
    {
        // A wrong-shape-but-valid-JSON key is the case where a parser's own message can echo input
        // values verbatim; the wrap must emit a fixed message instead of the parser's.
        var ex = Assert.Throws<PzConnectorException>(() => GcsAuth.CreateStorageClient(
            new ConnectorConfig(new Dictionary<string, object?>
            {
                ["auth"] = "service_account",
                ["key_json"] = "\"SECRETMARKER_NEVER_IN_MESSAGE\"",
            })));
        Assert.False(ex.IsTransient);
        Assert.Contains("'key_json'", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("SECRETMARKER_NEVER_IN_MESSAGE", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Network_shaped_upload_failures_are_classified_not_leaked_raw()
    {
        // TimeoutException is the representative of the network-failure shapes outside the
        // GoogleApiException/HttpRequestException pair (a token-endpoint or socket-level failure
        // surfaces this way); it must cross the session boundary wrapped and transient.
        var client = new FakeStorageClient { ThrowOnUpload = new TimeoutException("timed out") };
        var sink = new GcsSink(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["auth"] = "adc",
            ["root"] = "my-bucket/out",
        }), () => client);

        var schema = new Apache.Arrow.Schema(
            [new Apache.Arrow.Field("id", Apache.Arrow.Types.Int32Type.Default, true)], null);
        var spec = new OutputSpec("lake", "daily", "replace", "strict",
            new Dictionary<string, object?> { ["format"] = "json" });
        await using var session = await sink.BeginWriteAsync(spec, schema, CancellationToken.None);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await session.CommitAsync(CancellationToken.None));
        Assert.True(ex.IsTransient);
        Assert.Contains("daily.json", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Disposing_the_sink_disposes_the_client_it_created()
    {
        var client = new FakeStorageClient();
        var sink = new GcsSink(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["auth"] = "adc",
            ["root"] = "my-bucket/out",
        }), () => client);

        var schema = new Apache.Arrow.Schema(
            [new Apache.Arrow.Field("id", Apache.Arrow.Types.Int32Type.Default, true)], null);
        var spec = new OutputSpec("lake", "daily", "replace", "strict",
            new Dictionary<string, object?> { ["format"] = "json" });
        await using (var session = await sink.BeginWriteAsync(spec, schema, CancellationToken.None))
        {
            await session.AbortAsync(CancellationToken.None);
        }

        await sink.DisposeAsync();
        Assert.True(client.Disposed);
    }
}
