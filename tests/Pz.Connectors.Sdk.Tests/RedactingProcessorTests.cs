using System.Diagnostics;
using Pz.Connectors.Sdk.Telemetry;

namespace Pz.Connectors.Sdk.Tests;

public sealed class RedactingProcessorTests
{
    private static readonly string[] Keys =
    [
        "db.statement", "db.query.text", "db.query.parameter.id", "url.full", "url.query", "url.path",
        "http.url", "http.target", "messaging.message.body.size", "messaging.message.body",
        "messaging.kafka.payload_x", "exception.message",
    ];

    private static Activity Run(string sourceName)
    {
        using var source = new ActivitySource(sourceName);
        using var listener = new ActivityListener
        {
            ShouldListenTo = s => ReferenceEquals(s, source),
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
        };
        ActivitySource.AddActivityListener(listener);
        var activity = source.StartActivity("op")!;
        foreach (var key in Keys) activity.SetTag(key, "value");
        activity.SetTag("db.system", "cosmosdb");
        new RedactingProcessor().OnEnd(activity);
        return activity;
    }

    [Fact]
    public void Foreign_source_spans_lose_every_listed_key_and_keep_the_rest()
    {
        var activity = Run("Azure.Cosmos");

        foreach (var key in Keys) Assert.Null(activity.GetTagItem(key));
        Assert.Equal("cosmosdb", activity.GetTagItem("db.system"));
    }

    [Theory]
    [InlineData("Pz.Connector")]
    [InlineData("Pz.Connector.Foo")]
    public void Connector_sources_are_left_alone(string sourceName)
    {
        var activity = Run(sourceName);

        foreach (var key in Keys) Assert.Equal("value", activity.GetTagItem(key));
    }
}
