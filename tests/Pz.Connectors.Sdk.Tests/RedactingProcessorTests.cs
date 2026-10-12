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

    [Fact]
    public void The_sdks_own_source_is_left_alone()
    {
        var activity = Run("Pz.Connector");

        foreach (var key in Keys) Assert.Equal("value", activity.GetTagItem(key));
    }

    [Fact]
    public void The_sdks_own_spans_keep_their_exception_events_and_name()
    {
        using var source = new ActivitySource("Pz.Connector");
        using var listener = new ActivityListener
        {
            ShouldListenTo = s => ReferenceEquals(s, source),
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
        };
        ActivitySource.AddActivityListener(listener);
        var activity = source.StartActivity("own-op")!;
        activity.AddEvent(new ActivityEvent("exception", tags: new ActivityTagsCollection { ["exception.message"] = "own-message" }));
        activity.SetStatus(ActivityStatusCode.Error, "own-description");
        new RedactingProcessor().OnEnd(activity);

        Assert.Contains(Assert.Single(activity.Events).Tags, t => t.Key == "exception.message");
        Assert.Equal("own-description", activity.StatusDescription);
        Assert.Equal("own-op", activity.DisplayName);
        Assert.True(activity.Recorded);
    }
}
