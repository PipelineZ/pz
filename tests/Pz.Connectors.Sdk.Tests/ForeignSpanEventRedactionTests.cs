using System.Diagnostics;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Pz.Connectors.Sdk.Telemetry;

namespace Pz.Connectors.Sdk.Tests;

/// <summary>Drives the real tracer pipeline (redacting processor ahead of an in-memory exporter) to prove
/// that no exception text or event attribute from a client library's span is exported.</summary>
public sealed class ForeignSpanEventRedactionTests : IDisposable
{
    private const string Secret = "secret-value";
    private const string Foreign = "Foreign.Lib.Events";
    private const string Own = "Pz.Connector.EventsTest";

    private readonly List<Activity> _exported = [];
    private readonly ActivitySource _foreign = new(Foreign);
    private readonly ActivitySource _own = new(Own);
    private readonly TracerProvider _tracer;

    public ForeignSpanEventRedactionTests()
    {
        _tracer = OpenTelemetry.Sdk.CreateTracerProviderBuilder()
            .AddSource(Foreign, Own)
            .AddProcessor(new RedactingProcessor())
            .AddInMemoryExporter(_exported)
            .Build();
    }

    public void Dispose()
    {
        _tracer.Dispose();
        _foreign.Dispose();
        _own.Dispose();
    }

    private static string Everything(Activity a)
    {
        var parts = new List<string?> { a.DisplayName, a.StatusDescription };
        parts.AddRange(a.TagObjects.Select(t => $"{t.Key}={t.Value}"));
        foreach (var e in a.Events)
        {
            parts.Add(e.Name);
            parts.AddRange(e.Tags.Select(t => $"{t.Key}={t.Value}"));
        }

        return string.Join('|', parts);
    }

    [Fact]
    public void AddException_on_a_foreign_span_drops_the_span()
    {
        using (var span = _foreign.StartActivity("op")!)
        {
            span.AddException(new InvalidOperationException($"dup key: {{ _id: \"{Secret}\" }}"));
            span.SetStatus(ActivityStatusCode.Error, $"dup key {Secret}");
        }

        Assert.DoesNotContain(Secret, string.Concat(_exported.Select(Everything)));
        Assert.Empty(_exported);
    }

    [Fact]
    public void A_foreign_event_carrying_only_the_exception_type_is_exported()
    {
        using (var span = _foreign.StartActivity("op")!)
        {
            span.AddEvent(new ActivityEvent("exception", tags: new ActivityTagsCollection { ["exception.type"] = "System.Exception" }));
        }

        Assert.Single(Assert.Single(_exported).Events);
    }

    [Fact]
    public void A_hand_added_exception_event_on_a_foreign_span_is_not_exported()
    {
        using (var span = _foreign.StartActivity("op")!)
        {
            span.AddEvent(new ActivityEvent("exception", tags: new ActivityTagsCollection { ["exception.message"] = Secret }));
        }

        Assert.Empty(_exported);
    }

    [Fact]
    public void A_foreign_event_with_arbitrary_tags_is_not_exported()
    {
        using (var span = _foreign.StartActivity("op")!)
        {
            span.AddEvent(new ActivityEvent("custom", tags: new ActivityTagsCollection { ["row"] = Secret }));
        }

        Assert.Empty(_exported);
    }

    [Fact]
    public void A_foreign_event_without_tags_is_exported()
    {
        using (var span = _foreign.StartActivity("op")!)
        {
            span.AddEvent(new ActivityEvent("cache.miss"));
        }

        Assert.Equal("cache.miss", Assert.Single(Assert.Single(_exported).Events).Name);
    }

    [Fact]
    public void A_foreign_span_without_events_is_exported_with_listed_keys_redacted()
    {
        using (var span = _foreign.StartActivity("op")!)
        {
            span.SetTag("db.statement", $"select '{Secret}'");
            span.SetTag("db.system", "mongodb");
            span.SetStatus(ActivityStatusCode.Error, Secret);
        }

        var exported = Assert.Single(_exported);
        Assert.Null(exported.GetTagItem("db.statement"));
        Assert.Equal("mongodb", exported.GetTagItem("db.system"));
        Assert.Null(exported.StatusDescription);
        Assert.Equal(ActivityStatusCode.Error, exported.Status);
    }

    [Fact]
    public void The_sdks_own_spans_keep_their_exception_events()
    {
        using (var span = _own.StartActivity("op")!)
        {
            span.AddEvent(new ActivityEvent("exception", tags: new ActivityTagsCollection { ["exception.message"] = "own-message" }));
            span.SetStatus(ActivityStatusCode.Error, "own-description");
        }

        var exported = Assert.Single(_exported);
        Assert.Contains(Assert.Single(exported.Events).Tags, t => t.Key == "exception.message" && (string?)t.Value == "own-message");
        Assert.Equal("own-description", exported.StatusDescription);
    }
}
