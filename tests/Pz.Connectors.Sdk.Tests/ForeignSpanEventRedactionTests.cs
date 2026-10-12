using System.Diagnostics;
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using Pz.Connectors.Sdk.Telemetry;

namespace Pz.Connectors.Sdk.Tests;

/// <summary>Drives the real tracer pipeline (redacting processor ahead of an in-memory exporter) to prove
/// that no exception text or event attribute from a client library's span is exported.</summary>
public sealed class ForeignSpanEventRedactionTests : IDisposable
{
    private const string Secret = "secret-value";
    private const string Foreign = "Foreign.Lib.Events";

    private readonly List<Activity> _exported = [];
    private readonly ActivitySource _foreign = new(Foreign);
    private readonly TracerProvider _tracer;

    public ForeignSpanEventRedactionTests()
    {
        _tracer = OpenTelemetry.Sdk.CreateTracerProviderBuilder()
            .AddSource(Foreign)
            .AddProcessor(new RedactingProcessor())
            .AddInMemoryExporter(_exported)
            .Build();
    }

    public void Dispose()
    {
        _tracer.Dispose();
        _foreign.Dispose();
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
    public void A_foreign_event_with_a_free_text_name_is_not_exported()
    {
        using (var span = _foreign.StartActivity("op")!)
        {
            span.AddEvent(new ActivityEvent($"fetched {Secret}"));
        }

        Assert.Empty(_exported);
    }

    [Fact]
    public void A_foreign_span_name_never_reaches_the_export()
    {
        using (_foreign.StartActivity("GET /idx/_doc/secret-id")!)
        {
        }

        var exported = Assert.Single(_exported);
        Assert.DoesNotContain("secret-id", exported.DisplayName);
        Assert.Equal(Foreign, exported.DisplayName);
    }

    [Fact]
    public void A_foreign_span_name_is_the_source_and_operation()
    {
        using (var span = _foreign.StartActivity("find customers-acme")!)
        {
            span.SetTag("db.operation.name", "find");
            span.SetTag("http.request.method", "POST");
        }

        Assert.Equal($"{Foreign} find", Assert.Single(_exported).DisplayName);
    }

    [Fact]
    public void A_foreign_span_name_is_capped()
    {
        using (var span = _foreign.StartActivity("op")!)
        {
            span.SetTag("rpc.method", new string('x', 500));
        }

        Assert.Equal(RedactingProcessor.MaxNameLength, Assert.Single(_exported).DisplayName.Length);
    }

    [Fact]
    public void A_source_merely_prefixed_with_the_sdk_name_is_filtered()
    {
        const string Prefixed = "Pz.Connector.Foo";
        using var source = new ActivitySource(Prefixed);
        using var tracer = OpenTelemetry.Sdk.CreateTracerProviderBuilder()
            .AddSource(Prefixed)
            .AddProcessor(new RedactingProcessor())
            .AddInMemoryExporter(_exported)
            .Build();

        using (var span = source.StartActivity("GET /secret-id")!)
        {
            span.SetTag("db.statement", Secret);
        }

        var exported = Assert.Single(_exported);
        Assert.Null(exported.GetTagItem("db.statement"));
        Assert.DoesNotContain("secret-id", exported.DisplayName);
        Assert.True(RedactingProcessor.IsForeign(Prefixed));
        Assert.False(RedactingProcessor.IsForeign("Pz.Connector"));
    }

    [Fact]
    public void A_rejected_endpoint_is_described_without_userinfo_path_or_query()
    {
        var described = ConnectorTelemetry.Describe("ftp://user:pw@collector.example:2121/v1/traces?token=abc");

        Assert.Contains("collector.example", described);
        Assert.Contains("2121", described);
        Assert.DoesNotContain("pw", described);
        Assert.DoesNotContain("user", described);
        Assert.DoesNotContain("abc", described);
        Assert.DoesNotContain("traces", described);
        Assert.Equal("", ConnectorTelemetry.Describe("not a url token=abc"));
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
    public void Stacktrace_and_status_description_tags_on_a_foreign_span_are_stripped()
    {
        using (var span = _foreign.StartActivity("op")!)
        {
            span.SetTag("exception.stacktrace", Secret);
            span.SetTag("otel.status_description", Secret);
            span.SetTag("db.system", "mongodb");
        }

        var exported = Assert.Single(_exported);
        Assert.Null(exported.GetTagItem("exception.stacktrace"));
        Assert.Null(exported.GetTagItem("otel.status_description"));
        Assert.Equal("mongodb", exported.GetTagItem("db.system"));
    }

    [Fact]
    public void Only_allowlisted_tags_survive_on_a_foreign_span()
    {
        string[] allowed = ["db.namespace", "db.collection.name", "server.address", "db.operation.name", "db.system"];
        string[] denied =
        [
            "db.mongodb.lsid", "db.elasticsearch.path_parts.index", "azure.cosmosdb.client.id",
            "db.query.summary", "db.operation.parameter.id", "http.request.header.authorization",
            "error.message", "exception.message", "custom.key",
        ];
        using (var span = _foreign.StartActivity("op")!)
        {
            foreach (var key in allowed.Concat(denied))
            {
                span.SetTag(key, Secret);
            }
        }

        var exported = Assert.Single(_exported);
        Assert.Equal(allowed.Order(), exported.TagObjects.Select(t => t.Key).Order());
    }

    [Fact]
    public void Foreign_meter_instruments_keep_only_allowlisted_tag_keys()
    {
        var points = new List<Metric>();
        using var foreignMeter = new System.Diagnostics.Metrics.Meter("Foreign.Lib.Meter");
        var counter = foreignMeter.CreateCounter<long>("calls");
        using var provider = OpenTelemetry.Sdk.CreateMeterProviderBuilder()
            .AddMeter("Foreign.Lib.Meter")
            .AddView(instrument => RedactingProcessor.IsForeign(instrument.Meter.Name)
                ? new MetricStreamConfiguration { TagKeys = RedactingProcessor.SafeKeys.ToArray() }
                : null)
            .AddInMemoryExporter(points)
            .Build();

        counter.Add(1, new KeyValuePair<string, object?>("db.system", "mongodb"), new KeyValuePair<string, object?>("db.statement", Secret));
        provider.ForceFlush();

        var metric = Assert.Single(points);
        foreach (ref readonly var point in metric.GetMetricPoints())
        {
            var keys = new List<string>();
            foreach (var tag in point.Tags)
            {
                keys.Add(tag.Key);
            }

            Assert.Equal(["db.system"], keys);
        }
    }

    private static ActivityContext LinkTarget() =>
        new(ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded);

    [Fact]
    public void A_foreign_span_with_a_link_carrying_tags_is_not_exported()
    {
        var link = new ActivityLink(LinkTarget(), new ActivityTagsCollection { ["row"] = Secret });
        using (_foreign.StartActivity("op", ActivityKind.Internal, default(ActivityContext), links: [link])!)
        {
        }

        Assert.Empty(_exported);
    }

    [Fact]
    public void A_foreign_span_with_a_link_without_tags_is_exported()
    {
        var link = new ActivityLink(LinkTarget());
        using (_foreign.StartActivity("op", ActivityKind.Internal, default(ActivityContext), links: [link])!)
        {
        }

        Assert.Single(_exported);
    }

    [Fact]
    public void Dropping_holds_on_the_batch_export_path()
    {
        var batched = new List<Activity>();
        using var tracer = OpenTelemetry.Sdk.CreateTracerProviderBuilder()
            .AddSource(Foreign)
            .AddProcessor(new RedactingProcessor())
            .AddProcessor(new BatchActivityExportProcessor(new InMemoryExporter<Activity>(batched)))
            .Build();

        using (var bad = _foreign.StartActivity("bad")!)
        {
            bad.AddException(new InvalidOperationException(Secret));
        }

        using (_foreign.StartActivity("clean")!)
        {
        }

        Assert.True(tracer.ForceFlush());
        Assert.Equal(Foreign, Assert.Single(batched).DisplayName);
        Assert.DoesNotContain(Secret, string.Concat(batched.Select(Everything)));
    }
}
