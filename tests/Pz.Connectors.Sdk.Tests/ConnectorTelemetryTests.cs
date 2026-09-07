using System.Diagnostics;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Sdk;

namespace Pz.Connectors.Sdk.Tests;

public sealed class ConnectorTelemetryTests
{
    private static readonly ConnectorInfo Info = new("fake", "1.2.3", ProtocolVersion.Major);

    [Fact]
    public void Without_an_endpoint_nothing_listens_and_spans_are_no_ops()
    {
        using var telemetry = new ConnectorTelemetry(new PzConnectorHostOptions());

        Assert.False(telemetry.IsExporting);
        Assert.False(telemetry.ActivitySource.HasListeners());
        Assert.Null(telemetry.ActivitySource.StartActivity("x"));
    }

    [Fact]
    public void With_an_endpoint_the_source_is_listened_to_and_spans_are_recorded()
    {
        using var telemetry = new ConnectorTelemetry(new PzConnectorHostOptions());
        telemetry.Start("http://127.0.0.1:1", Info, "run-9");

        Assert.True(telemetry.IsExporting);
        using var activity = telemetry.ActivitySource.StartActivity("x");
        Assert.NotNull(activity);
        Assert.True(activity.IsAllDataRequested);
    }

    [Fact]
    public void Extra_sources_from_options_are_subscribed()
    {
        var extra = "ext-" + Guid.NewGuid().ToString("N");
        using var telemetry = new ConnectorTelemetry(new PzConnectorHostOptions { ActivitySources = [extra] });
        telemetry.Start("http://127.0.0.1:1", Info, "");

        using var source = new ActivitySource(extra);
        Assert.NotNull(source.StartActivity("y"));
    }

    [Fact]
    public void Start_is_idempotent_and_an_unparseable_endpoint_is_ignored()
    {
        using var telemetry = new ConnectorTelemetry(new PzConnectorHostOptions());
        telemetry.Start("not a url", Info, "");
        Assert.False(telemetry.IsExporting);

        telemetry.Start("http://127.0.0.1:1", Info, "");
        telemetry.Start("http://127.0.0.1:2", Info, "");
        Assert.True(telemetry.IsExporting);
    }

    [Fact]
    public async Task Flush_against_an_unreachable_collector_returns_within_the_bound()
    {
        using var telemetry = new ConnectorTelemetry(new PzConnectorHostOptions());
        telemetry.Start("http://127.0.0.1:1", Info, "");
        using (telemetry.ActivitySource.StartActivity("pending")) { }

        // Twice the bound is the test's own deadline; the assertion is that the call completes at all
        // rather than blocking on a collector that will never answer.
        var flush = Task.Run(telemetry.FlushAndDispose);
        await flush.WaitAsync(ConnectorTelemetry.FlushBound * 2);
    }
}
