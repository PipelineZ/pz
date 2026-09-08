using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Sdk;

namespace Pz.Connectors.Sdk.Tests;

/// <summary>Shares one collection with <see cref="TraceContextServerInterceptorTests"/>: an
/// <c>ActivitySource</c> listener registered by <c>AddSource</c> matches every instance with that
/// name process-wide, not just the one it was built from, so a test here asserting "no listener" can
/// otherwise observe another class's concurrently-running exporting <c>ConnectorTelemetry</c>. See
/// <see cref="ConnectorTelemetrySerializedCollection"/>.</summary>
[Collection("connector-telemetry-serialized")]
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

    [Fact]
    public async Task Flush_against_a_collector_that_accepts_but_never_answers_is_bounded_in_aggregate()
    {
        // A listener that accepts the TCP connection but never writes an HTTP/gRPC response is the
        // case a sequential shutdown would double-charge: each provider's own send blocks for its
        // full ExportTimeout waiting on a reply that never comes. What is proven is the concurrency
        // itself, by the elapsed time: run in parallel the pair costs about one ExportTimeout (2 s),
        // in sequence about two (4 s), so an elapsed time under FlushBound (3 s) separates them. The
        // outer WaitAsync stays a hang guard, not the claim -- 6 s passes either way.
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        _ = Task.Run(async () =>
        {
            try
            {
                while (true)
                {
                    using var client = await listener.AcceptTcpClientAsync().ConfigureAwait(false);
                    await Task.Delay(Timeout.InfiniteTimeSpan).ConfigureAwait(false);
                }
            }
            catch (Exception)
            {
                // Listener stopped from the finally block below; nothing left to accept.
            }
        });

        try
        {
            var port = ((IPEndPoint)listener.LocalEndpoint).Port;
            using var telemetry = new ConnectorTelemetry(new PzConnectorHostOptions());
            telemetry.Start($"http://127.0.0.1:{port}", Info, "");
            using (telemetry.ActivitySource.StartActivity("pending")) { }

            var stopwatch = Stopwatch.StartNew();
            var flush = Task.Run(telemetry.FlushAndDispose);
            await flush.WaitAsync(ConnectorTelemetry.FlushBound * 2);
            stopwatch.Stop();

            Assert.True(
                stopwatch.Elapsed < ConnectorTelemetry.FlushBound,
                $"the two provider Shutdowns did not overlap: flush took {stopwatch.Elapsed}");
        }
        finally
        {
            listener.Stop();
        }
    }
}

/// <summary>Forces <see cref="ConnectorTelemetryTests"/> and <see cref="TraceContextServerInterceptorTests"/>
/// to run sequentially with each other -- both build real OTel providers over the one process-wide
/// <c>ActivitySource</c> name, so any assertion that nothing is listening is only true while no other
/// class's exporting <c>ConnectorTelemetry</c> is concurrently alive. One shared collection beats an
/// assembly-wide <c>[CollectionBehavior(DisableTestParallelization = true)]</c>: every other class in
/// this assembly stays parallel-eligible.</summary>
[CollectionDefinition("connector-telemetry-serialized")]
public class ConnectorTelemetrySerializedCollection;
