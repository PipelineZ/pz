using System.Diagnostics;
using Grpc.Core;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Sdk;

namespace Pz.Connectors.Sdk.Tests;

/// <summary>Shares a collection with <see cref="ConnectorTelemetryTests"/> -- see
/// <see cref="ConnectorTelemetrySerializedCollection"/>.</summary>
[Collection("connector-telemetry-serialized")]
public sealed class TraceContextServerInterceptorTests
{
    private const string TraceParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    private static readonly ConnectorInfo Info = new("fake", "1.0.0", ProtocolVersion.Major);

    private static ConnectorTelemetry Exporting()
    {
        var telemetry = new ConnectorTelemetry(new PzConnectorHostOptions());
        telemetry.Start("http://127.0.0.1:1", Info, "");
        return telemetry;
    }

    /// <summary>A listened-to source of its own, standing in for the never-exported hosting activity
    /// ASP.NET Core makes current around every RPC. Its own name, not the SDK's: a listener registered
    /// for <c>Pz.Connector</c> matches every instance with that name process-wide, so an ambient span
    /// taken from the SDK's source would also make a "nothing is exporting" telemetry start recording.</summary>
    private static ActivitySource AmbientSource(out ActivityListener listener)
    {
        var source = new ActivitySource("pz-hosting-like-" + Guid.NewGuid().ToString("N"));
        listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == source.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
        };
        ActivitySource.AddActivityListener(listener);
        return source;
    }

    [Fact]
    public void The_rpc_span_is_named_after_the_method_and_parented_on_the_header()
    {
        using var telemetry = Exporting();
        telemetry.InstanceId = "conn-a";
        var interceptor = new TraceContextServerInterceptor(telemetry);
        var headers = new Metadata { { "traceparent", TraceParent }, { "tracestate", "v=1" } };

        using var activity = interceptor.Begin(new TestServerCallContext(
            CancellationToken.None, headers, "/pz.connector.v1.PzConnector/PlanRead"));

        Assert.NotNull(activity);
        Assert.Equal("pcp.PlanRead", activity.DisplayName);
        Assert.Equal(ActivityKind.Server, activity.Kind);
        Assert.Equal("0af7651916cd43dd8448eb211c80319c", activity.TraceId.ToHexString());
        Assert.Equal("b7ad6b7169203331", activity.ParentSpanId.ToHexString());
        Assert.Equal("v=1", activity.TraceStateString);
        Assert.Equal("conn-a", activity.GetTagItem("pz.instance"));
        Assert.Same(activity, Activity.Current);
    }

    [Fact]
    public void Without_a_header_the_span_is_a_new_root()
    {
        using var telemetry = Exporting();
        var interceptor = new TraceContextServerInterceptor(telemetry);

        using var activity = interceptor.Begin(new TestServerCallContext(
            CancellationToken.None, null, "/pz.connector.v1.PzConnector/Validate"));

        Assert.NotNull(activity);
        Assert.Equal("pcp.Validate", activity.DisplayName);
        Assert.Equal(default, activity.ParentSpanId);
        Assert.Null(activity.GetTagItem("pz.instance"));
    }

    [Fact]
    public void Without_a_header_the_span_does_not_adopt_an_ambient_activity()
    {
        // In the real process the ambient activity here is ASP.NET Core's hosting activity, which this
        // SDK never exports: adopting it would send a header-less RPC (pcp.Shutdown) out with a parent
        // id no collector can resolve. It must be a root instead, the way the Rust SDK makes it.
        using var telemetry = Exporting();
        var interceptor = new TraceContextServerInterceptor(telemetry);
        using var ambientSource = AmbientSource(out var listener);
        using var _l = listener;
        using var ambient = ambientSource.StartActivity("hosting-like");
        Assert.NotNull(ambient);

        using var activity = interceptor.Begin(new TestServerCallContext(
            CancellationToken.None, null, "/pz.connector.v1.PzConnector/Shutdown"));

        Assert.NotNull(activity);
        Assert.Null(activity.ParentId);
        Assert.Equal(default, activity.ParentSpanId);
        Assert.NotEqual(ambient.TraceId, activity.TraceId);
        Assert.Same(activity, Activity.Current);
    }

    [Fact]
    public void With_no_exporter_an_ambient_activity_survives_the_no_op()
    {
        // The no-listener path clears Activity.Current on its way to asking for a root span; nothing
        // was created, so whatever was current before must still be current after.
        using var telemetry = new ConnectorTelemetry(new PzConnectorHostOptions());
        var interceptor = new TraceContextServerInterceptor(telemetry);
        using var ambientSource = AmbientSource(out var listener);
        using var _l = listener;
        using var ambient = ambientSource.StartActivity("hosting-like");
        Assert.NotNull(ambient);

        Assert.Null(interceptor.Begin(new TestServerCallContext(
            CancellationToken.None, null, "/pz.connector.v1.PzConnector/Shutdown")));
        Assert.Same(ambient, Activity.Current);
    }

    [Fact]
    public void HostChannel_gets_no_span()
    {
        using var telemetry = Exporting();
        var interceptor = new TraceContextServerInterceptor(telemetry);

        Assert.Null(interceptor.Begin(new TestServerCallContext(
            CancellationToken.None, new Metadata { { "traceparent", TraceParent } },
            "/pz.connector.v1.PzConnector/HostChannel")));
    }

    [Fact]
    public void With_no_exporter_the_interceptor_is_a_no_op()
    {
        using var telemetry = new ConnectorTelemetry(new PzConnectorHostOptions());
        var interceptor = new TraceContextServerInterceptor(telemetry);

        Assert.Null(interceptor.Begin(new TestServerCallContext(
            CancellationToken.None, new Metadata { { "traceparent", TraceParent } },
            "/pz.connector.v1.PzConnector/PlanRead")));
        Assert.Null(Activity.Current);
    }

    [Fact]
    public async Task The_unary_handler_runs_inside_the_span_and_ends_it_afterwards()
    {
        using var telemetry = Exporting();
        var interceptor = new TraceContextServerInterceptor(telemetry);
        var context = new TestServerCallContext(
            CancellationToken.None, new Metadata { { "traceparent", TraceParent } },
            "/pz.connector.v1.PzConnector/Handshake");
        Activity? seen = null;

        var reply = await interceptor.UnaryServerHandler<string, string>("req", context, (_, _) =>
        {
            seen = Activity.Current;
            return Task.FromResult("ok");
        });

        Assert.Equal("ok", reply);
        Assert.NotNull(seen);
        Assert.Equal("pcp.Handshake", seen.DisplayName);
        Assert.NotEqual(default, seen.Duration);
        Assert.Null(Activity.Current);
    }
}
