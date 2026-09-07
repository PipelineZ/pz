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
