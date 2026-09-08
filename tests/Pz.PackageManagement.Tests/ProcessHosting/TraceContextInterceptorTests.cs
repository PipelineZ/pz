using System.Diagnostics;
using System.Text;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Pz.PackageManagement.ProcessHosting;

namespace Pz.PackageManagement.Tests.ProcessHosting;

/// <summary>The host-side half of trace propagation: a W3C traceparent is injected into call
/// metadata exactly when an Activity is current, and never otherwise (which is what keeps the
/// engine's no-listener zero-cost path header-free).</summary>
public sealed class TraceContextInterceptorTests
{
    private static readonly Method<string, string> Echo = new(
        MethodType.Unary, "test", "Echo",
        Marshallers.Create<string>(s => Encoding.UTF8.GetBytes(s), b => Encoding.UTF8.GetString(b)),
        Marshallers.Create<string>(s => Encoding.UTF8.GetBytes(s), b => Encoding.UTF8.GetString(b)));

    private sealed class RecordingInvoker : CallInvoker
    {
        public Metadata? Seen { get; private set; }

        public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
        {
            Seen = options.Headers;
            return new AsyncUnaryCall<TResponse>(
                Task.FromResult<TResponse>(null!), Task.FromResult(new Metadata()),
                () => Status.DefaultSuccess, () => [], () => { });
        }

        public override TResponse BlockingUnaryCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
            throw new NotSupportedException();

        public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options) =>
            throw new NotSupportedException();

        public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
            throw new NotSupportedException();

        public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options) =>
            throw new NotSupportedException();
    }

    [Fact]
    public async Task With_a_current_activity_traceparent_is_injected()
    {
        var sourceName = "pz-test-" + Guid.NewGuid().ToString("N");
        using var listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == sourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
        };
        ActivitySource.AddActivityListener(listener);
        using var source = new ActivitySource(sourceName);

        var invoker = new RecordingInvoker();
        var intercepted = invoker.Intercept(new TraceContextInterceptor());

        using (var activity = source.StartActivity("root"))
        {
            Assert.NotNull(activity);
            activity.TraceStateString = "vendor=1";
            await intercepted.AsyncUnaryCall(Echo, null, new CallOptions(), "hi");

            Assert.NotNull(invoker.Seen);
            Assert.Equal(activity.Id, invoker.Seen.GetValue("traceparent"));
            Assert.Equal("vendor=1", invoker.Seen.GetValue("tracestate"));
        }
    }

    [Fact]
    public async Task Without_a_current_activity_no_header_is_added()
    {
        Assert.Null(Activity.Current);
        var invoker = new RecordingInvoker();
        var intercepted = invoker.Intercept(new TraceContextInterceptor());

        await intercepted.AsyncUnaryCall(Echo, null, new CallOptions(), "hi");

        Assert.Null(invoker.Seen);
    }

    [Fact]
    public async Task Existing_headers_are_preserved()
    {
        var sourceName = "pz-test-" + Guid.NewGuid().ToString("N");
        using var listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == sourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
        };
        ActivitySource.AddActivityListener(listener);
        using var source = new ActivitySource(sourceName);
        var invoker = new RecordingInvoker();
        var intercepted = invoker.Intercept(new TraceContextInterceptor());

        using var activity = source.StartActivity("root");
        var headers = new Metadata { { "x-keep", "yes" } };
        await intercepted.AsyncUnaryCall(Echo, null, new CallOptions(headers), "hi");

        Assert.Equal("yes", invoker.Seen!.GetValue("x-keep"));
        Assert.NotNull(invoker.Seen.GetValue("traceparent"));
    }
}
