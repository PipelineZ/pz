using System.Diagnostics;
using Grpc.Core;
using Grpc.Core.Interceptors;

namespace Pz.PackageManagement.ProcessHosting;

/// <summary>Injects the current <see cref="Activity"/>'s W3C trace context into every PCP call's
/// request metadata. Explicit rather than relying on HttpClient's implicit propagation so the rule is
/// testable and stated once: no current Activity (the engine's no-listener zero-cost path) means no
/// header, so a connector without an endpoint never sees a traceparent it cannot use.</summary>
internal sealed class TraceContextInterceptor : Interceptor
{
    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        TRequest request, ClientInterceptorContext<TRequest, TResponse> context,
        AsyncUnaryCallContinuation<TRequest, TResponse> continuation) =>
        continuation(request, Inject(context));

    public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
        TRequest request, ClientInterceptorContext<TRequest, TResponse> context,
        AsyncServerStreamingCallContinuation<TRequest, TResponse> continuation) =>
        continuation(request, Inject(context));

    public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncClientStreamingCallContinuation<TRequest, TResponse> continuation) =>
        continuation(Inject(context));

    public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncDuplexStreamingCallContinuation<TRequest, TResponse> continuation) =>
        continuation(Inject(context));

    public override TResponse BlockingUnaryCall<TRequest, TResponse>(
        TRequest request, ClientInterceptorContext<TRequest, TResponse> context,
        BlockingUnaryCallContinuation<TRequest, TResponse> continuation) =>
        continuation(request, Inject(context));

    internal static ClientInterceptorContext<TRequest, TResponse> Inject<TRequest, TResponse>(
        ClientInterceptorContext<TRequest, TResponse> context)
        where TRequest : class
        where TResponse : class
    {
        var activity = Activity.Current;
        if (activity is null || activity.IdFormat != ActivityIdFormat.W3C || activity.Id is null)
        {
            return context;
        }

        var headers = context.Options.Headers ?? [];
        headers.Add("traceparent", activity.Id);
        if (activity.TraceStateString is { Length: > 0 } state)
        {
            headers.Add("tracestate", state);
        }

        return new ClientInterceptorContext<TRequest, TResponse>(
            context.Method, context.Host, context.Options.WithHeaders(headers));
    }
}
