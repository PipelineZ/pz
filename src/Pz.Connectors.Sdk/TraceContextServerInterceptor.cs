using System.Diagnostics;
using Grpc.Core;
using Grpc.Core.Interceptors;

namespace Pz.Connectors.Sdk;

/// <summary>One <c>pcp.&lt;Rpc&gt;</c> server span per control-plane RPC, parented on the W3C
/// <c>traceparent</c> the host put in the request metadata -- which is the engine's node span. Whatever
/// the connector starts inside the handler nests under it. Parsed explicitly rather than trusting
/// ASP.NET Core's hosting activity so the rule holds whatever the web host's own diagnostics settings
/// are -- and an RPC that carries no header becomes a root rather than a child of that same never-
/// exported hosting activity. <c>HostChannel</c> is skipped: it lives as long as the process and would be one endless
/// span.</summary>
internal sealed class TraceContextServerInterceptor(ConnectorTelemetry telemetry) : Interceptor
{
    // Each handler restores the ambient activity itself: a root span's Stop sets Activity.Current to
    // its (null) parent, not to whatever was current before the RPC, so anything the hosting
    // pipeline runs after the handler would otherwise lose its correlation on header-less RPCs.
    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request, ServerCallContext context, UnaryServerMethod<TRequest, TResponse> continuation)
    {
        var ambient = Activity.Current;
        using var activity = Begin(context);
        try
        {
            return await continuation(request, context).ConfigureAwait(false);
        }
        finally
        {
            activity?.Stop();
            Activity.Current = ambient;
        }
    }

    public override async Task ServerStreamingServerHandler<TRequest, TResponse>(
        TRequest request, IServerStreamWriter<TResponse> responseStream, ServerCallContext context,
        ServerStreamingServerMethod<TRequest, TResponse> continuation)
    {
        var ambient = Activity.Current;
        using var activity = Begin(context);
        try
        {
            await continuation(request, responseStream, context).ConfigureAwait(false);
        }
        finally
        {
            activity?.Stop();
            Activity.Current = ambient;
        }
    }

    public override async Task<TResponse> ClientStreamingServerHandler<TRequest, TResponse>(
        IAsyncStreamReader<TRequest> requestStream, ServerCallContext context,
        ClientStreamingServerMethod<TRequest, TResponse> continuation)
    {
        var ambient = Activity.Current;
        using var activity = Begin(context);
        try
        {
            return await continuation(requestStream, context).ConfigureAwait(false);
        }
        finally
        {
            activity?.Stop();
            Activity.Current = ambient;
        }
    }

    public override async Task DuplexStreamingServerHandler<TRequest, TResponse>(
        IAsyncStreamReader<TRequest> requestStream, IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context, DuplexStreamingServerMethod<TRequest, TResponse> continuation)
    {
        var ambient = Activity.Current;
        using var activity = Begin(context);
        try
        {
            await continuation(requestStream, responseStream, context).ConfigureAwait(false);
        }
        finally
        {
            activity?.Stop();
            Activity.Current = ambient;
        }
    }

    /// <summary>Null when nothing is exporting (no listener, so <c>StartActivity</c> is the BCL no-op)
    /// and for <c>HostChannel</c>.</summary>
    internal Activity? Begin(ServerCallContext context)
    {
        var method = context.Method;
        var slash = method.LastIndexOf('/');
        var rpc = slash >= 0 ? method[(slash + 1)..] : method;
        if (rpc == "HostChannel")
        {
            return null;
        }

        var parent = default(ActivityContext);
        if (context.RequestHeaders.GetValue("traceparent") is { } traceparent)
        {
            ActivityContext.TryParse(traceparent, context.RequestHeaders.GetValue("tracestate"), isRemote: true, out parent);
        }

        // No usable traceparent means the RPC has no upstream, so its span must be a trace ROOT. A
        // default parent context means "parent on Activity.Current", which inside this process is
        // ASP.NET Core's hosting activity -- never exported, so the span would go out carrying a parent
        // id no collector can resolve, while the Rust SDK makes the same RPCs roots. Clearing
        // Activity.Current across the call is how a root is asked for; it is put back only when no
        // activity was created, since otherwise the new activity IS the current one and every handler
        // below depends on that.
        var ambient = Activity.Current;
        if (parent == default)
        {
            Activity.Current = null;
        }

        var activity = telemetry.ActivitySource.StartActivity("pcp." + rpc, ActivityKind.Server, parent);
        if (activity is null)
        {
            Activity.Current = ambient;
            return null;
        }

        if (telemetry.InstanceId is { } instance)
        {
            activity.SetTag("pz.instance", instance);
        }

        return activity;
    }
}
