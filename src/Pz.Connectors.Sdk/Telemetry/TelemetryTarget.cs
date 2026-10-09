using Pz.Connectors.Protocol.V1;

namespace Pz.Connectors.Sdk;

/// <summary>Where the host told this connector to export: one gRPC endpoint (<see cref="GrpcEndpoint"/>), or
/// http/protobuf per-signal URLs plus an optional headers file the connector re-reads before every export.</summary>
internal sealed record TelemetryTarget(string? GrpcEndpoint, string? TracesEndpoint, string? MetricsEndpoint, string? HeadersFile)
{
    /// <summary>The target in the handshake's <c>HostInfo</c>, or null when the host is not exporting. A host older than
    /// 0.9.1 only ever sends <c>otel_endpoint</c>.</summary>
    public static TelemetryTarget? FromHostInfo(HostInfo info)
    {
        if (info.OtelProtocol == "http/protobuf")
        {
            var traces = info.HasOtelTracesEndpoint ? info.OtelTracesEndpoint : null;
            var metrics = info.HasOtelMetricsEndpoint ? info.OtelMetricsEndpoint : null;
            return traces is null && metrics is null
                ? null
                : new TelemetryTarget(null, traces, metrics, info.HasOtelHeadersFile ? info.OtelHeadersFile : null);
        }

        return info.HasOtelEndpoint ? new TelemetryTarget(info.OtelEndpoint, null, null, null) : null;
    }
}
