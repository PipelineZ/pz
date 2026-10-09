namespace Pz.PackageManagement.ProcessHosting;

/// <summary>What the host tells a spawned connector about the run it serves and where telemetry
/// goes. Every value crosses in the handshake's <c>HostInfo</c>, never through the environment (the
/// child's env is an allowlist). gRPC uses <see cref="OtelEndpoint"/>; http/protobuf
/// (<see cref="OtelProtocol"/> "http/protobuf") uses the per-signal URLs and an optional headers file
/// path. No endpoint at all means the connector builds no telemetry providers; <see cref="RunId"/>
/// null is a run-less verb (validate, plan, connector test).</summary>
public sealed record HostTelemetry(
    string? RunId, Uri? OtelEndpoint, string? OtelProtocol = null, Uri? OtelTracesEndpoint = null,
    Uri? OtelMetricsEndpoint = null, string? OtelHeadersFile = null)
{
    public static readonly HostTelemetry None = new(null, null);
}
