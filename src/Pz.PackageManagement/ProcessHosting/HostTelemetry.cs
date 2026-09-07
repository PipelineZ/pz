namespace Pz.PackageManagement.ProcessHosting;

/// <summary>What the host tells a spawned connector about the run it serves and where telemetry
/// goes. Both values cross in the handshake's <c>HostInfo</c>, never through the environment (the
/// child's env is an allowlist). <see cref="OtelEndpoint"/> null means the connector builds no
/// telemetry providers at all; <see cref="RunId"/> null is a run-less verb (validate, plan,
/// connector test).</summary>
public sealed record HostTelemetry(string? RunId, Uri? OtelEndpoint)
{
    public static readonly HostTelemetry None = new(null, null);
}
