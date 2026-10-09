namespace Pz.Cli.Otel;

public enum OtelProtocol { Grpc, HttpProtobuf }

/// <summary>Where a run's telemetry goes. gRPC uses one <see cref="Endpoint"/> for both signals, as before 0.9.1;
/// http/protobuf uses full per-signal URLs (no <c>/v1/...</c> appended) and may carry a headers file that is re-read
/// before every export, so a caller can refresh a token while the run is going.</summary>
public sealed record OtelOptions(OtelProtocol Protocol, Uri? Endpoint, Uri? TracesEndpoint, Uri? MetricsEndpoint, string? HeadersFile)
{
    public static readonly OtelOptions Off = new(OtelProtocol.Grpc, null, null, null, null);

    public bool IsOn => Protocol == OtelProtocol.Grpc ? Endpoint is not null : TracesEndpoint is not null || MetricsEndpoint is not null;
}
