namespace Pz.Connectors.Sdk;

/// <summary>Optional host settings. The SDK always exports its own <c>Pz.Connector</c>
/// <see cref="System.Diagnostics.ActivitySource"/> and <see cref="System.Diagnostics.Metrics.Meter"/>
/// (the ones on <see cref="PzConnectorContext"/>); list the names of any others a connector wants
/// exported alongside them -- a client library's own source, for instance.</summary>
public sealed class PzConnectorHostOptions
{
    public IReadOnlyList<string> ActivitySources { get; init; } = [];

    public IReadOnlyList<string> Meters { get; init; } = [];
}
