namespace Pz.Connectors.Sdk;

/// <summary>Optional host settings. The SDK always exports its own <c>Pz.Connector</c>
/// <see cref="System.Diagnostics.ActivitySource"/> and <see cref="System.Diagnostics.Metrics.Meter"/>
/// (the ones on <see cref="PzConnectorContext"/>); list the names of any others a connector wants
/// exported alongside them -- a client library's own source, for instance.</summary>
public sealed class PzConnectorHostOptions
{
    /// <summary>Names of extra <see cref="System.Diagnostics.ActivitySource"/>s to export. Libraries built on
    /// Azure.Core emit their source only when the AppContext switch
    /// <c>Azure.Experimental.EnableActivitySource</c> is set, so set it before creating their clients.
    /// Only a fixed set of safe attributes (system, operation and database names, server address and port,
    /// status codes) is exported from these spans; every other attribute is dropped. A span that records an
    /// exception (or any event attribute other than <c>exception.type</c>) or has a link carrying attributes
    /// is dropped rather than exported, since exception messages can carry data values.</summary>
    public IReadOnlyList<string> ActivitySources { get; init; } = [];

    /// <summary>Names of extra <see cref="System.Diagnostics.Metrics.Meter"/>s to export. Their instruments
    /// keep only the same fixed set of safe attribute keys as the spans above; other attributes are dropped
    /// (and measurements that differ only by them are aggregated together).</summary>
    public IReadOnlyList<string> Meters { get; init; } = [];
}
