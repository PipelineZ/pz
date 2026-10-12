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
    /// status codes) is exported from these spans; every other attribute is dropped. Host names and resource
    /// names (<c>server.address</c>, <c>db.namespace</c>, collection and destination names) are exported on
    /// purpose. A span's name is replaced by its source name plus the operation, if one is recorded. A span that
    /// has an event not named <c>exception</c>, an event attribute other than <c>exception.type</c>, or a link
    /// carrying attributes is dropped rather than exported, since exception messages and event names can carry
    /// data values.</summary>
    public IReadOnlyList<string> ActivitySources { get; init; } = [];

    /// <summary>Names of extra <see cref="System.Diagnostics.Metrics.Meter"/>s to export. Their instruments
    /// keep only the same fixed set of safe attribute keys as the spans above; other attributes are dropped
    /// (and measurements that differ only by them are aggregated together). Instruments a connector creates
    /// itself on <see cref="PzConnectorContext"/>'s <c>Meter</c> are not filtered: their labels must never
    /// carry configuration values or data.</summary>
    public IReadOnlyList<string> Meters { get; init; } = [];
}
