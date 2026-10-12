namespace Pz.Connectors.Abstractions;

/// <summary>Remote-side statistics a connector reports about the operation it is serving. Reached
/// through <see cref="ConnectorStats.Current"/>. Invalid values (negative, non-finite, malformed
/// names or units) are dropped, never thrown: reporting stats must not be able to fail a read or write.
/// A sink's stats are snapshotted after its write session is disposed, so anything reported until
/// <see cref="IAsyncDisposable.DisposeAsync"/> returns is included in the commit or abort response.</summary>
public interface IConnectorStats
{
    /// <summary>Requests issued to the remote system.</summary>
    void AddRequests(long n);

    /// <summary>Bytes moved over the wire, as the remote system counts them.</summary>
    void AddWireBytes(long n);

    /// <summary>Milliseconds spent waiting on the remote system.</summary>
    void AddRemoteMs(long ms);

    /// <summary>Requests the remote system throttled.</summary>
    void AddThrottled(long n);

    /// <summary>Billing-style cost in a single unit per operation (the first unit reported wins).</summary>
    void AddCost(double units, string unit);

    /// <summary>A connector-specific counter named <c>prefix.name</c> (lowercase; at most eight per operation).</summary>
    void AddExtra(string name, double value);
}
