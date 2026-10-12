namespace Pz.Connectors.Abstractions;

/// <summary>The ambient <see cref="IConnectorStats"/> for the current operation. Flows with the async
/// execution context, so it is visible inside async iterators and tasks the operation starts.</summary>
public static class ConnectorStats
{
    private static readonly AsyncLocal<IConnectorStats?> Ambient = new();

    /// <summary>Never null; a no-op outside an operation.</summary>
    public static IConnectorStats Current => Ambient.Value ?? NoOp.Instance;

    /// <summary>Sets <see cref="Current"/> for the async flow until disposed; restores the previous value.</summary>
    public static IDisposable Use(IConnectorStats stats)
    {
        ArgumentNullException.ThrowIfNull(stats);
        var previous = Ambient.Value;
        Ambient.Value = stats;
        return new Scope(previous);
    }

    private sealed class Scope(IConnectorStats? previous) : IDisposable
    {
        public void Dispose() => Ambient.Value = previous;
    }

    private sealed class NoOp : IConnectorStats
    {
        public static readonly NoOp Instance = new();

        public void AddRequests(long n) { }
        public void AddWireBytes(long n) { }
        public void AddRemoteMs(long ms) { }
        public void AddThrottled(long n) { }
        public void AddCost(double units, string unit) { }
        public void AddExtra(string name, double value) { }
    }
}
