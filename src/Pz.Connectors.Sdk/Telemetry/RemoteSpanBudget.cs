using Pz.Connectors.Abstractions;

namespace Pz.Connectors.Sdk.Telemetry;

/// <summary>The most <c>remote.*</c> spans one operation may open, shared by every partition and stream
/// of that operation. Past the cap calls are still counted as requests; only their spans are skipped.</summary>
internal sealed class RemoteSpanBudget
{
    internal const int Limit = 200;

    private static readonly AsyncLocal<RemoteSpanBudget?> Ambient = new();

    private int _taken;

    /// <summary>The budget of the operation the calling flow runs in; null outside one.</summary>
    public static RemoteSpanBudget? Current => Ambient.Value;

    /// <summary>True while the operation is still under its cap.</summary>
    public bool TryTake() => Interlocked.Increment(ref _taken) <= Limit;

    /// <summary>Sets <see cref="Current"/> for the async flow; the returned scope restores the previous one.</summary>
    public static IDisposable Use(RemoteSpanBudget budget)
    {
        var previous = Ambient.Value;
        Ambient.Value = budget;
        return new Scope(previous);
    }

    private sealed class Scope(RemoteSpanBudget? previous) : IDisposable
    {
        public void Dispose() => Ambient.Value = previous;
    }
}

/// <summary>Enters the ambient stats accumulator and span budget of one operation together, so the two
/// can never be set apart.</summary>
internal static class OperationScope
{
    public static IDisposable Enter(ConnectorStatsAccumulator stats, RemoteSpanBudget budget) =>
        new Both(ConnectorStats.Use(stats), RemoteSpanBudget.Use(budget));

    private sealed class Both(IDisposable stats, IDisposable budget) : IDisposable
    {
        public void Dispose()
        {
            budget.Dispose();
            stats.Dispose();
        }
    }
}
