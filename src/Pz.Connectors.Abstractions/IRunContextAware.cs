namespace Pz.Connectors.Abstractions;

/// <summary>Which run, and which node of it, an opened ISource/ISink works for.
///
/// <para><paramref name="Run"/> is unique to one run (or to one validation pass, which has no run of its
/// own): two runs in one long-lived process (<c>pz mcp</c>) never share it. <paramref name="Node"/> is pz's
/// content-addressed node id, stable across every attempt the engine makes at that node within the run;
/// null when the open serves no single node (a validation pass probing a connection).</para>
///
/// <para>Use it to scope state a connector keeps beyond one open to the run that made it (a refused
/// sign-in that no other node of the run should repeat), or to derive a key that a retried attempt of the
/// same node must reproduce (an idempotency key). It promises nothing across runs: <c>pz retry</c> is a
/// new run.</para>
///
/// <para>An out-of-process (PCP) connector receives it too, but the host starts one connector process per open, so
/// state the process keeps in memory under the run's scope still lives for one node only; a connector that must
/// share a decision across the nodes of a run has to keep it outside its process.</para></summary>
public sealed record ConnectorRunContext(string Run, string? Node)
{
    /// <summary>When the run started, on the engine's clock; null when the host has no run start (a validation
    /// pass, a direct test). The same for every attempt of every node in the run, so a value derived from "today"
    /// (an http <c>run_date</c>, and an idempotency key over the request it renders into) does not change when a
    /// retried attempt runs after midnight.</summary>
    public DateTimeOffset? StartedAt { get; init; }
}

/// <summary>Implemented by an ISource/ISink that wants to know the run it is opened for. The engine calls
/// UseRunContext at most once per opened ISource/ISink, after OpenAsync returns and before any
/// plan/read/write call -- the same ordering guarantee <see cref="IOperationGateAware.UseOperationGate"/>
/// documents. A connector must still behave correctly when it is never called (a host that has no run,
/// such as a direct test). Additive: existing connectors that do not implement this are unaffected.</summary>
public interface IRunContextAware
{
    void UseRunContext(ConnectorRunContext context);
}

/// <summary>Implemented by a connector whose connection check can run inside a run or validation pass, so state it
/// keeps past the check (a refused sign-in) is shared with the sources and sinks of that same pass instead of being
/// scoped to the check alone. The engine calls this instead of <see cref="IConnector.CheckConnectionAsync"/> when
/// it has a context (<c>pz validate --connect</c> gives its check and its schema probes one). It must answer exactly
/// as <see cref="IConnector.CheckConnectionAsync"/> would. Additive: a connector that does not implement it is
/// checked as before.</summary>
public interface IScopedConnectionCheck
{
    ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, ConnectorRunContext context,
        CancellationToken ct);
}
