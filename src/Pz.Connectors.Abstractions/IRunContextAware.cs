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
/// new run.</para></summary>
public sealed record ConnectorRunContext(string Run, string? Node);

/// <summary>Implemented by an ISource/ISink that wants to know the run it is opened for. The engine calls
/// UseRunContext at most once per opened ISource/ISink, after OpenAsync returns and before any
/// plan/read/write call -- the same ordering guarantee <see cref="IOperationGateAware.UseOperationGate"/>
/// documents. A connector must still behave correctly when it is never called (a host that has no run,
/// such as a direct test). Additive: existing connectors that do not implement this are unaffected.</summary>
public interface IRunContextAware
{
    void UseRunContext(ConnectorRunContext context);
}
