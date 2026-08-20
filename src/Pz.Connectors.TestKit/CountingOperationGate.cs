using Pz.Connectors.Abstractions;

namespace Pz.Connectors.TestKit;

/// <summary>Test double for IOperationGate: executes ops inline with NO pacing and NO retry of
/// its own (so a transient failure surfacing proves the connector performs no retry outside the
/// gate), records every opLabel and budget report, and can inject failures that are thrown
/// INSTEAD of invoking the op (the op body never runs for an injected failure).</summary>
public sealed class CountingOperationGate : IOperationGate
{
    private readonly Lock _lock = new();
    private readonly List<string> _labels = [];
    private readonly List<(int Remaining, DateTimeOffset ResetAt)> _budgets = [];
    private readonly Queue<Exception> _pendingFailures = new();

    public IReadOnlyList<string> Labels { get { lock (_lock) { return [.. _labels]; } } }
    public int Calls { get { lock (_lock) { return _labels.Count; } } }
    public IReadOnlyList<(int Remaining, DateTimeOffset ResetAt)> Budgets
    { get { lock (_lock) { return [.. _budgets]; } } }

    public void FailNextWith(Exception ex)
    {
        lock (_lock) { _pendingFailures.Enqueue(ex); }
    }

    public async Task<T> ExecuteAsync<T>(string opLabel, bool idempotent,
        Func<CancellationToken, Task<T>> op, CancellationToken ct)
    {
        Exception? injected = null;
        lock (_lock)
        {
            _labels.Add(opLabel);
            _pendingFailures.TryDequeue(out injected);
        }

        if (injected is not null)
        {
            throw injected;
        }

        return await op(ct).ConfigureAwait(false);
    }

    public void ReportBudget(int remaining, DateTimeOffset resetAt)
    {
        lock (_lock) { _budgets.Add((remaining, resetAt)); }
    }
}
