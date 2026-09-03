using System.Collections.Concurrent;
using Pz.DuckDb;

namespace Pz.Engine.Execution;

/// <summary>Per-run memo of connector setup statements (extension install/load, secrets, session
/// settings, attaches) that have already succeeded on the run's DuckDB session. Setup statements are
/// idempotent by contract, but not every statement is REPEATABLE — a session setting an extension
/// accepts only before it initialises is refused on the second node that re-issues it — so the
/// engine issues each distinct statement text once per run. Concurrent nodes that need the same
/// statement await the one in-flight execution rather than racing it (the second node's scan must
/// not run before the first node's attach has completed). A failed statement is forgotten, so a node
/// retry re-issues it. Keyed by exact statement text: two connections whose statements differ (a
/// different token, a different database) each run their own.</summary>
internal sealed class NativeSetupLedger
{
    // ConcurrentDictionary.GetOrAdd's valueFactory can run more than once under contention (it is not
    // atomic), and each invocation of NativeSetup.ExecuteSetupAsync would itself start a real DuckDB
    // execution -- so the dictionary stores a Lazy<Task>, not a Task directly. Only Lazy's own
    // ExecutionAndPublication thread-safety mode guarantees the delegate that starts the execution
    // runs once even if GetOrAdd's factory constructs more than one Lazy instance for the same key;
    // whichever Lazy the dictionary ends up publishing is the only one anyone ever calls .Value on.
    private readonly ConcurrentDictionary<string, Lazy<Task>> completed = new(StringComparer.Ordinal);

    internal async Task ExecuteOnceAsync(IDuckSession duck, string statement, CancellationToken ct)
    {
        var lazy = completed.GetOrAdd(statement,
            s => new Lazy<Task>(() => NativeSetup.ExecuteSetupAsync(duck, s, ct), LazyThreadSafetyMode.ExecutionAndPublication));
        try
        {
            await lazy.Value.ConfigureAwait(false);
        }
        catch
        {
            completed.TryRemove(new KeyValuePair<string, Lazy<Task>>(statement, lazy));
            throw;
        }
    }
}
