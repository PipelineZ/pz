using System.Collections.Concurrent;
using Pz.DuckDb;

namespace Pz.Engine.Execution;

/// <summary>Per-run memo of connector setup statements (extension install/load, secrets, session
/// settings, attaches) that have already succeeded on one DuckDB session. Setup statements are
/// idempotent by contract, but not every statement is REPEATABLE — a session setting an extension
/// accepts only before it initialises is refused on the second node that re-issues it — so the
/// engine issues each distinct statement text once per run. Concurrent nodes that need the same
/// statement await the one in-flight execution rather than racing it (the second node's scan must
/// not run before the first node's attach has completed). A failed statement is forgotten, so a node
/// retry re-issues it. Keyed by exact statement text: two connections whose statements differ (a
/// different token, a different database) each run their own. Bound to one <see cref="IDuckSession"/>
/// at construction (rather than taking it per call) so the memo can never be asked to skip a
/// statement's execution against a session other than the one it already ran on.</summary>
internal sealed class NativeSetupLedger(IDuckSession duck)
{
    // ConcurrentDictionary.GetOrAdd's valueFactory can run more than once under contention (it is not
    // atomic), and each invocation of NativeSetup.ExecuteSetupAsync would itself start a real DuckDB
    // execution -- so the dictionary stores a Lazy<Task>, not a Task directly. Once-ness across
    // DIFFERENT Lazy instances built for the same key comes from GetOrAdd itself: it publishes exactly
    // one of them into the dictionary and every caller (including the ones whose own Lazy lost the
    // race) gets that same published instance back — a losing Lazy is discarded unread, its .Value
    // never invoked, so its factory delegate never runs. LazyThreadSafetyMode.ExecutionAndPublication
    // is the separate guard for concurrent callers that all received the SAME published instance: it
    // serializes their concurrent .Value calls so the factory still runs (and completes) exactly once.
    private readonly ConcurrentDictionary<string, Lazy<Task>> completed = new(StringComparer.Ordinal);

    internal async Task ExecuteOnceAsync(string statement, CancellationToken ct)
    {
        // A caller whose Lazy lost the GetOrAdd race awaits the winner's execution, which observes only
        // the winner's token: every node of a run shares one cancellation token, so a follower's own
        // token is never consulted while it waits.
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
