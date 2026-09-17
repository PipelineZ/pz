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
        while (true)
        {
            // The execution observes only the WINNER's token — the caller whose Lazy GetOrAdd published.
            // Node attempts carry their own tokens (engine.node_timeout cancels one attempt, not the
            // run), so a follower waits on the shared execution under ITS token, and a winner that was
            // cancelled says nothing about a follower whose token is still live.
            var lazy = completed.GetOrAdd(statement,
                s => new Lazy<Task>(() => NativeSetup.ExecuteSetupAsync(duck, s, ct), LazyThreadSafetyMode.ExecutionAndPublication));
            var execution = lazy.Value;
            try
            {
                await execution.WaitAsync(ct).ConfigureAwait(false);
                return;
            }
            catch (OperationCanceledException) when (execution.IsCanceled && !ct.IsCancellationRequested)
            {
                // The winner was cancelled, this caller was not: forget that execution and issue the
                // statement again, as the winner this time or behind whoever gets there first.
                completed.TryRemove(new KeyValuePair<string, Lazy<Task>>(statement, lazy));
            }
            catch
            {
                // Forget the execution only if IT ended badly. A follower giving up on its own token
                // while the execution is still running must leave it published, or the next caller
                // would issue the statement a second time alongside it.
                if (execution.IsFaulted || execution.IsCanceled)
                {
                    completed.TryRemove(new KeyValuePair<string, Lazy<Task>>(statement, lazy));
                }

                throw;
            }
        }
    }
}
