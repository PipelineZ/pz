using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;

namespace Pz.Connectors.TestKit.Reference;

/// <summary>Capturing sink: every write session deep-copies (<see cref="RecordBatch.Clone"/>) the batches
/// it is handed — it never retains the engine-owned instance — and, on commit, appends a
/// <see cref="CommittedWrite"/> to the owning connector. Enforces at-most-once/mutually-exclusive
/// commit-or-abort; disposing an open session counts as an implicit abort. <c>mode: merge</c> instead
/// does a keyed upsert (last-writer-wins per <see
/// cref="OutputSpec.Keys"/>) against the output's previously committed rows -- see
/// <see cref="InMemorySinkWriteSession.CommitAsync"/> -- so the TestKit's merge acceptance facts can run
/// against this reference connector, not just against real connectors. <c>mode: replace</c> mode
/// overwrites the output's prior committed entries (<see cref="InMemoryConnector.RecordReplaceCommit"/>),
/// matching the declared <see cref="ConnectorCapabilities.ReplaceWrites"/> capability.</summary>
public sealed class InMemorySink(InMemoryConnector connector) : ISink
{
    public bool TryGetNativeCopy(OutputSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeCopy? copy)
    {
        copy = null;
        return false;
    }

    public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct)
    {
        if (string.Equals(spec.Mode, "merge", StringComparison.Ordinal))
        {
            // Fail fast, before any batch is ever written: a schema lacking a declared key column can
            // never be merged.
            var fieldNames = new HashSet<string>(schema.FieldsList.Select(f => f.Name), StringComparer.Ordinal);
            var missingKeys = spec.Keys.Where(k => !fieldNames.Contains(k)).ToArray();
            if (missingKeys.Length > 0)
            {
                throw new PzConnectorException(
                    $"output '{spec.Output}': merge key column(s) [{string.Join(", ", missingKeys)}] are " +
                    "not present in the write schema",
                    isTransient: false);
            }
        }

        return new ValueTask<ISinkWriteSession>(new InMemorySinkWriteSession(connector, spec, schema));
    }

    public ValueTask DisposeAsync() => default;
}

internal enum SessionState
{
    Open,
    Committed,
    Aborted,
}

internal sealed class InMemorySinkWriteSession(InMemoryConnector connector, OutputSpec spec, Schema schema) : ISinkWriteSession
{
    private readonly List<RecordBatch> _batches = [];
    private SessionState _state = SessionState.Open;
    private long _rowsWritten;

    /// <summary>Set the instant <see cref="CommitAsync"/> is entered, before the fault-injection check —
    /// independent of <see cref="_state"/>, which only advances to <see cref="SessionState.Committed"/> on
    /// success. Exists so <see cref="DisposeAsync"/> can tell "commit was attempted (and may have failed)"
    /// apart from "commit was never tried": per Commit-xor-Abort, once Commit has been attempted, Abort —
    /// including the implicit abort-on-dispose below — must never run, since Commit's true outcome is
    /// unknown and aborting could unwind a write that actually went through.</summary>
    private bool _commitAttempted;

    public ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
    {
        EnsureOpen("write to");

        var ordinal = _batches.Count;
        if (FaultInjection.GetInt(spec.Options, "fail_write_at_batch") == ordinal)
        {
            throw new PzConnectorException("injected write failure", FaultInjection.GetBool(spec.Options, "fail_transient"));
        }

        _batches.Add(batch.Clone());
        _rowsWritten += batch.Length;
        return ValueTask.CompletedTask;
    }

    public ValueTask<WriteResult> CommitAsync(CancellationToken ct)
    {
        EnsureOpen("commit");
        _commitAttempted = true;
        connector.RecordCommitAttempt();

        if (FaultInjection.GetBool(spec.Options, "fail_commit"))
        {
            throw new PzConnectorException("injected commit failure", FaultInjection.GetBool(spec.Options, "fail_transient"));
        }

        _state = SessionState.Committed;
        var result = new WriteResult(_rowsWritten, _batches.Count);

        if (string.Equals(spec.Mode, "merge", StringComparison.Ordinal))
        {
            // Keyed upsert (last-writer-wins per spec.Keys) against the output's previously
            // committed rows -- this session's own batches are folded into the merge below, so they (and
            // the stale prior committed batches they replace) are no longer needed afterward.
            var existing = connector.Committed.Where(c => c.Spec.Output == spec.Output).SelectMany(c => c.Batches).ToArray();
            var merged = MergeRows.Build(schema, spec.Keys, existing, _batches);
            connector.RecordMergeCommit(spec, merged, result);
            DisposeBatches();
        }
        else if (string.Equals(spec.Mode, "replace", StringComparison.Ordinal))
        {
            // Replace overwrites -- ownership of this session's batches passes into the
            // committed entry, exactly like the append branch below (no DisposeBatches here).
            connector.RecordReplaceCommit(spec, _batches.ToArray(), result);
        }
        else
        {
            connector.RecordCommit(new CommittedWrite(spec, _batches.ToArray(), result));
        }

        return new ValueTask<WriteResult>(result);
    }

    public ValueTask AbortAsync(CancellationToken ct)
    {
        EnsureOpen("abort");

        _state = SessionState.Aborted;
        DisposeBatches();
        connector.RecordAbort();
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        if (_state == SessionState.Open)
        {
            if (_commitAttempted)
            {
                // Commit was attempted (and threw) — per Commit-xor-Abort this must NOT count as an
                // abort, implicit or otherwise. Just release the locally-held batches.
                DisposeBatches();
            }
            else
            {
                _state = SessionState.Aborted;
                DisposeBatches();
                connector.RecordAbort();
            }
        }

        return ValueTask.CompletedTask;
    }

    private void EnsureOpen(string action)
    {
        if (_state != SessionState.Open)
        {
            throw new InvalidOperationException($"cannot {action} a session already {_state.ToString().ToLowerInvariant()}");
        }
    }

    private void DisposeBatches()
    {
        foreach (var batch in _batches) batch.Dispose();
        _batches.Clear();
    }
}
