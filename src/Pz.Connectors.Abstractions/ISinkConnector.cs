using Apache.Arrow;

namespace Pz.Connectors.Abstractions;

public interface ISinkConnector : IConnector
{
    ValueTask<ISink> OpenAsync(ConnectorConfig config, CancellationToken ct);
}

public interface ISink : IAsyncDisposable
{
    /// <summary>Fast path: expose this output as a DuckDB-native COPY. Return false for the universal path.</summary>
    bool TryGetNativeCopy(OutputSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeCopy? copy);

    /// <summary>Begins a write session for one output. The engine guarantees exactly one of
    /// <see cref="ISinkWriteSession.CommitAsync"/> or <see cref="ISinkWriteSession.AbortAsync"/> is
    /// called before disposal.</summary>
    ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct);

    /// <summary>Abort semantics for sessions this sink opens. The default is the contract for
    /// owned destinations. Surfaced by the engine in run artifacts on non-DiscardsAll write
    /// failures.</summary>
    AbortSemantics AbortSemantics => AbortSemantics.DiscardsAll;
}

public interface ISinkWriteSession : IAsyncDisposable
{
    /// <summary>Writes one batch. Ownership: <paramref name="batch"/> is engine-owned and valid only
    /// until this call returns — copy (<c>RecordBatch.Clone()</c>) anything needed later. The TestKit
    /// enforces that committed data is not reference-equal to handed-in batches.
    /// <paramref name="batch"/>'s buffers may be pooled, off-heap native memory returned to a
    /// shared pool the instant the caller disposes it after this call returns — retaining a reference
    /// to <paramref name="batch"/> or any of its buffers past that point is undefined behavior (the
    /// memory may already be zeroed and handed to an unrelated batch), not merely stale data.</summary>
    ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct);

    /// <summary>Makes all written batches durable/visible atomically where the destination allows
    /// (temp-write + swap for replace mode). Called at most once, mutually exclusive with Abort. Once
    /// this has been called, its outcome (succeeded, in-flight, or failed) is unknown to the caller, so
    /// <see cref="AbortAsync"/> must never be called afterward — see its doc for why.</summary>
    ValueTask<WriteResult> CommitAsync(CancellationToken ct);

    /// <summary>Discards the session's writes and cleans up temp state. Must be safe to call after a
    /// failed WriteBatchAsync. Called at most once, mutually exclusive with Commit. Abort must not be
    /// called after Commit has been attempted, even if that attempt failed: Commit's true outcome is
    /// unknown once it has been invoked, and aborting could unwind a write that actually went through.</summary>
    ValueTask AbortAsync(CancellationToken ct);
}
