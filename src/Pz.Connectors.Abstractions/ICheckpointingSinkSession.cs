namespace Pz.Connectors.Abstractions;

/// <summary>Optional session surface for sinks declaring
/// <see cref="ConnectorCapabilities.CheckpointableWrites"/>. The engine drains rows in a
/// content-deterministic order and asks, after each write, how many rows (counted from the
/// start of that order) are durably delivered downstream. On a later attempt the engine may
/// offer to resume past an acknowledged prefix instead of re-delivering from zero.</summary>
public interface ICheckpointingSinkSession : ISinkWriteSession
{
    /// <summary>Called once, before any <see cref="ISinkWriteSession.WriteBatchAsync"/>, when
    /// the engine holds a validated acknowledged prefix of <paramref name="acknowledgedRows"/>
    /// rows for this output. Return true to accept: the engine will then deliver only rows
    /// strictly after the prefix, and totals reported at commit must count the resumed prefix
    /// as written. Return false to decline (the engine re-delivers from zero). Must not throw.</summary>
    bool TryResumeFrom(long acknowledgedRows);

    /// <summary>Reports cumulative durably-delivered rows, counted in engine drain order and
    /// including any accepted resume prefix. Only rows whose delivery the destination has
    /// confirmed (e.g. 2xx responses) may be counted — never buffered/unsent rows. Return
    /// false when the count has not advanced since the last true. Must not throw.</summary>
    bool TryGetAcknowledgedRows(out long acknowledgedRows);
}
