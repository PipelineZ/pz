namespace Pz.Connectors.Abstractions;

/// <summary>What <see cref="ISinkWriteSession.AbortAsync"/> actually achieves for this sink.
/// Declared, not probed: the engine surfaces it in run artifacts so a failed write never
/// claims cleanup that did not happen. Owned-destination sinks are <see cref="DiscardsAll"/>.</summary>
public enum AbortSemantics
{
    /// <summary>Abort removes every trace of the session's writes (temp-write + discard).</summary>
    DiscardsAll,
    /// <summary>Abort attempts cleanup but cannot guarantee it (e.g. deletes may fail
    /// independently); some written data may remain visible downstream.</summary>
    BestEffort,
    /// <summary>Abort cleans up nothing: every delivered row is already visible downstream
    /// (destinations with side effects — you cannot un-POST).</summary>
    None,
}
