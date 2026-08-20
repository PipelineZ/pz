namespace Pz.Engine.State;

/// <summary>A per-dataset opaque sync-state token (delta link / change token). <see cref="Token"/> is
/// connector-owned and never inspected by the engine; <see cref="RunId"/> records the run that emitted
/// it (provenance/debugging, mirroring <see cref="Watermark.RunId"/>).</summary>
public sealed record SyncState(string Token, string RunId);
