namespace Pz.Core.Model;


/// <summary><see cref="Keys"/> backs <c>mode: merge</c> sink outputs -- DagCompiler validates mode/keys
/// consistency (PZ0209/PZ0211). It defaults to empty via the secondary constructor below rather than
/// <c>= []</c>: a record positional parameter cannot default to a collection expression (CS1736 -- not
/// a compile-time constant).
///
/// <see cref="AcceptDuplicates"/> is the explicit consent flag for at-least-once delivery semantics
/// (mode: append fed by an incremental dataset). See <c>https://pipelinez.dev/concepts/delivery-guarantees/</c>.
///
/// <see cref="Input"/> is not loaded from YAML -- a leftover `input:` key on a sink output is rejected
/// at load time (PZ0112). It is always constructed as "" by ProjectLoader and later synthesized by
/// DagCompiler from the pipeline whose inline `INSERT INTO {{ sink(...) }}` binds this output -- kept
/// as a field because it names the drained staging relation and is part of the SinkWrite NodeId.
///
/// YAML does not set <see cref="Mode"/>/<see cref="Keys"/>/<see cref="AcceptDuplicates"/> via top-level
/// <c>mode:</c>/<c>keys:</c>/<c>accept_duplicates:</c> keys (each refused as PZ0333
/// RetiredWriteSurface with a rewrite hint); instead a single <c>write: { strategy, keys, duplicates,
/// on_delete }</c> block maps onto these same fields (<c>write.strategy</c> -> <see cref="Mode"/>,
/// <c>write.keys</c> -> <see cref="Keys"/>, <c>write.duplicates: accept</c> -> <see
/// cref="AcceptDuplicates"/> = true). <c>write.on_delete</c> -> <see cref="OnDelete"/> (values
/// <c>delete</c>/<c>soft</c>/<c>ignore</c>, null = undeclared) -- the loader only validates the value and
/// that it requires <c>write.strategy: merge</c>; whether the output is actually cdc-fed (the other half
/// of "legal only on cdc-fed merge outputs") is DagCompiler's question (PZ0336/PZ0337).</summary>
public sealed record OutputDef(string Name, string Input, string Mode, string SchemaPolicy,
    IReadOnlyDictionary<string, object?> Options, IReadOnlyList<string> Keys, RetryDef? Retry = null,
    bool AcceptDuplicates = false, string? OnDelete = null)
{
    public OutputDef(string Name, string Input, string Mode, string SchemaPolicy,
        IReadOnlyDictionary<string, object?> Options)
        : this(Name, Input, Mode, SchemaPolicy, Options, [])
    {
    }
}
