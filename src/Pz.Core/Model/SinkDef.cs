namespace Pz.Core.Model;


/// <summary>A sink entity as the compiler resolved it: the <see cref="SinkWriteOptions"/> the author
/// declared (<c>entities: &lt;e&gt;: write:</c> or the <c>sink()</c> kwargs) plus the claiming pipeline.
/// <c>DagCompiler</c> synthesizes every instance; nothing is loaded from YAML.
///
/// <see cref="Keys"/> backs <c>strategy: merge</c> outputs -- DagCompiler validates mode/keys
/// consistency (PZ0209/PZ0211). It defaults to empty via the secondary constructor below rather than
/// <c>= []</c>: a record positional parameter cannot default to a collection expression (CS1736 -- not
/// a compile-time constant).
///
/// <see cref="AcceptDuplicates"/> is the explicit consent flag for at-least-once delivery semantics
/// (append fed by an incremental dataset). See <c>https://pipelinez.dev/concepts/delivery-guarantees/</c>.
///
/// <see cref="Input"/> is the staging relation of the pipeline whose <c>INSERT INTO {{ sink(...) }}</c>
/// binds this output. It names the drained relation and is part of the SinkWrite NodeId.
///
/// <see cref="OnDelete"/> (<c>delete</c>/<c>soft</c>/<c>ignore</c>, null = undeclared) is legal only on
/// a <c>merge</c> strategy fed by a cdc dataset; the write-option parsers check the value and the
/// strategy, DagCompiler checks the cdc feed (PZ0336/PZ0337).</summary>
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
