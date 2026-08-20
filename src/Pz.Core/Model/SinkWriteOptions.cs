namespace Pz.Core.Model;

/// <summary>The write options one <c>sink()</c> call declared. Field-for-field the half of
/// <see cref="OutputDef"/> an author controls -- <c>DagCompiler</c> combines it with the entity name
/// and the claiming pipeline to build the OutputDef itself. Defaults: an absent <c>write:</c> means
/// append/no keys/no consent/no on_delete, an absent <c>schema_policy:</c> means
/// fail_on_change.</summary>
public sealed record SinkWriteOptions(
    string Mode,
    IReadOnlyList<string> Keys,
    string SchemaPolicy,
    bool AcceptDuplicates,
    string? OnDelete,
    RetryDef? Retry,
    IReadOnlyDictionary<string, object?> Options)
{
    public static readonly SinkWriteOptions Default =
        new("append", [], "fail_on_change", false, null, null, new Dictionary<string, object?>());
}
