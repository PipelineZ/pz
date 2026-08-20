namespace Pz.Core.Model;

/// <summary>The read options one <c>source()</c> call declared — field-for-field the half of
/// <see cref="DatasetDef"/> an author controls. <c>DagCompiler</c>
/// combines it with the entity name to build the DatasetDef itself, exactly as
/// <see cref="SinkWriteOptions"/> feeds an OutputDef.
///
/// Defaults are "nothing declared", so an entity whose options live in <c>entities: &lt;e&gt;: read:</c>
/// and one whose call site passes the same values compile to the same NodeId — moving an option between
/// the two surfaces is cut-and-paste, not a rehash.</summary>
public sealed record SourceReadOptions(
    IReadOnlyDictionary<string, string>? Columns,
    SyncModeDef? Sync,
    RetryDef? Retry,
    IReadOnlyDictionary<string, object?> Options)
{
    public static readonly SourceReadOptions Default =
        new(null, null, null, new Dictionary<string, object?>());
}
