namespace Pz.Core.Model;

/// <summary>The write options one <c>sink()</c> call declared. Field-for-field the half of
/// <see cref="OutputDef"/> an author controls -- <c>DagCompiler</c> combines it with the entity name
/// and the claiming pipeline to build the OutputDef itself. Defaults: an absent <c>write:</c> means
/// append/no keys/no duplicates policy/no on_delete, an absent <c>schema_policy:</c> means
/// fail_on_change.
///
/// <see cref="Duplicates"/> is the declared <c>duplicates:</c> value (null = undeclared): <c>accept</c>
/// is the append consent to at-least-once delivery; <c>fail</c> and <c>latest</c> say what a merge does
/// with duplicate keys in one staged batch. <see cref="OrderBy"/> is the <c>order_by:</c> columns that
/// pick the <c>latest</c> survivor (empty when undeclared).</summary>
public sealed record SinkWriteOptions(
    string Mode,
    IReadOnlyList<string> Keys,
    string SchemaPolicy,
    string? Duplicates,
    string? OnDelete,
    RetryDef? Retry,
    IReadOnlyDictionary<string, object?> Options,
    IReadOnlyList<string>? OrderBy = null)
{
    public static readonly SinkWriteOptions Default =
        new("append", [], "fail_on_change", null, null, null, new Dictionary<string, object?>());

    /// <summary>The append consent to at-least-once delivery (<c>duplicates: accept</c>).</summary>
    public bool AcceptDuplicates => Duplicates == DuplicatesPolicy.Accept;
}

/// <summary>The <c>duplicates:</c> vocabulary, shared by both write surfaces and the executor.</summary>
public static class DuplicatesPolicy
{
    /// <summary>Append consents to at-least-once delivery.</summary>
    public const string Accept = "accept";

    /// <summary>A merge whose staged batch repeats a key fails instead of keeping an arbitrary row.</summary>
    public const string Fail = "fail";

    /// <summary>A merge keeps, per key, the row with the highest <c>order_by</c> values.</summary>
    public const string Latest = "latest";

    public static readonly string[] All = [Accept, Fail, Latest];

    /// <summary>The cross-field rules both write surfaces enforce, as error messages without a
    /// location prefix. <paramref name="duplicates"/> is already known to be in <see cref="All"/> or
    /// null.</summary>
    public static IEnumerable<string> CrossFieldErrors(string? duplicates, IReadOnlyList<string> orderBy, string strategy)
    {
        if (duplicates is Fail or Latest && strategy != "merge")
        {
            yield return $"'duplicates: {duplicates}' requires strategy: merge (got '{strategy}')";
        }

        if (duplicates == Latest && orderBy.Count == 0)
        {
            yield return "'duplicates: latest' requires 'order_by', the column(s) whose highest value wins";
        }

        if (orderBy.Count > 0 && duplicates != Latest)
        {
            yield return "'order_by' applies only with 'duplicates: latest'";
        }
    }
}
