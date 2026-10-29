using Pz.Core.Dag;
using Pz.Core.Validation;
using Pz.Engine.State;

namespace Pz.Engine.Execution;

/// <summary>Checks a stored watermark's cursor type against the cursor column. A watermark the engine wrote carries
/// the type its column landed as; one set by hand (<c>pz state set --type</c>) or kept across a source schema change
/// may not, and the read then fails on a bare conversion error that never names the watermark.
///
/// The guidance lives in the message, not in <see cref="PzError.Hint"/>: a node error's hint reaches no reader (see
/// <see cref="NodeFailureGuidance"/>).</summary>
internal static class CursorTypeGuard
{
    /// <summary>int and bigint share a canonical form (plain digits), so either one's value reads against the other.
    /// Every other pair must match exactly.</summary>
    internal static bool Fits(string storedType, string columnType) =>
        string.Equals(storedType, columnType, StringComparison.Ordinal)
        || (IsInteger(storedType) && IsInteger(columnType));

    /// <summary>The failure for a stored type that does not fit the column. <paramref name="columnIs"/> says where
    /// the column's type came from: "is declared as" (a columns: contract) or "landed as" (what the read staged).</summary>
    internal static PzError Mismatch(SourceDatasetDef def, Watermark stored, string columnIs, string columnType)
    {
        var key = WatermarkStore.Key(def.Source.Name, def.Dataset.Name);
        return new PzError(PzErrorCode.UnsupportedCursorType,
            $"source '{key}': the stored watermark has cursor type '{stored.TypeName}', but cursor column " +
            $"'{stored.Cursor}' {columnIs} '{columnType}': {Fix(key, stored.Cursor, columnType)}",
            def.Source.FilePath, null, null);
    }

    /// <summary>A note for a source load that failed while its stored watermark was one set by hand: without a
    /// columns: contract the column's type is unknown until rows land, and a database applies <c>cursor &gt; value</c>
    /// inside the read, so a wrong <c>--type</c> fails there first. Null when the watermark was written by a run, or
    /// the store cannot be read.</summary>
    internal static string? HandSetNote(DagNode node, RunContext ctx)
    {
        if (ctx.FullRefresh || ctx.Watermarks is null
            || node.Definition is not SourceDatasetDef { Dataset.SyncMode.Incremental: not null } def)
        {
            return null;
        }

        var key = WatermarkStore.Key(def.Source.Name, def.Dataset.Name);
        Watermark? stored;
        try
        {
            stored = ctx.Watermarks.Get(key);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return null; // the failure being reported is the read's, not this lookup's
        }

        return stored is { RunId: StateEdit.ManualRunId }
            ? $"the watermark for '{key}' was set by hand with cursor type '{stored.TypeName}'. If column " +
              $"'{stored.Cursor}' is not a {stored.TypeName}, {Fix(key, stored.Cursor, "<its type>")}"
            : null;
    }

    // `pz state set` refuses to change an existing entry's type, so the fix is clear-then-set.
    private static string Fix(string key, string cursor, string columnType) =>
        $"run `pz state clear {key} --yes`, then `pz state set {key} --value <value> --cursor {cursor} " +
        $"--type {columnType} --yes`; or run with --full-refresh to read the whole source.";

    private static bool IsInteger(string type) => type is "int" or "bigint";
}

/// <summary>Thrown from the staging trim when the landed cursor column does not fit the stored watermark's type; the
/// dispatcher reports its <see cref="Error"/> as is rather than as a generic node failure.</summary>
internal sealed class CursorTypeMismatchException(PzError error) : Exception(error.Message)
{
    public PzError Error { get; } = error;
}
