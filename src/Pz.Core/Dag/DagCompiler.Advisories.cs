using Pz.Core.Model;
using Pz.Core.Validation;

namespace Pz.Core.Dag;

/// <summary>Non-blocking notices and warnings derived from the finished DAG.</summary>
public static partial class DagCompiler
{
    /// <summary>The effectively-once advisory: an explicit <c>sync: {mode: auto}</c> dataset feeding a
    /// non-merge sink.</summary>
    private static void AddEffectivelyOnceNotices(CompiledDag compiled, List<string>? notices)
    {
        // Non-fatal, same `notices` mechanism as the "cursor unverified" notice. The effectively-once
        // guarantee an incremental dataset relies on holds only if every sink its lineage reaches is
        // idempotent (strategy: merge): a crash/retry, or a no-pushdown source re-running the same
        // extract, must not duplicate rows downstream. Trace each SourceLoad's structural descendants
        // to every SinkWrite it reaches and warn on each non-merge one. Deliberately NOT a hard error --
        // append-only sources with pushdown support can legitimately use strategy: append, and a hard
        // error would over-block those legitimate cases.
        foreach (var node in compiled.Nodes.Where(n => n.Kind == NodeKind.SourceLoad))
        {
            // Same explicit-only scope as the incremental pairing matrix -- an implicit `mode: auto`
            // dataset (SyncMode null) that a connector resolves to a feed is a planner-only concern (no
            // connector access here); this notice never fires for it, mirroring PZ0214's split. Only
            // `SyncMode.Auto` can reach this: an Incremental dataset cannot land on a non-merge sink --
            // PZ0335 refuses replace and PZ0214 refuses append-without-consent, while
            // append-with-consent is filtered by the AcceptDuplicates check below.
            if (node.Definition is not SourceDatasetDef sourceDef ||
                sourceDef.Dataset.SyncMode is not { Mode: SyncMode.Auto })
            {
                continue;
            }

            foreach (var sinkNode in compiled.Descendants(node.Id).Where(d => d.Kind == NodeKind.SinkWrite))
            {
                var sinkDef = (SinkOutputDef)sinkNode.Definition;
                // Consent recorded via `duplicates: accept` (PZ0214's own offered escape hatch) must
                // silence this recurring nag -- PZ0214 already surfaced merge as the alternative at
                // decision time, so repeating that hint here would contradict the consent the operator
                // just gave.
                if (sinkDef.Output.Mode == "merge" || sinkDef.Output.AcceptDuplicates)
                {
                    continue;
                }

                notices?.Add(
                    $"incremental/sync dataset '{sourceDef.Source.Name}.{sourceDef.Dataset.Name}' feeds non-merge sink " +
                    $"'{sinkDef.Sink.Name}.{sinkDef.Output.Name}' (mode: {sinkDef.Output.Mode}); effectively-once " +
                    "is not guaranteed -- a crash or retry may duplicate rows. Use write.strategy: merge for idempotency.");
            }
        }
    }

    /// <summary>A `watermark_format` whose finest unit is a day or coarser (`date`, `yyyy-MM`, `yyyy`,
    /// `yyyyMMdd`, ...) floors the lower bound and rounds the upper bound by that whole unit, so every run or
    /// window re-reads up to a day, month or year. Into an append sink each re-read lands again. Never
    /// refused (an API may take only dates); said once per (dataset, sink) as PZ0364, only for an incremental
    /// http dataset whose cursor is not a date. The option is the http connector's; core reads its text.</summary>
    private static List<PzWarning> CoarseWatermarkFormatWarnings(CompiledDag compiled, IReadOnlyList<DagNode> ordered)
    {
        var warnings = new List<PzWarning>();
        foreach (var node in ordered.Where(n => n.Kind == NodeKind.SourceLoad))
        {
            if (node.Definition is not SourceDatasetDef sourceDef
                || sourceDef.Source.Connector != "http"
                || sourceDef.Dataset.SyncMode?.Incremental is not { } incremental
                || sourceDef.Dataset.Options.GetValueOrDefault("watermark_format") is not string format
                || !IsCoarseWatermarkFormat(format)
                || CursorType(sourceDef.Dataset, incremental.Cursor) == "date")
            {
                continue;
            }

            foreach (var sinkNode in compiled.Descendants(node.Id).Where(d => d.Kind == NodeKind.SinkWrite))
            {
                var sinkDef = (SinkOutputDef)sinkNode.Definition;
                if (sinkDef.Output.Mode != "append")
                {
                    continue;
                }

                warnings.Add(new PzWarning(PzErrorCode.ConnectorConfigWarning,
                    $"dataset '{sourceDef.Source.Name}.{sourceDef.Dataset.Name}' renders its watermark with " +
                    $"'watermark_format: {format}', whose unit is a day or longer: every run or window re-reads " +
                    $"up to a whole unit, and append sink '{sinkDef.Sink.Name}.{sinkDef.Output.Name}' lands those " +
                    "rows again", sourceDef.Source.FilePath, null,
                    "use a finer format (one with hours, minutes or seconds) if the API accepts one, or write with " +
                    "strategy: merge so re-read rows replace themselves"));
            }
        }

        return warnings;
    }

    /// <summary>The cursor's type as the http connector reads it: the contract column's, or the raw
    /// `cursor_type`. A date cursor already renders as a date, so a date format re-reads nothing extra.</summary>
    private static string? CursorType(DatasetDef dataset, string cursor) =>
        dataset.Columns is { Count: > 0 } columns
            ? columns.GetValueOrDefault(cursor)
            : dataset.Options.GetValueOrDefault("cursor_type") as string;

    /// <summary>True for `date`, and for a custom format with no hour, minute, second or fraction letter
    /// outside quoted text. Presets other than `date` are second-level or finer. Mirrors the letter rules of
    /// the http connector's <c>WatermarkFormat.ParseCustom</c>; keep the two in step.</summary>
    private static bool IsCoarseWatermarkFormat(string format)
    {
        if (format == "date")
        {
            return true;
        }

        if (format is "iso8601" or "epoch_s" or "epoch_ms")
        {
            return false;
        }

        var quoted = false;
        foreach (var c in format)
        {
            if (c == '\'')
            {
                quoted = !quoted;
            }
            else if (!quoted && c is 'H' or 'm' or 's' or 'f')
            {
                return false;
            }
        }

        return format.Contains('y');
    }
}
