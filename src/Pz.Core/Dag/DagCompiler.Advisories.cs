using Pz.Core.Model;
using Pz.Core.Validation;

namespace Pz.Core.Dag;

/// <summary>Non-blocking warnings derived from the finished DAG.</summary>
public static partial class DagCompiler
{
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
