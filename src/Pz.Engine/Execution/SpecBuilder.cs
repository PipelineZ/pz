using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Engine.State;

namespace Pz.Engine.Execution;

/// <summary>Single place that turns node definitions into connector specs — the executors and the
/// planner must agree byte-for-byte on options (columns contract merge) or the planner would probe a
/// different spec than the executor runs.
///
/// Deliberate, documented exception: <see cref="ForSourceLoad(SourceDatasetDef, Watermark?)"/>'s
/// watermark-carrying overload is used ONLY by <see cref="SourceLoadExecutor"/>, at actual execution
/// time. <see cref="Pz.Engine.Planning.ExecutionPlanner"/> keeps probing connectors with the
/// watermark-free <see cref="ForSourceLoad(SourceDatasetDef)"/> overload, because a watermark is neither
/// known nor decided at planning time and never changes which tier a dataset plans into — a connector
/// is free to ignore <see cref="DatasetSpec.WatermarkCursor"/> entirely (its contract says ignoring it
/// is always correct). So this one option pair is allowed to differ between the planner's probe and the
/// executor's real spec without breaking the byte-for-byte agreement the rest of this class exists to
/// guarantee.</summary>
public static class SpecBuilder
{
    public static DatasetSpec ForSourceLoad(SourceDatasetDef def) => ForSourceLoad(def, null);

    /// <summary>Execution-time overload: identical to <see cref="ForSourceLoad(SourceDatasetDef)"/>
    /// except that a non-null <paramref name="wm"/> additionally stamps <see cref="DatasetSpec.WatermarkCursor"/>/
    /// <see cref="DatasetSpec.WatermarkValue"/>.</summary>
    public static DatasetSpec ForSourceLoad(SourceDatasetDef def, Watermark? wm) => ForSourceLoad(def, wm, null);

    /// <summary>Bounded-window overload — additionally stamps
    /// <see cref="DatasetSpec.WatermarkUpperBound"/>. Only <see cref="SourceLoadExecutor"/> passes a
    /// non-null bound; the planner keeps probing watermark-free (same exception as above).
    ///
    /// When <paramref name="wm"/> is null but the dataset declares
    /// a plain (not <see cref="IncrementalDef.DeclaredInSql"/>) <c>incremental:</c>,
    /// <see cref="DatasetSpec.WatermarkCursor"/> is still stamped with the declared cursor NAME (value
    /// stays null) -- covers both a genuine first run (no stored watermark yet) and
    /// <c>--full-refresh</c> (which deliberately passes wm: null). Per
    /// <see cref="DatasetSpec.WatermarkCursor"/>'s doc comment ("when set, alongside WatermarkValue"),
    /// every consumer already tolerates a cursor with no paired value the same as no cursor at all --
    /// this only lets guards that need to know "is this dataset incremental at all" (e.g. the http
    /// connector's truncation guard) arm from a dataset's very first run. Deliberately excludes
    /// DeclaredInSql datasets: <see cref="SourceLoadExecutor.EvaluateSqlBoundsAsync"/>'s "no bound
    /// could be evaluated" outcome (no stored watermark, full-refresh, or an inclusive union without
    /// the connector's capability) is documented/tested (<c>SqlBoundEvaluationTests</c>) to mean
    /// "push nothing at all", cursor included -- a SQL-declared bound is a pipeline-computed value,
    /// never a raw column name a guard could arm on independently of that computation.</summary>
    public static DatasetSpec ForSourceLoad(SourceDatasetDef def, Watermark? wm, string? upperBound)
    {
        var options = new Dictionary<string, object?>(def.Dataset.Options);
        if (def.Dataset.Columns is not null)
        {
            options["columns"] = def.Dataset.Columns;
        }

        var spec = new DatasetSpec(def.Source.Name, def.Dataset.Name, options);
        if (def.Dataset.SyncMode is { Mode: SyncMode.Cdc } cdc)
        {
            spec = spec with { ChangeCapture = true, ChangeCaptureSlot = cdc.Slot };
        }

        if (wm is not null)
        {
            return spec with { WatermarkCursor = wm.Cursor, WatermarkValue = wm.Value, WatermarkUpperBound = upperBound };
        }

        return def.Dataset.SyncMode?.Incremental is { DeclaredInSql: false } incremental
            ? spec with { WatermarkCursor = incremental.Cursor }
            : spec;
    }

    /// <summary>Inclusive-lower-bound overload — additionally stamps
    /// <see cref="DatasetSpec.WatermarkLowerInclusive"/> when both a watermark and the flag are present.
    /// Only <see cref="SourceLoadExecutor"/> passes lowerInclusive; the planner keeps probing watermark-free.</summary>
    public static DatasetSpec ForSourceLoad(SourceDatasetDef def, Watermark? wm, string? upperBound, bool lowerInclusive)
        => ForSourceLoad(def, wm, upperBound) is var spec && wm is not null && lowerInclusive
            ? spec with { WatermarkLowerInclusive = true } : spec;

    public static OutputSpec ForSinkOutput(SinkOutputDef def) =>
        new(def.Sink.Name, def.Output.Name, def.Output.Mode, def.Output.SchemaPolicy, def.Output.Options)
        {
            Keys = def.Output.Keys,
            OnDelete = def.Output.OnDelete,
        };
}
