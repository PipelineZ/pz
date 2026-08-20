namespace Pz.Connectors.Abstractions;

/// <summary>Marker: this source has no universal read path — PlanReadAsync always throws. The planner
/// turns engine.force_universal / files_per_partition into PZ0312 for these
/// instead of a doomed run — the source-side mirror of <see cref="INativeOnlySink"/>. Connector-level
/// only: a connector whose native-only-ness is per-FORMAT (LocalFiles parquet vs csv) cannot declare it
/// and keeps its run-time PlanReadAsync refusal.</summary>
public interface INativeOnlySource;
