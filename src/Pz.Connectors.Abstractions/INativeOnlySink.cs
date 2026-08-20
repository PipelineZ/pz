namespace Pz.Connectors.Abstractions;

/// <summary>Marker: this sink has no universal write path — BeginWriteAsync always throws. The planner
/// turns engine.force_universal into PZ0312 for these instead of a doomed run.</summary>
public interface INativeOnlySink;
