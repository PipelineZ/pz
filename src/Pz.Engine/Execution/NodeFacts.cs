using Pz.Core.Dag;

namespace Pz.Engine.Execution;

/// <summary>Each stage's own wall time in the node's last attempt. On the arrow tier producer and consumer run
/// concurrently, so the stages overlap and do not sum to the node's duration.</summary>
public sealed record StageTimings(TimeSpan? Extract, TimeSpan? Ingest, TimeSpan? Egress, TimeSpan? Write);

/// <summary>What a node ran through and how it got there. Names only, never connection config.
/// <see cref="Attempts"/> is the number of executor invocations; 0 when a breaker refused the node before any
/// attempt (writers omit it then).</summary>
public sealed record NodeFacts(string? Connector, string? Connection, string? Strategy, StageTimings? Stages,
    int Attempts)
{
    public const string NativeScan = "native_scan";
    public const string NativeCopy = "native_copy";
    public const string Arrow = "arrow";

    public static NodeFacts From(DagNode node, NodeFactsRecorder? recorder, int attempts)
    {
        var (connector, connection) = node.Definition switch
        {
            SourceDatasetDef d => (d.Source.Connector, d.Source.Name),
            SinkOutputDef s => (s.Sink.Connector, s.Sink.Name),
            _ => ((string?)null, (string?)null),
        };
        return new NodeFacts(connector, connection, recorder?.Strategy, recorder?.Stages(), attempts);
    }
}
