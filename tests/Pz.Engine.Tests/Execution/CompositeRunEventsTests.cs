using Pz.Core.Dag;
using Pz.Engine.Execution;
using Pz.Engine.Dispatch;

namespace Pz.Engine.Tests.Execution;

/// <summary>Trivial fan-out list: lets a run register both the crash-safe snapshot writer
/// and the bus publisher without either one's failure affecting the other.</summary>
public class CompositeRunEventsTests
{
    private sealed class ThrowingEvents : IRunEvents
    {
        public void RunStarted(string runId, string projectName, int nodeCount) =>
            throw new InvalidOperationException();
        public void NodeStarted(DagNode node) => throw new InvalidOperationException();
        public void NodeProgress(DagNode node, long rowsSoFar, long bytesSoFar, long batchesSoFar) =>
            throw new InvalidOperationException();
        public void RetryScheduled(DagNode node, int attempt, int maxAttempts, TimeSpan delay, string reason) =>
            throw new InvalidOperationException();
        public void BreakerStateChanged(string instance, string oldState, string newState, string trigger,
            TimeSpan coolDown) => throw new InvalidOperationException();
        public void SourceDriftDetected(DagNode node, string connection, string entity, string policy,
            IReadOnlyList<Pz.Engine.State.SchemaDriftDiffer.Change> changes,
            IReadOnlyList<Pz.Engine.State.SchemaColumn> observed, string hintsHash) =>
            throw new InvalidOperationException();
        public void MergeKeyDuplicatesDetected(DagNode node, string output, IReadOnlyList<string> keys,
            long duplicateGroups, long extraRows) => throw new InvalidOperationException();
        public void LossyIntegerInferenceDetected(DagNode node, string connection, string entity,
            IReadOnlyList<string> columns) => throw new InvalidOperationException();
        public void AmbiguousDateInferenceDetected(DagNode node, string connection, string entity,
            IReadOnlyList<string> columns, string format) => throw new InvalidOperationException();
        public void NodeCompleted(NodeResult result) => throw new InvalidOperationException();
        public void RunCompleted(string runId, RunStatus status, int succeeded, int failed, int skipped,
            TimeSpan duration) => throw new InvalidOperationException();
    }

    private sealed class RecordingEvents : IRunEvents
    {
        public int NodeCompletedCount { get; private set; }
        public void RunStarted(string runId, string projectName, int nodeCount) { }
        public void NodeStarted(DagNode node) { }
        public void NodeProgress(DagNode node, long rowsSoFar, long bytesSoFar, long batchesSoFar) { }
        public void RetryScheduled(DagNode node, int attempt, int maxAttempts, TimeSpan delay, string reason) { }
        public void BreakerStateChanged(string instance, string oldState, string newState, string trigger,
            TimeSpan coolDown) { }
        public void SourceDriftDetected(DagNode node, string connection, string entity, string policy,
            IReadOnlyList<Pz.Engine.State.SchemaDriftDiffer.Change> changes,
            IReadOnlyList<Pz.Engine.State.SchemaColumn> observed, string hintsHash) { }
        public void MergeKeyDuplicatesDetected(DagNode node, string output, IReadOnlyList<string> keys,
            long duplicateGroups, long extraRows) => MergeKeyDuplicatesCount++;
        public int MergeKeyDuplicatesCount { get; private set; }
        public void LossyIntegerInferenceDetected(DagNode node, string connection, string entity,
            IReadOnlyList<string> columns) => LossyIntegerInferenceCount++;
        public int LossyIntegerInferenceCount { get; private set; }
        public void AmbiguousDateInferenceDetected(DagNode node, string connection, string entity,
            IReadOnlyList<string> columns, string format) => AmbiguousDateInferenceCount++;
        public int AmbiguousDateInferenceCount { get; private set; }
        public void NodeCompleted(NodeResult result) => NodeCompletedCount++;
        public void RunCompleted(string runId, RunStatus status, int succeeded, int failed, int skipped,
            TimeSpan duration) { }
    }

    private static DagNode Node(string name) =>
        new(NodeId.Compute(name), NodeKind.SourceLoad, name, [], null, name);

    [Fact]
    public void Throwing_target_does_not_block_second_target()
    {
        var throwing = new ThrowingEvents();
        var recording = new RecordingEvents();
        var composite = new CompositeRunEvents(throwing, recording);

        composite.NodeCompleted(NodeResult.Skipped(Node("orders")));

        Assert.Equal(1, recording.NodeCompletedCount);
    }

    [Fact]
    public void All_members_swallow_exceptions_and_reach_second_target()
    {
        var throwing = new ThrowingEvents();
        var recording = new RecordingEvents();
        var composite = new CompositeRunEvents(throwing, recording);

        composite.RunStarted("run-1", "proj", 1);
        composite.NodeStarted(Node("a"));
        composite.NodeProgress(Node("a"), 1, 1, 1);
        composite.RetryScheduled(Node("a"), 1, 3, TimeSpan.FromSeconds(1), "x");
        composite.MergeKeyDuplicatesDetected(Node("a"), "out", ["id"], 1, 1);
        composite.LossyIntegerInferenceDetected(Node("a"), "crm", "orders", ["id"]);
        composite.AmbiguousDateInferenceDetected(Node("a"), "crm", "orders", ["when"], "%d/%m/%Y");
        composite.NodeCompleted(NodeResult.Skipped(Node("a")));
        composite.RunCompleted("run-1", RunStatus.Success, 1, 0, 0, TimeSpan.Zero);

        Assert.Equal(1, recording.NodeCompletedCount);
        Assert.Equal(1, recording.MergeKeyDuplicatesCount);
        Assert.Equal(1, recording.LossyIntegerInferenceCount);
        Assert.Equal(1, recording.AmbiguousDateInferenceCount);
    }
}
