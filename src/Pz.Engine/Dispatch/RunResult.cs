using Pz.Engine.Execution;

namespace Pz.Engine.Dispatch;

public enum RunStatus { Success, CompletedWithFailures, Fatal }

public sealed record RunResult(string RunId, IReadOnlyList<NodeResult> Nodes, RunStatus Status);
