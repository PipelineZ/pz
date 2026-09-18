using Pz.Core.Validation;

namespace Pz.Engine.Execution;

/// <summary>Thrown by <see cref="KindDispatchingExecutor"/> instead of returning a Failed result when a
/// timed-out node ignores cancellation. It is an exception, not a result, because it is a statement
/// about the run and not only the node: the dispatcher records the failure and then cancels everything
/// still pending, since work it could not stop may hold resources every other node needs.</summary>
public sealed class NodeUnresponsiveException(PzError error) : Exception(error.Message)
{
    public PzError Error { get; } = error;
}
