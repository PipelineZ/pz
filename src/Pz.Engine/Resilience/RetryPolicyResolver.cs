using Pz.Core.Dag;
using Pz.Core.Model;

namespace Pz.Engine.Resilience;

/// <summary>Maps a node to its effective
/// <see cref="RetryPolicy"/> — nearest-wins field-wise cascade of the dataset/output block over the
/// owning source/sink instance block over <see cref="RetryPolicy.Default"/>. Pipeline/Check nodes
/// resolve to the default, which is inert for them (their failures are never transient
/// <c>PzConnectorException</c>s — see <see cref="Pz.Engine.Execution.KindDispatchingExecutor"/>'s doc
/// comment).</summary>
public static class RetryPolicyResolver
{
    public static RetryPolicy Resolve(DagNode node) => node.Definition switch
    {
        SourceDatasetDef def => Resolve(def.Dataset.Retry, def.Source.Retry),
        SinkOutputDef def => Resolve(def.Output.Retry, def.Sink.Retry),
        _ => RetryPolicy.Default,
    };

    /// <summary>Field-wise cascade: nearest (dataset/output) ?? instance ?? default, per field —
    /// loader-validated bounds (PZ0121) make each present field safe to take verbatim. Public because
    /// `pz plan`'s retry display resolves from the defs without a node.</summary>
    public static RetryPolicy Resolve(RetryDef? nearest, RetryDef? instance = null) => new(
        nearest?.MaxAttempts ?? instance?.MaxAttempts ?? RetryPolicy.Default.MaxAttempts,
        nearest?.BaseDelay ?? instance?.BaseDelay ?? RetryPolicy.Default.BaseDelay,
        nearest?.MaxDelay ?? instance?.MaxDelay ?? RetryPolicy.Default.MaxDelay);
}
