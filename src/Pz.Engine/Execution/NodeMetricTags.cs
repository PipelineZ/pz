using System.Collections.Frozen;
using Pz.Core.Dag;

namespace Pz.Engine.Execution;

internal static class NodeMetricTags
{
    /// <summary>The <c>pz.node.kind</c> metric tag is constant per <see cref="NodeKind"/> — caching it avoids
    /// both the enum <c>ToString()</c> allocation and a fresh boxed tag value on every meter call.</summary>
    internal static readonly FrozenDictionary<NodeKind, KeyValuePair<string, object?>> Kind =
        new Dictionary<NodeKind, KeyValuePair<string, object?>>
        {
            [NodeKind.SourceLoad] = new("pz.node.kind", "SourceLoad"),
            [NodeKind.Pipeline] = new("pz.node.kind", "Pipeline"),
            [NodeKind.Check] = new("pz.node.kind", "Check"),
            [NodeKind.SinkWrite] = new("pz.node.kind", "SinkWrite"),
        }.ToFrozenDictionary();
}
