using Pz.Connectors.Abstractions;
using Pz.Core.Model;

namespace Pz.Engine.Planning;

internal enum ResolvedReadShape { Full, Feed, Incremental, Cdc }

/// <summary>An explicitly or implicitly declared `mode: auto` dataset (the
/// two are semantically identical -- see <see cref="SyncModeDef"/>'s doc comment) resolves its actual
/// read shape from the opened connector's <see cref="INaturalReadShapeSource"/>, when it implements
/// one; a connector that doesn't is assumed <see cref="ResolvedReadShape.Full"/>. Engine-only (planner
/// + executors, which hold the opened <see cref="ISource"/>) -- Pz.Core has no connector-capability
/// access and keeps its own narrower, explicit-declaration-only checks (see DagCompiler/
/// WatermarkInference's doc comments).</summary>
internal static class ReadShapeResolver
{
    public static ResolvedReadShape Resolve(DatasetDef dataset, ISource source, DatasetSpec spec)
        => dataset.SyncMode?.Mode switch
        {
            SyncMode.Incremental => ResolvedReadShape.Incremental,
            SyncMode.Cdc => ResolvedReadShape.Cdc,
            _ => source is INaturalReadShapeSource s && s.GetNaturalReadShape(spec) == NaturalReadShape.Feed
                ? ResolvedReadShape.Feed
                : ResolvedReadShape.Full,
        };
}
