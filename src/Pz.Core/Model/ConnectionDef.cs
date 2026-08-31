namespace Pz.Core.Model;

/// <summary>A connection is a place with credentials. One record covers both directions, because
/// <c>warehouse</c> read from and <c>warehouse</c> written to are one database — one concurrency
/// budget, one rate limiter, one circuit breaker (see
/// <c>Pz.Engine.Execution.InstanceKey</c>). Direction is the function a pipeline calls, not the block
/// an author declared.
///
/// <paramref name="Datasets"/> is the read side, loaded from YAML. <see cref="Outputs"/> is the write
/// side, synthesized by <c>DagCompiler</c> from the sink() call sites — hence an init-only property
/// with an empty default rather than a positional parameter: the loader never fills it.
///
/// <paramref name="AllowUnsignedExtensions"/> opts this connection's native scans/copies out of the
/// planner's unsigned-packaged-extension gate (PZ0359, <c>Pz.Engine.Planning.ExecutionPlanner</c>) --
/// a `LOAD '&lt;path&gt;'` setup statement DuckDB never signature-verifies, as opposed to a bare
/// `LOAD &lt;name&gt;` resolved from DuckDB's own signed repository.</summary>
public sealed record ConnectionDef(string Name, string Connector,
    IReadOnlyDictionary<string, object?> Connection, IReadOnlyList<DatasetDef> Datasets, string FilePath,
    RetryDef? Retry = null, int? MaxConcurrency = null, RateLimitDef? RateLimit = null,
    bool AllowUnsignedExtensions = false)
{
    public IReadOnlyList<OutputDef> Outputs { get; init; } = [];

    /// <summary>The write options an <c>entities: &lt;e&gt;: write:</c> block declared, keyed by entity
    /// name. The YAML half of the two write surfaces --
    /// <c>DagCompiler</c> takes these when a sink() call passed no kwargs, the call's kwargs when it did,
    /// and refuses when both (PZ0341). There is never a merge of the two.</summary>
    public IReadOnlyDictionary<string, SinkWriteOptions> EntityWrites { get; init; } =
        new Dictionary<string, SinkWriteOptions>();
}
