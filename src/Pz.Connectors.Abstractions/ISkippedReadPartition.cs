namespace Pz.Connectors.Abstractions;

/// <summary>Implemented by a partition whose read can be skipped, in whole or in part, by a rule of the
/// dataset's own configuration (an http <c>on_status</c> rule whose action is <c>skip</c>): the request was
/// answered, the answer said "no data here this time", and the partition yielded nothing for it instead of
/// failing. The engine reads <see cref="ReadSkipped"/> once the partition's ReadAsync has finished.
///
/// <para>A skipped read is not an empty window: the rows it did not read may exist. So for an incremental
/// dataset whose read reports a skip, the engine captures no watermark candidate for the node in that run
/// (in particular, an empty window is NOT advanced to its ceiling) and says so in a notice; the next run reads
/// the same slice again. A read that merely found no rows must not report a skip.</para>
///
/// <para>Additive: a partition that does not implement this is treated as never skipped, exactly as
/// before.</para></summary>
public interface ISkippedReadPartition : IDatasetPartition
{
    /// <summary>True when a rule skipped this partition's read, or part of it, during the ReadAsync that just
    /// finished.</summary>
    bool ReadSkipped { get; }
}
