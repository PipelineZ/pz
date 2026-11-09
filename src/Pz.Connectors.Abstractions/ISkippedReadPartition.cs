namespace Pz.Connectors.Abstractions;

/// <summary>Implemented by a partition whose read can be skipped, in whole or in part, by a rule of the
/// dataset's own configuration (an http <c>on_status</c> rule whose action is <c>skip</c>): the request was
/// answered, the answer said "no data here this time", and the partition yielded nothing for it instead of
/// failing.
///
/// <para>A skipped read is not an empty read: the rows it did not read may exist. On any node that would commit a
/// watermark or a sync token (an incremental dataset, YAML <c>incremental:</c> or SQL-declared, first run and
/// <c>--full-refresh</c> included; a feed read that hands back a sync token, in or out of process; a CDC read) the
/// engine therefore fails the node with PZ0540, naming <see cref="SkippedBy"/>, and commits nothing the read
/// produced: neither the watermark nor the sync token moves, and no sink receives the rows. On a full read that
/// commits neither, the skip is a success.</para>
///
/// <para>When the engine asks: once the partition's ReadAsync has finished, and, for an
/// <see cref="ICheckpointingPartition"/>, before it stores each checkpoint. A checkpointing partition must therefore
/// set <see cref="SkippedBy"/> no later than the first checkpoint it offers after the skip, so a checkpoint past a
/// skipped part is never stored and no resumed attempt can start beyond it. A partition read again from a
/// checkpoint reports only what that read skipped.</para>
///
/// <para>Additive: a partition that does not implement this is treated as never skipped, exactly as
/// before.</para></summary>
public interface ISkippedReadPartition : IDatasetPartition
{
    /// <summary>Which rule skipped this partition's read (or part of it) and what it matched, for example
    /// <c>on_status[0] (skip) matched HTTP 400 from https://api.example.com/r</c>; null when nothing was skipped.
    /// It is shown to the user as is, so it must hold no secret.</summary>
    string? SkippedBy { get; }
}
