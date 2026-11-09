using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Planning;

namespace Pz.Engine.Execution;

/// <summary>Fails a SourceLoad that would commit a watermark or a sync token when a rule of the dataset's config skipped
/// (<see cref="ISkippedReadPartition"/>). Asked once a partition's read has finished and before a checkpoint of it is
/// stored, so neither the rows nor a resume position past the skip is committed. A non-incremental dataset is never
/// failed: a skip there is the success the rule asked for.</summary>
internal sealed class SkippedReadGuard(SourceDatasetDef def, ResolvedReadShape shape)
{
    // Any node that would commit a watermark or a sync token: an incremental dataset in any form (YAML or
    // SQL-declared), a feed (a sync-token change feed, in or out of process) and CDC.
    private readonly bool _incremental = shape is ResolvedReadShape.Incremental or ResolvedReadShape.Feed
        or ResolvedReadShape.Cdc || def.Dataset.SyncMode is { Mode: SyncMode.Incremental };

    public void Check(IDatasetPartition partition)
    {
        if (_incremental && partition is ISkippedReadPartition { SkippedBy: { } rule })
        {
            throw new CodedNodeFailureException(new PzError(PzErrorCode.SkippedReadOnIncremental,
                $"source '{def.Source.Name}.{def.Dataset.Name}': {rule}, but a read cannot be skipped on an incremental " +
                "dataset or a feed: the rows it did not read may exist, so nothing from this read was committed (the " +
                "watermark or sync token did not move and no sink received it)",
                def.Source.FilePath, null,
                "narrow the rule (its status and body_contains) so it matches only answers that mean no data, or use " +
                "action: permanent (fail) or transient (retry)"));
        }
    }
}

/// <summary>A node failure that already carries its coded <see cref="PzError"/>: the dispatching executor reports it
/// as is, and never retries it.</summary>
public sealed class CodedNodeFailureException(PzError error) : Exception(error.Message)
{
    public PzError Error { get; } = error;
}
