namespace Pz.Connectors.Abstractions;

/// <summary>Optional identity for a planned partition. When the connector
/// declares <see cref="ConnectorCapabilities.StablePartitionIds"/>, every partition it plans must
/// implement this interface. Ids are engine-opaque, non-empty, unique within one plan, and
/// STABLE: planning the same DatasetSpec again (a later attempt in the same run, or a later
/// `pz retry`) must yield the same id for the same logical slice of data. Ids are never logged
/// or emitted in events — they live only inside the run's staging database.</summary>
public interface IIdentifiedPartition : IDatasetPartition
{
    string PartitionId { get; }
}

/// <summary>Optional checkpoint support for a partition (requires
/// <see cref="IIdentifiedPartition"/>, and the connector must declare BOTH
/// <see cref="ConnectorCapabilities.CheckpointableReads"/> and
/// <see cref="ConnectorCapabilities.StablePartitionIds"/>). The engine calls
/// <see cref="TryGetCheckpoint"/> only after it has durably staged every row the partition has
/// yielded so far; returning true hands the engine an opaque resume token covering exactly those
/// rows. On a retry the engine calls <see cref="TryResumeFrom"/> with the last persisted token
/// BEFORE ReadAsync; returning true means the subsequent ReadAsync yields only rows strictly
/// after the token's coverage; returning false means the token is no longer usable and the
/// engine discards the checkpointed prefix and restarts the partition from scratch — never throw
/// for an unusable token. Tokens are engine-opaque and must never be logged by the connector.</summary>
public interface ICheckpointingPartition : IIdentifiedPartition
{
    bool TryResumeFrom(string checkpoint);
    bool TryGetCheckpoint(out string? checkpoint);
}
