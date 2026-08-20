using Pz.Connectors.Abstractions;
using Xunit;

namespace Pz.Connectors.Toolkit.Tests.Abi;

public sealed class SyncStateAbiTests
{
    [Fact]
    public void SyncState_capability_flag_is_distinct()
    {
        Assert.False(ConnectorCapabilities.None.HasFlag(ConnectorCapabilities.SyncState));
        Assert.True((ConnectorCapabilities.SyncState | ConnectorCapabilities.BoundedWindow)
            .HasFlag(ConnectorCapabilities.SyncState));
    }

    [Fact]
    public void DatasetSpec_carries_prior_sync_state()
    {
        var spec = new DatasetSpec("s", "d", new Dictionary<string, object?>())
        {
            PriorSyncState = "https://api/delta?token=abc",
        };
        Assert.Equal("https://api/delta?token=abc", spec.PriorSyncState);
        Assert.Null(new DatasetSpec("s", "d", new Dictionary<string, object?>()).PriorSyncState);
    }

    private sealed class FakePartition : IDatasetPartition, ISyncStatePartition
    {
        public async IAsyncEnumerable<Apache.Arrow.RecordBatch> ReadAsync(
            BatchOptions options,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
        {
            await Task.CompletedTask;
            yield break;
        }

        public bool TryGetSyncStateCandidate(out string? candidate)
        {
            candidate = "tok-1";
            return true;
        }
    }

    [Fact]
    public void ISyncStatePartition_exposes_candidate()
    {
        ISyncStatePartition p = new FakePartition();
        Assert.True(p.TryGetSyncStateCandidate(out var c));
        Assert.Equal("tok-1", c);
    }
}
