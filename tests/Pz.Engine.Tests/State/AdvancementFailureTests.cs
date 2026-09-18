using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Execution;
using Pz.Engine.State;

namespace Pz.Engine.Tests.State;

/// <summary>Advancement runs after every sink has committed, so each dataset's write is its own event:
/// one store failure must cost that dataset its watermark and nothing else, and the caller must learn
/// exactly which datasets did not advance. Results are built by hand, as in
/// <see cref="SyncStateAdvancementTests"/> — the walk is a pure function of them.</summary>
public sealed class AdvancementFailureTests
{
    private static readonly string[] Datasets = ["a", "b", "c"];

    private static (CompiledDag Dag, List<NodeResult> Results) ThreeIncrementalDatasets()
    {
        var nodes = new List<DagNode>();
        var results = new List<NodeResult>();
        for (var i = 0; i < Datasets.Length; i++)
        {
            var id = new NodeId(new string((char)('a' + i), 16));
            var dataset = new DatasetDef(Datasets[i], new Dictionary<string, object?>(), null);
            var source = new ConnectionDef("crm", "inmemory", new Dictionary<string, object?>(), [dataset], "connections.yml");
            nodes.Add(new DagNode(id, NodeKind.SourceLoad, $"src_crm__{Datasets[i]}", [], null,
                new SourceDatasetDef(source, dataset)));
            results.Add(new NodeResult(id, NodeKind.SourceLoad, $"src_crm__{Datasets[i]}", NodeStatus.Success, 1,
                TimeSpan.Zero, null, WatermarkCandidate: new Watermark("updated_at", "BIGINT", "10", "run-1")));
        }

        return (new CompiledDag(nodes), results);
    }

    private static PzConfigException StoreError(string code) =>
        new(new PzError(code, $"store said {code}", "project.yml", null, null));

    /// <summary>Throws what <paramref name="failure"/> returns for a key, as often as it returns it.</summary>
    private sealed class FaultyStore(Func<string, int, Exception?> failure) : IKeyedStateStore<Watermark>
    {
        public readonly Dictionary<string, Watermark> Written = [];
        public readonly Dictionary<string, int> Attempts = [];

        public void Set(string key, Watermark value)
        {
            var attempt = Attempts[key] = Attempts.GetValueOrDefault(key) + 1;
            if (failure(key, attempt) is { } ex)
            {
                throw ex;
            }

            Written[key] = value;
        }

        public Watermark? Get(string key, Action<string>? notice = null) => Written.GetValueOrDefault(key);

        public IReadOnlyList<KeyValuePair<string, Watermark>>? ListAll(Action<string>? notice = null) => [.. Written];

        public void Remove(string key) => Written.Remove(key);
    }

    private static IReadOnlyList<AdvancementFailure> Advance(FaultyStore store, List<TimeSpan>? waits = null)
    {
        var (dag, results) = ThreeIncrementalDatasets();
        return WatermarkAdvancement.Advance(dag, results, new WatermarkStore(store), wait: d => waits?.Add(d));
    }

    [Fact]
    public void One_failing_dataset_does_not_stop_the_others_from_advancing()
    {
        var store = new FaultyStore((key, _) =>
            key == "crm.a" ? StoreError(PzErrorCode.StateConcurrencyConflict) : null);

        var failures = Advance(store);

        Assert.Equal(["crm.b", "crm.c"], store.Written.Keys.Order(StringComparer.Ordinal));
        var failure = Assert.Single(failures);
        Assert.Equal("crm.a", failure.Key);
        Assert.Contains(PzErrorCode.StateConcurrencyConflict, failure.Reason);
    }

    [Fact]
    public void Every_dataset_that_did_not_advance_is_reported()
    {
        var store = new FaultyStore((key, _) => key == "crm.b" ? null : new IOException("disk full"));

        var failures = Advance(store);

        Assert.Equal(["crm.a", "crm.c"], failures.Select(f => f.Key).Order(StringComparer.Ordinal));
        Assert.All(failures, f => Assert.Contains("disk full", f.Reason));
    }

    /// <summary>An unreachable store is the one failure worth a second try: the sinks have already
    /// committed, so giving up means re-extracting next run for the sake of one dropped connection.</summary>
    [Fact]
    public void An_unavailable_store_is_retried_and_the_dataset_still_advances()
    {
        var waits = new List<TimeSpan>();
        var store = new FaultyStore((key, attempt) =>
            key == "crm.a" && attempt <= 2 ? StoreError(PzErrorCode.StateStoreUnavailable) : null);

        var failures = Advance(store, waits);

        Assert.Empty(failures);
        Assert.Equal(3, store.Attempts["crm.a"]);
        Assert.Equal(2, waits.Count);
        Assert.Equal(3, store.Written.Count);
    }

    [Fact]
    public void A_store_that_stays_unavailable_is_given_up_on_after_three_attempts()
    {
        var store = new FaultyStore((key, _) =>
            key == "crm.a" ? StoreError(PzErrorCode.StateStoreUnavailable) : null);

        var failures = Advance(store, []);

        Assert.Equal("crm.a", Assert.Single(failures).Key);
        Assert.Equal(3, store.Attempts["crm.a"]);
        Assert.Equal(2, store.Written.Count);
    }

    /// <summary>A lost race says another run already advanced this dataset; trying again cannot win it.</summary>
    [Fact]
    public void A_concurrency_conflict_is_not_retried()
    {
        var store = new FaultyStore((key, _) =>
            key == "crm.a" ? StoreError(PzErrorCode.StateConcurrencyConflict) : null);

        Advance(store, []);

        Assert.Equal(1, store.Attempts["crm.a"]);
    }

    [Fact]
    public void Cancellation_is_never_recorded_as_a_failed_dataset()
    {
        var store = new FaultyStore((_, _) => new OperationCanceledException());

        Assert.Throws<OperationCanceledException>(() => Advance(store));
    }
}
