using Pz.Cli.Commands;
using Pz.Core.Dag;
using Pz.Diagnostics.Events;
using Pz.Engine.Artifacts;
using Pz.Engine.Execution;

namespace Pz.Cli.Tests;

/// <summary>Write scheduling of <see cref="SnapshotRunEvents"/>: a successful SinkWrite is durable before
/// its completion callback returns, other completions coalesce onto a background writer, and after
/// <see cref="SnapshotRunEvents.Flush"/> nothing written in the background can land after a direct
/// terminal write.</summary>
// Injected store failures print the latched warning to Console.Error, so this class serializes with the
// classes that capture it.
[Collection("console-and-env-serialized")]
public class SnapshotRunEventsTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    [Fact]
    public void Sink_commit_is_persisted_before_NodeCompleted_returns()
    {
        var store = new RecordingStore();
        using var events = NewEvents(store);

        events.NodeCompleted(Result("pipe_a", NodeKind.Pipeline));
        events.NodeCompleted(Result("sink_a", NodeKind.SinkWrite));

        var last = store.Writes[^1];
        Assert.Contains("sink_a", last.Ids);
        Assert.Equal("running", last.Status);
    }

    [Fact]
    public async Task Non_sink_completions_coalesce()
    {
        var store = new RecordingStore();
        store.BlockFirstWrite();
        using var events = NewEvents(store);

        var producer = Task.Run(() =>
        {
            for (var i = 0; i < 100; i++)
            {
                events.NodeCompleted(Result($"pipe_{i}", NodeKind.Pipeline));
            }
        });
        try
        {
            // Times out (rather than hangs) when completions block behind the pending write.
            await producer.WaitAsync(Bound);
        }
        finally
        {
            store.Release();
        }

        events.Flush();

        var writes = store.Writes;
        Assert.InRange(writes.Count, 1, 3);
        Assert.Equal(100, writes[^1].Count);
        Assert.Equal(Enumerable.Range(0, 100).Select(i => $"pipe_{i}"), writes[^1].Ids);
    }

    [Fact]
    public async Task No_background_write_lands_after_the_terminal_snapshot()
    {
        var store = new RecordingStore();
        store.BlockFirstWrite();
        using var events = NewEvents(store);

        var all = Enumerable.Range(0, 50).Select(i => Result($"pipe_{i}", NodeKind.Pipeline)).ToList();
        var producer = Task.Run(() =>
        {
            foreach (var result in all)
            {
                events.NodeCompleted(result);
            }
        });
        try
        {
            // Times out (rather than hangs) when completions block behind the pending write.
            await producer.WaitAsync(Bound);
        }
        finally
        {
            store.Release();
        }

        events.Flush();
        events.TryWriteSnapshot(all, "success");
        var countAfterTerminal = store.Writes.Count;
        events.Flush();

        var writes = store.Writes;
        Assert.Equal(countAfterTerminal, writes.Count);
        Assert.Equal("success", writes[^1].Status);
        Assert.Equal(50, writes[^1].Count);
        Assert.Single(writes, w => w.Status == "success");
    }

    [Fact]
    public void Completions_after_Flush_are_written_before_NodeCompleted_returns()
    {
        var store = new RecordingStore();
        using var events = NewEvents(store);

        events.NodeCompleted(Result("pipe_a", NodeKind.Pipeline));
        events.Flush();
        events.NodeCompleted(Result("pipe_b", NodeKind.Pipeline));

        Assert.Equal(["pipe_a", "pipe_b"], store.Writes[^1].Ids);
    }

    [Fact]
    public async Task Failed_sink_completions_coalesce_like_any_other_node()
    {
        var store = new RecordingStore();
        store.BlockFirstWrite();
        using var events = NewEvents(store);

        var producer = Task.Run(() =>
        {
            events.NodeCompleted(Result("pipe_a", NodeKind.Pipeline));
            events.NodeCompleted(Result("sink_a", NodeKind.SinkWrite, NodeStatus.Failed));
        });
        try
        {
            // Times out (rather than hangs) when completions block behind the pending write.
            await producer.WaitAsync(Bound);
        }
        finally
        {
            store.Release();
        }

        events.Flush();
        Assert.Equal(["pipe_a", "sink_a"], store.Writes[^1].Ids);
    }

    // The background write holding the write lock copied its snapshot before the sink completed, then
    // fails; the sink's own write must still run and succeed before NodeCompleted returns.
    [Fact]
    public async Task Sink_commit_is_persisted_even_when_an_in_flight_background_write_fails()
    {
        var store = new RecordingStore();
        store.BlockFirstWrite();
        store.Fail(1);
        using var events = NewEvents(store);

        events.NodeCompleted(Result("pipe_a", NodeKind.Pipeline));
        Assert.True(store.BlockedWriteEntered.Wait(Bound), "background write never started");

        var sink = Task.Run(() => events.NodeCompleted(Result("sink_a", NodeKind.SinkWrite)));
        store.Release();
        await sink.WaitAsync(Bound);

        Assert.Contains("sink_a", store.Persisted[^1].Ids);
    }

    // A failed write must not mark its state as persisted: the next write of the same state (here the
    // final background write at Flush) has to retry it rather than skip it.
    [Fact]
    public async Task A_failed_write_is_retried_by_the_next_write_of_the_same_state()
    {
        var store = new RecordingStore();
        store.BlockFirstWrite();
        store.Fail(2);
        using var events = NewEvents(store);

        events.NodeCompleted(Result("pipe_a", NodeKind.Pipeline));
        Assert.True(store.BlockedWriteEntered.Wait(Bound), "background write never started");

        // The sink waits for the blocked write to release the write lock, so its own write is store call 2.
        var sink = Task.Run(() => events.NodeCompleted(Result("sink_a", NodeKind.SinkWrite)));
        store.Release();
        await sink.WaitAsync(Bound);
        Assert.False(store.Writes[^1].Succeeded);

        events.Flush();

        Assert.Equal(["pipe_a", "sink_a"], store.Persisted[^1].Ids);
    }

    private static SnapshotRunEvents NewEvents(IRunArtifactStore store) =>
        new(store, "fixed-run-id", "2026-10-24T00:00:00.000Z");

    private static NodeResult Result(string name, NodeKind kind, NodeStatus status = NodeStatus.Success) =>
        new(new NodeId(name), kind, name, status, 1, TimeSpan.FromMilliseconds(1), null);

    private sealed record Write(string Status, int Count, IReadOnlyList<string> Ids, bool Succeeded);

    private sealed class RecordingStore : IRunArtifactStore
    {
        private readonly Lock _gate = new();
        private readonly List<Write> _writes = [];
        private readonly ManualResetEventSlim _release = new(initialState: true);
        private readonly HashSet<int> _failing = [];
        private bool _blockNext;
        private int _calls;

        /// <summary>Set once a blocked write has entered the store (after its snapshot was copied).</summary>
        public ManualResetEventSlim BlockedWriteEntered { get; } = new();

        /// <summary>Every recorded write, failed ones included.</summary>
        public IReadOnlyList<Write> Writes
        {
            get
            {
                lock (_gate)
                {
                    return [.. _writes];
                }
            }
        }

        public IReadOnlyList<Write> Persisted => [.. Writes.Where(w => w.Succeeded)];

        public void BlockFirstWrite()
        {
            _release.Reset();
            _blockNext = true;
        }

        /// <summary>Fails the store calls with these 1-based ordinals.</summary>
        public void Fail(params int[] ordinals)
        {
            lock (_gate)
            {
                _failing.UnionWith(ordinals);
            }
        }

        public void Release() => _release.Set();

        public void WriteSnapshot(string runId, string startedAtIso, IReadOnlyList<NodeResult> completed,
            string status, long? eventsDropped = null, RunPhasesPayload? phases = null,
            IReadOnlyList<Pz.DuckDb.LoadedExtension>? extensions = null)
        {
            bool block;
            bool fail;
            lock (_gate)
            {
                block = _blockNext;
                _blockNext = false;
                fail = _failing.Contains(++_calls);
            }

            if (block)
            {
                BlockedWriteEntered.Set();
                _release.Wait();
            }

            lock (_gate)
            {
                _writes.Add(new Write(status, completed.Count, [.. completed.Select(r => r.Name)], !fail));
            }

            if (fail)
            {
                throw new IOException("injected snapshot failure");
            }
        }

        public PriorRun? ReadLatest() => null;

        public IEnumerable<PriorRun> ReadAllNewestFirst() => [];

        public IReadOnlyList<RunCandidate> ListCandidates() => [];

        public void Delete(string runId) { }
    }
}
