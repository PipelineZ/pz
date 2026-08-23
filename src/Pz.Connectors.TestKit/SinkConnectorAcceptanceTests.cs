using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;
using Xunit;

namespace Pz.Connectors.TestKit;

/// <summary>Acceptance contract every <see cref="ISinkConnector"/> must satisfy. Connector authors
/// subclass this, supplying fixtures via the abstract members below; the provided <c>[Fact]</c>s are the
/// executable spec — do not override them.</summary>
public abstract class SinkConnectorAcceptanceTests
{
    protected abstract ISinkConnector CreateSink();
    protected abstract ConnectorConfig ValidConfig { get; }
    protected abstract OutputSpec SmallOutput { get; }

    /// <summary>Read back what a committed session persisted, for content assertions. ROW ORDER IS NOT
    /// PART OF THIS CONTRACT: a destination with no inherent order (an object store, a table written as
    /// several files) may hand rows back in any order, and no fact below assumes otherwise — the
    /// property they test is that every row is there, not that it came back in insertion order.</summary>
    protected abstract ValueTask<IReadOnlyList<RecordBatch>> ReadCommittedAsync(ISinkConnector connector, OutputSpec spec);

    private static readonly Schema FixedSchema = new(
    [
        new Field("id", Int64Type.Default, nullable: false),
        new Field("name", StringType.Default, nullable: false),
    ], null);

    /// <summary>Invoked first by every <c>[SkippableFact]</c> below. No-op by
    /// default (InMemory/LocalFiles subclasses need no change); docker-backed subclasses override this
    /// with <c>DockerFacts.SkipUnlessDocker()</c> so the suite SKIPs cleanly instead of failing when
    /// docker is absent. It receives nothing identifying the caller, so it can only skip the suite as a
    /// whole — override <see cref="ShouldRun"/> to skip a subset.</summary>
    protected virtual void GateFact()
    {
    }

    /// <summary>Per-fact opt-out, by fact method name. Every fact runs by default, so an existing
    /// subclass is unaffected. Override to skip a SUBSET — a connector that cannot satisfy one fact for
    /// a structural reason no capability flag expresses would otherwise have to skip the whole suite
    /// through <see cref="GateFact"/> and lose the facts it does satisfy.</summary>
    protected virtual bool ShouldRun(string fact) => true;

    /// <summary>What each fact actually calls: the suite-wide <see cref="GateFact"/> first (so an
    /// override that skips on absent docker still runs first), then this fact's own
    /// <see cref="ShouldRun"/> verdict. <paramref name="fact"/> is filled in by the compiler with the
    /// calling fact's method name.</summary>
    private void Gate([CallerMemberName] string fact = "")
    {
        GateFact();
        Skip.IfNot(ShouldRun(fact), $"subclass excluded '{fact}' via ShouldRun");
    }

    /// <summary>A <c>mode: merge</c> output spec (non-empty <see
    /// cref="OutputSpec.Keys"/>) opts a connector into the <c>Merge_*</c> facts below; null (the default)
    /// mirrors <see cref="GetSpecWithPartitionOverride"/>'s null-hook precedent -- those facts become
    /// Skip-free no-ops, so sink subclasses that have not opted in (LocalFiles) keep compiling and
    /// passing.</summary>
    protected virtual OutputSpec? MergeOutput => null;

    /// <summary>Resets whatever backing store <see cref="MergeOutput"/> targets to a clean slate before
    /// each merge fact runs. No-op by default -- <see cref="CreateSink"/> already returns a fresh, empty
    /// store every call for the in-process reference connector -- but a docker-backed subclass whose
    /// merge target is a real, physically-persistent table overrides this to drop/recreate it, so
    /// re-running the acceptance suite (including twice in a row, to shake out flakiness) always starts
    /// from known state.</summary>
    protected virtual Task ResetMergeTargetAsync() => Task.CompletedTask;

    /// <summary>A <c>mode: replace</c> output spec opts a connector into the replace-honesty fact
    /// below; null (the default) makes it a Skip-free no-op — subclasses that have not opted in
    /// compile and pass unchanged (the MergeOutput precedent).</summary>
    protected virtual OutputSpec? ReplaceOutput => null;

    /// <summary>An output spec for the checkpoint-resume fact — only
    /// meaningful when the connector declares CheckpointableWrites. Null default: no-op.</summary>
    protected virtual OutputSpec? CheckpointOutput => null;

    [SkippableFact]
    public async Task Commit_persists_all_written_batches()
    {
        Gate();
        var connector = CreateSink();
        await using var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        await using var session = await sink.BeginWriteAsync(SmallOutput, FixedSchema, CancellationToken.None);

        var batches = BuildBatches(batchCount: 3, rowsPerBatch: 50);
        foreach (var batch in batches)
        {
            await session.WriteBatchAsync(batch, CancellationToken.None);
            batch.Dispose();
        }

        var result = await session.CommitAsync(CancellationToken.None);
        Assert.Equal(150, result.RowsWritten);

        var committed = await ReadCommittedAsync(connector, SmallOutput);
        Assert.Equal(150, committed.Sum(b => (long)b.Length));

        // Sorted, not indexed: the property is that all 150 rows survived the commit. Reading
        // committed[0] and committed[^1] as "first" and "last" would instead require an ordering
        // ReadCommittedAsync never promises, failing nondeterministically on any unordered destination.
        Assert.Equal(Enumerable.Range(0, 150).Select(i => (long)i), CommittedIds(committed).Order());
    }

    [SkippableFact]
    public async Task Abort_discards_everything()
    {
        Gate();
        var connector = CreateSink();
        await using var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        await using (var session = await sink.BeginWriteAsync(SmallOutput, FixedSchema, CancellationToken.None))
        {
            var batches = BuildBatches(batchCount: 2, rowsPerBatch: 10);
            foreach (var batch in batches)
            {
                await session.WriteBatchAsync(batch, CancellationToken.None);
                batch.Dispose();
            }

            await session.AbortAsync(CancellationToken.None);
        }

        var committed = await ReadCommittedAsync(connector, SmallOutput);
        // Emptiness is exactly the DiscardsAll contract. A BestEffort/None
        // sink's abort is still required to succeed (asserted above by not throwing), but what
        // remains visible is the destination's truth, not the TestKit's to assert generically.
        if (sink.AbortSemantics == AbortSemantics.DiscardsAll)
        {
            Assert.Empty(committed);
        }
    }

    [SkippableFact]
    public async Task Double_commit_is_rejected()
    {
        Gate();
        var connector = CreateSink();
        await using var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        await using var session = await sink.BeginWriteAsync(SmallOutput, FixedSchema, CancellationToken.None);

        await session.CommitAsync(CancellationToken.None);

        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await session.CommitAsync(CancellationToken.None));
    }

    [SkippableFact]
    public async Task Commit_after_abort_is_rejected()
    {
        Gate();
        var connector = CreateSink();
        await using var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        await using var session = await sink.BeginWriteAsync(SmallOutput, FixedSchema, CancellationToken.None);

        await session.AbortAsync(CancellationToken.None);

        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await session.CommitAsync(CancellationToken.None));
    }

    [SkippableFact]
    public async Task Sink_does_not_retain_engine_owned_batch_instances()
    {
        Gate();
        var connector = CreateSink();
        await using var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        await using var session = await sink.BeginWriteAsync(SmallOutput, FixedSchema, CancellationToken.None);

        var handed = BuildBatches(batchCount: 2, rowsPerBatch: 10);
        try
        {
            foreach (var batch in handed)
            {
                await session.WriteBatchAsync(batch, CancellationToken.None);

                // Engine-owned again the moment the call returns: the engine's pool refills these
                // buffers with the next batch's rows. Zeroing them is that refill, made deterministic —
                // a connector that kept the instance instead of copying out of it now holds rows of
                // zeros and commits recognisably wrong DATA. Disposal is deliberately left to the
                // finally below rather than done here: handing the memory back to the pool outright
                // would make a retaining connector's later read undefined instead of merely wrong, and
                // "undefined" is what let this fact pass connectors that were violating the protocol.
                Recycle(batch);
            }

            await session.CommitAsync(CancellationToken.None);

            var committed = await ReadCommittedAsync(connector, SmallOutput);
            foreach (var committedBatch in committed)
            {
                foreach (var handedBatch in handed)
                {
                    Assert.NotSame(handedBatch, committedBatch);
                }
            }

            // Content, not just instance identity: a connector that reads its rows back out of the
            // destination hands back fresh batches either way, so NotSame alone can never catch it.
            Assert.Equal(Enumerable.Range(0, 20).Select(i => (long)i), CommittedIds(committed).Order());
        }
        finally
        {
            foreach (var batch in handed)
            {
                batch.Dispose();
            }
        }
    }

    /// <summary>Overwrites every buffer a batch owns with zeros — what the engine's buffer pool does to
    /// a batch it has taken back. Zeros rather than a poison pattern on purpose: the result is still a
    /// structurally valid Arrow batch (in-range offsets, readable values), so a connector that retained
    /// it fails an assertion instead of faulting somewhere unhelpful.</summary>
    private static void Recycle(RecordBatch batch)
    {
        foreach (var array in batch.Arrays)
        {
            foreach (var buffer in array.Data.Buffers)
            {
                System.Runtime.InteropServices.MemoryMarshal.AsMemory(buffer.Memory).Span.Clear();
            }
        }
    }

    /// <summary>Every id column value across the committed batches, in whatever order the destination
    /// handed them back — callers sort. Nulls surface as a distinguishable sentinel rather than
    /// throwing, so a batch whose validity bitmap was zeroed fails as a value mismatch.</summary>
    private static IEnumerable<long> CommittedIds(IReadOnlyList<RecordBatch> batches)
    {
        foreach (var batch in batches)
        {
            var ids = (Int64Array)batch.Column(0);
            for (var i = 0; i < batch.Length; i++)
            {
                yield return ids.GetValue(i) ?? long.MinValue;
            }
        }
    }

    [SkippableFact]
    public async Task Replace_mode_overwrites_the_prior_commit()
    {
        Gate();
        var connector = CreateSink();
        if (ReplaceOutput is not { } output ||
            !connector.Capabilities.HasFlag(ConnectorCapabilities.ReplaceWrites))
        {
            return;
        }

        await using var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        await CommitRowsAsync(sink, output, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], "first");
        await CommitRowsAsync(sink, output, [0, 1, 2, 3, 4], "second");

        var committed = await ReadCommittedAsync(connector, output);
        Assert.Equal(5, committed.Sum(b => (long)b.Length));
    }

    [SkippableFact]
    public async Task Checkpointable_sessions_implement_the_interface()
    {
        Gate();
        var connector = CreateSink();
        if (!connector.Capabilities.HasFlag(ConnectorCapabilities.CheckpointableWrites) ||
            CheckpointOutput is not { } output)
        {
            return;
        }

        await using var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        await using var session = await sink.BeginWriteAsync(output, FixedSchema, CancellationToken.None);
        Assert.IsAssignableFrom<ICheckpointingSinkSession>(session);
    }

    [SkippableFact]
    public async Task Checkpoint_resume_delivers_strictly_after_the_acknowledged_prefix()
    {
        Gate();
        var connector = CreateSink();
        if (!connector.Capabilities.HasFlag(ConnectorCapabilities.CheckpointableWrites) ||
            CheckpointOutput is not { } output)
        {
            return;
        }

        await using var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None);

        long ack;
        await using (var first = await sink.BeginWriteAsync(output, FixedSchema, CancellationToken.None))
        {
            var checkpointing = Assert.IsAssignableFrom<ICheckpointingSinkSession>(first);
            foreach (var batch in BuildBatches(batchCount: 2, rowsPerBatch: 25))
            {
                await first.WriteBatchAsync(batch, CancellationToken.None);
                batch.Dispose();
            }

            Assert.True(checkpointing.TryGetAcknowledgedRows(out ack));
            Assert.InRange(ack, 1, 50);
            await first.AbortAsync(CancellationToken.None);
        }

        await using (var second = await sink.BeginWriteAsync(output, FixedSchema, CancellationToken.None))
        {
            var checkpointing = Assert.IsAssignableFrom<ICheckpointingSinkSession>(second);
            Skip.If(!checkpointing.TryResumeFrom(ack), "connector declined resume — nothing more to assert generically");

            foreach (var batch in BuildBatches(batchCount: 1, rowsPerBatch: (int)(100 - ack), startId: ack))
            {
                await second.WriteBatchAsync(batch, CancellationToken.None);
                batch.Dispose();
            }

            var result = await second.CommitAsync(CancellationToken.None);
            Assert.Equal(100, result.RowsWritten); // resumed prefix counts
        }

        var committed = await ReadCommittedAsync(connector, CheckpointOutput!);
        var ids = committed.SelectMany(b =>
        {
            var column = (Int64Array)b.Column(0);
            return Enumerable.Range(0, b.Length).Select(i => column.GetValue(i)!.Value);
        }).ToList();
        Assert.Equal(100, ids.Count);                       // no duplicate at the boundary
        Assert.Equal(Enumerable.Range(0, 100).Select(i => (long)i), ids.Order()); // no gap
    }

    [SkippableFact]
    public async Task Merge_upserts_by_keys()
    {
        Gate();
        if (MergeOutput is null)
        {
            Assert.True(true);
            return;
        }

        await ResetMergeTargetAsync();
        var mergeOutput = MergeOutput;
        var connector = CreateSink();
        await using var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None);

        await CommitRowsAsync(sink, mergeOutput, ids: [.. Enumerable.Range(0, 10)], namePrefix: "batch1");
        // Half the keys (5..9) overlap batch1; 10..14 are new -- non-key ("name") changes for every
        // overlapping key.
        await CommitRowsAsync(sink, mergeOutput, ids: [.. Enumerable.Range(5, 10)], namePrefix: "batch2");

        var committed = await ReadCommittedAsync(connector, mergeOutput);
        // RAW row count (not just the folded map below) is the decisive check: a sink that merely
        // appended both commits without deduping by key would still fold to the same 15-entry map once
        // read back through ToKeyedMap (last-one-wins), silently masking a broken merge -- but it would
        // hand back 20 raw rows (10 + 10), not 15.
        Assert.Equal(15, committed.Sum(b => (long)b.Length));

        var byId = ToKeyedMap(committed);
        Assert.Equal(15, byId.Count);
        for (var id = 0; id < 5; id++)
        {
            Assert.Equal($"batch1-{id}", byId[id]);
        }

        for (var id = 5; id < 15; id++)
        {
            Assert.Equal($"batch2-{id}", byId[id]);
        }
    }

    [SkippableFact]
    public async Task Merge_is_idempotent()
    {
        Gate();
        if (MergeOutput is null)
        {
            Assert.True(true);
            return;
        }

        await ResetMergeTargetAsync();
        var mergeOutput = MergeOutput;
        var connector = CreateSink();
        await using var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None);

        var ids = Enumerable.Range(0, 10).ToArray();
        await CommitRowsAsync(sink, mergeOutput, ids, namePrefix: "row");
        var firstCommitted = await ReadCommittedAsync(connector, mergeOutput);

        // Same batch, a SEPARATE session -- must reproduce the identical state, not double it. RAW row
        // count is the decisive check here (see Merge_upserts_by_keys): re-committing identical rows
        // folds to an identical map either way, but a naive append would double the raw row count.
        await CommitRowsAsync(sink, mergeOutput, ids, namePrefix: "row");
        var secondCommitted = await ReadCommittedAsync(connector, mergeOutput);

        Assert.Equal(10, firstCommitted.Sum(b => (long)b.Length));
        Assert.Equal(10, secondCommitted.Sum(b => (long)b.Length));

        var firstState = ToKeyedMap(firstCommitted);
        var secondState = ToKeyedMap(secondCommitted);
        Assert.Equal(firstState.Count, secondState.Count);
        foreach (var (id, name) in firstState)
        {
            Assert.Equal(name, secondState[id]);
        }
    }

    [SkippableFact]
    public async Task Merge_same_session_duplicate_keys_resolve_last_writer_wins()
    {
        Gate();
        if (MergeOutput is null)
        {
            Assert.True(true);
            return;
        }

        await ResetMergeTargetAsync();
        var mergeOutput = MergeOutput;
        var connector = CreateSink();
        await using var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None);

        // Same key (id: 0), two DIFFERENT non-key values, both written within a SINGLE session (as two
        // separate batches) -- this is exactly the shape that makes a single ON CONFLICT insert over the
        // whole temp table see the same key twice. The contract (InMemorySink's MergeRows.Absorb) is
        // last-writer-wins: the LATER batch's value must be what survives.
        await using (var session = await sink.BeginWriteAsync(mergeOutput, FixedSchema, CancellationToken.None))
        {
            await WriteRowAsync(session, id: 0, name: "first");
            await WriteRowAsync(session, id: 0, name: "second");
            await session.CommitAsync(CancellationToken.None);
        }

        var committed = await ReadCommittedAsync(connector, mergeOutput);
        // RAW row count is the decisive check (see Merge_upserts_by_keys): a finalize that errored, or one
        // that silently dropped the duplicate rather than resolving it, would not land at exactly 1 row.
        Assert.Equal(1, committed.Sum(b => (long)b.Length));

        var byId = ToKeyedMap(committed);
        Assert.Equal("second", byId[0]);
    }

    [SkippableFact]
    public async Task Merge_batch_missing_key_column_errors_cleanly()
    {
        Gate();
        if (MergeOutput is null)
        {
            Assert.True(true);
            return;
        }

        await ResetMergeTargetAsync();
        var mergeOutput = MergeOutput;
        var connector = CreateSink();
        await using var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None);

        var fieldsMissingKeys = FixedSchema.FieldsList.Where(f => !mergeOutput.Keys.Contains(f.Name)).ToArray();
        var schemaMissingKeys = new Schema(fieldsMissingKeys, null);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(mergeOutput, schemaMissingKeys, CancellationToken.None));

        Assert.False(ex.IsTransient);
        foreach (var key in mergeOutput.Keys)
        {
            Assert.Contains(key, ex.Message, StringComparison.Ordinal);
        }
    }

    /// <summary>Routing: a connector declaring <see cref="ConnectorCapabilities.GatedOperations"/>
    /// must implement <see cref="IOperationGateAware"/> on its <see cref="ISink"/> and route every
    /// remote write operation through the engine-supplied gate. Unlike the source-side twin there is
    /// no failure-injection fact here: injecting into an unknown first op would leave
    /// Commit-xor-Abort in an undefined state across arbitrary connectors, so the no-untracked-retry
    /// contract is proven source-side only.</summary>
    [SkippableFact]
    public async Task Gated_connector_routes_writes_through_gate()
    {
        Gate();
        var connector = CreateSink();
        Skip.If(!connector.Capabilities.HasFlag(ConnectorCapabilities.GatedOperations),
            "connector does not declare GatedOperations");

        await using var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        Assert.True(sink is IOperationGateAware,
            "a connector declaring GatedOperations must implement IOperationGateAware on its ISink");
        var gate = new CountingOperationGate();
        ((IOperationGateAware)sink).UseOperationGate(gate);

        await using var session = await sink.BeginWriteAsync(SmallOutput, FixedSchema, CancellationToken.None);
        var batch = BuildBatches(batchCount: 1, rowsPerBatch: 10)[0];
        await session.WriteBatchAsync(batch, CancellationToken.None);
        batch.Dispose();
        await session.CommitAsync(CancellationToken.None);

        Assert.True(gate.Calls >= 1, "a gated write produced zero gate operations");
    }

    /// <summary>Label hygiene: opLabel is a STATIC, connector-authored identifier -- never a
    /// URL, parameter, or any value derived from config/payloads (secret/PII hygiene binding
    /// convention). A URL scheme separator, query-string marker, or embedded space would betray a
    /// dynamic label.</summary>
    [SkippableFact]
    public async Task Gated_sink_op_labels_are_static_tokens()
    {
        Gate();
        var connector = CreateSink();
        Skip.If(!connector.Capabilities.HasFlag(ConnectorCapabilities.GatedOperations),
            "connector does not declare GatedOperations");

        await using var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        Assert.True(sink is IOperationGateAware,
            "a connector declaring GatedOperations must implement IOperationGateAware on its ISink");
        var gate = new CountingOperationGate();
        ((IOperationGateAware)sink).UseOperationGate(gate);

        await using var session = await sink.BeginWriteAsync(SmallOutput, FixedSchema, CancellationToken.None);
        var batch = BuildBatches(batchCount: 1, rowsPerBatch: 10)[0];
        await session.WriteBatchAsync(batch, CancellationToken.None);
        batch.Dispose();
        await session.CommitAsync(CancellationToken.None);

        foreach (var label in gate.Labels)
        {
            Assert.DoesNotContain("://", label);
            Assert.DoesNotContain("?", label);
            Assert.DoesNotContain(" ", label);
        }
    }

    /// <summary>Writes a single row as its own batch into an already-open session -- used to construct
    /// a SINGLE session containing multiple batches that share a merge key (<see
    /// cref="Merge_same_session_duplicate_keys_resolve_last_writer_wins"/>), unlike <see
    /// cref="CommitRowsAsync"/> which owns (and commits) an entire session for one batch of many ids.</summary>
    private static async Task WriteRowAsync(ISinkWriteSession session, long id, string name)
    {
        var builder = new ArrowBatchBuilder(FixedSchema);
        builder.AppendRow([id, name]);
        using var batch = builder.Flush()!;
        await session.WriteBatchAsync(batch, CancellationToken.None);
    }

    private static async Task CommitRowsAsync(ISink sink, OutputSpec output, IReadOnlyList<int> ids, string namePrefix)
    {
        await using var session = await sink.BeginWriteAsync(output, FixedSchema, CancellationToken.None);
        var builder = new ArrowBatchBuilder(FixedSchema);
        foreach (var id in ids)
        {
            builder.AppendRow([(long)id, $"{namePrefix}-{id}"]);
        }

        using var batch = builder.Flush()!;
        await session.WriteBatchAsync(batch, CancellationToken.None);
        await session.CommitAsync(CancellationToken.None);
    }

    /// <summary>Collapses committed batches (id: Int64, name: Utf8 -- <see cref="FixedSchema"/>) into a
    /// keyed map for content assertions; a later row for the same id overwrites an earlier one, though a
    /// correctly-merged store should never actually hand back duplicate ids.</summary>
    private static Dictionary<long, string> ToKeyedMap(IReadOnlyList<RecordBatch> batches)
    {
        var map = new Dictionary<long, string>();
        foreach (var batch in batches)
        {
            var ids = (Int64Array)batch.Column(0);
            var names = (StringArray)batch.Column(1);
            for (var i = 0; i < batch.Length; i++)
            {
                map[ids.GetValue(i)!.Value] = names.GetString(i);
            }
        }

        return map;
    }

    private static List<RecordBatch> BuildBatches(int batchCount, int rowsPerBatch, long startId = 0)
    {
        var batches = new List<RecordBatch>(batchCount);
        var nextId = startId;
        for (var b = 0; b < batchCount; b++)
        {
            var builder = new ArrowBatchBuilder(FixedSchema);
            for (var r = 0; r < rowsPerBatch; r++)
            {
                builder.AppendRow([nextId, $"row-{nextId}"]);
                nextId++;
            }

            batches.Add(builder.Flush()!);
        }

        return batches;
    }
}
