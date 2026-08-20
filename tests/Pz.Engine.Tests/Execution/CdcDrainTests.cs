using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.DuckDb;
using Pz.Engine.Execution;

namespace Pz.Engine.Tests.Execution;

/// <summary>Engine drain for cdc: after the upsert drain loop
/// finishes, a cdc-fed <c>on_delete: delete</c>/<c>soft</c> output streams delete-key batches
/// (exactly the output's merge <see cref="OutputDef.Keys"/>, in declaration order) through
/// <see cref="IDeleteApplyingWriteSession.ApplyDeleteKeysAsync"/> before Commit --
/// <c>on_delete: ignore</c> and an empty <c>__deletes</c> both skip the delete drain entirely. A
/// delete-apply failure routes through the same Abort path as an upsert-batch failure. The
/// PZ0340 guard (merge keys present and non-null in <c>__deletes</c>) runs before
/// <c>BeginWriteAsync</c>, so a doomed drain never opens a session. Harness mirrors
/// <see cref="SinkCheckpointTests"/> (stub sink recording session) plus
/// <see cref="CdcLandingTests"/>'s real-staging setup for the <c>__deletes</c> side table.</summary>
public sealed class CdcDrainTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-cdc-drain-tests", Guid.NewGuid().ToString("N"));
    private DuckSession _duck = null!;

    private const string Canonical = "staging.stg_orders";
    private const string Deletes = "staging.src_src__orders__deletes";

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _duck = DuckSession.Open(Path.Combine(_dir, "staging.duckdb"));
        await _duck.ExecuteAsync("create schema if not exists staging");
        await _duck.ExecuteAsync($"create table {Canonical} as select range as id, 'x' as name from range(3)");
    }

    public async Task DisposeAsync()
    {
        await _duck.DisposeAsync();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private RunContext Context(ISinkConnector sink) => new(
        _duck, Registry(sink), new RunPaths(_dir, "test-run"), NullRunEvents.Instance,
        Batch: new BatchOptions(TargetBatchBytes: 256));

    private static ConnectorRegistry Registry(ISinkConnector sink)
    {
        var reg = new ConnectorRegistry();
        reg.AddSink("cdcdelete", sink);
        return reg;
    }

    /// <summary><paramref name="onDelete"/> null/"ignore"/"delete"/"soft". <paramref name="withOrigin"/>
    /// false omits <see cref="SinkOutputDef.CdcDeleteOrigin"/> entirely (not exercised by these tests,
    /// kept for completeness of the guard shape).</summary>
    private static DagNode SinkNode(string? onDelete, bool withOrigin = true)
    {
        var sink = new ConnectionDef("cap", "cdcdelete", new Dictionary<string, object?>(), [],
            "sinks/cap.yml") { Outputs = [new OutputDef("out", "stg_orders", "merge", "fail_on_change", new Dictionary<string, object?>(),
                ["id"], OnDelete: onDelete)] };
        var def = new SinkOutputDef(sink, sink.Outputs[0]);
        if (withOrigin)
        {
            def = def with { CdcDeleteOrigin = new CdcOrigin("src", "orders") };
        }

        return new DagNode(new NodeId("ffffffffffffffff"), NodeKind.SinkWrite, "cap.out", [], null, def);
    }

    [Fact]
    public async Task Upsert_batches_precede_delete_batches_which_precede_commit()
    {
        await _duck.ExecuteAsync($"create table {Deletes} as select * from (values (10), (11)) t(id)");
        var connector = new DeleteApplyingSinkConnector();

        var result = await new SinkWriteExecutor().ExecuteAsync(SinkNode("delete"), Context(connector), default);

        Assert.Equal(NodeStatus.Success, result.Status);
        var session = connector.Session!;
        Assert.Equal(["write", "delete", "commit"], session.Calls);
        Assert.Single(session.DeleteBatchSchemas);
        Assert.Equal(["id"], session.DeleteBatchSchemas[0].FieldsList.Select(f => f.Name));
        Assert.Equal(3, session.UpsertRows);
        Assert.Equal(2, session.DeleteRows);
        // Deletes never add to RowsMoved -- it stays "rows in the output" (the 3 upserted rows).
        Assert.Equal(3, result.RowsMoved);
    }

    [Fact]
    public async Task OnDelete_soft_also_drains_deletes()
    {
        await _duck.ExecuteAsync($"create table {Deletes} as select * from (values (10)) t(id)");
        var connector = new DeleteApplyingSinkConnector();

        var result = await new SinkWriteExecutor().ExecuteAsync(SinkNode("soft"), Context(connector), default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(["write", "delete", "commit"], connector.Session!.Calls);
    }

    [Fact]
    public async Task OnDelete_ignore_never_drains_deletes_even_with_rows_present()
    {
        await _duck.ExecuteAsync($"create table {Deletes} as select * from (values (10)) t(id)");
        var connector = new DeleteApplyingSinkConnector();

        var result = await new SinkWriteExecutor().ExecuteAsync(SinkNode("ignore"), Context(connector), default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(["write", "commit"], connector.Session!.Calls);
        Assert.Empty(connector.Session!.DeleteBatchSchemas);
    }

    [Fact]
    public async Task Empty_deletes_relation_drains_zero_delete_batches()
    {
        await _duck.ExecuteAsync($"create table {Deletes} (id bigint)");
        var connector = new DeleteApplyingSinkConnector();

        var result = await new SinkWriteExecutor().ExecuteAsync(SinkNode("delete"), Context(connector), default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(["write", "commit"], connector.Session!.Calls);
    }

    [Fact]
    public async Task Missing_merge_key_column_fails_PZ0340_before_opening_a_session()
    {
        await _duck.ExecuteAsync($"create table {Deletes} as select 1 as not_id");
        var connector = new DeleteApplyingSinkConnector();

        var result = await new SinkWriteExecutor().ExecuteAsync(SinkNode("delete"), Context(connector), default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.NotNull(result.Error);
        Assert.Equal(PzErrorCode.CdcDeleteKeysUnavailable, result.Error!.Code);
        Assert.Contains("out", result.Error.Message);
        Assert.Contains("id", result.Error.Message);
        Assert.Equal("on_delete: delete|soft requires the merge keys to flow unchanged from the cdc dataset " +
            "(rename them in the pipeline's upsert view only)", result.Error.Hint);
        Assert.Null(connector.Session); // no session opened -- guard ran before BeginWriteAsync
    }

    [Fact]
    public async Task Null_merge_key_value_fails_PZ0340_before_opening_a_session()
    {
        await _duck.ExecuteAsync($"create table {Deletes} as select CAST(NULL AS BIGINT) as id");
        var connector = new DeleteApplyingSinkConnector();

        var result = await new SinkWriteExecutor().ExecuteAsync(SinkNode("delete"), Context(connector), default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.NotNull(result.Error);
        Assert.Equal(PzErrorCode.CdcDeleteKeysUnavailable, result.Error!.Code);
        Assert.Contains("out", result.Error.Message);
        Assert.Contains("id", result.Error.Message);
        Assert.Equal("on_delete: delete|soft requires the merge keys to flow unchanged from the cdc dataset " +
            "(rename them in the pipeline's upsert view only)", result.Error.Hint);
        Assert.Null(connector.Session);
    }

    [Fact]
    public async Task Delete_apply_failure_aborts_and_never_commits()
    {
        await _duck.ExecuteAsync($"create table {Deletes} as select * from (values (10)) t(id)");
        var connector = new DeleteApplyingSinkConnector(throwOnApplyDeletes: true);

        await Assert.ThrowsAsync<PzConnectorException>(
            () => new SinkWriteExecutor().ExecuteAsync(SinkNode("delete"), Context(connector), default));

        var session = connector.Session!;
        Assert.Contains("delete", session.Calls);
        Assert.DoesNotContain("commit", session.Calls);
        Assert.True(session.Aborted);
    }

    [Fact]
    public async Task Session_not_implementing_delete_interface_fails_as_connector_defect()
    {
        await _duck.ExecuteAsync($"create table {Deletes} as select * from (values (10)) t(id)");
        var connector = new PlainSinkConnector();

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => new SinkWriteExecutor().ExecuteAsync(SinkNode("delete"), Context(connector), default));

        Assert.Contains("IDeleteApplyingWriteSession", ex.Message);
        Assert.True(connector.Session!.Aborted); // write-phase failure -> existing Abort contract
    }

    [Fact]
    public async Task Checkpointing_session_composed_with_delete_drain_fails_as_connector_defect()
    {
        await _duck.ExecuteAsync($"create table {Deletes} as select * from (values (10)) t(id)");
        var connector = new CheckpointingDeleteSinkConnector();

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => new SinkWriteExecutor().ExecuteAsync(SinkNode("delete"), Context(connector), default));

        Assert.Contains("ICheckpointingSinkSession", ex.Message);
    }
}

/// <summary>Records the call order (WriteBatchAsync as "write", ApplyDeleteKeysAsync as "delete",
/// CommitAsync as "commit") and each delete-key batch's schema, so the drain ordering guarantee and
/// the exact-merge-keys-in-declaration-order contract can be asserted directly. <c>throwOnApplyDeletes</c>
/// models a connector-side delete failure.</summary>
internal sealed class DeleteApplyingSinkConnector(bool throwOnApplyDeletes = false) : ISinkConnector, ISink
{
    public RecordingSession? Session { get; private set; }

    public ConnectorInfo Info => new("cdcdelete", "0.1.0", ProtocolVersion.Major);
    public ConnectorCapabilities Capabilities => ConnectorCapabilities.Merge | ConnectorCapabilities.ApplyDeletes;
    public string ConnectionConfigSchema => "{}";
    public string DatasetConfigSchema => "{}";
    public AbortSemantics AbortSemantics => AbortSemantics.DiscardsAll;

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
        new(ValidationResult.Success);
    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new ConnectionCheck(true));
    public ValueTask<ISink> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(this);

    public bool TryGetNativeCopy(OutputSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeCopy? copy)
    {
        copy = null;
        return false;
    }

    public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct)
    {
        Session = new RecordingSession(throwOnApplyDeletes);
        return new(Session);
    }

    public ValueTask DisposeAsync() => default;
}

internal sealed class RecordingSession(bool throwOnApplyDeletes) : IDeleteApplyingWriteSession
{
    public List<string> Calls { get; } = [];
    public List<Schema> DeleteBatchSchemas { get; } = [];
    public long UpsertRows { get; private set; }
    public long DeleteRows { get; private set; }
    public bool Aborted { get; private set; }

    public ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
    {
        Calls.Add("write");
        UpsertRows += batch.Length;
        return ValueTask.CompletedTask;
    }

    public ValueTask ApplyDeleteKeysAsync(RecordBatch keyBatch, CancellationToken ct)
    {
        Calls.Add("delete");
        DeleteBatchSchemas.Add(keyBatch.Schema);
        DeleteRows += keyBatch.Length;
        if (throwOnApplyDeletes)
        {
            throw new PzConnectorException("delete apply failed", isTransient: false);
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask<WriteResult> CommitAsync(CancellationToken ct)
    {
        Calls.Add("commit");
        return new(new WriteResult(UpsertRows, Calls.Count(c => c == "write")));
    }

    public ValueTask AbortAsync(CancellationToken ct)
    {
        Aborted = true;
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync() => default;
}

/// <summary>A sink whose session implements only <see cref="ISinkWriteSession"/> -- neither
/// <see cref="IDeleteApplyingWriteSession"/> nor <see cref="ICheckpointingSinkSession"/> -- exercising
/// the runtime defense-in-depth check (behind the planner's PZ0339 gate, never reached in this
/// engine-level test).</summary>
internal sealed class PlainSinkConnector : ISinkConnector, ISink
{
    public PlainSession? Session { get; private set; }

    public ConnectorInfo Info => new("cdcdelete", "0.1.0", ProtocolVersion.Major);
    public ConnectorCapabilities Capabilities => ConnectorCapabilities.Merge;
    public string ConnectionConfigSchema => "{}";
    public string DatasetConfigSchema => "{}";
    public AbortSemantics AbortSemantics => AbortSemantics.DiscardsAll;

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
        new(ValidationResult.Success);
    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new ConnectionCheck(true));
    public ValueTask<ISink> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(this);

    public bool TryGetNativeCopy(OutputSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeCopy? copy)
    {
        copy = null;
        return false;
    }

    public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct)
    {
        Session = new PlainSession();
        return new(Session);
    }

    public ValueTask DisposeAsync() => default;
}

internal sealed class PlainSession : ISinkWriteSession
{
    public bool Aborted { get; private set; }

    public ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct) => ValueTask.CompletedTask;

    public ValueTask<WriteResult> CommitAsync(CancellationToken ct) => new(new WriteResult(0, 0));

    public ValueTask AbortAsync(CancellationToken ct)
    {
        Aborted = true;
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync() => default;
}

/// <summary>A session implementing BOTH <see cref="ICheckpointingSinkSession"/> and
/// <see cref="IDeleteApplyingWriteSession"/> -- a shape no first-party sink has, but the engine
/// must still refuse defensively rather than silently pick one behavior.</summary>
internal sealed class CheckpointingDeleteSinkConnector : ISinkConnector, ISink
{
    public ConnectorInfo Info => new("cdcdelete", "0.1.0", ProtocolVersion.Major);
    public ConnectorCapabilities Capabilities => ConnectorCapabilities.Merge | ConnectorCapabilities.ApplyDeletes |
        ConnectorCapabilities.CheckpointableWrites;
    public string ConnectionConfigSchema => "{}";
    public string DatasetConfigSchema => "{}";
    public AbortSemantics AbortSemantics => AbortSemantics.None;

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
        new(ValidationResult.Success);
    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new ConnectionCheck(true));
    public ValueTask<ISink> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(this);

    public bool TryGetNativeCopy(OutputSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeCopy? copy)
    {
        copy = null;
        return false;
    }

    public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct) =>
        new(new Session());

    public ValueTask DisposeAsync() => default;

    private sealed class Session : ICheckpointingSinkSession, IDeleteApplyingWriteSession
    {
        public bool TryResumeFrom(long acknowledgedRows) => false;

        public bool TryGetAcknowledgedRows(out long acknowledgedRows)
        {
            acknowledgedRows = 0;
            return false;
        }

        public ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct) => ValueTask.CompletedTask;

        public ValueTask ApplyDeleteKeysAsync(RecordBatch keyBatch, CancellationToken ct) => ValueTask.CompletedTask;

        public ValueTask<WriteResult> CommitAsync(CancellationToken ct) => new(new WriteResult(0, 0));

        public ValueTask AbortAsync(CancellationToken ct) => ValueTask.CompletedTask;

        public ValueTask DisposeAsync() => default;
    }
}
