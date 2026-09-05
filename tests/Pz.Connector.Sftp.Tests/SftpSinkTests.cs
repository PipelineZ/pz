using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Sftp.Tests;

/// <summary>Protocol tests for <see cref="SftpSink"/> (session dispatch, replace/append naming,
/// gate wiring) and <see cref="SftpPartitionedWriteSession"/> (grouping/slicing, commit-xor-abort,
/// sequential-not-concurrent dispatch, shared-fs disposal) -- all against
/// <see cref="FakeSftpFileSystem"/>, no live server.</summary>
public class SftpSinkTests
{
    private static readonly Schema TwoColumnSchema = new(
    [
        new Field("id", Int32Type.Default, nullable: true),
        new Field("name", StringType.Default, nullable: true),
    ], null);

    private static readonly Schema PartitionSchema = new(
    [
        new Field("id", Int32Type.Default, nullable: true),
        new Field("event_time", new TimestampType(TimeUnit.Microsecond, "UTC"), nullable: true),
    ], null);

    private static SftpSink NewSink(FakeSftpFileSystem fake, string? root) =>
        new(new SftpConnectionSettings("host", 22, "user", "pw", null, null, null, root), _ => fake);

    private static OutputSpec Output(string output, string mode, params (string Key, object? Value)[] options) =>
        new("sink", output, mode, "fail_on_change", options.ToDictionary(o => o.Key, o => o.Value));

    private static RecordBatch BuildBatch(int startId, params string[] names)
    {
        var idBuilder = new Int32Array.Builder();
        var nameBuilder = new StringArray.Builder();
        for (var i = 0; i < names.Length; i++)
        {
            idBuilder.Append(startId + i);
            nameBuilder.Append(names[i]);
        }

        return new RecordBatch(TwoColumnSchema, [idBuilder.Build(), nameBuilder.Build()], names.Length);
    }

    private static RecordBatch BuildPartitionBatch((int Id, DateTimeOffset When)[] rows)
    {
        var idBuilder = new Int32Array.Builder();
        var timeBuilder = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"));
        foreach (var (id, when) in rows)
        {
            idBuilder.Append(id);
            timeBuilder.Append(when);
        }

        return new RecordBatch(PartitionSchema, [idBuilder.Build(), timeBuilder.Build()], rows.Length);
    }

    // ---------- SftpSink: replace/append naming ----------

    [Fact]
    public async Task Replace_lands_a_stable_named_file()
    {
        var fake = new FakeSftpFileSystem();
        var sink = NewSink(fake, root: null);
        var spec = Output("orders", "replace", ("path", "dir"), ("format", "csv"));

        var session = await sink.BeginWriteAsync(spec, TwoColumnSchema, CancellationToken.None);
        using var batch = BuildBatch(1, "Alice");
        await session.WriteBatchAsync(batch, CancellationToken.None);
        await session.CommitAsync(CancellationToken.None);
        await session.DisposeAsync();

        Assert.True(fake.FileExists("dir/orders.csv"));
    }

    [Fact]
    public async Task Append_accumulates_two_guid_suffixed_files_across_two_sessions()
    {
        var fake = new FakeSftpFileSystem();
        var sink = NewSink(fake, root: null);
        var spec = Output("orders", "append", ("path", "dir"), ("format", "csv"));

        var session1 = await sink.BeginWriteAsync(spec, TwoColumnSchema, CancellationToken.None);
        using (var batch1 = BuildBatch(1, "Alice"))
        {
            await session1.WriteBatchAsync(batch1, CancellationToken.None);
        }

        await session1.CommitAsync(CancellationToken.None);
        await session1.DisposeAsync();

        var session2 = await sink.BeginWriteAsync(spec, TwoColumnSchema, CancellationToken.None);
        using (var batch2 = BuildBatch(2, "Bob"))
        {
            await session2.WriteBatchAsync(batch2, CancellationToken.None);
        }

        await session2.CommitAsync(CancellationToken.None);
        await session2.DisposeAsync();

        var landed = fake.ListFiles("dir", recursive: false)
            .Where(f => f.StartsWith("dir/orders-", StringComparison.Ordinal))
            .ToArray();
        Assert.Equal(2, landed.Length);
        Assert.All(landed, f => Assert.EndsWith(".csv", f, StringComparison.Ordinal));
        Assert.NotEqual(landed[0], landed[1]);
    }

    [Fact]
    public async Task Tsv_write_lands_a_tab_separated_file()
    {
        var fake = new FakeSftpFileSystem();
        var sink = NewSink(fake, root: null);
        var spec = Output("orders", "replace", ("path", "dir"), ("format", "tsv"));

        var session = await sink.BeginWriteAsync(spec, TwoColumnSchema, CancellationToken.None);
        using var batch = BuildBatch(1, "x");
        await session.WriteBatchAsync(batch, CancellationToken.None);
        await session.CommitAsync(CancellationToken.None);
        await session.DisposeAsync();

        Assert.True(fake.FileExists("dir/orders.tsv"));
        using var stream = fake.OpenRead("dir/orders.tsv");
        using var reader = new StreamReader(stream);
        Assert.Equal("id\tname\n1\tx\n", await reader.ReadToEndAsync());
    }

    [Fact]
    public async Task Json_array_write_is_PZ0361()
    {
        var fake = new FakeSftpFileSystem();
        var sink = NewSink(fake, root: null);
        var spec = Output("events", "replace", ("path", "dir"), ("format", "json"), ("layout", "array"));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(spec, TwoColumnSchema, CancellationToken.None));

        Assert.StartsWith("PZ0361: output 'events': json 'layout: array' is native-only", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Invalid_format_names_the_output()
    {
        var fake = new FakeSftpFileSystem();
        var sink = NewSink(fake, root: null);
        var spec = Output("orders", "replace", ("path", "dir"), ("format", "xml"));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(spec, TwoColumnSchema, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("orders", ex.Message, StringComparison.Ordinal);
        Assert.Empty(fake.Operations);   // validated before the connection is ever dialed
    }

    [Fact]
    public async Task TryGetNativeCopy_always_returns_false()
    {
        var fake = new FakeSftpFileSystem();
        var sink = NewSink(fake, root: null);
        var spec = Output("orders", "replace", ("path", "dir"), ("format", "csv"));

        Assert.False(sink.TryGetNativeCopy(spec, out var copy));
        Assert.Null(copy);
    }

    [Fact]
    public async Task TryGetNativeCopy_refuses_a_native_only_option_at_plan_time()
    {
        var fake = new FakeSftpFileSystem();
        var sink = NewSink(fake, root: null);
        var spec = Output("events", "replace", ("path", "dir"), ("format", "json"), ("layout", "array"));

        var ex = Assert.Throws<PzConnectorException>(() => sink.TryGetNativeCopy(spec, out _));
        Assert.StartsWith("PZ0361: output '", ex.Message, StringComparison.Ordinal);
        Assert.Empty(fake.Operations);   // refused before the connection is ever dialed
    }

    // ---------- SftpSink: partition_by fan-out (via the real dispatch path) ----------

    [Fact]
    public async Task Fan_out_lands_two_files_under_rendered_folders_sharing_the_object_name()
    {
        var fake = new FakeSftpFileSystem();
        var sink = NewSink(fake, root: "root");
        var spec = Output("orders", "replace",
            ("path", "out/{yyyy}/{MM}/{dd}/"), ("format", "csv"), ("partition_by", "event_time"));

        var session = await sink.BeginWriteAsync(spec, PartitionSchema, CancellationToken.None);
        var day12 = new DateTimeOffset(2026, 7, 12, 8, 0, 0, TimeSpan.Zero);
        var day13 = new DateTimeOffset(2026, 7, 13, 23, 0, 0, TimeSpan.Zero);
        using var batch = BuildPartitionBatch([(1, day12), (2, day13)]);
        await session.WriteBatchAsync(batch, CancellationToken.None);
        await session.CommitAsync(CancellationToken.None);
        await session.DisposeAsync();

        Assert.True(fake.FileExists("root/out/2026/07/12/orders.csv"));
        Assert.True(fake.FileExists("root/out/2026/07/13/orders.csv"));
    }

    [Fact]
    public async Task Partition_by_column_missing_from_schema_fails_fast_permanent_before_connecting()
    {
        var fake = new FakeSftpFileSystem();
        var sink = NewSink(fake, root: null);
        var spec = Output("orders", "replace",
            ("path", "out/{yyyy}/{MM}/{dd}/"), ("format", "csv"), ("partition_by", "event_time"));

        // TwoColumnSchema has {id, name} -- no event_time column for partition_by to route on.
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(spec, TwoColumnSchema, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("event_time", ex.Message, StringComparison.Ordinal);
        Assert.Contains("not present", ex.Message, StringComparison.Ordinal);
        Assert.Empty(fake.Operations);   // never connected -- validated before any resource opens
    }

    [Fact]
    public async Task Partition_by_non_timestamp_column_fails_fast_permanent_before_connecting()
    {
        var fake = new FakeSftpFileSystem();
        var sink = NewSink(fake, root: null);
        var spec = Output("orders", "replace",
            ("path", "out/{yyyy}/{MM}/{dd}/"), ("format", "csv"), ("partition_by", "id"));

        // 'id' exists but is int32, not a timestamp/date -- cannot drive calendar tokens.
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(spec, PartitionSchema, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("id", ex.Message, StringComparison.Ordinal);
        Assert.Contains("timestamp/date", ex.Message, StringComparison.Ordinal);
        Assert.Empty(fake.Operations);
    }

    [Fact]
    public async Task Fan_out_abort_aborts_every_opened_inner_session()
    {
        var fake = new FakeSftpFileSystem();
        var sink = NewSink(fake, root: null);
        var spec = Output("orders", "replace",
            ("path", "out/{yyyy}/{MM}/{dd}/"), ("format", "csv"), ("partition_by", "event_time"));

        var session = await sink.BeginWriteAsync(spec, PartitionSchema, CancellationToken.None);
        var day12 = new DateTimeOffset(2026, 7, 12, 8, 0, 0, TimeSpan.Zero);
        var day13 = new DateTimeOffset(2026, 7, 13, 23, 0, 0, TimeSpan.Zero);
        using var batch = BuildPartitionBatch([(1, day12), (2, day13)]);
        await session.WriteBatchAsync(batch, CancellationToken.None);

        await session.AbortAsync(CancellationToken.None);
        await session.DisposeAsync();

        // Both inner temp files were opened and then removed by abort -- nothing landed at either
        // final path, and both temps are gone.
        Assert.False(fake.FileExists("out/2026/07/12/orders.csv"));
        Assert.False(fake.FileExists("out/2026/07/13/orders.csv"));
        Assert.Contains(fake.Operations, op => op.StartsWith("open-write:out/2026/07/12/", StringComparison.Ordinal));
        Assert.Contains(fake.Operations, op => op.StartsWith("open-write:out/2026/07/13/", StringComparison.Ordinal));
        Assert.DoesNotContain(fake.Operations, op => op.StartsWith("rename:", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Gate_threads_into_every_inner_session_of_a_fan_out()
    {
        var fake = new FakeSftpFileSystem();
        var sink = NewSink(fake, root: null);
        var gate = new CountingOperationGate();
        ((IOperationGateAware)sink).UseOperationGate(gate);

        var spec = Output("orders", "replace",
            ("path", "out/{yyyy}/{MM}/{dd}/"), ("format", "csv"), ("partition_by", "event_time"));

        var session = await sink.BeginWriteAsync(spec, PartitionSchema, CancellationToken.None);
        var day12 = new DateTimeOffset(2026, 7, 12, 8, 0, 0, TimeSpan.Zero);
        var day13 = new DateTimeOffset(2026, 7, 13, 23, 0, 0, TimeSpan.Zero);
        using var batch = BuildPartitionBatch([(1, day12), (2, day13)]);
        await session.WriteBatchAsync(batch, CancellationToken.None);
        await session.CommitAsync(CancellationToken.None);
        await session.DisposeAsync();

        // One open_write + one commit_rename per folder -- the gate saw both inner sessions.
        Assert.Equal(2, gate.Labels.Count(l => l == "sftp.open_write"));
        Assert.Equal(2, gate.Labels.Count(l => l == "sftp.commit_rename"));
    }

    [Fact]
    public async Task Gate_threads_into_the_single_output_session_too()
    {
        var fake = new FakeSftpFileSystem();
        var sink = NewSink(fake, root: null);
        var gate = new CountingOperationGate();
        ((IOperationGateAware)sink).UseOperationGate(gate);

        var spec = Output("orders", "replace", ("path", "dir"), ("format", "csv"));
        var session = await sink.BeginWriteAsync(spec, TwoColumnSchema, CancellationToken.None);
        using var batch = BuildBatch(1, "Alice");
        await session.WriteBatchAsync(batch, CancellationToken.None);
        await session.CommitAsync(CancellationToken.None);
        await session.DisposeAsync();

        Assert.Equal(["sftp.open_write", "sftp.commit_rename"], gate.Labels);
    }

    // ---------- SftpPartitionedWriteSession: grouping/slicing/commit-abort/dispose, exercised directly ----------

    /// <summary>In-memory inner session: captures which id values land in it and how it was closed,
    /// so grouping/slicing can be exercised with no real write session underneath. Lockstep copy of
    /// AzureUniversalWriterTests.FakeInnerSession (tests/Pz.Connector.AzureBlob.Tests).</summary>
    private sealed class FakeInnerSession : ISinkWriteSession
    {
        public List<int> Ids { get; } = [];
        public int BatchCount { get; private set; }
        public bool Committed { get; private set; }
        public bool Aborted { get; private set; }
        public bool Disposed { get; private set; }

        public ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
        {
            var ids = (Int32Array)batch.Column(0);
            for (var i = 0; i < batch.Length; i++)
            {
                Ids.Add(ids.GetValue(i)!.Value);
            }

            BatchCount++;
            return ValueTask.CompletedTask;
        }

        public ValueTask<WriteResult> CommitAsync(CancellationToken ct)
        {
            Committed = true;
            return ValueTask.FromResult(new WriteResult(Ids.Count, BatchCount));
        }

        public ValueTask AbortAsync(CancellationToken ct)
        {
            Aborted = true;
            return ValueTask.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            Disposed = true;
            return ValueTask.CompletedTask;
        }
    }

    private sealed class DisposeTrackingFs(ISftpFileSystem inner) : ISftpFileSystem
    {
        public bool Disposed { get; private set; }
        public IEnumerable<string> ListFiles(string directory, bool recursive) => inner.ListFiles(directory, recursive);
        public Stream OpenRead(string path) => inner.OpenRead(path);
        public Stream OpenWrite(string path) => inner.OpenWrite(path);
        public void Rename(string oldPath, string newPath) => inner.Rename(oldPath, newPath);
        public void Delete(string path) => inner.Delete(path);
        public bool FileExists(string path) => inner.FileExists(path);
        public bool DirectoryExists(string path) => inner.DirectoryExists(path);
        public void CreateDirectories(string path) => inner.CreateDirectories(path);
        public void Dispose() => Disposed = true;
    }

    [Fact]
    public async Task Groups_rows_by_rendered_folder_and_commits_every_partition()
    {
        var opened = new Dictionary<string, FakeInnerSession>(StringComparer.Ordinal);
        ValueTask<ISinkWriteSession> Open(string folder)
        {
            var session = new FakeInnerSession();
            opened[folder] = session;
            return ValueTask.FromResult<ISinkWriteSession>(session);
        }

        var session = new SftpPartitionedWriteSession(
            Open, new FakeSftpFileSystem(), "out/{yyyy}/{MM}/{dd}/", partitionColIndex: 1, PartitionSchema);

        var day12 = new DateTimeOffset(2026, 7, 12, 8, 0, 0, TimeSpan.Zero);
        var day13 = new DateTimeOffset(2026, 7, 13, 23, 30, 0, TimeSpan.Zero);
        using var batch = BuildPartitionBatch([(1, day12), (2, day13), (3, day12)]);
        await session.WriteBatchAsync(batch, CancellationToken.None);

        Assert.Equal(2, opened.Count);
        Assert.Equal([1, 3], opened["out/2026/07/12/"].Ids);
        Assert.Equal([2], opened["out/2026/07/13/"].Ids);

        var result = await session.CommitAsync(CancellationToken.None);
        Assert.Equal(3, result.RowsWritten);
        Assert.Equal(2, result.BatchesWritten);
        Assert.All(opened.Values, s => Assert.True(s.Committed));

        await session.DisposeAsync();
        Assert.All(opened.Values, s => Assert.True(s.Disposed));

        // Commit-xor-abort: reuse after commit is rejected.
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await session.AbortAsync(CancellationToken.None));
    }

    [Fact]
    public async Task Abort_aborts_every_opened_partition_without_committing_any()
    {
        var opened = new List<FakeInnerSession>();
        ValueTask<ISinkWriteSession> Open(string folder)
        {
            var session = new FakeInnerSession();
            opened.Add(session);
            return ValueTask.FromResult<ISinkWriteSession>(session);
        }

        var session = new SftpPartitionedWriteSession(
            Open, new FakeSftpFileSystem(), "out/{yyyy}/{MM}/{dd}/", partitionColIndex: 1, PartitionSchema);

        var day12 = new DateTimeOffset(2026, 7, 12, 8, 0, 0, TimeSpan.Zero);
        var day13 = new DateTimeOffset(2026, 7, 13, 1, 0, 0, TimeSpan.Zero);
        using var batch = BuildPartitionBatch([(1, day12), (2, day13)]);
        await session.WriteBatchAsync(batch, CancellationToken.None);

        await session.AbortAsync(CancellationToken.None);

        Assert.Equal(2, opened.Count);
        Assert.All(opened, s => Assert.True(s.Aborted));
        Assert.All(opened, s => Assert.False(s.Committed));
    }

    [Fact]
    public async Task Dispose_disposes_the_shared_fs_only_after_every_inner_session_is_disposed()
    {
        var tracker = new DisposeTrackingFs(new FakeSftpFileSystem());
        var opened = new List<FakeInnerSession>();
        ValueTask<ISinkWriteSession> Open(string folder)
        {
            var session = new FakeInnerSession();
            opened.Add(session);
            return ValueTask.FromResult<ISinkWriteSession>(session);
        }

        var session = new SftpPartitionedWriteSession(
            Open, tracker, "out/{yyyy}/{MM}/{dd}/", partitionColIndex: 1, PartitionSchema);

        var day12 = new DateTimeOffset(2026, 7, 12, 8, 0, 0, TimeSpan.Zero);
        using var batch = BuildPartitionBatch([(1, day12)]);
        await session.WriteBatchAsync(batch, CancellationToken.None);
        await session.CommitAsync(CancellationToken.None);

        Assert.False(tracker.Disposed);
        await session.DisposeAsync();

        Assert.True(opened[0].Disposed);
        Assert.True(tracker.Disposed);
    }

    /// <summary>Blocks inside <see cref="WriteBatchAsync"/> until <paramref name="release"/> completes --
    /// lets a test prove a second folder is never even opened while the first folder's write is still
    /// in flight (sequential dispatch), the mirror image of AzureUniversalWriterTests'
    /// concurrent-dispatch proof.</summary>
    private sealed class BlockingInnerSession(Task release) : ISinkWriteSession
    {
        public ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct) => new(release);
        public ValueTask<WriteResult> CommitAsync(CancellationToken ct) => ValueTask.FromResult(new WriteResult(0, 0));
        public ValueTask AbortAsync(CancellationToken ct) => ValueTask.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    [Fact]
    public async Task Fan_out_opens_the_second_folder_only_after_the_first_folders_write_completes()
    {
        var release = new TaskCompletionSource();
        var openedFolders = new List<string>();

        ValueTask<ISinkWriteSession> Open(string folder)
        {
            openedFolders.Add(folder);
            return ValueTask.FromResult<ISinkWriteSession>(new BlockingInnerSession(release.Task));
        }

        var session = new SftpPartitionedWriteSession(
            Open, new FakeSftpFileSystem(), "out/{yyyy}/{MM}/{dd}/", partitionColIndex: 1, PartitionSchema);

        var day12 = new DateTimeOffset(2026, 7, 12, 8, 0, 0, TimeSpan.Zero);
        var day13 = new DateTimeOffset(2026, 7, 13, 1, 0, 0, TimeSpan.Zero);
        using var batch = BuildPartitionBatch([(1, day12), (2, day13)]);

        // Not awaited: the write loop runs synchronously up to its first genuine suspension point --
        // day12's WriteBatchAsync, blocked on `release` -- so by the time this call returns control
        // here, exactly one folder has been opened. Task.WhenAll fan-out would have opened both before
        // suspending on either.
        var writeTask = session.WriteBatchAsync(batch, CancellationToken.None);

        Assert.Single(openedFolders);
        Assert.Equal("out/2026/07/12/", openedFolders[0]);

        release.SetResult();
        await writeTask;

        Assert.Equal(2, openedFolders.Count);
    }
}
