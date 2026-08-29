using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;
using Renci.SshNet.Common;
using Sylvan.Data.Csv;

namespace Pz.Connector.Sftp.Tests;

/// <summary>Protocol tests for <see cref="SftpWriteSessionBase"/> and its three format sessions:
/// temp-upload + rename-promote commit, best-effort abort, the Open/Committed/Aborted state machine
/// (parity with <c>LocalFileWriteSessionBase</c>, connectors/Pz.Connector.LocalFiles/LocalFilesSink.cs),
/// stale-temp sweep, and gated op labels — all against <see cref="FakeSftpFileSystem"/> and
/// <see cref="CountingOperationGate"/>, no live server. Most protocol assertions run against the csv
/// session only: the state machine, commit/abort/gate wiring, and sweep all live on the shared base
/// class, so one format exercises them fully; csv/json/parquet each get their own round-trip test for
/// the format-specific serialization.</summary>
public class SftpWriteSessionTests
{
    private static readonly Schema TwoColumnSchema = new(
    [
        new Field("id", Int32Type.Default, nullable: true),
        new Field("name", StringType.Default, nullable: true),
    ], null);

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

    private static Task<SftpCsvWriteSession> CreateCsvAsync(
        ISftpFileSystem fs, string tempPath, string finalPath, IOperationGate? gate, bool ownsFileSystem = false) =>
        SftpCsvWriteSession.CreateAsync(fs, ownsFileSystem, tempPath, finalPath, TwoColumnSchema, gate, "sftp csv sink test", CancellationToken.None);

    // ---------- commit / abort / dispose protocol (csv exercises the shared base for all formats) ----------

    [Fact]
    public async Task Commit_lands_final_content_and_removes_temp()
    {
        var fs = new FakeSftpFileSystem();
        var gate = new CountingOperationGate();
        var session = await CreateCsvAsync(fs, "out/.pz-tmp-a-orders.csv", "out/orders.csv", gate);

        using var batch = BuildBatch(1, "Alice", "Bob");
        await session.WriteBatchAsync(batch, CancellationToken.None);
        var result = await session.CommitAsync(CancellationToken.None);
        await session.DisposeAsync();

        Assert.Equal(2, result.RowsWritten);
        Assert.Equal(1, result.BatchesWritten);
        Assert.True(fs.FileExists("out/orders.csv"));
        Assert.False(fs.FileExists("out/.pz-tmp-a-orders.csv"));
    }

    [Fact]
    public async Task Abort_leaves_no_trace()
    {
        var fs = new FakeSftpFileSystem();
        var gate = new CountingOperationGate();
        var session = await CreateCsvAsync(fs, "out/.pz-tmp-a-orders.csv", "out/orders.csv", gate);

        using var batch = BuildBatch(1, "Alice");
        await session.WriteBatchAsync(batch, CancellationToken.None);
        await session.AbortAsync(CancellationToken.None);
        await session.DisposeAsync();

        Assert.False(fs.FileExists("out/.pz-tmp-a-orders.csv"));
        Assert.False(fs.FileExists("out/orders.csv"));
        Assert.Contains("sftp.delete_temp", gate.Labels);
        Assert.DoesNotContain("sftp.commit_rename", gate.Labels);
    }

    [Fact]
    public async Task Dispose_without_commit_aborts()
    {
        var fs = new FakeSftpFileSystem();
        var gate = new CountingOperationGate();
        var session = await CreateCsvAsync(fs, "out/.pz-tmp-a-orders.csv", "out/orders.csv", gate);

        using var batch = BuildBatch(1, "Alice");
        await session.WriteBatchAsync(batch, CancellationToken.None);
        await session.DisposeAsync();

        Assert.False(fs.FileExists("out/.pz-tmp-a-orders.csv"));
        Assert.False(fs.FileExists("out/orders.csv"));
        Assert.Contains("sftp.delete_temp", gate.Labels);
    }

    [Fact]
    public async Task Dispose_after_failed_commit_leaves_temp_alone()
    {
        var fs = new FakeSftpFileSystem
        {
            FailOn = op => op.StartsWith("rename:", StringComparison.Ordinal)
                ? new SftpPermissionDeniedException("blocked")
                : null,
        };
        var gate = new CountingOperationGate();
        var session = await CreateCsvAsync(fs, "out/.pz-tmp-a-orders.csv", "out/orders.csv", gate);

        using var batch = BuildBatch(1, "Alice");
        await session.WriteBatchAsync(batch, CancellationToken.None);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await session.CommitAsync(CancellationToken.None));
        Assert.False(ex.IsTransient);

        // Dispose after a failed commit must not throw, and per Commit-xor-Abort must not run the
        // implicit abort either -- the temp's fate is unknown (the rename may or may not have landed),
        // so it is left exactly as commit left it.
        await session.DisposeAsync();

        Assert.True(fs.FileExists("out/.pz-tmp-a-orders.csv"));
        Assert.False(fs.FileExists("out/orders.csv"));
    }

    [Fact]
    public async Task Rename_failure_transient_exception_surfaces_transient()
    {
        var fs = new FakeSftpFileSystem
        {
            FailOn = op => op.StartsWith("rename:", StringComparison.Ordinal)
                ? new SshConnectionException("dropped")
                : null,
        };
        var gate = new CountingOperationGate();
        var session = await CreateCsvAsync(fs, "out/.pz-tmp-a-orders.csv", "out/orders.csv", gate);
        using var batch = BuildBatch(1, "Alice");
        await session.WriteBatchAsync(batch, CancellationToken.None);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await session.CommitAsync(CancellationToken.None));

        Assert.True(ex.IsTransient);
        await session.DisposeAsync();
    }

    [Fact]
    public async Task Rename_failure_permanent_exception_surfaces_permanent()
    {
        var fs = new FakeSftpFileSystem
        {
            FailOn = op => op.StartsWith("rename:", StringComparison.Ordinal)
                ? new SftpPathNotFoundException("missing")
                : null,
        };
        var gate = new CountingOperationGate();
        var session = await CreateCsvAsync(fs, "out/.pz-tmp-a-orders.csv", "out/orders.csv", gate);
        using var batch = BuildBatch(1, "Alice");
        await session.WriteBatchAsync(batch, CancellationToken.None);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await session.CommitAsync(CancellationToken.None));

        Assert.False(ex.IsTransient);
        await session.DisposeAsync();
    }

    [Fact]
    public async Task Replace_overwrites_a_preexisting_final_file()
    {
        var fs = new FakeSftpFileSystem();
        fs.Seed("out/orders.csv", "id,name\n99,Old\n"u8.ToArray());
        var gate = new CountingOperationGate();
        var session = await CreateCsvAsync(fs, "out/.pz-tmp-a-orders.csv", "out/orders.csv", gate);

        using var batch = BuildBatch(1, "Alice");
        await session.WriteBatchAsync(batch, CancellationToken.None);
        await session.CommitAsync(CancellationToken.None);
        await session.DisposeAsync();

        using var reader = new StreamReader(fs.OpenRead("out/orders.csv"));
        var text = await reader.ReadToEndAsync();
        Assert.DoesNotContain("Old", text, StringComparison.Ordinal);
        Assert.Contains("Alice", text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Stale_temp_for_the_same_output_is_swept_on_commit_a_different_outputs_temp_is_not()
    {
        var fs = new FakeSftpFileSystem();
        // A dead attempt's leftover temp for THIS output.
        fs.Seed("out/.pz-tmp-deadbeef-orders.csv", "id,name\n1,Stale\n"u8.ToArray());
        // A stale temp for a DIFFERENT output -- must survive the sweep.
        fs.Seed("out/.pz-tmp-cafebabe-products.csv", "id,name\n1,Other\n"u8.ToArray());

        var gate = new CountingOperationGate();
        var session = await CreateCsvAsync(fs, "out/.pz-tmp-abc123-orders.csv", "out/orders.csv", gate);

        using var batch = BuildBatch(1, "Alice");
        await session.WriteBatchAsync(batch, CancellationToken.None);
        await session.CommitAsync(CancellationToken.None);
        await session.DisposeAsync();

        Assert.True(fs.FileExists("out/orders.csv"));
        Assert.False(fs.FileExists("out/.pz-tmp-deadbeef-orders.csv"));
        Assert.True(fs.FileExists("out/.pz-tmp-cafebabe-products.csv"));
    }

    // ---------- gate wiring ----------

    [Fact]
    public async Task Gate_labels_are_exactly_the_three_documented_ops()
    {
        var fs = new FakeSftpFileSystem();
        var gate = new CountingOperationGate();

        var committed = await CreateCsvAsync(fs, "out/.pz-tmp-a-orders.csv", "out/orders.csv", gate);
        using (var b1 = BuildBatch(1, "Alice"))
        {
            await committed.WriteBatchAsync(b1, CancellationToken.None);
        }

        await committed.CommitAsync(CancellationToken.None);
        await committed.DisposeAsync();

        var aborted = await CreateCsvAsync(fs, "out/.pz-tmp-b-products.csv", "out/products.csv", gate);
        using (var b2 = BuildBatch(1, "Widget"))
        {
            await aborted.WriteBatchAsync(b2, CancellationToken.None);
        }

        await aborted.AbortAsync(CancellationToken.None);
        await aborted.DisposeAsync();

        Assert.Equal(
            ["sftp.open_write", "sftp.commit_rename", "sftp.open_write", "sftp.delete_temp"],
            gate.Labels);
    }

    [Fact]
    public async Task Works_with_no_gate()
    {
        var fs = new FakeSftpFileSystem();
        var session = await CreateCsvAsync(fs, "out/.pz-tmp-a-orders.csv", "out/orders.csv", gate: null);

        using var batch = BuildBatch(1, "Alice");
        await session.WriteBatchAsync(batch, CancellationToken.None);
        var result = await session.CommitAsync(CancellationToken.None);
        await session.DisposeAsync();

        Assert.Equal(1, result.RowsWritten);
        Assert.True(fs.FileExists("out/orders.csv"));
    }

    // ---------- ownsFileSystem ----------

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
    public async Task OwnsFileSystem_true_disposes_the_fs_after_the_state_machine_work()
    {
        var tracker = new DisposeTrackingFs(new FakeSftpFileSystem());
        var gate = new CountingOperationGate();
        var session = await SftpCsvWriteSession.CreateAsync(
            tracker, ownsFileSystem: true, "out/.pz-tmp-a-orders.csv", "out/orders.csv", TwoColumnSchema, gate,
            "ctx", CancellationToken.None);

        using var batch = BuildBatch(1, "Alice");
        await session.WriteBatchAsync(batch, CancellationToken.None);
        await session.CommitAsync(CancellationToken.None);

        Assert.False(tracker.Disposed);
        await session.DisposeAsync();
        Assert.True(tracker.Disposed);
    }

    [Fact]
    public async Task OwnsFileSystem_false_never_disposes_the_fs()
    {
        var tracker = new DisposeTrackingFs(new FakeSftpFileSystem());
        var gate = new CountingOperationGate();
        var session = await SftpCsvWriteSession.CreateAsync(
            tracker, ownsFileSystem: false, "out/.pz-tmp-a-orders.csv", "out/orders.csv", TwoColumnSchema, gate,
            "ctx", CancellationToken.None);

        using var batch = BuildBatch(1, "Alice");
        await session.WriteBatchAsync(batch, CancellationToken.None);
        await session.CommitAsync(CancellationToken.None);
        await session.DisposeAsync();

        Assert.False(tracker.Disposed);
    }

    // ---------- state machine parity with LocalFileWriteSessionBase ----------

    [Fact]
    public async Task Write_after_commit_throws()
    {
        var fs = new FakeSftpFileSystem();
        var gate = new CountingOperationGate();
        var session = await CreateCsvAsync(fs, "out/.pz-tmp-a-orders.csv", "out/orders.csv", gate);
        using var batch = BuildBatch(1, "Alice");
        await session.WriteBatchAsync(batch, CancellationToken.None);
        await session.CommitAsync(CancellationToken.None);

        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await session.WriteBatchAsync(batch, CancellationToken.None));

        await session.DisposeAsync();
    }

    [Fact]
    public async Task Abort_after_commit_throws()
    {
        var fs = new FakeSftpFileSystem();
        var gate = new CountingOperationGate();
        var session = await CreateCsvAsync(fs, "out/.pz-tmp-a-orders.csv", "out/orders.csv", gate);
        using var batch = BuildBatch(1, "Alice");
        await session.WriteBatchAsync(batch, CancellationToken.None);
        await session.CommitAsync(CancellationToken.None);

        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await session.AbortAsync(CancellationToken.None));

        await session.DisposeAsync();
    }

    [Fact]
    public async Task Commit_after_abort_throws()
    {
        var fs = new FakeSftpFileSystem();
        var gate = new CountingOperationGate();
        var session = await CreateCsvAsync(fs, "out/.pz-tmp-a-orders.csv", "out/orders.csv", gate);
        using var batch = BuildBatch(1, "Alice");
        await session.WriteBatchAsync(batch, CancellationToken.None);
        await session.AbortAsync(CancellationToken.None);

        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await session.CommitAsync(CancellationToken.None));

        await session.DisposeAsync();
    }

    // ---------- MakeTempPath ----------

    [Fact]
    public void MakeTempPath_follows_the_pz_tmp_naming_convention_SweepStaleTemps_recognizes()
    {
        var tempPath = SftpWriteSessionBase.MakeTempPath("out/orders.csv");

        Assert.StartsWith("out/.pz-tmp-", tempPath, StringComparison.Ordinal);
        Assert.EndsWith("-orders.csv", tempPath, StringComparison.Ordinal);
    }

    [Fact]
    public void MakeTempPath_is_unique_per_call()
    {
        var a = SftpWriteSessionBase.MakeTempPath("out/orders.csv");
        var b = SftpWriteSessionBase.MakeTempPath("out/orders.csv");

        Assert.NotEqual(a, b);
    }

    // ---------- format round-trips (read the fake's committed bytes back with the csv/parquet/json readers) ----------

    [Fact]
    public async Task Csv_round_trips_through_the_fake()
    {
        var fs = new FakeSftpFileSystem();
        var gate = new CountingOperationGate();
        var session = await CreateCsvAsync(fs, "out/.pz-tmp-a-orders.csv", "out/orders.csv", gate);

        using var batch = BuildBatch(1, "Alice", "Bob", "Carol");
        await session.WriteBatchAsync(batch, CancellationToken.None);
        await session.CommitAsync(CancellationToken.None);
        await session.DisposeAsync();

        using var textReader = new StreamReader(fs.OpenRead("out/orders.csv"));
        using var csvReader = CsvDataReader.Create(textReader);

        var batches = new List<RecordBatch>();
        await foreach (var b in CsvArrowReader.ReadAsync(
            csvReader, TwoColumnSchema, ["int", "varchar"], [0, 1], "out/orders.csv", BatchOptions.Default, 0,
            CancellationToken.None))
        {
            batches.Add(b);
        }

        Assert.Single(batches);
        var readBack = batches[0];
        Assert.Equal(3, readBack.Length);
        var ids = (Int32Array)readBack.Column(0);
        var names = (StringArray)readBack.Column(1);
        Assert.Equal([1, 2, 3], [ids.GetValue(0), ids.GetValue(1), ids.GetValue(2)]);
        Assert.Equal(["Alice", "Bob", "Carol"], [names.GetString(0), names.GetString(1), names.GetString(2)]);
    }

    [Fact]
    public async Task Json_round_trips_through_the_fake()
    {
        var fs = new FakeSftpFileSystem();
        var gate = new CountingOperationGate();
        var session = await SftpJsonWriteSession.CreateAsync(
            fs, ownsFileSystem: false, "out/.pz-tmp-a-orders.json", "out/orders.json", gate,
            "sftp json sink test", CancellationToken.None);

        using var batch = BuildBatch(1, "Alice", "Bob");
        await session.WriteBatchAsync(batch, CancellationToken.None);
        await session.CommitAsync(CancellationToken.None);
        await session.DisposeAsync();

        var columns = new Dictionary<string, string> { ["id"] = "int", ["name"] = "varchar" };
        var batches = new List<RecordBatch>();
        await foreach (var b in SftpJsonReader.ReadAsync(
            fs.OpenRead("out/orders.json"), columns, "test", BatchOptions.Default, CancellationToken.None))
        {
            batches.Add(b);
        }

        Assert.Single(batches);
        var readBack = batches[0];
        Assert.Equal(2, readBack.Length);
        var ids = (Int32Array)readBack.Column(0);
        var names = (StringArray)readBack.Column(1);
        Assert.Equal(1, ids.GetValue(0));
        Assert.Equal(2, ids.GetValue(1));
        Assert.Equal("Alice", names.GetString(0));
        Assert.Equal("Bob", names.GetString(1));
    }

    [Fact]
    public async Task Parquet_round_trips_through_the_fake()
    {
        var fs = new FakeSftpFileSystem();
        var gate = new CountingOperationGate();
        var session = await SftpParquetWriteSession.CreateAsync(
            fs, ownsFileSystem: false, "out/.pz-tmp-a-orders.parquet", "out/orders.parquet", TwoColumnSchema, gate,
            "sftp parquet sink test", CancellationToken.None);

        using var batch = BuildBatch(1, "Alice", "Bob");
        await session.WriteBatchAsync(batch, CancellationToken.None);
        await session.CommitAsync(CancellationToken.None);
        await session.DisposeAsync();

        var batches = new List<RecordBatch>();
        await foreach (var b in SftpParquetReader.ReadAsync(
            fs.OpenRead("out/orders.parquet"), null, "test", BatchOptions.Default, CancellationToken.None))
        {
            batches.Add(b);
        }

        Assert.Single(batches);
        var readBack = batches[0];
        Assert.Equal(2, readBack.Length);
        var ids = (Int32Array)readBack.Column(0);
        var names = (StringArray)readBack.Column(1);
        Assert.Equal(1, ids.GetValue(0));
        Assert.Equal(2, ids.GetValue(1));
        Assert.Equal("Alice", names.GetString(0));
        Assert.Equal("Bob", names.GetString(1));
    }

    [Fact]
    public async Task Parquet_decimal_column_fails_fast_before_touching_the_fs()
    {
        var schema = new Schema([new Field("amount", new Decimal128Type(18, 2), nullable: true)], null);
        var fs = new FakeSftpFileSystem();
        var gate = new CountingOperationGate();

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await SftpParquetWriteSession.CreateAsync(
            fs, ownsFileSystem: false, "out/.pz-tmp-a-amt.parquet", "out/amt.parquet", schema, gate,
            "ctx", CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("amount", ex.Message, StringComparison.Ordinal);
        Assert.Contains("csv or json", ex.Message, StringComparison.Ordinal);
        // Validated before any remote op -- open_write was never even attempted.
        Assert.Empty(gate.Labels);
    }
}
