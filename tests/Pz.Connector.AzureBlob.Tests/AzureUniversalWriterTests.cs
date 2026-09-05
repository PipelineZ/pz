using Apache.Arrow;
using Apache.Arrow.Types;
using Parquet;
using Pz.Connector.AzureBlob;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.AzureBlob.Tests;

/// <summary>Offline (no docker) round-trip tests for <see cref="AzureBlobFormat"/>'s batch->bytes
/// serializers: write N batches of a 2-column schema (id:int64, name:utf8) to a <see cref="MemoryStream"/>,
/// read the bytes back independently, and assert row count + values. This exercises the exact serialization
/// bodies <see cref="AzureWriteSession"/> reuses, without needing Azurite.</summary>
public sealed class AzureUniversalWriterTests
{
    private static readonly Schema FixedSchema = new(
    [
        new Field("id", Int64Type.Default, nullable: true),
        new Field("name", StringType.Default, nullable: true),
    ], null);

    private static RecordBatch BuildBatch(int startId, int rows)
    {
        var idBuilder = new Int64Array.Builder();
        var nameBuilder = new StringArray.Builder();
        for (var i = 0; i < rows; i++)
        {
            idBuilder.Append(startId + i);
            nameBuilder.Append($"row-{startId + i}");
        }

        return new RecordBatch(FixedSchema, [idBuilder.Build(), nameBuilder.Build()], rows);
    }

    [Fact]
    public async Task Parquet_round_trips_all_batches()
    {
        using var b0 = BuildBatch(0, 3);
        using var b1 = BuildBatch(100, 5);
        using var b2 = BuildBatch(200, 2);

        using var ms = new MemoryStream();
        await AzureBlobFormat.WriteParquetAsync(ms, FixedSchema, [b0, b1, b2], CancellationToken.None);

        using var readable = new MemoryStream(ms.ToArray());
        await using var reader = await ParquetReader.CreateAsync(readable);
        Assert.Equal(3, reader.RowGroupCount);

        var idField = reader.Schema.DataFields.Single(f => f.Name == "id");
        var nameField = reader.Schema.DataFields.Single(f => f.Name == "name");

        var ids = new List<long>();
        var names = new List<string?>();
        foreach (var rowGroup in reader.RowGroups)
        {
            var rowCount = (int)rowGroup.RowCount;
            var idValues = new long?[rowCount];
            await rowGroup.ReadAsync<long>(idField, idValues);
            var nameValues = new string?[rowCount];
            await rowGroup.ReadAsync(nameField, nameValues);

            ids.AddRange(idValues.Select(v => v!.Value));
            names.AddRange(nameValues);
        }

        Assert.Equal(10, ids.Count);
        Assert.Equal([0, 1, 2, 100, 101, 102, 103, 104, 200, 201], ids);
        Assert.Equal(["row-0", "row-1", "row-2", "row-100", "row-101", "row-102", "row-103", "row-104", "row-200", "row-201"], names);
    }

    [Fact]
    public async Task Csv_round_trips_all_batches()
    {
        using var b0 = BuildBatch(0, 3);
        using var b1 = BuildBatch(100, 5);
        using var b2 = BuildBatch(200, 2);

        using var ms = new MemoryStream();
        await AzureBlobFormat.WriteCsvAsync(ms, FixedSchema, [b0, b1, b2], ',', CancellationToken.None);

        var text = ReadCsvText(ms);
        var lines = text.Split('\n', StringSplitOptions.RemoveEmptyEntries);

        Assert.Equal("id,name", lines[0]);
        Assert.Equal(11, lines.Length); // header + 10 rows

        var ids = new List<long>();
        var names = new List<string>();
        for (var i = 1; i < lines.Length; i++)
        {
            var parts = lines[i].Split(',');
            ids.Add(long.Parse(parts[0]));
            names.Add(parts[1]);
        }

        Assert.Equal([0, 1, 2, 100, 101, 102, 103, 104, 200, 201], ids);
        Assert.Equal(["row-0", "row-1", "row-2", "row-100", "row-101", "row-102", "row-103", "row-104", "row-200", "row-201"], names);
    }

    [Fact]
    public async Task Csv_writes_tab_separated_bytes_when_given_a_tab_delimiter()
    {
        using var b0 = BuildBatch(0, 3);
        using var b1 = BuildBatch(100, 5);
        using var b2 = BuildBatch(200, 2);

        using var ms = new MemoryStream();
        await AzureBlobFormat.WriteCsvAsync(ms, FixedSchema, [b0, b1, b2], '\t', CancellationToken.None);

        var text = ReadCsvText(ms);
        var lines = text.Split('\n', StringSplitOptions.RemoveEmptyEntries);

        Assert.Equal("id\tname", lines[0]);
        Assert.Equal(11, lines.Length); // header + 10 rows

        var ids = new List<long>();
        var names = new List<string>();
        for (var i = 1; i < lines.Length; i++)
        {
            var parts = lines[i].Split('\t');
            ids.Add(long.Parse(parts[0]));
            names.Add(parts[1]);
        }

        Assert.Equal([0, 1, 2, 100, 101, 102, 103, 104, 200, 201], ids);
        Assert.Equal(["row-0", "row-1", "row-2", "row-100", "row-101", "row-102", "row-103", "row-104", "row-200", "row-201"], names);
    }

    [Fact]
    public async Task Csv_quotes_values_containing_commas()
    {
        var idBuilder = new Int64Array.Builder();
        var nameBuilder = new StringArray.Builder();
        idBuilder.Append(1);
        nameBuilder.Append("a,b\"c");
        using var batch = new RecordBatch(FixedSchema, [idBuilder.Build(), nameBuilder.Build()], 1);

        using var ms = new MemoryStream();
        await AzureBlobFormat.WriteCsvAsync(ms, FixedSchema, [batch], ',', CancellationToken.None);

        var text = ReadCsvText(ms);
        Assert.Contains("\"a,b\"\"c\"", text, StringComparison.Ordinal);
    }

    // --- Partitioned (fan-out) write session: runtime column check + grouping/slicing, both offline ---

    private static readonly Schema PartitionSchema = new(
    [
        new Field("id", Int64Type.Default, nullable: true),
        new Field("event_time", new TimestampType(TimeUnit.Microsecond, "UTC"), nullable: true),
    ], null);

    private static RecordBatch BuildPartitionBatch((long Id, DateTimeOffset When)[] rows)
    {
        var idBuilder = new Int64Array.Builder();
        var timeBuilder = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"));
        foreach (var (id, when) in rows)
        {
            idBuilder.Append(id);
            timeBuilder.Append(when);
        }

        return new RecordBatch(PartitionSchema, [idBuilder.Build(), timeBuilder.Build()], rows.Length);
    }

    [Fact]
    public async Task Partition_by_column_missing_from_schema_fails_fast_permanent()
    {
        var config = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["auth"] = "connection_string",
            ["connection_string"] = "UseDevelopmentStorage=true",
        });
        await using var sink = new AzureSink(config);

        // FixedSchema has {id, name} -- no event_time column for partition_by to route on.
        var spec = new OutputSpec("sink", "orders", "replace", "fail_on_change", new Dictionary<string, object?>
        {
            ["container"] = "lake",
            ["path"] = "out/{yyyy}/{MM}/{dd}/",
            ["format"] = "parquet",
            ["partition_by"] = "event_time",
        });

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(spec, FixedSchema, CancellationToken.None));
        Assert.False(ex.IsTransient);
        Assert.Contains("event_time", ex.Message, StringComparison.Ordinal);
        Assert.Contains("not present", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Partition_by_non_timestamp_column_fails_fast_permanent()
    {
        var config = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["auth"] = "connection_string",
            ["connection_string"] = "UseDevelopmentStorage=true",
        });
        await using var sink = new AzureSink(config);

        // 'id' exists but is int64, not a timestamp/date -- cannot drive calendar tokens.
        var spec = new OutputSpec("sink", "orders", "replace", "fail_on_change", new Dictionary<string, object?>
        {
            ["container"] = "lake",
            ["path"] = "out/{yyyy}/{MM}/{dd}/",
            ["format"] = "parquet",
            ["partition_by"] = "id",
        });

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(spec, FixedSchema, CancellationToken.None));
        Assert.False(ex.IsTransient);
        Assert.Contains("id", ex.Message, StringComparison.Ordinal);
        Assert.Contains("timestamp/date", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>In-memory inner session: captures which id values land in it and how it was closed, so the
    /// fan-out grouping/slicing can be exercised with no live container.</summary>
    private sealed class FakeInnerSession : ISinkWriteSession
    {
        public List<long> Ids { get; } = [];
        public int BatchCount { get; private set; }
        public bool Committed { get; private set; }
        public bool Aborted { get; private set; }
        public bool Disposed { get; private set; }

        public ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
        {
            // Copy synchronously -- batch is owned by the fan-out session only until this call returns.
            var ids = (Int64Array)batch.Column(0);
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

    [Fact]
    public async Task Fan_out_groups_rows_by_rendered_folder_and_commits_every_partition()
    {
        var opened = new Dictionary<string, FakeInnerSession>(StringComparer.Ordinal);
        ValueTask<ISinkWriteSession> Open(string folder)
        {
            var session = new FakeInnerSession();
            opened[folder] = session;
            return ValueTask.FromResult<ISinkWriteSession>(session);
        }

        var session = new AzurePartitionedWriteSession(Open, "out/{yyyy}/{MM}/{dd}/", partitionColIndex: 1, PartitionSchema);

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

        // Commit-xor-abort: reuse after commit is rejected, and abort after commit is forbidden.
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await session.AbortAsync(CancellationToken.None));
        using var more = BuildPartitionBatch([(4, day12)]);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await session.WriteBatchAsync(more, CancellationToken.None));
    }

    [Fact]
    public async Task Fan_out_abort_aborts_every_opened_partition()
    {
        var opened = new List<FakeInnerSession>();
        ValueTask<ISinkWriteSession> Open(string folder)
        {
            var session = new FakeInnerSession();
            opened.Add(session);
            return ValueTask.FromResult<ISinkWriteSession>(session);
        }

        var session = new AzurePartitionedWriteSession(Open, "out/{yyyy}/{MM}/{dd}/", partitionColIndex: 1, PartitionSchema);

        var day12 = new DateTimeOffset(2026, 7, 12, 8, 0, 0, TimeSpan.Zero);
        var day13 = new DateTimeOffset(2026, 7, 13, 1, 0, 0, TimeSpan.Zero);
        using var batch = BuildPartitionBatch([(1, day12), (2, day13)]);
        await session.WriteBatchAsync(batch, CancellationToken.None);

        await session.AbortAsync(CancellationToken.None);

        Assert.Equal(2, opened.Count);
        Assert.All(opened, s => Assert.True(s.Aborted));
        Assert.All(opened, s => Assert.False(s.Committed));
    }

    /// <summary>Blocks inside <see cref="WriteBatchAsync"/> until released, signalling arrival first -- lets
    /// a test prove two folders' writes are in flight at once rather than one strictly after the other.</summary>
    private sealed class BlockingInnerSession(Action onArrived, Task release) : ISinkWriteSession
    {
        public ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
        {
            onArrived();
            return new ValueTask(release);
        }

        public ValueTask<WriteResult> CommitAsync(CancellationToken ct) => ValueTask.FromResult(new WriteResult(0, 0));

        public ValueTask AbortAsync(CancellationToken ct) => ValueTask.CompletedTask;

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    [Fact]
    public async Task Fan_out_writes_distinct_folders_concurrently_not_sequentially()
    {
        var release = new TaskCompletionSource();
        var barrierReached = new TaskCompletionSource();
        var arrived = 0;

        void OnArrived()
        {
            // Only reachable by BOTH folders' WriteBatchAsync calls if they're in flight at the same
            // time: a strictly sequential loop would block the second call behind the first, which
            // never returns until `release` completes -- so the barrier would never be reached.
            if (Interlocked.Increment(ref arrived) == 2)
            {
                barrierReached.TrySetResult();
            }
        }

        ValueTask<ISinkWriteSession> Open(string folder) =>
            ValueTask.FromResult<ISinkWriteSession>(new BlockingInnerSession(OnArrived, release.Task));

        var session = new AzurePartitionedWriteSession(Open, "out/{yyyy}/{MM}/{dd}/", partitionColIndex: 1, PartitionSchema);

        var day12 = new DateTimeOffset(2026, 7, 12, 8, 0, 0, TimeSpan.Zero);
        var day13 = new DateTimeOffset(2026, 7, 13, 1, 0, 0, TimeSpan.Zero);
        using var batch = BuildPartitionBatch([(1, day12), (2, day13)]);

        var writeTask = session.WriteBatchAsync(batch, CancellationToken.None);

        var completed = await Task.WhenAny(barrierReached.Task, Task.Delay(TimeSpan.FromSeconds(5)));
        Assert.Same(barrierReached.Task, completed);

        release.SetResult();
        await writeTask;
    }

    // The universal csv writer emits no BOM -- its bytes must match native COPY's exactly; the
    // BOM-detecting reader is kept as a consumer-faithful decode.
    private static string ReadCsvText(MemoryStream ms)
    {
        ms.Position = 0;
        using var reader = new StreamReader(ms, System.Text.Encoding.UTF8, detectEncodingFromByteOrderMarks: true);
        return reader.ReadToEnd();
    }

    [Fact]
    public async Task Decimal128_parquet_column_fails_fast_naming_the_column()
    {
        var schema = new Schema([new Field("amount", new Decimal128Type(18, 2), nullable: true)], null);
        using var ms = new MemoryStream();

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await AzureBlobFormat.WriteParquetAsync(ms, schema, [], CancellationToken.None));
        Assert.False(ex.IsTransient);
        Assert.Contains("amount", ex.Message, StringComparison.Ordinal);
    }
}
