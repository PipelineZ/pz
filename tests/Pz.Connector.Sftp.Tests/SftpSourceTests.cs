using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Parquet;
using Parquet.Schema;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;
using Xunit;

namespace Pz.Connector.Sftp.Tests;

internal static class FakeSftpFileSystemTestExtensions
{
    public static void AddFile(this FakeSftpFileSystem fake, string path, string content) =>
        fake.Seed(path, Encoding.UTF8.GetBytes(content));
}

public class SftpSourceTests
{
    private static SftpSource NewSource(FakeSftpFileSystem fake, string? root) =>
        new(new SftpConnectionSettings("host", 22, "user", "pw", null, null, null, root), _ => fake);

    private static DatasetSpec Spec(string dataset, params (string Key, object? Value)[] options) =>
        new("sftp", dataset, options.ToDictionary(o => o.Key, o => o.Value));

    private static IReadOnlyDictionary<string, string> Columns(params (string Name, string Type)[] cols) =>
        cols.ToDictionary(c => c.Name, c => c.Type);

    private static async Task<List<RecordBatch>> DrainAsync(IDatasetPartition partition)
    {
        var batches = new List<RecordBatch>();
        await foreach (var batch in partition.ReadAsync(BatchOptions.Default, default))
        {
            batches.Add(batch);
        }

        return batches;
    }

    // -- Pivotal tests (from the task brief) -----------------------------------------------

    [Fact]
    public async Task Csv_schema_prunes_contract_to_header_in_contract_order()
    {
        var fake = new FakeSftpFileSystem();
        fake.AddFile("/data/orders.csv", "id,name,extra\n1,a,x\n");
        var source = NewSource(fake, root: "/data");
        var spec = Spec("orders", ("columns", Columns(("name", "varchar"), ("id", "int"), ("gone", "int"))));

        var schema = (await source.GetSchemaAsync(spec, default)).Schema;
        Assert.Equal(["name", "id"], schema.FieldsList.Select(f => f.Name));
    }

    [Fact]
    public async Task Plan_groups_files_per_partition()
    {
        var fake = new FakeSftpFileSystem();
        foreach (var n in new[] { "a", "b", "c", "d", "e" })
        {
            fake.AddFile($"/in/{n}.csv", "id\n1\n");
        }

        var source = NewSource(fake, root: null);
        var parts = await source.PlanReadAsync(
            Spec("x", ("path", "/in/*.csv"), ("files_per_partition", 2)), ReadHints.None, default);
        Assert.Equal(3, parts.Count);   // 2+2+1, ordinally sorted file order
    }

    [Fact]
    public async Task Windowed_read_applies_row_bounds()
    {
        // csv with ts rows on 26th..29th, window (27th, 29th]: expect rows 28,29 only -- file-level
        // cover can't help here (an untemplated path), so this proves the ROW-level filter.
        var fake = new FakeSftpFileSystem();
        fake.AddFile("/data/events.csv",
            "ts,val\n" +
            "2026-08-26T00:00:00,a\n" +
            "2026-08-27T00:00:00,b\n" +
            "2026-08-28T00:00:00,c\n" +
            "2026-08-29T00:00:00,d\n");

        var source = NewSource(fake, root: "/data");
        var columns = Columns(("ts", "timestamp"), ("val", "varchar"));
        var spec = Spec("events", ("columns", columns)) with
        {
            WatermarkCursor = "ts",
            WatermarkValue = "2026-08-27T00:00:00",
            WatermarkUpperBound = "2026-08-29T00:00:00",
        };

        var parts = await source.PlanReadAsync(spec, ReadHints.None, default);
        Assert.Single(parts);

        var batches = await DrainAsync(parts[0]);
        var vals = batches.SelectMany(b =>
            Enumerable.Range(0, b.Length).Select(i => ((StringArray)b.Column(1)).GetString(i))).ToArray();

        Assert.Equal(["c", "d"], vals);
    }

    /// <summary>A row wider than Sylvan's default 16KiB read buffer must still read: sftp is
    /// universal-tier only (no native-scan fallback to fall back to, unlike LocalFiles), so the
    /// library's own "Row 1 was too large. Try increasing the MaxBufferSize setting." would make any
    /// wide-row csv unreadable. Exercises both call sites that share <see cref="SftpSource.CsvOptions"/>
    /// -- the schema peek and the row read -- against the same file.</summary>
    [Fact]
    public async Task Csv_row_wider_than_the_readers_default_buffer_round_trips()
    {
        var payload = new string('x', 20 * 1024);
        var fake = new FakeSftpFileSystem();
        fake.AddFile("/data/wide.csv", $"id,payload\n1,{payload}\n");
        var source = NewSource(fake, root: "/data");
        var spec = Spec("wide", ("columns", Columns(("id", "bigint"), ("payload", "varchar"))));

        var schema = (await source.GetSchemaAsync(spec, default)).Schema;
        Assert.Equal(["id", "payload"], schema.FieldsList.Select(f => f.Name));

        var parts = await source.PlanReadAsync(spec, ReadHints.None, default);
        var batches = await DrainAsync(parts[0]);

        Assert.Equal(payload, ((StringArray)batches[0].Column(1)).GetString(0));
    }

    [Fact]
    public async Task No_match_names_dataset_and_pattern()
    {
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => NewSource(new FakeSftpFileSystem(), "/data")
                .GetSchemaAsync(Spec("orders"), default).AsTask());
        Assert.False(ex.IsTransient);
        Assert.Contains("orders", ex.Message);
        Assert.Contains("/data/orders.csv", ex.Message);
    }

    // -- Plus: parquet round-trip, json contract requirement, connection isolation, ------
    // -- files_per_partition validation -----------------------------------------------------

    [Fact]
    public async Task Parquet_round_trips_through_a_partition()
    {
        var idField = new DataField("id", typeof(int), isNullable: true);
        var nameField = new DataField("name", typeof(string), isNullable: true);
        var bytes = await BuildParquetBytesAsync(idField, nameField);

        var fake = new FakeSftpFileSystem();
        fake.Seed("/data/orders.parquet", bytes);
        var source = NewSource(fake, root: "/data");
        var spec = Spec("orders", ("format", "parquet"));

        var schema = (await source.GetSchemaAsync(spec, default)).Schema;
        Assert.Equal(["id", "name"], schema.FieldsList.Select(f => f.Name));

        var parts = await source.PlanReadAsync(spec, ReadHints.None, default);
        Assert.Single(parts);

        var batches = await DrainAsync(parts[0]);
        Assert.Equal(2, batches.Sum(b => b.Length));
        var ids = batches
            .SelectMany(b => Enumerable.Range(0, b.Length).Select(i => ((Int32Array)b.Column(0)).GetValue(i)))
            .ToArray();
        Assert.Equal([1, 2], ids);
    }

    [Fact]
    public async Task Parquet_declared_contract_reports_contract_projection()
    {
        var idField = new DataField("id", typeof(int), isNullable: true);
        var nameField = new DataField("name", typeof(string), isNullable: true);
        var bytes = await BuildParquetBytesAsync(idField, nameField);

        var fake = new FakeSftpFileSystem();
        fake.Seed("/data/orders.parquet", bytes);
        var source = NewSource(fake, root: "/data");
        var spec = Spec("orders", ("format", "parquet"), ("columns", Columns(("id", "int"))));

        var schema = (await source.GetSchemaAsync(spec, default)).Schema;
        Assert.Equal(["id"], schema.FieldsList.Select(f => f.Name));
    }

    [Fact]
    public async Task Parquet_declared_contract_missing_column_throws()
    {
        var idField = new DataField("id", typeof(int), isNullable: true);
        var bytes = await BuildParquetBytesAsync(idField);

        var fake = new FakeSftpFileSystem();
        fake.Seed("/data/orders.parquet", bytes);
        var source = NewSource(fake, root: "/data");
        var spec = Spec("orders", ("format", "parquet"), ("columns", Columns(("missing", "varchar"))));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => source.GetSchemaAsync(spec, default).AsTask());
        Assert.False(ex.IsTransient);
        Assert.Contains("missing", ex.Message);
    }

    private static async Task<byte[]> BuildParquetBytesAsync(params DataField[] fields)
    {
        var stream = new MemoryStream();
        await using (var writer = await ParquetWriter.CreateAsync(new ParquetSchema(fields), stream))
        {
            using var rg = writer.CreateRowGroup();
            if (fields.Length >= 1)
            {
                int?[] ids = [1, 2];
                await rg.WriteAsync<int>(fields[0], ids);
            }

            if (fields.Length >= 2)
            {
                string?[] names = ["a", "b"];
                await rg.WriteAsync(fields[1], names);
            }
        }

        return stream.ToArray();
    }

    // Positional binding: the engine binds a landed batch's columns to GetSchemaAsync's reported
    // schema BY POSITION (SourceLoadExecutor -> IngestArrowAsync -> ArrowInterop), so a declared
    // contract's order must win over the parquet file's own physical column order.
    [Fact]
    public async Task Parquet_declared_contract_delivers_batches_in_contract_order_not_footer_order()
    {
        // Physical footer order: b (varchar), a (int) -- the reverse of the contract's declared order.
        var bField = new DataField("b", typeof(string), isNullable: true);
        var aField = new DataField("a", typeof(int), isNullable: true);
        var bytes = await BuildParquetBValuesThenAValuesAsync(bField, aField);

        var fake = new FakeSftpFileSystem();
        fake.Seed("/data/orders.parquet", bytes);
        var source = NewSource(fake, root: "/data");
        var spec = Spec("orders", ("format", "parquet"), ("columns", Columns(("a", "int"), ("b", "varchar"))));

        var schema = (await source.GetSchemaAsync(spec, default)).Schema;
        Assert.Equal(["a", "b"], schema.FieldsList.Select(f => f.Name));

        var parts = await source.PlanReadAsync(spec, ReadHints.None, default);
        var batches = await DrainAsync(parts[0]);

        Assert.Equal(["a", "b"], batches[0].Schema.FieldsList.Select(f => f.Name));
        Assert.Equal(10, ((Int32Array)batches[0].Column(0)).GetValue(0));
        Assert.Equal("x", ((StringArray)batches[0].Column(1)).GetString(0));
    }

    private static async Task<byte[]> BuildParquetBValuesThenAValuesAsync(DataField bField, DataField aField)
    {
        var stream = new MemoryStream();
        await using (var writer = await ParquetWriter.CreateAsync(new ParquetSchema([bField, aField]), stream))
        {
            using var rg = writer.CreateRowGroup();
            string?[] bValues = ["x", "y"];
            int?[] aValues = [10, 20];
            await rg.WriteAsync(bField, bValues);
            await rg.WriteAsync<int>(aField, aValues);
        }

        return stream.ToArray();
    }

    // Secondary case from the same review finding: two files in one windowed partition whose
    // footers disagree on physical column order must not desync the window filter's cursor
    // ordinal, which is computed once (off the first file's batch schema) and reused for every
    // later file's batches.
    [Fact]
    public async Task Windowed_multi_file_parquet_partition_keeps_cursor_ordinal_consistent_across_differing_footer_orders()
    {
        var file1 = await BuildParquetTsValAsync(
            tsFirst: true,
            ts: [Utc(2026, 8, 27), Utc(2026, 8, 28)],
            val: ["v27", "v28"]);
        var file2 = await BuildParquetTsValAsync(
            tsFirst: false,
            ts: [Utc(2026, 8, 29)],
            val: ["v29"]);

        var fake = new FakeSftpFileSystem();
        fake.Seed("/data/a.parquet", file1);
        fake.Seed("/data/b.parquet", file2);
        var source = NewSource(fake, root: "/data");
        var columns = Columns(("ts", "timestamp"), ("val", "varchar"));
        var spec = Spec("events",
            ("format", "parquet"), ("path", "*.parquet"), ("files_per_partition", 2), ("columns", columns)) with
        {
            WatermarkCursor = "ts",
            WatermarkValue = "2026-08-27T00:00:00",
            WatermarkUpperBound = "2026-08-29T00:00:00",
        };

        var parts = await source.PlanReadAsync(spec, ReadHints.None, default);
        Assert.Single(parts);

        var batches = await DrainAsync(parts[0]);
        var vals = batches
            .SelectMany(b => Enumerable.Range(0, b.Length).Select(i => ((StringArray)b.Column(1)).GetString(i)))
            .ToArray();

        Assert.Equal(["v28", "v29"], vals);
    }

    private static DateTime Utc(int y, int m, int d) => new(y, m, d, 0, 0, 0, DateTimeKind.Utc);

    private static async Task<byte[]> BuildParquetTsValAsync(bool tsFirst, DateTime[] ts, string[] val)
    {
        var tsField = new DateTimeDataField(
            "ts", DateTimeFormat.DateAndTime, isAdjustedToUTC: true, unit: DateTimeTimeUnit.Micros, isNullable: true);
        var valField = new DataField("val", typeof(string), isNullable: true);
        var fields = tsFirst ? new DataField[] { tsField, valField } : new DataField[] { valField, tsField };

        var stream = new MemoryStream();
        await using (var writer = await ParquetWriter.CreateAsync(new ParquetSchema(fields), stream))
        {
            using var rg = writer.CreateRowGroup();
            DateTime?[] tsValues = ts.Select(t => (DateTime?)t).ToArray();
            string?[] valValues = val.Select(v => (string?)v).ToArray();
            if (tsFirst)
            {
                await rg.WriteAsync<DateTime>(tsField, tsValues);
                await rg.WriteAsync(valField, valValues);
            }
            else
            {
                await rg.WriteAsync(valField, valValues);
                await rg.WriteAsync<DateTime>(tsField, tsValues);
            }
        }

        return stream.ToArray();
    }

    [Fact]
    public async Task Json_without_contract_reports_permanent_error()
    {
        var fake = new FakeSftpFileSystem();
        var source = NewSource(fake, root: "/data");
        var spec = Spec("events", ("format", "json"));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => source.GetSchemaAsync(spec, default).AsTask());
        Assert.False(ex.IsTransient);
        Assert.Contains("events", ex.Message);
        Assert.Contains("columns:", ex.Message);

        var ex2 = await Assert.ThrowsAsync<PzConnectorException>(
            () => source.PlanReadAsync(spec, ReadHints.None, default).AsTask());
        Assert.False(ex2.IsTransient);
    }

    [Fact]
    public async Task Json_with_contract_is_the_schema_and_reads_through_a_partition()
    {
        var fake = new FakeSftpFileSystem();
        fake.AddFile("/data/events.json", "{\"id\":1}\n{\"id\":2}\n");
        var source = NewSource(fake, root: "/data");
        var spec = Spec("events", ("format", "json"), ("columns", Columns(("id", "int"))));

        var schema = (await source.GetSchemaAsync(spec, default)).Schema;
        Assert.Equal(["id"], schema.FieldsList.Select(f => f.Name));

        var parts = await source.PlanReadAsync(spec, ReadHints.None, default);
        Assert.Single(parts);
        var batches = await DrainAsync(parts[0]);
        Assert.Equal(2, batches.Sum(b => b.Length));
    }

    [Fact]
    public async Task Partitions_open_independent_connections_per_read()
    {
        var shared = new FakeSftpFileSystem();
        shared.AddFile("/in/a.csv", "id\n1\n");
        shared.AddFile("/in/b.csv", "id\n2\n");

        var opened = new System.Collections.Concurrent.ConcurrentBag<CountingWrapper>();
        var source = new SftpSource(
            new SftpConnectionSettings("host", 22, "user", "pw", null, null, null, Root: null),
            _ =>
            {
                var wrapper = new CountingWrapper(shared);
                opened.Add(wrapper);
                return wrapper;
            });

        var spec = Spec("x", ("path", "/in/*.csv"), ("files_per_partition", 1), ("columns", Columns(("id", "int"))));
        var parts = await source.PlanReadAsync(spec, ReadHints.None, default);
        Assert.Equal(2, parts.Count);

        var openedAfterPlan = opened.Count; // one connection for PlanReadAsync's own ListMatches

        var results = await Task.WhenAll(parts.Select(DrainAsync));
        Assert.All(results, batches => Assert.Single(batches));

        // Each partition opened its OWN connection distinct from the plan-time one and from each other.
        Assert.Equal(openedAfterPlan + 2, opened.Count);
        Assert.Equal(opened.Count, opened.Distinct().Count());
    }

    /// <summary>Delegates every call to a shared fake so seeded file content is visible from every
    /// connection, while remaining a distinct <see cref="ISftpFileSystem"/> instance the counting
    /// factory can tell apart from the others.</summary>
    private sealed class CountingWrapper(FakeSftpFileSystem inner) : ISftpFileSystem
    {
        public IEnumerable<string> ListFiles(string directory, bool recursive) => inner.ListFiles(directory, recursive);
        public Stream OpenRead(string path) => inner.OpenRead(path);
        public Stream OpenWrite(string path) => inner.OpenWrite(path);
        public void Rename(string oldPath, string newPath) => inner.Rename(oldPath, newPath);
        public void Delete(string path) => inner.Delete(path);
        public bool FileExists(string path) => inner.FileExists(path);
        public bool DirectoryExists(string path) => inner.DirectoryExists(path);
        public void CreateDirectories(string path) => inner.CreateDirectories(path);
        public void Dispose()
        {
        }
    }

    [Fact]
    public async Task FilesPerPartition_NonPositive_ThrowsPermanentError()
    {
        var fake = new FakeSftpFileSystem();
        fake.AddFile("/in/a.csv", "id\n1\n");
        var source = NewSource(fake, root: null);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            source.PlanReadAsync(
                Spec("x", ("path", "/in/*.csv"), ("files_per_partition", 0), ("columns", Columns(("id", "int")))),
                ReadHints.None, default).AsTask());

        Assert.False(ex.IsTransient);
        Assert.Contains("files_per_partition", ex.Message);
    }

    [Fact]
    public async Task FilesPerPartition_NonNumericString_ThrowsPermanentError()
    {
        var fake = new FakeSftpFileSystem();
        fake.AddFile("/in/a.csv", "id\n1\n");
        var source = NewSource(fake, root: null);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            source.PlanReadAsync(
                Spec("x", ("path", "/in/*.csv"), ("files_per_partition", "not-a-number"), ("columns", Columns(("id", "int")))),
                ReadHints.None, default).AsTask());

        Assert.False(ex.IsTransient);
        Assert.Contains("files_per_partition", ex.Message);
    }

    [Fact]
    public async Task FilesPerPartition_AsStringInteger_IsAccepted()
    {
        var fake = new FakeSftpFileSystem();
        foreach (var n in new[] { "a", "b", "c" })
        {
            fake.AddFile($"/in/{n}.csv", "id\n1\n");
        }

        var source = NewSource(fake, root: null);
        var parts = await source.PlanReadAsync(
            Spec("x", ("path", "/in/*.csv"), ("files_per_partition", "2")), ReadHints.None, default);
        Assert.Equal(2, parts.Count);
    }

    [Fact]
    public async Task ContractLessCsv_ReadsEveryHeaderColumnAsVarchar()
    {
        var fake = new FakeSftpFileSystem();
        fake.AddFile("/data/raw.csv", "a,b\n1,x\n2,y\n");
        var source = NewSource(fake, root: "/data");
        var spec = Spec("raw");

        var schema = (await source.GetSchemaAsync(spec, default)).Schema;
        Assert.Equal(["a", "b"], schema.FieldsList.Select(f => f.Name));
        Assert.All(schema.FieldsList, f => Assert.Equal(ArrowTypeId.String, f.DataType.TypeId));

        var parts = await source.PlanReadAsync(spec, ReadHints.None, default);
        var batches = await DrainAsync(parts[0]);
        Assert.Equal(2, batches.Sum(b => b.Length));
        Assert.Equal("1", ((StringArray)batches[0].Column(0)).GetString(0));
    }

    [Fact]
    public async Task CsvPartition_MissingDeclaredColumn_ThrowsNamingFileAndColumn()
    {
        var fake = new FakeSftpFileSystem();
        fake.AddFile("/data/orders.csv", "id\n1\n");
        var source = NewSource(fake, root: "/data");
        var spec = Spec("orders", ("columns", Columns(("id", "int"), ("name", "varchar"))));

        var parts = await source.PlanReadAsync(spec, ReadHints.None, default);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => DrainAsync(parts[0]));
        Assert.False(ex.IsTransient);
        Assert.Contains("orders.csv", ex.Message);
        Assert.Contains("'name'", ex.Message);
    }

    [Fact]
    public async Task UnsupportedFormat_ThrowsNamingDataset()
    {
        var fake = new FakeSftpFileSystem();
        var source = NewSource(fake, root: "/data");
        var spec = Spec("orders", ("format", "xml"));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => source.GetSchemaAsync(spec, default).AsTask());
        Assert.False(ex.IsTransient);
        Assert.Contains("orders", ex.Message);
    }

    [Fact]
    public async Task Gate_wraps_list_and_open_read_operations()
    {
        var fake = new FakeSftpFileSystem();
        fake.AddFile("/data/orders.csv", "id\n1\n");
        var source = NewSource(fake, root: "/data");
        var gate = new RecordingGate();
        ((IOperationGateAware)source).UseOperationGate(gate);

        var spec = Spec("orders", ("columns", Columns(("id", "int"))));
        var parts = await source.PlanReadAsync(spec, ReadHints.None, default);
        await DrainAsync(parts[0]);

        Assert.Contains("sftp.list", gate.Labels);
        Assert.Contains("sftp.open_read", gate.Labels);
    }

    private sealed class RecordingGate : IOperationGate
    {
        public List<string> Labels { get; } = [];

        public Task<T> ExecuteAsync<T>(string opLabel, bool idempotent, Func<CancellationToken, Task<T>> op, CancellationToken ct)
        {
            Labels.Add(opLabel);
            return op(ct);
        }

        public void ReportBudget(int remaining, DateTimeOffset resetAt)
        {
        }
    }

    [Fact]
    public async Task MidStream_failure_is_classified_and_redacted_via_SftpErrors_Map()
    {
        var fake = new FakeSftpFileSystem();
        var csv = "id\n" + string.Concat(Enumerable.Range(1, 500).Select(i => $"{i}\n"));
        fake.AddFile("/data/orders.csv", csv);

        var source = new SftpSource(
            new SftpConnectionSettings("host", 22, "user", "pw", null, null, null, Root: "/data"),
            _ => new MidStreamFailureFileSystem(fake, "/data/orders.csv"));
        var spec = Spec("orders", ("columns", Columns(("id", "int"))));

        var parts = await source.PlanReadAsync(spec, ReadHints.None, default);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => DrainAsync(parts[0]));

        // IOException is one of SftpErrors.IsTransient's classified-transient shapes -- proves the
        // exception was actually routed through SftpErrors.Map, not just an unclassified bubble-up
        // (an un-mapped exception from ReadAsync would never surface as a PzConnectorException at all).
        Assert.True(ex.IsTransient);
        Assert.Contains("orders", ex.Message);
        Assert.Contains("orders.csv", ex.Message);
    }

    // Codebase-wide convention (KindDispatchingExecutor's doc comment; every comparable catch in
    // SourceLoadExecutor/SinkWriteExecutor): cancellation is NEVER wrapped into a permanent
    // exception -- it must propagate raw so the dispatcher can tell cancellation apart from a
    // genuine failure.
    [Fact]
    public async Task MidStream_cancellation_propagates_unwrapped_not_as_PzConnectorException()
    {
        var fake = new FakeSftpFileSystem();
        var csv = "id\n" + string.Concat(Enumerable.Range(1, 500).Select(i => $"{i}\n"));
        fake.AddFile("/data/orders.csv", csv);

        var source = new SftpSource(
            new SftpConnectionSettings("host", 22, "user", "pw", null, null, null, Root: "/data"),
            _ => new MidStreamFailureFileSystem(fake, "/data/orders.csv", () => new OperationCanceledException()));
        var spec = Spec("orders", ("columns", Columns(("id", "int"))));

        var parts = await source.PlanReadAsync(spec, ReadHints.None, default);

        // A raw OperationCanceledException, not a PzConnectorException -- ThrowsAsync requires an
        // exact type match, so this fails the test if the mid-stream catch ever wraps it again.
        await Assert.ThrowsAsync<OperationCanceledException>(() => DrainAsync(parts[0]));
    }

    /// <summary>Delegates every call to a real fake, except <see cref="OpenRead"/> for one path, whose
    /// stream fails partway through being read -- simulating a dropped connection (or a cancellation)
    /// DURING streaming (the fake's own FailOn hook only guards the OpenRead call itself, not the
    /// bytes read afterward, so it cannot exercise this).</summary>
    private sealed class MidStreamFailureFileSystem(FakeSftpFileSystem inner, string failPath, Func<Exception>? makeException = null)
        : ISftpFileSystem
    {
        public IEnumerable<string> ListFiles(string directory, bool recursive) => inner.ListFiles(directory, recursive);

        public Stream OpenRead(string path)
        {
            var real = inner.OpenRead(path);
            return path == failPath
                ? new ThrowingAfterBytesStream(real, okBytes: 6, makeException ?? (() => new IOException("connection reset by peer")))
                : real;
        }

        public Stream OpenWrite(string path) => inner.OpenWrite(path);
        public void Rename(string oldPath, string newPath) => inner.Rename(oldPath, newPath);
        public void Delete(string path) => inner.Delete(path);
        public bool FileExists(string path) => inner.FileExists(path);
        public bool DirectoryExists(string path) => inner.DirectoryExists(path);
        public void CreateDirectories(string path) => inner.CreateDirectories(path);
        public void Dispose()
        {
        }
    }

    /// <summary>Serves up to <paramref name="okBytes"/> bytes from <paramref name="inner"/>, then
    /// throws <paramref name="makeException"/>'s exception on every read after -- either an
    /// unclassified, third-party-shaped mid-read failure a real dropped SSH connection would produce,
    /// or (for the cancellation test) the shape SSH.NET/`Stream.ReadAsync` itself would throw when the
    /// engine's `CancellationToken` fires mid-read.</summary>
    private sealed class ThrowingAfterBytesStream(Stream inner, int okBytes, Func<Exception> makeException) : Stream
    {
        private int _served;

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (_served >= okBytes)
            {
                throw makeException();
            }

            var n = inner.Read(buffer, offset, Math.Min(count, okBytes - _served));
            _served += n;
            return n;
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
