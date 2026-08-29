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
}
