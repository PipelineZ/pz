using Apache.Arrow;
using Apache.Arrow.Types;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Parquet;
using Pz.Connectors.Abstractions;
using Pz.TestSupport;

namespace Pz.Connector.AzureBlob.Tests;

/// <summary>Azurite Testcontainers e2e (docker+network gated -- see <see cref="AzuriteFixture"/>) for the
/// universal SINK path: drives the real <see cref="AzureSink"/>/<see cref="AzureWriteSession"/>
/// against a live Azurite instance, proving the whole write-batch -&gt; commit-xor-abort -&gt; temp-blob-promote
/// round trip (mirrors <see cref="AzureNativeEndToEndTests"/>, which proves the native read path -- writes
/// are universal-tier, reads native-only).
///
/// CRITICAL: every test writes under a unique guid-suffixed prefix -- the shared <c>pz-e2e</c> container
/// (<see cref="AzuriteFixture"/>) is not cleaned between test runs/collections, so a fixed prefix would
/// eventually collide with blobs left over from a prior run.</summary>
[Collection("azurite")]
public sealed class AzureUniversalSinkEndToEndTests(AzuriteFixture fixture)
{
    private static readonly Schema FixedSchema = new(
    [
        new Field("id", Int64Type.Default, nullable: true),
        new Field("name", StringType.Default, nullable: true),
    ], null);

    private static ConnectorConfig Config(AzuriteFixture fixture) => new(new Dictionary<string, object?>
    {
        ["auth"] = "connection_string",
        ["connection_string"] = fixture.ConnectionString,
    });

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

    private static OutputSpec ParquetOutput(string prefix, string mode = "replace") => new(
        "sink", "orders", mode, "fail_on_change",
        new Dictionary<string, object?>
        {
            ["container"] = AzuriteFixture.Container,
            ["path"] = prefix,
            ["format"] = "parquet",
        });

    [SkippableFact]
    public async Task Commit_promotes_temp_to_final_and_leaves_no_temp_blob()
    {
        DockerFacts.SkipUnlessDocker();

        var prefix = $"out-{Guid.NewGuid():N}";
        await using var sink = new AzureSink(Config(fixture));
        var spec = ParquetOutput(prefix);

        var session = await sink.BeginWriteAsync(spec, FixedSchema, CancellationToken.None);
        using var b0 = BuildBatch(0, 50);
        using var b1 = BuildBatch(50, 50);
        using var b2 = BuildBatch(100, 50);
        await session.WriteBatchAsync(b0, CancellationToken.None);
        await session.WriteBatchAsync(b1, CancellationToken.None);
        await session.WriteBatchAsync(b2, CancellationToken.None);

        var result = await session.CommitAsync(CancellationToken.None);
        await session.DisposeAsync();

        Assert.Equal(150, result.RowsWritten);
        Assert.Equal(3, result.BatchesWritten);

        var container = new BlobServiceClient(fixture.ConnectionString).GetBlobContainerClient(AzuriteFixture.Container);

        var finalBlob = container.GetBlobClient($"{prefix}/orders.parquet");
        Assert.True((await finalBlob.ExistsAsync()).Value);

        var download = await finalBlob.OpenReadAsync();
        await using (download.ConfigureAwait(false))
        {
            // Verify the row count straight off the parquet footer/row-group metadata via Parquet.Net,
            // never through the connector's own read path.
            await using var reader = await ParquetReader.CreateAsync(download, leaveStreamOpen: true);
            var total = 0L;
            for (var rg = 0; rg < reader.RowGroupCount; rg++)
            {
                using var rowGroup = reader.OpenRowGroupReader(rg);
                total += rowGroup.RowCount;
            }

            Assert.Equal(150, total);
        }

        var remainingUnderPrefix = new List<string>();
        await foreach (var item in container.GetBlobsAsync(traits: BlobTraits.None, states: BlobStates.All, prefix: prefix, cancellationToken: default))
        {
            remainingUnderPrefix.Add(item.Name);
        }

        Assert.DoesNotContain(remainingUnderPrefix, name => name.Contains(".pz-tmp-", StringComparison.Ordinal));
        Assert.Equal([$"{prefix}/orders.parquet"], remainingUnderPrefix);
    }

    [SkippableFact]
    public async Task Abort_deletes_temp_blob_and_final_is_never_created()
    {
        DockerFacts.SkipUnlessDocker();

        var prefix = $"out-{Guid.NewGuid():N}";
        await using var sink = new AzureSink(Config(fixture));
        var spec = ParquetOutput(prefix);

        var session = await sink.BeginWriteAsync(spec, FixedSchema, CancellationToken.None);
        using var b0 = BuildBatch(0, 10);
        using var b1 = BuildBatch(10, 10);
        await session.WriteBatchAsync(b0, CancellationToken.None);
        await session.WriteBatchAsync(b1, CancellationToken.None);

        await session.AbortAsync(CancellationToken.None);
        await session.DisposeAsync();

        var container = new BlobServiceClient(fixture.ConnectionString).GetBlobContainerClient(AzuriteFixture.Container);

        var finalBlob = container.GetBlobClient($"{prefix}/orders.parquet");
        Assert.False((await finalBlob.ExistsAsync()).Value);

        var remainingUnderPrefix = new List<string>();
        await foreach (var item in container.GetBlobsAsync(traits: BlobTraits.None, states: BlobStates.All, prefix: prefix, cancellationToken: default))
        {
            remainingUnderPrefix.Add(item.Name);
        }

        Assert.Empty(remainingUnderPrefix);
    }

    [SkippableFact]
    public async Task Csv_commit_round_trips_and_leaves_no_temp_blob()
    {
        DockerFacts.SkipUnlessDocker();

        var prefix = $"out-{Guid.NewGuid():N}";
        await using var sink = new AzureSink(Config(fixture));
        var spec = new OutputSpec("sink", "orders", "replace", "fail_on_change", new Dictionary<string, object?>
        {
            ["container"] = AzuriteFixture.Container,
            ["path"] = prefix,
            ["format"] = "csv",
        });

        var session = await sink.BeginWriteAsync(spec, FixedSchema, CancellationToken.None);
        using var b0 = BuildBatch(0, 4);
        await session.WriteBatchAsync(b0, CancellationToken.None);
        var result = await session.CommitAsync(CancellationToken.None);
        await session.DisposeAsync();

        Assert.Equal(4, result.RowsWritten);

        var container = new BlobServiceClient(fixture.ConnectionString).GetBlobContainerClient(AzuriteFixture.Container);
        var finalBlob = container.GetBlobClient($"{prefix}/orders.csv");
        Assert.True((await finalBlob.ExistsAsync()).Value);

        var download = await finalBlob.OpenReadAsync();
        string text;
        await using (download.ConfigureAwait(false))
        {
            using var reader = new StreamReader(download, System.Text.Encoding.UTF8, detectEncodingFromByteOrderMarks: true);
            text = await reader.ReadToEndAsync();
        }

        var lines = text.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        Assert.Equal("id,name", lines[0]);
        Assert.Equal(5, lines.Length);

        var remainingUnderPrefix = new List<string>();
        await foreach (var item in container.GetBlobsAsync(traits: BlobTraits.None, states: BlobStates.All, prefix: prefix, cancellationToken: default))
        {
            remainingUnderPrefix.Add(item.Name);
        }

        Assert.DoesNotContain(remainingUnderPrefix, name => name.Contains(".pz-tmp-", StringComparison.Ordinal));
    }

    [SkippableFact]
    public async Task Json_commit_round_trips_and_leaves_no_temp_blob()
    {
        DockerFacts.SkipUnlessDocker();

        var prefix = $"out-{Guid.NewGuid():N}";
        await using var sink = new AzureSink(Config(fixture));
        var spec = new OutputSpec("sink", "orders", "replace", "fail_on_change", new Dictionary<string, object?>
        {
            ["container"] = AzuriteFixture.Container,
            ["path"] = prefix,
            ["format"] = "json",
        });

        var session = await sink.BeginWriteAsync(spec, FixedSchema, CancellationToken.None);
        using var b0 = BuildBatch(0, 2);
        using var b1 = BuildBatch(2, 2);
        await session.WriteBatchAsync(b0, CancellationToken.None);
        await session.WriteBatchAsync(b1, CancellationToken.None);
        var result = await session.CommitAsync(CancellationToken.None);
        await session.DisposeAsync();

        Assert.Equal(4, result.RowsWritten);
        Assert.Equal(2, result.BatchesWritten);

        var container = new BlobServiceClient(fixture.ConnectionString).GetBlobContainerClient(AzuriteFixture.Container);
        var finalBlob = container.GetBlobClient($"{prefix}/orders.json");
        Assert.True((await finalBlob.ExistsAsync()).Value);

        var download = await finalBlob.OpenReadAsync();
        string text;
        await using (download.ConfigureAwait(false))
        {
            using var reader = new StreamReader(download, System.Text.Encoding.UTF8, detectEncodingFromByteOrderMarks: true);
            text = await reader.ReadToEndAsync();
        }

        Assert.Equal(
            "{\"id\":0,\"name\":\"row-0\"}\n{\"id\":1,\"name\":\"row-1\"}\n{\"id\":2,\"name\":\"row-2\"}\n{\"id\":3,\"name\":\"row-3\"}\n",
            text);

        var remainingUnderPrefix = new List<string>();
        await foreach (var item in container.GetBlobsAsync(traits: BlobTraits.None, states: BlobStates.All, prefix: prefix, cancellationToken: default))
        {
            remainingUnderPrefix.Add(item.Name);
        }

        Assert.DoesNotContain(remainingUnderPrefix, name => name.Contains(".pz-tmp-", StringComparison.Ordinal));
        Assert.Equal([$"{prefix}/orders.json"], remainingUnderPrefix);
    }

    [SkippableFact]
    public async Task Json_abort_deletes_temp_blob_and_final_is_never_created()
    {
        DockerFacts.SkipUnlessDocker();

        var prefix = $"out-{Guid.NewGuid():N}";
        await using var sink = new AzureSink(Config(fixture));
        var spec = new OutputSpec("sink", "orders", "replace", "fail_on_change", new Dictionary<string, object?>
        {
            ["container"] = AzuriteFixture.Container,
            ["path"] = prefix,
            ["format"] = "json",
        });

        var session = await sink.BeginWriteAsync(spec, FixedSchema, CancellationToken.None);
        using var b0 = BuildBatch(0, 3);
        await session.WriteBatchAsync(b0, CancellationToken.None);
        await session.AbortAsync(CancellationToken.None);
        await session.DisposeAsync();

        var container = new BlobServiceClient(fixture.ConnectionString).GetBlobContainerClient(AzuriteFixture.Container);
        var finalBlob = container.GetBlobClient($"{prefix}/orders.json");
        Assert.False((await finalBlob.ExistsAsync()).Value);

        var remainingUnderPrefix = new List<string>();
        await foreach (var item in container.GetBlobsAsync(traits: BlobTraits.None, states: BlobStates.All, prefix: prefix, cancellationToken: default))
        {
            remainingUnderPrefix.Add(item.Name);
        }

        Assert.Empty(remainingUnderPrefix);
    }

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

    // Reads the "id" column straight off the committed parquet blob via Parquet.Net -- verification-only,
    // and never touches the connector's read path.
    private static async Task<List<long>> ReadParquetIdsAsync(BlobContainerClient container, string blobName)
    {
        var download = await container.GetBlobClient(blobName).OpenReadAsync();
        var ids = new List<long>();
        await using (download.ConfigureAwait(false))
        {
            await using var reader = await ParquetReader.CreateAsync(download, leaveStreamOpen: true);
            var idField = reader.Schema.DataFields.Single(f => f.Name == "id");

            for (var rg = 0; rg < reader.RowGroupCount; rg++)
            {
                using var rowGroup = reader.OpenRowGroupReader(rg);
                var rowCount = checked((int)rowGroup.RowCount);
                var values = new long?[rowCount];
                await rowGroup.ReadAsync<long>(idField, values);
                for (var i = 0; i < rowCount; i++)
                {
                    ids.Add(values[i]!.Value);
                }
            }
        }

        return ids;
    }

    private static async Task<List<long>> ReadJsonIdsAsync(BlobContainerClient container, string blobName)
    {
        var download = await container.GetBlobClient(blobName).OpenReadAsync();
        string text;
        await using (download.ConfigureAwait(false))
        {
            using var reader = new StreamReader(download, System.Text.Encoding.UTF8, detectEncodingFromByteOrderMarks: true);
            text = await reader.ReadToEndAsync();
        }

        var ids = new List<long>();
        foreach (var line in text.Split('\n', StringSplitOptions.RemoveEmptyEntries))
        {
            using var doc = System.Text.Json.JsonDocument.Parse(line);
            ids.Add(doc.RootElement.GetProperty("id").GetInt64());
        }

        return ids;
    }

    [SkippableFact]
    public async Task Partitioned_json_write_fans_out_by_row_date()
    {
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();

        var prefix = $"out-{Guid.NewGuid():N}";
        await using var sink = new AzureSink(Config(fixture));
        var spec = new OutputSpec("sink", "orders", "replace", "fail_on_change", new Dictionary<string, object?>
        {
            ["container"] = AzuriteFixture.Container,
            ["path"] = $"{prefix}/{{yyyy}}/{{MM}}/{{dd}}/",
            ["format"] = "json",
            ["partition_by"] = "event_time",
        });

        var day12 = new DateTimeOffset(2026, 7, 12, 8, 0, 0, TimeSpan.Zero);
        var day13 = new DateTimeOffset(2026, 7, 13, 21, 30, 0, TimeSpan.Zero);

        var session = await sink.BeginWriteAsync(spec, PartitionSchema, CancellationToken.None);
        using var batch = BuildPartitionBatch([(1, day12), (2, day13), (3, day12), (4, day13), (5, day12)]);
        await session.WriteBatchAsync(batch, CancellationToken.None);
        var result = await session.CommitAsync(CancellationToken.None);
        await session.DisposeAsync();

        Assert.Equal(5, result.RowsWritten);
        Assert.Equal(2, result.BatchesWritten); // one ndjson-append batch per folder

        var container = new BlobServiceClient(fixture.ConnectionString).GetBlobContainerClient(AzuriteFixture.Container);

        var blob12 = $"{prefix}/2026/07/12/orders.json";
        var blob13 = $"{prefix}/2026/07/13/orders.json";
        Assert.True((await container.GetBlobClient(blob12).ExistsAsync()).Value);
        Assert.True((await container.GetBlobClient(blob13).ExistsAsync()).Value);

        Assert.Equal([1, 3, 5], await ReadJsonIdsAsync(container, blob12));
        Assert.Equal([2, 4], await ReadJsonIdsAsync(container, blob13));

        // Per-partition atomic: each folder promoted its own temp blob; none left dangling.
        var remaining = new List<string>();
        await foreach (var item in container.GetBlobsAsync(traits: BlobTraits.None, states: BlobStates.All, prefix: prefix, cancellationToken: default))
        {
            remaining.Add(item.Name);
        }

        Assert.DoesNotContain(remaining, name => name.Contains(".pz-tmp-", StringComparison.Ordinal));
        Assert.Equal([blob12, blob13], remaining.OrderBy(n => n, StringComparer.Ordinal).ToList());
    }

    [SkippableFact]
    public async Task Partitioned_write_fans_out_by_row_date()
    {
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();

        var prefix = $"out-{Guid.NewGuid():N}";
        await using var sink = new AzureSink(Config(fixture));
        var spec = new OutputSpec("sink", "orders", "replace", "fail_on_change", new Dictionary<string, object?>
        {
            ["container"] = AzuriteFixture.Container,
            ["path"] = $"{prefix}/{{yyyy}}/{{MM}}/{{dd}}/",
            ["format"] = "parquet",
            ["partition_by"] = "event_time",
        });

        var day12 = new DateTimeOffset(2026, 7, 12, 8, 0, 0, TimeSpan.Zero);
        var day13 = new DateTimeOffset(2026, 7, 13, 21, 30, 0, TimeSpan.Zero);

        var session = await sink.BeginWriteAsync(spec, PartitionSchema, CancellationToken.None);
        using var batch = BuildPartitionBatch([(1, day12), (2, day13), (3, day12), (4, day13), (5, day12)]);
        await session.WriteBatchAsync(batch, CancellationToken.None);
        var result = await session.CommitAsync(CancellationToken.None);
        await session.DisposeAsync();

        Assert.Equal(5, result.RowsWritten);
        Assert.Equal(2, result.BatchesWritten); // one row-group batch per folder

        var container = new BlobServiceClient(fixture.ConnectionString).GetBlobContainerClient(AzuriteFixture.Container);

        var blob12 = $"{prefix}/2026/07/12/orders.parquet";
        var blob13 = $"{prefix}/2026/07/13/orders.parquet";
        Assert.True((await container.GetBlobClient(blob12).ExistsAsync()).Value);
        Assert.True((await container.GetBlobClient(blob13).ExistsAsync()).Value);

        Assert.Equal([1, 3, 5], await ReadParquetIdsAsync(container, blob12));
        Assert.Equal([2, 4], await ReadParquetIdsAsync(container, blob13));

        // Per-partition atomic: each folder promoted its own temp blob; none left dangling.
        var remaining = new List<string>();
        await foreach (var item in container.GetBlobsAsync(traits: BlobTraits.None, states: BlobStates.All, prefix: prefix, cancellationToken: default))
        {
            remaining.Add(item.Name);
        }

        Assert.DoesNotContain(remaining, name => name.Contains(".pz-tmp-", StringComparison.Ordinal));
        Assert.Equal([blob12, blob13], remaining.OrderBy(n => n, StringComparer.Ordinal).ToList());
    }

    [SkippableFact]
    public async Task Commit_xor_abort_state_machine_rejects_reuse_after_commit()
    {
        DockerFacts.SkipUnlessDocker();

        var prefix = $"out-{Guid.NewGuid():N}";
        await using var sink = new AzureSink(Config(fixture));
        var spec = ParquetOutput(prefix);

        var session = await sink.BeginWriteAsync(spec, FixedSchema, CancellationToken.None);
        using var b0 = BuildBatch(0, 1);
        await session.WriteBatchAsync(b0, CancellationToken.None);
        await session.CommitAsync(CancellationToken.None);

        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await session.CommitAsync(CancellationToken.None));
        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await session.AbortAsync(CancellationToken.None));
        using var b1 = BuildBatch(1, 1);
        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await session.WriteBatchAsync(b1, CancellationToken.None));

        // Dispose after a completed commit must not delete/abort anything -- it is a pure no-op release.
        await session.DisposeAsync();

        var container = new BlobServiceClient(fixture.ConnectionString).GetBlobContainerClient(AzuriteFixture.Container);
        Assert.True((await container.GetBlobClient($"{prefix}/orders.parquet").ExistsAsync()).Value);
    }
}
