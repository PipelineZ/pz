using Apache.Arrow;
using Apache.Arrow.Types;
using Azure.Storage.Blobs;
using Parquet;
using Parquet.Schema;
using Pz.Connectors.Abstractions;
using Pz.TestSupport;

namespace Pz.Connector.AzureBlob.Tests;

/// <summary>Azurite Testcontainers e2e (docker+network gated -- see <see cref="AzuriteFixture"/>) for <see
/// cref="AzureSource.GetSchemaAsync"/>, the one SDK-driven surface on this connector -- reads themselves
/// execute on the native DuckDB `azure` extension tier, proven by <see cref="AzureNativeEndToEndTests"/>.
/// Drives ONLY <c>GetSchemaAsync</c> -- no partition/read/streaming code is exercised here.
///
/// CRITICAL: every test uploads under a unique guid-suffixed prefix -- the shared <c>pz-e2e</c> container
/// (<see cref="AzuriteFixture"/>) is not cleaned between test runs/collections, so a fixed prefix would
/// eventually collide with blobs left over from a prior run.</summary>
[Collection("azurite")]
public sealed class AzureSchemaPeekEndToEndTests(AzuriteFixture fixture)
{
    private static ConnectorConfig Config(AzuriteFixture fixture) => new(new Dictionary<string, object?>
    {
        ["auth"] = "connection_string",
        ["connection_string"] = fixture.ConnectionString,
    });

    private static async Task<byte[]> BuildParquetBytesAsync(int rows, int startId)
    {
        var idField = new DataField<long>("id");
        var nameField = new DataField<string>("name");
        var schema = new ParquetSchema(idField, nameField);

        var ids = new long?[rows];
        var names = new List<string?>(rows);
        for (var i = 0; i < rows; i++)
        {
            ids[i] = startId + i;
            names.Add($"row-{startId + i}");
        }

        using var ms = new MemoryStream();
        var writer = await ParquetWriter.CreateAsync(schema, ms);
        try
        {
            using var rowGroup = writer.CreateRowGroup();
            await rowGroup.WriteAsync(idField, new ReadOnlyMemory<long?>(ids));
            await rowGroup.WriteAsync(nameField, names);
        }
        finally
        {
            await writer.DisposeAsync();
        }

        return ms.ToArray();
    }

    /// <summary><see cref="AzureSource.GetSchemaAsync"/> stops listing after the first match instead of buffering
    /// every matched blob's name first. Seeds two matching csv blobs where only the first (lexicographically,
    /// matching Azure's listing order) has a `name` column -- the second has just `id` -- so if the peek
    /// ever drifted from "first match only" (e.g. peeked the last blob, or unioned/intersected headers
    /// across all matches) the pruned schema would come back missing `name`.</summary>
    [SkippableFact]
    public async Task GetSchemaAsync_over_multi_blob_layout_peeks_only_the_first_match()
    {
        DockerFacts.SkipUnlessDocker();

        var prefix = $"in-{Guid.NewGuid():N}";
        var service = new BlobServiceClient(fixture.ConnectionString);
        var containerClient = service.GetBlobContainerClient(AzuriteFixture.Container);

        // "a.csv" sorts before "b.csv" in Azure's listing order, so it's the first match.
        await containerClient.UploadBlobAsync($"{prefix}/a.csv", new BinaryData("id,name\n1,alice\n"u8.ToArray()));
        await containerClient.UploadBlobAsync($"{prefix}/b.csv", new BinaryData("id\n2\n"u8.ToArray()));

        var source = new AzureSource(Config(fixture));
        var spec = new DatasetSpec("lake", "data", new Dictionary<string, object?>
        {
            ["container"] = AzuriteFixture.Container,
            ["path"] = $"{prefix}/*.csv",
            ["format"] = "csv",
            ["columns"] = new Dictionary<string, string> { ["id"] = "int", ["name"] = "varchar" },
        });

        var schema = await source.GetSchemaAsync(spec, CancellationToken.None);
        Assert.Equal(2, schema.Schema.FieldsList.Count);
        Assert.Contains(schema.Schema.FieldsList, f => f.Name == "id");
        Assert.Contains(schema.Schema.FieldsList, f => f.Name == "name");
    }

    /// <summary>Single-blob csv peek: <see cref="AzureSource.GetSchemaAsync"/> downloads only the header
    /// row (via Sylvan) and reports the declared `columns:` contract entries that are also present in that
    /// header, pruning out any contract entry the header doesn't actually have -- here the blob's header
    /// has `id,name,extra` but the contract only declares `id`/`name`, so `extra` must not appear in the
    /// returned schema even though it's a real column in the blob.</summary>
    [SkippableFact]
    public async Task GetSchemaAsync_csv_peeks_header_row_pruned_to_declared_contract()
    {
        DockerFacts.SkipUnlessDocker();

        var prefix = $"in-{Guid.NewGuid():N}";
        var service = new BlobServiceClient(fixture.ConnectionString);
        var containerClient = service.GetBlobContainerClient(AzuriteFixture.Container);

        const string Csv = "id,name,extra\n1,alice,x\n2,bob,y\n";
        await containerClient.UploadBlobAsync($"{prefix}/data.csv", new BinaryData(Csv));

        var source = new AzureSource(Config(fixture));
        var spec = new DatasetSpec("lake", "data", new Dictionary<string, object?>
        {
            ["container"] = AzuriteFixture.Container,
            ["path"] = $"{prefix}/data.csv",
            ["format"] = "csv",
            ["columns"] = new Dictionary<string, string> { ["id"] = "int", ["name"] = "varchar" },
        });

        var schema = await source.GetSchemaAsync(spec, CancellationToken.None);

        Assert.Equal(2, schema.Schema.FieldsList.Count);
        Assert.DoesNotContain(schema.Schema.FieldsList, f => f.Name == "extra");
        var idField = Assert.Single(schema.Schema.FieldsList, f => f.Name == "id");
        Assert.IsType<Int32Type>(idField.DataType);
        var nameField = Assert.Single(schema.Schema.FieldsList, f => f.Name == "name");
        Assert.IsType<StringType>(nameField.DataType);
    }

    /// <summary>Single-blob parquet peek: <see cref="AzureSource.GetSchemaAsync"/> downloads the blob and
    /// reads its footer via <see cref="AzureParquetReader.ReadSchema"/>, mapping parquet types to Arrow
    /// types -- proven offline (no docker) by <see cref="AzureParquetReaderTests"/>; this is the one place
    /// that pipeline runs end-to-end against a real Azurite blob rather than an in-memory stream.</summary>
    [SkippableFact]
    public async Task GetSchemaAsync_parquet_peeks_footer_schema_with_correct_arrow_types()
    {
        DockerFacts.SkipUnlessDocker();

        var prefix = $"in-{Guid.NewGuid():N}";
        var service = new BlobServiceClient(fixture.ConnectionString);
        var containerClient = service.GetBlobContainerClient(AzuriteFixture.Container);

        var bytes = await BuildParquetBytesAsync(rows: 3, startId: 0);
        await containerClient.UploadBlobAsync($"{prefix}/data.parquet", new BinaryData(bytes));

        var source = new AzureSource(Config(fixture));
        var spec = new DatasetSpec("lake", "data", new Dictionary<string, object?>
        {
            ["container"] = AzuriteFixture.Container,
            ["path"] = $"{prefix}/data.parquet",
            ["format"] = "parquet",
        });

        var schema = await source.GetSchemaAsync(spec, CancellationToken.None);

        Assert.Equal(2, schema.Schema.FieldsList.Count);
        var idField = schema.Schema.FieldsList[0];
        Assert.Equal("id", idField.Name);
        Assert.IsType<Int64Type>(idField.DataType);
        var nameField = schema.Schema.FieldsList[1];
        Assert.Equal("name", nameField.Name);
        Assert.IsType<StringType>(nameField.DataType);
    }

    /// <summary>For json there is no header row to peek and no footer to read, so
    /// <see cref="AzureSource.GetSchemaAsync"/>
    /// reports the declared `columns:` contract AS the schema, in declared order -- no blob content is
    /// even needed to determine it, though a real matching blob is still uploaded so listing/first-match
    /// resolution is exercised the same way as the other formats.</summary>
    [SkippableFact]
    public async Task GetSchemaAsync_json_declared_contract_is_the_schema()
    {
        DockerFacts.SkipUnlessDocker();

        var prefix = $"in-{Guid.NewGuid():N}";
        var service = new BlobServiceClient(fixture.ConnectionString);
        var containerClient = service.GetBlobContainerClient(AzuriteFixture.Container);

        const string Ndjson = "{\"id\":1,\"name\":\"alice\"}\n{\"id\":2,\"name\":\"bob\"}\n";
        await containerClient.UploadBlobAsync($"{prefix}/data.json", new BinaryData(Ndjson));

        var source = new AzureSource(Config(fixture));
        var spec = new DatasetSpec("lake", "data", new Dictionary<string, object?>
        {
            ["container"] = AzuriteFixture.Container,
            ["path"] = $"{prefix}/data.json",
            ["format"] = "json",
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["name"] = "varchar" },
        });

        var schema = await source.GetSchemaAsync(spec, CancellationToken.None);

        Assert.Equal(2, schema.Schema.FieldsList.Count);
        Assert.Equal("id", schema.Schema.FieldsList[0].Name);
        Assert.IsType<Int64Type>(schema.Schema.FieldsList[0].DataType);
        Assert.Equal("name", schema.Schema.FieldsList[1].Name);
        Assert.IsType<StringType>(schema.Schema.FieldsList[1].DataType);
    }

    [SkippableFact]
    public async Task GetSchemaAsync_no_matching_blobs_is_a_clean_named_permanent_error()
    {
        DockerFacts.SkipUnlessDocker();

        var prefix = $"in-{Guid.NewGuid():N}";
        var source = new AzureSource(Config(fixture));
        var spec = new DatasetSpec("lake", "data", new Dictionary<string, object?>
        {
            ["container"] = AzuriteFixture.Container,
            ["path"] = $"{prefix}/*.parquet",
            ["format"] = "parquet",
        });

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await source.GetSchemaAsync(spec, CancellationToken.None));
        Assert.False(ex.IsTransient);
        Assert.Contains("no blobs matched", ex.Message, StringComparison.Ordinal);
    }
}
