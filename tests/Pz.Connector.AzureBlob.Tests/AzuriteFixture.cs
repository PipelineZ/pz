using Azure.Storage.Blobs;
using Pz.TestSupport;
using Testcontainers.Azurite;

namespace Pz.Connector.AzureBlob.Tests;

/// <summary>Shared Azurite container for the azure e2e suite (mirrors <c>MinioFixture</c>). The
/// constructor calls <see cref="DockerFacts.SkipUnlessDocker"/> and <see cref="DockerFacts.SkipIfOffline"/>
/// before any Testcontainers call, so a docker-less or explicitly-offline machine never attempts to start
/// a container or pull an image. Pinned image tag. Azurite emulates the Blob endpoint faithfully (ADLS
/// Gen2 `abfss://` is NOT covered -- that is the documented e2e gap; `abfss` is unit-tested only).</summary>
public sealed class AzuriteFixture : IAsyncLifetime
{
    // Pinned above 3.33.0: the Azure.Storage.Blobs 12.24.0 SDK's default REST API version
    // (2025-05-05) is rejected by Azurite versions older than 3.35.0 ("API version ... is not
    // supported by Azurite" / InvalidHeaderValue) -- 3.35.0 is the first pinned tag confirmed to
    // accept it.
    private const string ImageName = "mcr.microsoft.com/azure-storage/azurite:3.35.0";

    /// <summary>Container created during <see cref="InitializeAsync"/> -- DuckDB's <c>COPY ... TO
    /// 'az://'</c> cannot create containers itself, so this fixture creates it up front via the Azure
    /// Storage Blobs SDK (the same approach <c>MinioFixture</c> uses for its bucket).</summary>
    public const string Container = "pz-e2e";

    private AzuriteContainer? _container;

    public AzuriteFixture()
    {
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();
    }

    /// <summary>Azurite well-known dev account connection string -- the shape DuckDB's <c>azure</c>
    /// secret's <c>connection_string</c> option expects.</summary>
    public string ConnectionString { get; private set; } = "";

    public async Task InitializeAsync()
    {
        _container = new AzuriteBuilder(ImageName).Build();
        await _container.StartAsync().ConfigureAwait(false);
        ConnectionString = _container.GetConnectionString();

        var service = new BlobServiceClient(ConnectionString);
        await service.CreateBlobContainerAsync(Container).ConfigureAwait(false);
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
        {
            await _container.DisposeAsync().ConfigureAwait(false);
        }
    }
}

[CollectionDefinition("azurite")]
public sealed class AzuriteCollection : ICollectionFixture<AzuriteFixture>;
