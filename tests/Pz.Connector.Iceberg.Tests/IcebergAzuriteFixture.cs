using Azure.Storage.Blobs;
using Pz.TestSupport;
using Testcontainers.Azurite;

namespace Pz.Connector.Iceberg.Tests;

/// <summary>One Azurite container holding a mirror of a table the REST fixture wrote. Azurite
/// emulates the Blob endpoint only (no DFS endpoint), so it can host an <c>az://</c> files root but
/// never a REST catalog's warehouse. The constructor calls <see cref="DockerFacts.SkipUnlessDocker"/>
/// and <see cref="DockerFacts.SkipIfOffline"/> before any Testcontainers call.</summary>
public sealed class IcebergAzuriteFixture : IAsyncLifetime
{
    // Pinned in step with tests/Pz.Connector.AzureBlob.Tests/AzuriteFixture.cs: older tags reject
    // the Azure.Storage.Blobs 12.24.0 SDK's default REST API version.
    private const string ImageName = "mcr.microsoft.com/azure-storage/azurite:3.35.0";

    public const string Container = "pz-iceberg-e2e";

    private AzuriteContainer? container;

    public IcebergAzuriteFixture()
    {
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();
    }

    /// <summary>Azurite's well-known dev-account connection string — the shape DuckDB's
    /// <c>type azure</c> secret's <c>connection_string</c> expects.</summary>
    public string ConnectionString { get; private set; } = "";

    public BlobContainerClient Blobs { get; private set; } = null!;

    public string Root => $"az://{Container}/";

    public async Task InitializeAsync()
    {
        container = new AzuriteBuilder(ImageName).Build();
        await container.StartAsync().ConfigureAwait(false);
        ConnectionString = container.GetConnectionString();
        Blobs = new BlobServiceClient(ConnectionString).GetBlobContainerClient(Container);
        await Blobs.CreateAsync().ConfigureAwait(false);
    }

    public async Task DisposeAsync()
    {
        if (container is not null)
        {
            await container.DisposeAsync().ConfigureAwait(false);
        }
    }
}
