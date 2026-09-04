using Azure.Storage.Blobs;
using Pz.TestSupport;
using Testcontainers.Azurite;

namespace Pz.Connector.Iceberg.Tests;

/// <summary>One Azurite container holding a mirror of a table the REST fixture wrote. Azurite
/// emulates the Blob endpoint only (no DFS endpoint), so it can host an <c>az://</c> files root but
/// never a REST catalog's warehouse. A class fixture must never throw <see cref="SkipException"/>
/// from its constructor -- xunit wraps that in a TestClassException and reports FAILED, not Skipped
/// (see <see cref="DockerFacts.IsAvailable"/>) -- so the guards live in the consuming test method
/// (<see cref="DockerFacts.SkipUnlessDocker"/>/<see cref="DockerFacts.SkipIfOffline"/>) and this
/// fixture's <see cref="InitializeAsync"/> no-ops without docker or under PZ_TESTS_OFFLINE.</summary>
public sealed class IcebergAzuriteFixture : IAsyncLifetime
{
    // Pinned in step with tests/Pz.Connector.AzureBlob.Tests/AzuriteFixture.cs: older tags reject
    // the Azure.Storage.Blobs 12.24.0 SDK's default REST API version.
    private const string ImageName = "mcr.microsoft.com/azure-storage/azurite:3.35.0";

    public const string Container = "pz-iceberg-e2e";

    private AzuriteContainer? container;

    /// <summary>Azurite's well-known dev-account connection string — the shape DuckDB's
    /// <c>type azure</c> secret's <c>connection_string</c> expects.</summary>
    public string ConnectionString { get; private set; } = "";

    public BlobContainerClient Blobs { get; private set; } = null!;

    public string Root => $"az://{Container}/";

    public async Task InitializeAsync()
    {
        if (!DockerFacts.IsAvailable || DockerFacts.IsOffline)
        {
            return;
        }

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
