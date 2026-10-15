using Azure.Storage.Blobs;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using Pz.TestSupport;

namespace Pz.Connector.AzureBlob.Tests;

/// <summary>Azurite serving HTTPS with a certificate the host's system trust store accepts -- the only emulator
/// setup that exercises TLS. The DuckDB azure extension's default transport cannot find the system CA bundle on
/// Linux, so a native read or write here fails exactly as it does against a real storage account unless the
/// connector selects the curl transport. Opt-in: <c>PZ_TEST_AZURITE_TLS_DIR</c> names a directory holding
/// <c>cert.pem</c> (SAN <c>IP:127.0.0.1</c>, already installed into the system trust store, e.g. with
/// <c>update-ca-certificates</c>) and <c>key.pem</c>. A class fixture that throws a skip is reported as failed,
/// so this one no-ops when disabled and the skip lives in the test method.</summary>
public sealed class AzuriteTlsFixture : IAsyncLifetime
{
    private const string ImageName = "mcr.microsoft.com/azure-storage/azurite:3.35.0";
    private const int BlobPort = 10000;

    // Azurite's well-known dev account key; not a secret.
    private const string DevAccountKey =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    public const string Category = "AzuriteTls";
    public const string Container = "pz-tls";

    private IContainer? container;

    public static string? CertDir =>
        Environment.GetEnvironmentVariable("PZ_TEST_AZURITE_TLS_DIR") is { Length: > 0 } dir ? dir : null;

    public static bool IsEnabled => CertDir is not null && DockerFacts.IsAvailable;

    public string ConnectionString { get; private set; } = "";

    public async Task InitializeAsync()
    {
        if (!IsEnabled)
        {
            return;
        }

        container = new ContainerBuilder(ImageName)
            .WithPortBinding(BlobPort, assignRandomHostPort: true)
            .WithBindMount(Path.GetFullPath(CertDir!), "/certs", AccessMode.ReadOnly)
            .WithCommand("azurite-blob", "--blobHost", "0.0.0.0", "--cert", "/certs/cert.pem", "--key", "/certs/key.pem",
                "--skipApiVersionCheck", "--loose")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("successfully listens on https"))
            .Build();
        await container.StartAsync().ConfigureAwait(false);

        ConnectionString = "DefaultEndpointsProtocol=https;AccountName=devstoreaccount1;" +
            $"AccountKey={DevAccountKey};BlobEndpoint=https://127.0.0.1:{container.GetMappedPublicPort(BlobPort)}/devstoreaccount1;";
        await new BlobServiceClient(ConnectionString).CreateBlobContainerAsync(Container).ConfigureAwait(false);
    }

    public async Task DisposeAsync()
    {
        if (container is not null)
        {
            await container.DisposeAsync().ConfigureAwait(false);
        }
    }
}
