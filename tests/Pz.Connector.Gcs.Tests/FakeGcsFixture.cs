using System.Net;
using System.Net.Sockets;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Google.Cloud.Storage.V1;
using Pz.TestSupport;

namespace Pz.Connector.Gcs.Tests;

/// <summary>Shared fake-gcs-server container for the gcs UNIVERSAL-tier e2e suite: a real
/// implementation of the GCS JSON api the Google SDK talks to. The host port is picked up front and
/// bound explicitly because fake-gcs-server bakes its <c>-external-url</c> into resumable-upload
/// session URLs -- a randomly mapped port would hand the SDK an unreachable continuation URL. Same
/// skip practice as the docker suites everywhere else.</summary>
public sealed class FakeGcsFixture : IAsyncLifetime
{
    private const string ImageName = "fsouza/fake-gcs-server:1.52.2";

    public const string Bucket = "pz-gcs-e2e";
    public const string Project = "pz-test";

    private IContainer? _container;

    public FakeGcsFixture()
    {
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();
    }

    public string BaseUrl { get; private set; } = "";

    /// <summary>An SDK client against the fake server: explicit BaseUri + unauthenticated access
    /// (the emulator validates no tokens), the exact seam
    /// <c>GcsSink</c>'s client factory injects in these tests.</summary>
    public StorageClient CreateClient() =>
        new StorageClientBuilder
        {
            BaseUri = $"{BaseUrl}/storage/v1/",
            UnauthenticatedAccess = true,
        }.Build();

    public async Task InitializeAsync()
    {
        var hostPort = FreeTcpPort();
        BaseUrl = $"http://localhost:{hostPort}";

        _container = new ContainerBuilder(ImageName)
            .WithPortBinding(hostPort, 4443)
            .WithCommand("-scheme", "http", "-external-url", BaseUrl)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(r =>
                r.ForPort(4443).ForPath("/_internal/healthcheck")))
            .Build();
        await _container.StartAsync().ConfigureAwait(false);

        var client = CreateClient();
        await client.CreateBucketAsync(Project, Bucket).ConfigureAwait(false);
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
        {
            await _container.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static int FreeTcpPort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }
}

[CollectionDefinition("fake-gcs")]
public sealed class FakeGcsCollection : ICollectionFixture<FakeGcsFixture>;
