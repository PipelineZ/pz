using Amazon.S3;
using Pz.TestSupport;
using Testcontainers.Minio;

namespace Pz.Connector.S3.Tests;

/// <summary>Shared MinIO container for the S3 e2e suite (one collection, one container -- container
/// startup is the expensive part). The constructor calls <see cref="DockerFacts.SkipUnlessDocker"/> and
/// <see cref="DockerFacts.SkipIfOffline"/> before any Testcontainers call, so a docker-less or
/// explicitly-offline machine never attempts to start a container or pull an image. Pinned image tag
/// (mirrors <c>PostgresContainerFixture</c>'s "postgres:16-alpine" pinning practice).</summary>
public sealed class MinioFixture : IAsyncLifetime
{
    private const string ImageName = "minio/minio:RELEASE.2025-09-07T16-13-09Z";

    /// <summary>Bucket created during <see cref="InitializeAsync"/> -- DuckDB's <c>COPY ... TO 's3://'</c>
    /// cannot create buckets itself, so this fixture creates it up front via the AWS S3 SDK (the same
    /// approach the upstream Testcontainers.Minio test suite itself uses).</summary>
    public const string Bucket = "pz-e2e";

    private MinioContainer? _container;

    public MinioFixture()
    {
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();
    }

    public string Host { get; private set; } = "";

    public int Port { get; private set; }

    public string AccessKey { get; private set; } = "";

    public string SecretKey { get; private set; } = "";

    /// <summary>"host:port" -- the shape DuckDB's <c>s3</c> secret <c>endpoint</c> option expects (no
    /// scheme; <c>use_ssl</c> controls http vs. https separately).</summary>
    public string Endpoint => $"{Host}:{Port}";

    public async Task InitializeAsync()
    {
        _container = new MinioBuilder(ImageName).Build();
        await _container.StartAsync().ConfigureAwait(false);

        var connectionString = _container.GetConnectionString();
        var uri = new Uri(connectionString);
        Host = uri.Host;
        Port = uri.Port;
        AccessKey = _container.GetAccessKey();
        SecretKey = _container.GetSecretKey();

        var config = new AmazonS3Config
        {
            ServiceURL = connectionString,
            ForcePathStyle = true,
            AuthenticationRegion = "us-east-1",
        };
        using var client = new AmazonS3Client(AccessKey, SecretKey, config);
        await client.PutBucketAsync(Bucket).ConfigureAwait(false);
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
        {
            await _container.DisposeAsync().ConfigureAwait(false);
        }
    }
}

[CollectionDefinition("minio")]
public sealed class MinioCollection : ICollectionFixture<MinioFixture>;
