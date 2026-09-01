using Amazon.S3;
using Pz.TestSupport;
using Testcontainers.Minio;

namespace Pz.Connector.Gcs.Tests;

/// <summary>Shared MinIO container for the gcs NATIVE-tier e2e suite. DuckDB's <c>type gcs</c>
/// secret is the s3 interop protocol with a different default endpoint, so MinIO -- pointed at via
/// the connection's <c>endpoint</c> override -- exercises the exact statement shapes the connector
/// generates (<c>gs://</c> URLs, hmac key pair) without needing Google's real interop endpoint.
/// Same skip/pinning practice as the s3 suite's MinioFixture.</summary>
public sealed class GcsMinioFixture : IAsyncLifetime
{
    private const string ImageName = "minio/minio:RELEASE.2025-09-07T16-13-09Z";

    /// <summary>Bucket created during <see cref="InitializeAsync"/> -- DuckDB's COPY cannot create
    /// buckets itself, so this fixture creates it up front via the AWS S3 SDK.</summary>
    public const string Bucket = "pz-gcs-e2e";

    private MinioContainer? _container;

    public GcsMinioFixture()
    {
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();
    }

    public string AccessKey { get; private set; } = "";

    public string SecretKey { get; private set; } = "";

    /// <summary>"host:port" -- the shape DuckDB's secret <c>endpoint</c> option expects (no scheme;
    /// <c>use_ssl</c> controls http vs. https separately).</summary>
    public string Endpoint { get; private set; } = "";

    public async Task InitializeAsync()
    {
        _container = new MinioBuilder(ImageName).Build();
        await _container.StartAsync().ConfigureAwait(false);

        var connectionString = _container.GetConnectionString();
        var uri = new Uri(connectionString);
        Endpoint = $"{uri.Host}:{uri.Port}";
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

[CollectionDefinition("gcs-minio")]
public sealed class GcsMinioCollection : ICollectionFixture<GcsMinioFixture>;
