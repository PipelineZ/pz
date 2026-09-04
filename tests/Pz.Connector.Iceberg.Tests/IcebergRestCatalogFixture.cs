using Amazon.S3;
using Azure.Storage.Blobs;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Pz.TestSupport;
using Testcontainers.Minio;

namespace Pz.Connector.Iceberg.Tests;

/// <summary>One Apache Iceberg REST catalog (the upstream <c>iceberg-rest-fixture</c> image: an
/// in-memory JDBC catalog that speaks the REST spec, no authentication) whose warehouse is a MinIO
/// bucket, both on one Docker network. The engine's DuckDB session on the host reaches the catalog
/// and the bucket through their mapped ports; the catalog reaches the bucket by network alias. The
/// constructor calls <see cref="DockerFacts.SkipUnlessDocker"/> and <see cref="DockerFacts.SkipIfOffline"/>
/// before any Testcontainers call, so a docker-less or explicitly-offline machine never attempts to
/// start a container; the SkipException re-thrown at test-class construction is reported as a Skip
/// by Xunit.SkippableFact.</summary>
public sealed class IcebergRestCatalogFixture : IAsyncLifetime
{
    private const string MinioImage = "minio/minio:RELEASE.2025-09-07T16-13-09Z";
    private const string RestImage = "apache/iceberg-rest-fixture:1.10.1";

    /// <summary>The warehouse bucket. DuckDB cannot create buckets, and neither can the catalog, so
    /// the fixture creates it up front via the AWS S3 SDK.</summary>
    public const string Bucket = "warehouse";

    private INetwork? network;
    private MinioContainer? minio;
    private IContainer? rest;

    public IcebergRestCatalogFixture()
    {
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();
    }

    /// <summary>The catalog's REST endpoint as the host sees it.</summary>
    public string Endpoint { get; private set; } = "";

    /// <summary>"host:port" -- the shape DuckDB's <c>s3</c> secret <c>endpoint</c> option expects.</summary>
    public string StorageEndpoint { get; private set; } = "";

    public string AccessKey { get; private set; } = "";

    public string SecretKey { get; private set; } = "";

    public string Warehouse => $"s3://{Bucket}/";

    public async Task InitializeAsync()
    {
        network = new NetworkBuilder().Build();
        await network.CreateAsync().ConfigureAwait(false);

        minio = new MinioBuilder(MinioImage).WithNetwork(network).WithNetworkAliases("minio").Build();
        await minio.StartAsync().ConfigureAwait(false);
        var uri = new Uri(minio.GetConnectionString());
        StorageEndpoint = $"{uri.Host}:{uri.Port}";
        AccessKey = minio.GetAccessKey();
        SecretKey = minio.GetSecretKey();

        var config = new AmazonS3Config
        {
            ServiceURL = minio.GetConnectionString(),
            ForcePathStyle = true,
            AuthenticationRegion = "us-east-1",
        };
        using (var client = new AmazonS3Client(AccessKey, SecretKey, config))
        {
            await client.PutBucketAsync(Bucket).ConfigureAwait(false);
        }

        rest = new ContainerBuilder(RestImage)
            .WithNetwork(network)
            .WithEnvironment("CATALOG_WAREHOUSE", Warehouse)
            .WithEnvironment("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
            .WithEnvironment("CATALOG_S3_ENDPOINT", "http://minio:9000")
            .WithEnvironment("CATALOG_S3_PATH__STYLE__ACCESS", "true")
            .WithEnvironment("AWS_ACCESS_KEY_ID", AccessKey)
            .WithEnvironment("AWS_SECRET_ACCESS_KEY", SecretKey)
            .WithEnvironment("AWS_REGION", "us-east-1")
            .WithPortBinding(8181, assignRandomHostPort: true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(r => r.ForPort(8181).ForPath("/v1/config")))
            .Build();
        await rest.StartAsync().ConfigureAwait(false);
        Endpoint = $"http://{rest.Hostname}:{rest.GetMappedPublicPort(8181)}";
    }

    public async Task DisposeAsync()
    {
        if (rest is not null)
        {
            await rest.DisposeAsync().ConfigureAwait(false);
        }

        if (minio is not null)
        {
            await minio.DisposeAsync().ConfigureAwait(false);
        }

        if (network is not null)
        {
            await network.DisposeAsync().ConfigureAwait(false);
        }
    }

    private AmazonS3Client CreateClient() => new(AccessKey, SecretKey, new AmazonS3Config
    {
        ServiceURL = $"http://{StorageEndpoint}",
        ForcePathStyle = true,
        AuthenticationRegion = "us-east-1",
    });

    /// <summary>The newest metadata file version of a table the catalog wrote (its files carry no
    /// version-hint), for a <c>files</c>-catalog read that must name it explicitly.</summary>
    public async Task<string> LatestMetadataVersionAsync(string ns, string table)
    {
        using var client = CreateClient();
        var listing = await client.ListObjectsV2Async(new Amazon.S3.Model.ListObjectsV2Request
        {
            BucketName = Bucket,
            Prefix = $"{ns}/{table}/metadata/",
        }).ConfigureAwait(false);
        const string suffix = ".metadata.json";
        var newest = listing.S3Objects
            .Select(o => o.Key)
            .Where(k => k.EndsWith(suffix, StringComparison.Ordinal))
            .Select(k => k[(k.LastIndexOf('/') + 1)..^suffix.Length])
            .OrderByDescending(v => v, StringComparer.Ordinal)
            .First();
        return newest;
    }

    /// <summary>Copies every object of one table (<c>ns/table/**</c>) from the MinIO warehouse into
    /// an Azure container under the same keys, so an <c>az://</c> files root can scan a table only a
    /// REST catalog could have written.</summary>
    public async Task MirrorTableAsync(string ns, string table, BlobContainerClient target)
    {
        using var client = CreateClient();
        var listing = await client.ListObjectsV2Async(new Amazon.S3.Model.ListObjectsV2Request
        {
            BucketName = Bucket,
            Prefix = $"{ns}/{table}/",
        }).ConfigureAwait(false);
        Assert.False(listing.IsTruncated);
        foreach (var key in listing.S3Objects.Select(o => o.Key))
        {
            using var response = await client.GetObjectAsync(Bucket, key).ConfigureAwait(false);
            await target.GetBlobClient(key).UploadAsync(response.ResponseStream, overwrite: true).ConfigureAwait(false);
        }
    }
}

[CollectionDefinition("iceberg-rest")]
public sealed class IcebergRestCatalogCollection : ICollectionFixture<IcebergRestCatalogFixture>;
