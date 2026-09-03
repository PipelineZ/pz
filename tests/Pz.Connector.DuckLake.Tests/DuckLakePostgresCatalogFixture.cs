using Pz.TestSupport;
using Testcontainers.PostgreSql;

namespace Pz.Connector.DuckLake.Tests;

/// <summary>One postgres container for the postgres-catalog suite. The constructor calls
/// <see cref="DockerFacts.SkipUnlessDocker"/> before any Testcontainers call, so a docker-less
/// machine never attempts to start a container; the SkipException re-thrown at test-class
/// construction is reported as a Skip by Xunit.SkippableFact.</summary>
public sealed class DuckLakePostgresCatalogFixture : IAsyncLifetime
{
    private PostgreSqlContainer? container;

    public DuckLakePostgresCatalogFixture()
    {
        DockerFacts.SkipUnlessDocker();
    }

    public string Host { get; private set; } = "";
    public int Port { get; private set; }
    public string Database => "pz";
    public string User => "pz";
    public string Password => "pz";

    public async Task InitializeAsync()
    {
        container = new PostgreSqlBuilder("postgres:16-alpine")
            .WithDatabase(Database).WithUsername(User).WithPassword(Password).Build();
        await container.StartAsync();
        Host = container.Hostname;
        Port = container.GetMappedPublicPort(5432);
    }

    public async Task DisposeAsync()
    {
        if (container is not null)
        {
            await container.DisposeAsync();
        }
    }
}

[CollectionDefinition("ducklake-postgres")]
public sealed class DuckLakePostgresCatalogCollection : ICollectionFixture<DuckLakePostgresCatalogFixture>;
