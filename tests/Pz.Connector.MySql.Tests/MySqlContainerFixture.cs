using Pz.Connectors.Abstractions;
using Pz.TestSupport;
using Testcontainers.MySql;

namespace Pz.Connector.MySql.Tests;

/// <summary>Shared MySQL container for the whole collection (one container, many order-independent
/// tests -- each test picks its own unique table name). Unlike <c>MsSqlContainerFixture</c> (which
/// starts eagerly in <see cref="IAsyncLifetime.InitializeAsync"/>), this fixture lazy-starts on first
/// use via <see cref="EnsureStartedAsync"/>: xunit constructs every collection fixture up front
/// regardless of whether any test in the collection actually runs, so an eager start would pay for a
/// docker pull/container boot even when every test in the file is about to skip under
/// <see cref="DockerFacts.SkipIfOffline"/>. Callers MUST run <see cref="DockerFacts.SkipUnlessDocker"/>
/// and <see cref="DockerFacts.SkipIfOffline"/> themselves before calling <see cref="EnsureStartedAsync"/>
/// -- this type never checks either gate itself.</summary>
public sealed class MySqlContainerFixture : IAsyncLifetime
{
    private readonly SemaphoreSlim _startGate = new(1, 1);
    private MySqlContainer? _container;

    public string Hostname { get; private set; } = "";
    public int Port { get; private set; }
    public string Database => "pz";
    public string Username => "pz";
    public string Password => "pz_pw";

    /// <summary>Ready-made connection dictionary matching the connector's config keys
    /// (host/port/database/user/password -- see <c>MySqlSqlGenTests.Config</c>).</summary>
    public ConnectorConfig Config => new(new Dictionary<string, object?>
    {
        ["host"] = Hostname,
        ["port"] = Port,
        ["database"] = Database,
        ["user"] = Username,
        ["password"] = Password,
    });

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        if (_container is not null)
        {
            await _container.DisposeAsync().ConfigureAwait(false);
        }
    }

    public async Task EnsureStartedAsync()
    {
        if (_container is not null)
        {
            return;
        }

        await _startGate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_container is not null)
            {
                return;
            }

            var container = new MySqlBuilder("mysql:8.4")
                .WithDatabase(Database)
                .WithUsername(Username)
                .WithPassword(Password)
                .Build();
            await container.StartAsync().ConfigureAwait(false);

            Hostname = container.Hostname;
            Port = container.GetMappedPublicPort(3306);
            _container = container;
        }
        finally
        {
            _startGate.Release();
        }
    }
}

[CollectionDefinition("mysql")]
public sealed class MySqlCollection : ICollectionFixture<MySqlContainerFixture>;
