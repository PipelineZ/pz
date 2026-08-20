using Npgsql;
using Pz.TestSupport;
using Testcontainers.PostgreSql;

namespace Pz.Connector.Postgres.Tests;

/// <summary>Dedicated postgres container for the cdc suite: started with <c>wal_level=logical</c> (the
/// non-cdc suites keep the stock <see cref="PostgresContainerFixture"/> so they never pay for WAL
/// bookkeeping). The default superuser role the image creates has <c>rolsuper</c>, so the REPLICATION
/// prerequisite is satisfied out of the box; publications and cdc-specific tables are seeded per-test so
/// each test controls its own prerequisite state.
/// <see cref="PostgresSourceAcceptance"/> also shares this container (its "orders" table, seeded below)
/// so its change-capture facts run against a wal_level=logical server -- as with
/// <c>MsSqlContainerFixture</c>, one shared container serves both the plain acceptance suite and its
/// cdc-specific tests. Skips cleanly on a docker-less machine, same mechanism as
/// <see cref="PostgresContainerFixture"/>.</summary>
public sealed class PostgresCdcContainerFixture : IAsyncLifetime
{
    private PostgreSqlContainer? _container;

    public PostgresCdcContainerFixture()
    {
        DockerFacts.SkipUnlessDocker();
    }

    public string Host { get; private set; } = "";

    public int Port { get; private set; }

    public string Database { get; private set; } = "";

    public string User { get; private set; } = "";

    public string Password { get; private set; } = "";

    public string ConnectionString { get; private set; } = "";

    public async Task InitializeAsync()
    {
        _container = new PostgreSqlBuilder("postgres:16-alpine")
            .WithDatabase("pz")
            .WithUsername("pz")
            .WithPassword("pz")
            // docker-entrypoint.sh prepends "postgres" when the command starts with '-', so this becomes
            // `postgres -c wal_level=logical -c max_replication_slots=64` -- wal_level=logical is the one
            // server setting logical replication slots require; max_replication_slots is raised well past
            // the 10-slot default because every cdc test in this collection creates its own never-cleaned
            // -up slot (each test's slot name is derived from its own unique dataset name, so a slot from
            // an earlier test is never a NAME conflict for a later one, but it does count against this
            // server-wide ceiling for the life of the container). Past the ceiling the server reports
            // "all replication slots are in use".
            .WithCommand("-c", "wal_level=logical", "-c", "max_replication_slots=64")
            .Build();
        await _container.StartAsync().ConfigureAwait(false);

        ConnectionString = _container.GetConnectionString();
        var csb = new NpgsqlConnectionStringBuilder(ConnectionString);
        Host = csb.Host!;
        Port = csb.Port;
        Database = csb.Database!;
        User = csb.Username!;
        Password = csb.Password!;

        await SeedAsync().ConfigureAwait(false);
    }

    // "orders": PostgresSourceAcceptance's SmallDataset -- >= 100 rows (TestKit requires >= 100 and
    // >= 2 batches under a 4KB batch target), same shape as PostgresContainerFixture's copy;
    // PostgresSourceAcceptance lives on this container so its change-capture facts get the
    // wal_level=logical server they need.
    private async Task SeedAsync()
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync().ConfigureAwait(false);
        await using var command = new NpgsqlCommand("""
            create table public.orders (
                id integer primary key,
                name text not null,
                amount double precision not null,
                flag boolean not null,
                created timestamptz not null
            )
            """, connection);
        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
        await using var insert = new NpgsqlCommand("""
            insert into public.orders (id, name, amount, flag, created)
            select i, 'row-' || i, i * 1.5, (i % 2 = 0),
                   timestamptz '2026-01-01T00:00:00Z' + (i || ' minutes')::interval
            from generate_series(0, 149) as i
            """, connection);
        await insert.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
        {
            await _container.DisposeAsync().ConfigureAwait(false);
        }
    }
}

[CollectionDefinition("postgres-cdc")]
public sealed class PostgresCdcCollection : ICollectionFixture<PostgresCdcContainerFixture>;
