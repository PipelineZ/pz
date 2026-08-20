using Npgsql;
using Pz.TestSupport;
using Testcontainers.PostgreSql;

namespace Pz.Connector.Postgres.Tests;

/// <summary>Shared postgres container + seed data for the acceptance and type-matrix suites (one
/// collection, one container -- container startup is the expensive part). The constructor calls <see
/// cref="DockerFacts.SkipUnlessDocker"/> before any Testcontainers call, so a docker-less machine never
/// attempts to start a container.
///
/// Skip mechanism: a collection fixture constructor throwing <see cref="Xunit.SkipException"/> is
/// recorded by xunit's collection-fixture setup and re-thrown when each test class in the collection
/// is constructed; every test class in this collection (<see cref="PgTypeMatrixTests"/>,
/// <see cref="PostgresConnectivityTests"/>, <see cref="PostgresPartitioningTests"/>) uses
/// <c>[SkippableFact]</c>/<c>[SkippableTheory]</c>, so <c>Xunit.SkippableFact</c>'s message-bus
/// wrapper intercepts the re-thrown exception uniformly and reports a clean Skip everywhere in this
/// collection, not a Failure.</summary>
public sealed class PostgresContainerFixture : IAsyncLifetime
{
    private PostgreSqlContainer? _container;

    public PostgresContainerFixture()
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

    public async Task DisposeAsync()
    {
        if (_container is not null)
        {
            await _container.DisposeAsync().ConfigureAwait(false);
        }
    }

    private async Task SeedAsync()
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync().ConfigureAwait(false);

        // "orders": the acceptance suite's SmallDataset -- >= 100 rows (TestKit requires >= 100 and >= 2
        // batches under a 4KB batch target), 5 columns spanning a representative slice of the v0 matrix.
        await ExecuteAsync(connection, """
            create table public.orders (
                id integer primary key,
                name text not null,
                amount double precision not null,
                flag boolean not null,
                created timestamptz not null
            )
            """).ConfigureAwait(false);
        await ExecuteAsync(connection, """
            insert into public.orders (id, name, amount, flag, created)
            select i, 'row-' || i, i * 1.5, (i % 2 = 0),
                   timestamptz '2026-01-01T00:00:00Z' + (i || ' minutes')::interval
            from generate_series(0, 149) as i
            """).ConfigureAwait(false);

        // "matrix": the full v0 type matrix covering every DataReaderSource-mapped CLR
        // shape, incl. a 9-scale numeric and a NULL row for every column.
        await ExecuteAsync(connection, """
            create table public.matrix (
                id integer primary key,
                c_int integer,
                c_bigint bigint,
                c_double double precision,
                c_numeric numeric(38, 9),
                c_text text,
                c_bool boolean,
                c_date date,
                c_timestamp timestamp,
                c_timestamptz timestamptz
            )
            """).ConfigureAwait(false);

        await using (var insert = new NpgsqlCommand(
            """
            insert into public.matrix
                (id, c_int, c_bigint, c_double, c_numeric, c_text, c_bool, c_date, c_timestamp, c_timestamptz)
            values
                (1, 42, 9000000000, 1.5, 12345.123456789, 'hello', true,
                 date '2026-03-27', timestamp '2026-03-27 10:30:00', timestamptz '2026-03-27 10:30:00+00')
            """,
            connection))
        {
            await insert.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        await ExecuteAsync(connection, "insert into public.matrix (id) values (2)").ConfigureAwait(false);
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
    }
}

[CollectionDefinition("postgres")]
public sealed class PostgresCollection : ICollectionFixture<PostgresContainerFixture>;
