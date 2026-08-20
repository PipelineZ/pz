using Pz.Connectors.Abstractions;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Execution;
using Pz.Engine.Validation;

namespace Pz.Connector.Postgres.Tests;

/// <summary>Tier 5 (`ConnectivityValidator`) against a real, running postgres (see
/// <see cref="PostgresContainerFixture"/>): unlike LocalFiles' CSV (whose `GetSchemaAsync` merely echoes
/// the declared contract -- CSV has no independent type info to discover), Postgres genuinely queries
/// `information_schema`-equivalent metadata via `PostgresSource.GetSchemaAsync`'s `select ... limit 0`,
/// so this is the one connector that can demonstrate a real type-mismatch drift signal end to end.</summary>
[Collection("postgres")]
public sealed class PostgresConnectivityTests(PostgresContainerFixture fixture)
{
    private PzProject Project(ConnectionDef source) =>
        new("proj", "0.1.0", new EngineConfig(), new Dictionary<string, object?>(), [],
            [source], []);

    private Dictionary<string, object?> ValidConnection() => new()
    {
        ["host"] = fixture.Host,
        ["port"] = fixture.Port,
        ["database"] = fixture.Database,
        ["user"] = fixture.User,
        ["password"] = fixture.Password,
    };

    [SkippableFact]
    public async Task Postgres_drift_detected_against_live_schema()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("postgres", new PostgresConnector());

        // public.orders.name is a real `text` column (Utf8) -- declaring it "bigint" under the alias
        // "email" is a genuine type mismatch a live schema query can actually catch, unlike CSV.
        var source = new ConnectionDef("pg", "postgres", ValidConnection(),
            [new DatasetDef("customers",
                new Dictionary<string, object?> { ["query"] = "select name as email from public.orders limit 5" },
                new Dictionary<string, string> { ["email"] = "bigint" })],
            "sources/pg.yml");

        var result = await ConnectivityValidator.RunAsync(Project(source), registry, default);

        var error = Assert.Single(result.Errors);
        Assert.Equal(PzErrorCode.SchemaDrift, error.Code);
        Assert.Contains("email", error.Message, StringComparison.Ordinal);
        Assert.Contains("bigint", error.Message, StringComparison.Ordinal);
    }

    [SkippableFact]
    public async Task Postgres_connectivity_ok_and_no_drift_on_matching_contract()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("postgres", new PostgresConnector());

        var source = new ConnectionDef("pg", "postgres", ValidConnection(),
            [new DatasetDef("orders",
                new Dictionary<string, object?>(),
                new Dictionary<string, string> { ["id"] = "int", ["name"] = "varchar" })],
            "sources/pg.yml");

        var result = await ConnectivityValidator.RunAsync(Project(source), registry, default);

        Assert.Empty(result.Errors);
    }

    [SkippableFact]
    public async Task Postgres_connection_check_failure_is_PZ0330()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("postgres", new PostgresConnector());

        var unreachable = new Dictionary<string, object?>
        {
            ["host"] = "127.0.0.1",
            ["port"] = 1, // nothing listens on port 1
            ["database"] = fixture.Database,
            ["user"] = fixture.User,
            ["password"] = fixture.Password,
        };
        var source = new ConnectionDef("pg", "postgres", unreachable, [], "sources/pg.yml");

        var result = await ConnectivityValidator.RunAsync(Project(source), registry, default);

        var error = Assert.Single(result.Errors);
        Assert.Equal(PzErrorCode.ConnectionCheckFailed, error.Code);
        Assert.Contains("pg", error.Message, StringComparison.Ordinal);
    }
}
