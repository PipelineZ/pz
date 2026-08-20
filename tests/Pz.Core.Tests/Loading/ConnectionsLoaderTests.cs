using Pz.Core.Loading;
using Pz.Core.Model;
using Pz.Core.Validation;

namespace Pz.Core.Tests.Loading;

/// <summary>One connections.yml at the project root, three
/// levels deep -- connection, entity, direction. Connector config is flat, because connectors already
/// publish a JSON Schema for it and a nesting level buys nothing.</summary>
public class ConnectionsLoaderTests
{
    private static readonly IReadOnlyDictionary<string, string> Env =
        new Dictionary<string, string> { ["WH_HOST"] = "db.internal" };

    private static string TempProject(string connectionsYaml)
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-connections-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "project.yml"), "name: t\nversion: \"1\"\n");
        File.WriteAllText(Path.Combine(dir, "connections.yml"), connectionsYaml);
        return dir;
    }

    private static PzProject Load(string connectionsYaml)
    {
        var dir = TempProject(connectionsYaml);
        try
        {
            return ProjectLoader.Load(dir, Env);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    private static IReadOnlyList<PzError> Errors(string connectionsYaml) =>
        Assert.Throws<PzValidationException>(() => Load(connectionsYaml)).Errors;

    [Fact]
    public void Connector_config_is_flat_and_env_interpolated()
    {
        var connection = Assert.Single(Load("""
            warehouse:
              connector: postgres
              host: ${WH_HOST}
              database: prod
            """).Connections);

        Assert.Equal("warehouse", connection.Name);
        Assert.Equal("postgres", connection.Connector);
        Assert.Equal("db.internal", connection.Connection["host"]);
        Assert.Equal("prod", connection.Connection["database"]);
        Assert.Equal("connections.yml", connection.FilePath);
    }

    [Fact]
    public void Instance_tuning_is_not_connector_config()
    {
        var connection = Assert.Single(Load("""
            warehouse:
              connector: postgres
              host: h
              max_concurrency: 4
              rate_limit: { requests_per_minute: 600 }
              retry: { max_attempts: 3 }
            """).Connections);

        Assert.Equal(4, connection.MaxConcurrency);
        Assert.Equal(600, connection.RateLimit!.RequestsPerMinute);
        Assert.Equal(3, connection.Retry!.MaxAttempts);
        Assert.Equal(["host"], connection.Connection.Keys);
    }

    [Fact]
    public void An_entity_read_block_becomes_a_dataset()
    {
        var connection = Assert.Single(Load("""
            warehouse:
              connector: postgres
              host: h
              entities:
                raw.orders:
                  read:
                    partition_column: order_id
                    partitions: 8
                    columns: { id: bigint }
                    sync: { mode: incremental, cursor: updated_at }
            """).Connections);

        var dataset = Assert.Single(connection.Datasets);
        Assert.Equal("raw.orders", dataset.Name);
        Assert.Equal("order_id", dataset.Options["partition_column"]);
        Assert.Equal(8L, dataset.Options["partitions"]);
        Assert.Equal("bigint", dataset.Columns!["id"]);
        Assert.Equal("updated_at", dataset.SyncMode!.Incremental!.Cursor);
        Assert.DoesNotContain("columns", dataset.Options.Keys);
        Assert.DoesNotContain("sync", dataset.Options.Keys);
    }

    [Fact]
    public void A_write_only_entity_declares_no_dataset()
    {
        var connection = Assert.Single(Load("""
            mart:
              connector: postgres
              host: h
              entities:
                mart.orders_current:
                  write:
                    strategy: merge
                    keys: [order_id]
            """).Connections);

        Assert.Empty(connection.Datasets);
        var write = connection.EntityWrites["mart.orders_current"];
        Assert.Equal("merge", write.Mode);
        Assert.Equal(["order_id"], write.Keys);
    }

    [Fact]
    public void An_entity_written_and_read_back_appears_once()
    {
        var connection = Assert.Single(Load("""
            warehouse:
              connector: postgres
              host: h
              entities:
                mart.orders:
                  read:
                    columns: { id: bigint }
                  write:
                    strategy: replace
            """).Connections);

        Assert.Equal("mart.orders", Assert.Single(connection.Datasets).Name);
        Assert.Equal("replace", connection.EntityWrites["mart.orders"].Mode);
    }

    [Fact]
    public void An_entity_with_neither_direction_is_a_shape_error()
    {
        var error = Assert.Single(Errors("""
            warehouse:
              connector: postgres
              host: h
              entities:
                raw.orders: {}
            """), e => e.Code == PzErrorCode.YamlShape);

        Assert.Contains("raw.orders", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void An_unknown_key_under_an_entity_is_refused_not_ignored()
    {
        var error = Assert.Single(Errors("""
            warehouse:
              connector: postgres
              host: h
              entities:
                raw.orders:
                  reed: { columns: { id: bigint } }
            """), e => e.Code == PzErrorCode.YamlShape);

        Assert.Contains("reed", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_malformed_entity_name_is_PZ0344() =>
        Assert.Single(Errors("""
            warehouse:
              connector: postgres
              host: h
              entities:
                "raw.":
                  read: {}
            """), e => e.Code == PzErrorCode.EntityNameInvalid);

    [Fact]
    public void A_connection_without_a_connector_is_refused()
    {
        var error = Assert.Single(Errors("""
            warehouse:
              host: h
            """), e => e.Code == PzErrorCode.YamlShape);

        Assert.Contains("warehouse", error.Message, StringComparison.Ordinal);
        Assert.Contains("connector", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Rate_limit_under_an_entity_read_is_refused_as_instance_level() =>
        Assert.Single(Errors("""
            warehouse:
              connector: postgres
              host: h
              entities:
                raw.orders:
                  read:
                    rate_limit: { requests_per_minute: 60 }
            """), e => e.Code == PzErrorCode.RateLimitConfigInvalid);

    [Fact]
    public void An_absent_file_loads_an_empty_project()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-connections-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "project.yml"), "name: t\nversion: \"1\"\n");
        try
        {
            Assert.Empty(ProjectLoader.Load(dir, Env).Connections);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void A_leftover_sources_directory_is_PZ0346_with_the_block_to_write()
    {
        var dir = TempProject("lake:\n  connector: localfiles\n");
        Directory.CreateDirectory(Path.Combine(dir, "sources"));
        File.WriteAllText(Path.Combine(dir, "sources", "crm.yml"), """
            source: crm
            connector: localfiles
            connection: {}
            max_concurrency: 2
            datasets:
              customers:
                path: data/customers.csv
                format: csv
                columns:
                  id: bigint
            """);
        try
        {
            var error = Assert.Single(
                Assert.Throws<PzValidationException>(() => ProjectLoader.Load(dir, Env)).Errors,
                e => e.Code == PzErrorCode.RetiredConnectionDirectory);

            Assert.Contains("sources/crm.yml", error.ToString(), StringComparison.Ordinal);
            foreach (var fragment in new[]
            {
                "crm:", "connector: localfiles", "max_concurrency: 2", "entities:", "customers:", "read:",
                "path: data/customers.csv", "columns:", "id: bigint",
            })
            {
                Assert.Contains(fragment, error.Hint!, StringComparison.Ordinal);
            }
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void A_leftover_sinks_directory_is_PZ0346_too()
    {
        var dir = TempProject("lake:\n  connector: localfiles\n");
        Directory.CreateDirectory(Path.Combine(dir, "sinks"));
        File.WriteAllText(Path.Combine(dir, "sinks", "warehouse.yml"),
            "sink: warehouse\nconnector: postgres\nconnection:\n  host: h\n");
        try
        {
            var error = Assert.Single(
                Assert.Throws<PzValidationException>(() => ProjectLoader.Load(dir, Env)).Errors,
                e => e.Code == PzErrorCode.RetiredConnectionDirectory);

            Assert.Contains("warehouse:", error.Hint!, StringComparison.Ordinal);
            Assert.Contains("host: h", error.Hint, StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Theory]
    [InlineData("columns: [id, amount]")]
    [InlineData("columns: 42")]
    public void A_non_mapping_columns_contract_is_refused_not_silently_dropped(string columns)
    {
        var error = Assert.Single(Errors($"""
            warehouse:
              connector: postgres
              host: h
              entities:
                orders:
                  read:
                    {columns}
            """), e => e.Code == PzErrorCode.YamlShape);

        Assert.Contains("columns", error.Message, StringComparison.Ordinal);
        Assert.Contains("mapping", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_non_scalar_column_type_is_refused()
    {
        var error = Assert.Single(Errors("""
            warehouse:
              connector: postgres
              host: h
              entities:
                orders:
                  read:
                    columns:
                      id: { nested: true }
            """), e => e.Code == PzErrorCode.YamlShape);

        Assert.Contains("id", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_bodyless_columns_key_still_means_no_contract()
    {
        var connection = Assert.Single(Load("""
            warehouse:
              connector: postgres
              host: h
              entities:
                orders:
                  read:
                    columns:
            """).Connections);

        Assert.Null(Assert.Single(connection.Datasets).Columns);
    }

    [Fact]
    public void A_dbt_style_outputs_block_is_PZ0347_not_a_connection_named_outputs()
    {
        var error = Assert.Single(Errors("""
            warehouse:
              connector: postgres
              host: h

            outputs:
              dev:
                threads: 1
            """), e => e.Code == PzErrorCode.RetiredOutputsBlock);

        Assert.DoesNotContain("connector", error.Message, StringComparison.Ordinal);
    }
}
