using Pz.Connector.AzureBlob;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Execution;
using Pz.Engine.Validation;

namespace Pz.Connector.AzureBlob.Tests;

/// <summary>Tier-3 strictness for azureblob source datasets: unknown or typo'd
/// dataset options must fail `pz validate` with PZ0301 instead of being silently ignored —
/// parity with postgres/localfiles. Driven through the real engine seam
/// (<see cref="ConnectorConfigValidator"/> + <see cref="ConnectorRegistry"/> + the real
/// <see cref="AzureConnector"/>), not the connector's schema string in isolation, so the
/// known-good option matrix below also pins that a strict schema never over-rejects a valid
/// project.</summary>
public sealed class AzureDatasetSchemaTests
{
    private static readonly Dictionary<string, object?> ValidConnection = new()
    {
        ["auth"] = "connection_string",
        ["connection_string"] = "UseDevelopmentStorage=true",
    };

    private static PzProject Project(ConnectionDef source) =>
        new("proj", "0.1.0", new EngineConfig(), new Dictionary<string, object?>(), [], [source], []);

    private static ConnectorRegistry Registry()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("azureblob", new AzureConnector());
        return registry;
    }

    [Fact]
    public async Task Known_good_dataset_option_matrix_produces_no_errors()
    {
        var source = new ConnectionDef("lake", "azureblob", ValidConnection,
            [
                // Minimal parquet dataset: container + path only.
                new DatasetDef("events", new Dictionary<string, object?>
                {
                    ["container"] = "raw",
                    ["path"] = "events/*.parquet",
                }, null),
                // Full surface: scheme + explicit format + a columns contract.
                new DatasetDef("customers", new Dictionary<string, object?>
                {
                    ["scheme"] = "abfss",
                    ["container"] = "raw",
                    ["path"] = "crm/customers.csv",
                    ["format"] = "csv",
                }, new Dictionary<string, string> { ["id"] = "bigint", ["name"] = "varchar" }),
            ], "sources/lake.yml");

        var errors = await ConnectorConfigValidator.ValidateAsync(Project(source), Registry(), default);

        Assert.Empty(errors);
    }

    [Fact]
    public async Task Files_per_partition_passes_tier3_so_the_plan_time_PZ0312_refusal_owns_it()
    {
        // files_per_partition on this native-only source is refused at PLAN time with PZ0312 and a
        // targeted remedial message. The strict schema must not preempt that with a generic
        // unknown-option error.
        var source = new ConnectionDef("lake", "azureblob", ValidConnection,
            [new DatasetDef("events", new Dictionary<string, object?>
            {
                ["container"] = "raw",
                ["path"] = "events/*.parquet",
                ["files_per_partition"] = 512L,
            }, null)], "sources/lake.yml");

        var errors = await ConnectorConfigValidator.ValidateAsync(Project(source), Registry(), default);

        Assert.Empty(errors);
    }

    [Fact]
    public async Task Unknown_dataset_option_is_PZ0301_naming_the_option()
    {
        // A stale or typo'd option must fail loudly at validate time, not be silently ignored.
        var source = new ConnectionDef("lake", "azureblob", ValidConnection,
            [new DatasetDef("events", new Dictionary<string, object?>
            {
                ["container"] = "raw",
                ["path"] = "events/*.parquet",
                ["max_rows_per_second"] = 100L,
            }, null)], "sources/lake.yml");

        var errors = await ConnectorConfigValidator.ValidateAsync(Project(source), Registry(), default);

        var error = Assert.Single(errors);
        Assert.Equal(PzErrorCode.ConnectorConfigInvalid, error.Code);
        Assert.Contains("max_rows_per_second", error.Message, StringComparison.Ordinal);
        Assert.Equal("sources/lake.yml", error.File);
    }

    [Fact]
    public async Task Unsupported_format_is_PZ0301()
    {
        // Without the enum, an unknown format would fall through AzureSource's else-branch and
        // silently plan a read_parquet scan over non-parquet objects.
        var source = new ConnectionDef("lake", "azureblob", ValidConnection,
            [new DatasetDef("events", new Dictionary<string, object?>
            {
                ["container"] = "raw",
                ["path"] = "events/*.avro",
                ["format"] = "avro",
            }, null)], "sources/lake.yml");

        var errors = await ConnectorConfigValidator.ValidateAsync(Project(source), Registry(), default);

        var error = Assert.Single(errors);
        Assert.Equal(PzErrorCode.ConnectorConfigInvalid, error.Code);
        Assert.Contains("format", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Missing_container_and_path_are_PZ0301_at_validate_time()
    {
        // ParseDataset would throw the same complaint at plan time; the schema's `required` moves
        // it to validate time where every dataset's problems aggregate into one report.
        var source = new ConnectionDef("lake", "azureblob", ValidConnection,
            [new DatasetDef("events", new Dictionary<string, object?>
            {
                ["scheme"] = "az",
            }, null)], "sources/lake.yml");

        var errors = await ConnectorConfigValidator.ValidateAsync(Project(source), Registry(), default);

        Assert.Equal(2, errors.Count);
        Assert.All(errors, e => Assert.Equal(PzErrorCode.ConnectorConfigInvalid, e.Code));
        Assert.Contains(errors, e => e.Message.Contains("container", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Message.Contains("path", StringComparison.Ordinal));
    }
}
