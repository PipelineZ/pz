using Pz.Core.Loading;
using Pz.Core.Validation;

namespace Pz.Core.Tests.Loading;

/// <summary>The dataset key IS the object name. A leftover
/// <c>schema:</c>/<c>table:</c> is refused (PZ0348) with the joined name to rename the key to, and a
/// name that cannot name anything is PZ0344.</summary>
public class RetiredEntityQualifierTests
{
    private static readonly IReadOnlyDictionary<string, string> Env = new Dictionary<string, string>();

    private static IReadOnlyList<PzError> LoadErrors(string sourceYaml)
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-entity-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "project.yml"), "name: t\nversion: \"1\"\n");
        File.WriteAllText(Path.Combine(dir, "connections.yml"), sourceYaml);
        try
        {
            return Assert.Throws<PzValidationException>(() => ProjectLoader.Load(dir, Env)).Errors;
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Theory]
    [InlineData("table: orders_v2", "orders_v2")]
    [InlineData("schema: dbo", "dbo.orders")]
    public void A_dataset_qualifier_is_PZ0348_with_the_entity_name_to_use(string line, string expected)
    {
        var error = Assert.Single(LoadErrors($$"""
            erp:
              connector: sqlserver
              entities:
                orders:
                  read:
                    {{line}}
            """), e => e.Code == PzErrorCode.RetiredEntityQualifier);

        Assert.Contains("entity 'orders'", error.Message, StringComparison.Ordinal);
        Assert.Contains($"'{expected}'", error.Hint!, StringComparison.Ordinal);
    }

    [Fact]
    public void Both_qualifiers_are_reported_once_with_the_joined_name()
    {
        var errors = LoadErrors("""
            erp:
              connector: sqlserver
              entities:
                orders:
                  read:
                    schema: dbo
                    table: orders_v2
            """);

        var error = Assert.Single(errors, e => e.Code == PzErrorCode.RetiredEntityQualifier);
        Assert.Contains("'dbo.orders_v2'", error.Hint!, StringComparison.Ordinal);
    }

    [Fact]
    public void A_malformed_dataset_name_is_PZ0344()
    {
        var errors = LoadErrors("""
            erp:
              connector: sqlserver
              entities:
                "dbo.":
                  read:
                    partitions: 2
            """);

        var error = Assert.Single(errors, e => e.Code == PzErrorCode.EntityNameInvalid);
        Assert.Contains("empty dotted segment", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_dotted_dataset_name_loads_clean_and_keeps_its_own_options()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-entity-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "project.yml"), "name: t\nversion: \"1\"\n");
        File.WriteAllText(Path.Combine(dir, "connections.yml"), """
            erp:
              connector: sqlserver
              entities:
                dbo.orders:
                  read:
                    partitions: 4
            """);
        try
        {
            var dataset = Assert.Single(Assert.Single(ProjectLoader.Load(dir, Env).Connections).Datasets);
            Assert.Equal("dbo.orders", dataset.Name);
            Assert.Equal(4L, dataset.Options["partitions"]);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }
}
