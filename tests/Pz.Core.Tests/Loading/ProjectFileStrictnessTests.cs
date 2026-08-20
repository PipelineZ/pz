using Pz.Core.Loading;
using Pz.Core.Validation;

namespace Pz.Core.Tests.Loading;

/// <summary>A project.yml value that is present but unreadable is an
/// error, never a silent fallback to the default — a typo'd `threads: banana` must not quietly run
/// on 4 threads.</summary>
public class ProjectFileStrictnessTests
{
    private static readonly IReadOnlyDictionary<string, string> Env = new Dictionary<string, string>();

    private static Pz.Core.Model.PzProject Load(string projectYaml)
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-projectfile-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "project.yml"), projectYaml);
        File.WriteAllText(Path.Combine(dir, "connections.yml"), "crm:\n  connector: localfiles\n");
        try
        {
            return ProjectLoader.Load(dir, Env);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    private static IReadOnlyList<PzError> Errors(string projectYaml) =>
        Assert.Throws<PzValidationException>(() => Load(projectYaml)).Errors;

    [Theory]
    [InlineData("threads: banana")]
    [InlineData("threads: 99999999999")]
    [InlineData("batch_bytes: many")]
    [InlineData("force_universal: yes")]
    [InlineData("check_samples: sure")]
    [InlineData("duckdb: fast")]
    public void An_unreadable_engine_value_is_PZ0120(string engineLine)
    {
        var error = Assert.Single(Errors($"""
            name: t
            version: "1"
            engine:
              {engineLine}
            """), e => e.Code == PzErrorCode.InvalidEngineConfig);

        Assert.Contains(engineLine.Split(':')[0], error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_non_mapping_engine_block_is_PZ0120() =>
        Assert.Single(Errors("""
            name: t
            version: "1"
            engine: banana
            """), e => e.Code == PzErrorCode.InvalidEngineConfig);

    [Fact]
    public void A_non_mapping_vars_block_is_PZ0102()
    {
        var error = Assert.Single(Errors("""
            name: t
            version: "1"
            vars:
              - 1
              - 2
            """), e => e.Code == PzErrorCode.VarsInvalid);

        Assert.Contains("vars", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_non_list_connectors_block_is_refused()
    {
        var error = Assert.Single(Errors("""
            name: t
            version: "1"
            connectors: banana
            """), e => e.Code == PzErrorCode.YamlShape);

        Assert.Contains("connectors", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_non_mapping_connectors_entry_is_refused() =>
        Assert.Single(Errors("""
            name: t
            version: "1"
            connectors:
              - banana
            """), e => e.Code == PzErrorCode.YamlShape);

    [Fact]
    public void An_unknown_top_level_key_is_refused_not_ignored()
    {
        var error = Assert.Single(Errors("""
            name: t
            version: "1"
            frobnicate: true
            """), e => e.Code == PzErrorCode.YamlShape);

        Assert.Contains("frobnicate", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_project_level_outputs_block_is_PZ0347()
    {
        var error = Assert.Single(Errors("""
            name: t
            version: "1"
            outputs:
              dev:
                threads: 1
            """), e => e.Code == PzErrorCode.RetiredOutputsBlock);

        Assert.Contains("connections.yml", error.Hint!, StringComparison.Ordinal);
    }

    [Fact]
    public void The_documented_pz_version_constraint_key_is_accepted()
    {
        var project = Load("""
            name: t
            version: "1"
            pz: ">=0.1 <1.0"
            """);

        Assert.Equal("t", project.Name);
    }

    [Fact]
    public void Bodyless_optional_blocks_still_mean_the_defaults()
    {
        var project = Load("""
            name: t
            version: "1"
            vars:
            engine:
            connectors:
            """);

        Assert.Empty(project.Vars);
        Assert.Equal(4, project.Engine.Threads);
        Assert.Empty(project.Connectors);
    }
}
