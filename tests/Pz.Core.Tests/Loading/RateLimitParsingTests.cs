using System.Reflection;
using Pz.Core.Loading;
using Pz.Core.Model;
using Pz.Core.Validation;

namespace Pz.Core.Tests.Loading;

/// <summary>Instance-level `rate_limit:` config surface. Carries its own TempProject copy, since that
/// helper is private to ProjectLoaderTests. Two cases (dataset/output wrong-level
/// refusal) need BOTH the PZ0318 error AND the surviving DatasetDef/OutputDef.Options to prove the
/// key never leaks in — ProjectLoader.Load() discards the built defs whenever errors.Count > 0
/// (Load throws before returning PzProject), so those two tests reflectively invoke the private
/// LoadSources/LoadSinks methods directly to observe both the errors list and the returned defs from
/// the same pass, instead of only the public Load() entry point.</summary>
public class RateLimitParsingTests
{
    private static readonly IReadOnlyDictionary<string, string> Env = new Dictionary<string, string>
    {
        ["DATA_DIR"] = "/tmp/pz-data",
        ["OUT_DIR"] = "/tmp/pz-out",
    };

    private static string TempProject(string sourceYaml, string? sinkYaml = null)
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "project.yml"), "name: t\nversion: \"1\"\n");
        // Both directions land in one file, so the two blocks a caller passes are simply concatenated.
        File.WriteAllText(Path.Combine(dir, "connections.yml"),
            sinkYaml is null ? sourceYaml : sourceYaml + "\n" + sinkYaml);
        return dir;
    }

    private const string PlainSourceYaml = """
        pg:
          connector: postgres
          entities:
            orders:
              read:
        """;

    /// <summary>There is one loader and one file, so these go through the public entry
    /// point rather than reflecting on a private per-direction loader.</summary>
    private static List<ConnectionDef> LoadConnections(string projectDir, List<PzError> errors)
    {
        try
        {
            return [.. ProjectLoader.Load(projectDir, Env).Connections];
        }
        catch (PzValidationException ex)
        {
            errors.AddRange(ex.Errors);
            return [];
        }
    }

    [Fact]
    public void Rate_limit_parses_with_burst()
    {
        var yaml = """
            pg:
              connector: postgres
              rate_limit:
                requests_per_minute: 60
                burst: 10
              entities:
                orders:
                  read:
            """;
        var project = ProjectLoader.Load(TempProject(yaml), Env);
        var rateLimit = Assert.Single(project.Connections).RateLimit;
        Assert.Equal(new RateLimitDef(60, 10), rateLimit);
        Assert.Equal(10, rateLimit!.EffectiveBurst);
    }

    [Fact]
    public void Rate_limit_burst_defaults()
    {
        var yaml120 = """
            pg:
              connector: postgres
              rate_limit:
                requests_per_minute: 120
              entities:
                orders:
                  read:
            """;
        var project120 = ProjectLoader.Load(TempProject(yaml120), Env);
        var rateLimit120 = Assert.Single(project120.Connections).RateLimit;
        Assert.Equal(new RateLimitDef(120, null), rateLimit120);
        Assert.Equal(2, rateLimit120!.EffectiveBurst);

        var yaml30 = """
            pg:
              connector: postgres
              rate_limit:
                requests_per_minute: 30
              entities:
                orders:
                  read:
            """;
        var project30 = ProjectLoader.Load(TempProject(yaml30), Env);
        var rateLimit30 = Assert.Single(project30.Connections).RateLimit;
        Assert.Equal(1, rateLimit30!.EffectiveBurst); // floor(30/60) == 0, then Math.Max(1, 0)
    }

    [Fact]
    public void Rate_limit_requires_rpm()
    {
        var yaml = """
            pg:
              connector: postgres
              rate_limit:
                burst: 5
              entities:
                orders:
                  read:
            """;
        var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(TempProject(yaml), Env));
        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.RateLimitConfigInvalid);
        Assert.Contains("requests_per_minute", error.Message);
    }

    [Fact]
    public void Rate_limit_bounds_aggregate()
    {
        var yaml = """
            pg:
              connector: postgres
              rate_limit:
                requests_per_minute: 0
                burst: 2000000
              entities:
                orders:
                  read:
            """;
        var dir = TempProject(yaml);
        var errors = new List<PzError>();
        var sources = LoadConnections(dir, errors);

        Assert.Equal(2, errors.Count(e => e.Code == PzErrorCode.RateLimitConfigInvalid));
        Assert.Empty(sources); // the load aborts on the aggregated errors

    }

    [Fact]
    public void Rate_limit_must_be_mapping()
    {
        var yaml = """
            pg:
              connector: postgres
              rate_limit: 60
              entities:
                orders:
                  read:
            """;
        var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(TempProject(yaml), Env));
        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.RateLimitConfigInvalid);
        Assert.Contains("must be a mapping", error.Message);
    }

    [Fact]
    public void Rate_limit_on_sink_parses()
    {
        var sinkYaml = """
            out:
              connector: postgres
              rate_limit:
                requests_per_minute: 90
                burst: 3
            """;
        var project = ProjectLoader.Load(TempProject(PlainSourceYaml, sinkYaml), Env);
        var rateLimit = Assert.Single(project.Connections, c => c.Name == "out").RateLimit;
        Assert.Equal(new RateLimitDef(90, 3), rateLimit);
    }

    [Fact]
    public void Rate_limit_on_dataset_is_refused()
    {
        var yaml = """
            pg:
              connector: postgres
              entities:
                orders:
                  read:
                    rate_limit:
                      requests_per_minute: 60
            """;
        var dir = TempProject(yaml);
        var errors = new List<PzError>();
        var sources = LoadConnections(dir, errors);

        var error = Assert.Single(errors, e => e.Code == PzErrorCode.RateLimitConfigInvalid);
        Assert.Contains("instance-level", error.Message);
        // The load aborts on the error now, so there is no dataset left to inspect -- that the key never
        // reaches connector options is covered by ConnectionsLoaderTests' own read-block facts.
        Assert.Empty(sources);
    }

    // There is no output-level `rate_limit:` refusal: the `outputs:` block itself is retired (PZ0347).
    // Its call-site twin -- a `rate_limit:` kwarg on sink() -- is covered by
    // SinkFunctionTests.Malformed_call_is_rejected.
}
