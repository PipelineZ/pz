using Pz.Core.Validation;

namespace Pz.Core.Tests.Validation;

public class PzErrorTests
{
    [Fact]
    public void Exception_message_lists_every_error()
    {
        var errors = new[]
        {
            new PzError(PzErrorCode.YamlShape, "missing 'name'", "project.yml", 1, "add name:"),
            new PzError(PzErrorCode.UndeclaredEnvVar, "environment variable 'X' is not set", "sources/a.yml", null, null),
        };
        var ex = new PzValidationException(errors);
        Assert.Contains("2 validation error(s)", ex.Message);
        Assert.Contains("PZ0101", ex.Message);
        Assert.Contains("PZ0103", ex.Message);
        Assert.Contains("sources/a.yml", ex.Message);
    }

    [Fact]
    public void Error_renders_code_file_line_hint()
    {
        var full = new PzError("PZ0101", "missing 'name'", "project.yml", 1, "add name:");
        Assert.Equal("PZ0101: missing 'name' (project.yml:1) — hint: add name:", full.ToString());
        var bare = new PzError("PZ0202", "cycle detected", null, null, null);
        Assert.Equal("PZ0202: cycle detected", bare.ToString());
        var fileOnly = new PzError("PZ0110", "duplicate pipeline 'x'", "pipelines/x.sql", null, null);
        Assert.Equal("PZ0110: duplicate pipeline 'x' (pipelines/x.sql)", fileOnly.ToString());
    }

    [Fact]
    public void Config_exception_message_is_the_rendered_error()
    {
        var error = new PzError("PZ0305", "connector 'postgres' is not installed", null, null, "run 'pz restore'");
        var ex = new PzConfigException(error);
        Assert.Equal(error.ToString(), ex.Message);
        Assert.Same(error, ex.Error);
    }
}
