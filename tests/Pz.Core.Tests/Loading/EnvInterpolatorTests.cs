using Pz.Core.Loading;
using Pz.Core.Validation;

namespace Pz.Core.Tests.Loading;

public class EnvInterpolatorTests
{
    [Fact]
    public void Interpolate_replaces_declared_variable()
    {
        var errors = new List<PzError>();
        var result = EnvInterpolator.Interpolate(
            "Server=${DB_HOST};", new Dictionary<string, string> { ["DB_HOST"] = "srv1" }, "sources/db.yml", errors);

        Assert.Equal("Server=srv1;", result);
        Assert.Empty(errors);
    }

    [Fact]
    public void Interpolate_reports_undeclared_variable_and_keeps_reference_text()
    {
        var errors = new List<PzError>();
        var result = EnvInterpolator.Interpolate(
            "pw=${MISSING_SECRET}", new Dictionary<string, string>(), "sinks/mart.yml", errors);

        Assert.Equal("pw=${MISSING_SECRET}", result);
        var error = Assert.Single(errors);
        Assert.Equal(PzErrorCode.UndeclaredEnvVar, error.Code);
        Assert.Contains("MISSING_SECRET", error.Message, StringComparison.Ordinal);
        // Secret hygiene: the error names the VARIABLE, never a value.
    }

    [Fact]
    public void A_doubled_dollar_before_a_brace_is_a_literal_escape()
    {
        var errors = new List<PzError>();
        var result = EnvInterpolator.Interpolate(
            "template=$${NAME}", new Dictionary<string, string>(), "sources/db.yml", errors);

        Assert.Equal("template=${NAME}", result);
        Assert.Empty(errors); // never reported as an undeclared reference either
    }

    [Fact]
    public void An_escaped_literal_and_a_real_reference_coexist_in_one_value()
    {
        var errors = new List<PzError>();
        var result = EnvInterpolator.Interpolate(
            "$${A}${B}", new Dictionary<string, string> { ["B"] = "b1" }, "sources/db.yml", errors);

        Assert.Equal("${A}b1", result);
        Assert.Empty(errors);
    }
}
