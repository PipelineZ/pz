using Pz.Cli.Commands;
using Pz.Core.Validation;

namespace Pz.Cli.Tests;

/// <summary>Pure parsing/formatting logic shared by every project-loading verb (see
/// <see cref="SharedInputHelpers"/>'s own summary). Joins "console-and-env-serialized" (see
/// RestoreCommandTests.cs) because <see cref="SharedInputHelpers.WriteWarnings"/> writes to the
/// process-global <see cref="Console.Error"/> and <see cref="SharedInputHelpers.SnapshotEnvironment"/>
/// reads the process-global environment.</summary>
[Collection("console-and-env-serialized")]
public sealed class SharedInputHelpersTests
{
    [Fact]
    public void SnapshotEnvironment_captures_current_process_environment_variables()
    {
        const string key = "PZ_SHARED_INPUT_HELPERS_TEST_VAR";
        Environment.SetEnvironmentVariable(key, "hello");
        try
        {
            var snapshot = SharedInputHelpers.SnapshotEnvironment();

            Assert.Equal("hello", snapshot[key]);
        }
        finally
        {
            Environment.SetEnvironmentVariable(key, null);
        }
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void ParseVars_blank_input_returns_null(string? json) =>
        Assert.Null(SharedInputHelpers.ParseVars(json));

    [Fact]
    public void ParseVars_parses_every_scalar_type()
    {
        var result = SharedInputHelpers.ParseVars(
            """{"name":"acme","count":3,"ratio":1.5,"active":true,"disabled":false,"missing":null}""");

        Assert.NotNull(result);
        Assert.Equal("acme", result!["name"]);
        // This pair characterized a live bug when the file was written and now pins its fix: the
        // conditional in ToObject widened to double, so a whole number arrived as double and every
        // downstream integer reader -- all of which expect the loader's long -- silently refused it.
        // `--vars '{"n":3}'` used as a numeric write option failed with PZ0121 "(got '3')".
        Assert.IsType<long>(result["count"]);
        Assert.Equal(3L, result["count"]);
        Assert.Equal(1.5, result["ratio"]);
        Assert.Equal(true, result["active"]);
        Assert.Equal(false, result["disabled"]);
        Assert.Null(result["missing"]);
    }

    [Fact]
    public void ParseVars_parses_nested_objects_and_arrays_recursively()
    {
        var result = SharedInputHelpers.ParseVars("""{"tags":["a","b"],"nested":{"inner":1}}""");

        Assert.NotNull(result);
        var tags = Assert.IsType<List<object?>>(result!["tags"]);
        Assert.Equal(new List<object?> { "a", "b" }, tags);

        var nested = Assert.IsType<Dictionary<string, object?>>(result["nested"]);
        Assert.IsType<long>(nested["inner"]);
        Assert.Equal(1L, nested["inner"]);
    }

    [Fact]
    public void ParseVars_empty_object_returns_empty_dictionary()
    {
        var result = SharedInputHelpers.ParseVars("{}");

        Assert.NotNull(result);
        Assert.Empty(result!);
    }

    [Theory]
    [InlineData("not json")]
    [InlineData("{ bad")]
    public void ParseVars_malformed_json_is_PZ0102(string json)
    {
        var ex = Assert.Throws<PzValidationException>(() => SharedInputHelpers.ParseVars(json));

        var error = Assert.Single(ex.Errors);
        Assert.Equal(PzErrorCode.VarsInvalid, error.Code);
        Assert.Contains("--vars is not valid JSON", error.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("[1,2]", "a JSON array")]
    [InlineData("\"hi\"", "a JSON string")]
    [InlineData("42", "a JSON number")]
    [InlineData("true", "a JSON boolean")]
    [InlineData("null", "JSON null")]
    public void ParseVars_non_object_root_is_PZ0102_naming_the_kind(string json, string expectedKindPhrase)
    {
        var ex = Assert.Throws<PzValidationException>(() => SharedInputHelpers.ParseVars(json));

        var error = Assert.Single(ex.Errors);
        Assert.Equal(PzErrorCode.VarsInvalid, error.Code);
        Assert.Contains("--vars must be a JSON object", error.Message, StringComparison.Ordinal);
        Assert.Contains(expectedKindPhrase, error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void WriteWarnings_formats_code_and_message_only()
    {
        var actual = CaptureStderr(() =>
            SharedInputHelpers.WriteWarnings([new PzWarning("PZ0900", "something to note", null, null, null)]));

        Assert.Equal("warning: PZ0900 something to note" + Environment.NewLine, actual);
    }

    [Fact]
    public void WriteWarnings_includes_file_and_hint_when_present()
    {
        var actual = CaptureStderr(() => SharedInputHelpers.WriteWarnings([
            new PzWarning("PZ0900", "something to note", "sinks/out.yml", null, "do the thing"),
        ]));

        Assert.Equal(
            "warning: PZ0900 something to note (sinks/out.yml) — do the thing" + Environment.NewLine,
            actual);
    }

    [Fact]
    public void WriteWarnings_writes_one_line_per_warning_in_order()
    {
        var actual = CaptureStderr(() => SharedInputHelpers.WriteWarnings([
            new PzWarning("PZ0900", "first", null, null, null),
            new PzWarning("PZ0901", "second", "a.yml", null, null),
        ]));

        var lines = actual.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
        Assert.Equal(["warning: PZ0900 first", "warning: PZ0901 second (a.yml)"], lines);
    }

    [Fact]
    public void WriteWarnings_empty_list_writes_nothing() =>
        Assert.Equal("", CaptureStderr(() => SharedInputHelpers.WriteWarnings([])));

    private static string CaptureStderr(Action action)
    {
        var stderr = new StringWriter();
        var original = Console.Error;
        Console.SetError(stderr);
        try
        {
            action();
        }
        finally
        {
            Console.SetError(original);
        }

        return stderr.ToString();
    }
}
