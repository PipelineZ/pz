using System.Text.Json;
using Pz.Cli.Commands;

namespace Pz.Cli.Tests;

/// <summary>`pz preview` end to end against <see cref="PreviewFixture"/>: the JSON contract with PipelineX,
/// the run modes, every refusal code. See the "console-and-env-serialized" collection in
/// RestoreCommandTests.cs.</summary>
[Collection("console-and-env-serialized")]
public sealed class PreviewCommandTests : IDisposable
{
    internal const string Check = "check_stg_customers_not_null_id";
    internal const string Sink = "lake.customers_out";

    private readonly string _work =
        PreviewFixture.Write(Path.Combine(Path.GetTempPath(), "pz-preview-tests", Guid.NewGuid().ToString("N")));

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    internal static (int Exit, string Stdout, string Stderr) Pz(params string[] args)
    {
        var stdout = new StringWriter();
        var stderr = new StringWriter();
        var (originalOut, originalErr) = (Console.Out, Console.Error);
        Console.SetOut(stdout);
        Console.SetError(stderr);
        try
        {
            var exit = CliApp.Build().Parse(args).Invoke();
            return (exit, stdout.ToString(), stderr.ToString());
        }
        finally
        {
            Console.SetOut(originalOut);
            Console.SetError(originalErr);
        }
    }

    private string RunOnce(params string[] extra)
    {
        Assert.Equal(ExitCodes.Ok, Pz(["run", "--project", _work, .. extra]).Exit);
        return Path.GetFileName(PreviewFixture.LatestRunDir(_work));
    }

    private (int Exit, string Stdout, string Stderr) Preview(params string[] args) =>
        Pz(["preview", .. args, "--project", _work]);

    [Fact]
    public void Run_mode_json_matches_the_contract()
    {
        var runId = RunOnce();

        var (exit, stdout, _) = Preview("stg_customers", "--run", runId, "--json");

        Assert.Equal(ExitCodes.Ok, exit);
        using var doc = JsonDocument.Parse(stdout);
        var root = doc.RootElement;
        Assert.Equal(["node", "mode", "runId", "columns", "rows", "totalRows", "limit", "sliced"],
            root.EnumerateObject().Select(p => p.Name));
        Assert.Equal("stg_customers", root.GetProperty("node").GetString());
        Assert.Equal("run", root.GetProperty("mode").GetString());
        Assert.Equal(runId, root.GetProperty("runId").GetString());
        Assert.Equal(1500, root.GetProperty("totalRows").GetInt64());
        Assert.Equal(50, root.GetProperty("limit").GetInt32());
        Assert.False(root.GetProperty("sliced").GetBoolean());
        Assert.Equal(
            ["id/BIGINT/False", "email/VARCHAR/False", "contact/VARCHAR/True", "note/VARCHAR/False"],
            root.GetProperty("columns").EnumerateArray().Select(c =>
                $"{c.GetProperty("name").GetString()}/{c.GetProperty("type").GetString()}/{c.GetProperty("redacted").GetBoolean()}"));
        var rows = root.GetProperty("rows").EnumerateArray().ToList();
        Assert.Equal(50, rows.Count);
        Assert.All(rows, r =>
        {
            Assert.Equal(JsonValueKind.Number, r[0].ValueKind);
            Assert.Equal(JsonValueKind.String, r[1].ValueKind);
            Assert.Contains("@example.com", r[1].GetString(), StringComparison.Ordinal); // marks do not propagate
            Assert.Equal("•••", r[2].GetString());
        });
    }

    [Fact]
    public void Source_node_redacts_its_contract_marks()
    {
        var runId = RunOnce();

        var (exit, stdout, _) = Preview(PreviewFixture.Source, "--run", runId, "--json");

        Assert.Equal(ExitCodes.Ok, exit);
        using var doc = JsonDocument.Parse(stdout);
        var email = doc.RootElement.GetProperty("columns").EnumerateArray()
            .Select((c, i) => (c.GetProperty("name").GetString(), i)).Single(c => c.Item1 == "email").i;
        Assert.All(doc.RootElement.GetProperty("rows").EnumerateArray(), r => Assert.Equal("•••", r[email].GetString()));
    }

    [Fact]
    public void View_pipeline_is_previewable_from_a_run() =>
        Assert.Equal(ExitCodes.Ok, Preview("v_customers", "--run", RunOnce(), "--json").Exit);

    [Theory]
    [InlineData("eph_customers")]
    [InlineData(Check)]
    [InlineData(Sink)]
    public void Ephemeral_check_and_sink_are_PZ0538_exit_2(string node)
    {
        var (exit, stdout, _) = Preview(node, "--run", RunOnce(), "--json");

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Equal("PZ0538", JsonDocument.Parse(stdout).RootElement.GetProperty("error").GetProperty("code").GetString());
    }

    [Fact]
    public void Run_dir_needs_no_project_files()
    {
        var runDir = Path.Combine(_work, ".pz", "runs", RunOnce());
        var copy = Path.Combine(Path.GetTempPath(), "pz-preview-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(copy);
        try
        {
            foreach (var file in Directory.EnumerateFiles(runDir))
            {
                File.Copy(file, Path.Combine(copy, Path.GetFileName(file)));
            }

            Directory.Delete(_work, recursive: true);

            var (exit, stdout, _) = Pz("preview", "stg_customers", "--run-dir", copy, "--json");

            Assert.Equal(ExitCodes.Ok, exit);
            var root = JsonDocument.Parse(stdout).RootElement;
            Assert.Equal(JsonValueKind.Null, root.GetProperty("runId").ValueKind);
            Assert.Equal(50, root.GetProperty("rows").GetArrayLength());
        }
        finally
        {
            Directory.Delete(copy, recursive: true);
        }
    }

    [Fact]
    public void Missing_run_is_PZ0537_json_error_object()
    {
        var (exit, stdout, _) = Preview("stg_customers", "--run", "nope", "--json");

        Assert.Equal(ExitCodes.ConfigError, exit);
        var root = JsonDocument.Parse(stdout).RootElement;
        Assert.Equal(["error"], root.EnumerateObject().Select(p => p.Name));
        Assert.Equal("PZ0537", root.GetProperty("error").GetProperty("code").GetString());
        Assert.False(string.IsNullOrEmpty(root.GetProperty("error").GetProperty("message").GetString()));
    }

    [Fact]
    public void Node_that_did_not_run_is_PZ0537()
    {
        var runId = RunOnce("--select", PreviewFixture.Source);

        var (exit, stdout, _) = Preview("stg_customers", "--run", runId, "--json");

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Equal("PZ0537", JsonDocument.Parse(stdout).RootElement.GetProperty("error").GetProperty("code").GetString());
    }

    [Theory]
    [InlineData("stg_customers")]
    [InlineData("stg_customers", "--run", "r", "--fresh")]
    [InlineData("stg_customers", "--run", "r", "--run-dir", "/tmp")]
    [InlineData("stg_customers", "--run", "r", "--limit", "0")]
    [InlineData("stg_customers", "--run", "r", "--limit", "501")]
    [InlineData("stg_customers", "--run", "r", "--slice", "5")]
    [InlineData("stg_customers", "--fresh", "--slice", "0")]
    [InlineData("stg_customers", "--fresh", "--slice", "100001")]
    [InlineData("stg_customers", "--run", "r", "--vars", "{}")]
    [InlineData("stg_customers", "--run", "../x")]
    [InlineData("stg_customers", "--run", "a/b")]
    public void Invalid_options_are_PZ0539(params string[] args)
    {
        var (exit, _, stderr) = Preview(args);

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0539", stderr, StringComparison.Ordinal);
    }

    [Fact]
    public void Text_mode_prints_a_table()
    {
        var (exit, stdout, _) = Preview("stg_customers", "--run", RunOnce());

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Contains("contact", stdout, StringComparison.Ordinal);
        Assert.Contains("•••", stdout, StringComparison.Ordinal);
        Assert.Contains("50 of 1500 rows", stdout, StringComparison.Ordinal);
    }

    [Fact]
    public void Unicode_is_not_escaped_in_json()
    {
        var (_, stdout, _) = Preview("stg_customers", "--run", RunOnce(), "--json");

        Assert.Contains("\"•••\"", stdout, StringComparison.Ordinal);
        Assert.DoesNotContain("\\u2022", stdout, StringComparison.OrdinalIgnoreCase);
    }

    // -- --fresh (no prior pz run in these tests) ------------------------------------------------------

    private static string ErrorCode(string stdout) =>
        JsonDocument.Parse(stdout).RootElement.GetProperty("error").GetProperty("code").GetString()!;

    [Fact]
    public void Fresh_json_matches_the_contract()
    {
        var (exit, stdout, stderr) = Preview("stg_customers", "--fresh", "--json");

        Assert.True(exit == ExitCodes.Ok, stderr + stdout);
        var root = JsonDocument.Parse(stdout).RootElement;
        Assert.Equal("fresh", root.GetProperty("mode").GetString());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("runId").ValueKind);
        Assert.Equal(JsonValueKind.Null, root.GetProperty("totalRows").ValueKind);
        Assert.True(root.GetProperty("sliced").GetBoolean());
        Assert.Equal(50, root.GetProperty("limit").GetInt32());
        var rows = root.GetProperty("rows").EnumerateArray().ToList();
        Assert.Equal(50, rows.Count);
        Assert.All(rows, r => Assert.Equal("•••", r[2].GetString()));
    }

    [Fact]
    public void Fresh_slice_caps_every_source_read()
    {
        var (exit, stdout, _) = Preview("stg_customers", "--fresh", "--slice", "100", "--limit", "500", "--json");

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Equal(100, JsonDocument.Parse(stdout).RootElement.GetProperty("rows").GetArrayLength());
    }

    [Fact]
    public void Fresh_previews_an_ephemeral_pipeline()
    {
        var (exit, stdout, stderr) = Preview("eph_customers", "--fresh", "--json");

        Assert.True(exit == ExitCodes.Ok, stderr + stdout);
        var rows = JsonDocument.Parse(stdout).RootElement.GetProperty("rows").EnumerateArray().ToList();
        Assert.NotEmpty(rows);
        Assert.All(rows, r => Assert.Equal(0, r[0].GetInt64() % 2));
    }

    [Theory]
    [InlineData(Check)]
    [InlineData(Sink)]
    public void Fresh_sink_or_check_is_PZ0538(string node)
    {
        var (exit, stdout, _) = Preview(node, "--fresh", "--json");

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Equal("PZ0538", ErrorCode(stdout));
    }

    [Fact]
    public void Fresh_unknown_node_is_SelectorNoMatch_exit_2()
    {
        var (exit, stdout, _) = Preview("nope", "--fresh", "--json");

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Equal("PZ0210", ErrorCode(stdout));
    }

    [Fact]
    public void Fresh_leaves_no_trace_in_the_project()
    {
        Assert.Equal(ExitCodes.Ok, Preview("stg_customers", "--fresh", "--json").Exit);

        Assert.False(Directory.Exists(Path.Combine(_work, ".pz", "runs")));
        Assert.False(Directory.Exists(Path.Combine(_work, ".pz", "state")));
        Assert.False(Directory.Exists(Path.Combine(_work, "out")));
    }

    [Fact]
    public void Fresh_ignores_the_state_url()
    {
        var before = Environment.GetEnvironmentVariable("PZ_STATE_URL");
        Environment.SetEnvironmentVariable("PZ_STATE_URL", "http://127.0.0.1:1/");
        try
        {
            Assert.Equal(ExitCodes.Ok, Preview("stg_customers", "--fresh", "--json").Exit);
        }
        finally
        {
            Environment.SetEnvironmentVariable("PZ_STATE_URL", before);
        }
    }

    [Fact]
    public void Fresh_node_failure_is_exit_1_with_the_nodes_code()
    {
        File.Delete(Path.Combine(_work, "data", "customers.csv"));

        var (exit, stdout, _) = Preview("stg_customers", "--fresh", "--json");

        Assert.Equal(ExitCodes.NodeFailures, exit);
        var code = ErrorCode(stdout);
        Assert.StartsWith("PZ", code, StringComparison.Ordinal);
        Assert.DoesNotContain(code, new[] { "PZ0537", "PZ0538", "PZ0539" });
    }

    [Fact]
    public void Fresh_json_stdout_is_one_object_even_with_notes()
    {
        var (exit, stdout, _) = Preview("stg_customers", "--fresh", "--json");

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.DoesNotContain('\n', stdout.Trim());
        using var doc = JsonDocument.Parse(stdout);
        Assert.Equal(JsonValueKind.Object, doc.RootElement.ValueKind);
    }
}
