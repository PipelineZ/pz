using Pz.Cli;

namespace Pz.Cli.Tests;

/// <summary>An http dataset the connector refuses to parse -- a cross-field rule no JSON Schema can state --
/// is a config error: `pz validate` reports it as PZ0301 naming the connection, dataset and option, and
/// `pz plan` refuses it the same way (exit 2), never as the PZ0500 "bug in pz" catch-all.</summary>
// See the "console-and-env-serialized" collection definition in RestoreCommandTests.cs: this class redirects
// Console.Out/Error to assert on CLI output.
[Collection("console-and-env-serialized")]
public sealed class HttpDatasetRefusalTests : IDisposable
{
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-http-refusal-tests", Guid.NewGuid().ToString("N"));

    private const string Contract =
        "        columns: { id: bigint, updated_at: timestamp }\n" +
        "        sync: { mode: incremental, cursor: updated_at }\n";

    public static TheoryData<string, string, string> Refusals => new()
    {
        {
            "on_status skip on an incremental dataset",
            "        path: /items\n        on_status:\n          - { status: 404, action: skip }\n" + Contract,
            "action 'skip' is not allowed on a dataset that is or could be incremental"
        },
        {
            "stop_at_watermark without cursor",
            "        path: /items\n        stop_at_watermark: true\n",
            "'stop_at_watermark' requires 'cursor'"
        },
        {
            "for_each.values with max_items",
            "        path: /items/{{ item.id }}\n        for_each:\n          values: [ { id: a }, { id: b } ]\n" +
            "          max_items: 1\n",
            "for_each.max_items cannot be combined with for_each.values"
        },
        {
            "watermark_time_zone without watermark_format",
            "        path: /items\n        watermark_time_zone: Europe/Bucharest\n",
            "'watermark_time_zone' needs a 'watermark_format'"
        },
        {
            "for_each max_items on an incremental dataset",
            "        path: /items/{{ item.id }}\n        for_each:\n          path: /lists\n" +
            "          fields: { id: /id }\n          max_items: 5\n" + Contract,
            "for_each on an incremental dataset cannot use max_items"
        },
    };

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Theory]
    [MemberData(nameof(Refusals))]
    public void Validate_reports_the_refusal_as_PZ0301_naming_the_dataset(string _, string read, string expected)
    {
        WriteProject(read);

        var (exit, stderr) = Invoke("validate");

        Assert.Equal(ExitCodes.ConfigError, exit);
        var line = Assert.Single(stderr.Split('\n'), l => l.Contains(expected, StringComparison.Ordinal));
        Assert.StartsWith("error PZ0301: connection 'api' dataset 'items': ", line);
        Assert.Contains("(connections.yml)", line);
        Assert.DoesNotContain("PZ0500", stderr);
    }

    [Fact]
    public void Validate_reports_every_refusal_of_every_dataset_at_once()
    {
        WriteProject("        path: /items\n        stop_at_watermark: true\n        watermark_time_zone: UTC\n",
            "        path: /other\n        watermark_time_zone: UTC\n");

        var (exit, stderr) = Invoke("validate");

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("connection 'api' dataset 'items': 'stop_at_watermark' requires 'cursor'", stderr);
        Assert.Contains("connection 'api' dataset 'items': 'watermark_time_zone' needs a 'watermark_format'", stderr);
        Assert.Contains("connection 'api' dataset 'other': 'watermark_time_zone' needs a 'watermark_format'", stderr);
    }

    [Theory]
    [MemberData(nameof(Refusals))]
    public void Plan_refuses_the_dataset_as_PZ0301_not_PZ0500(string _, string read, string expected)
    {
        WriteProject(read);

        var (exit, stderr) = Invoke("plan");

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.DoesNotContain("PZ0500", stderr);
        Assert.DoesNotContain("bug in pz", stderr);
        var line = Assert.Single(stderr.Split('\n'), l => l.Contains(expected, StringComparison.Ordinal));
        Assert.StartsWith("error PZ0301: connection 'api' dataset 'items': ", line);
    }

    private void WriteProject(string itemsRead, string? otherRead = null)
    {
        Directory.CreateDirectory(Path.Combine(_work, "pipelines"));
        File.WriteAllText(Path.Combine(_work, "project.yml"), "name: refusals\nversion: \"1.0\"\n");
        var other = otherRead is null ? "" : "    other:\n      read:\n" + otherRead;
        File.WriteAllText(Path.Combine(_work, "connections.yml"),
            "api:\n  connector: http\n  base_url: https://api.example.com\n  entities:\n" +
            "    items:\n      read:\n" + itemsRead + other +
            "\nlake:\n  connector: localfiles\n  root: out\n");
        File.WriteAllText(Path.Combine(_work, "pipelines", "items_log.sql"),
            "INSERT INTO {{ sink('lake', 'items_log', format: 'parquet', strategy: 'append', duplicates: 'accept') }}\n" +
            "select * from {{ source('api', 'items') }}\n");
    }

    private (int Exit, string Stderr) Invoke(string verb)
    {
        var stdout = new StringWriter();
        var stderr = new StringWriter();
        var originalOut = Console.Out;
        var originalErr = Console.Error;
        Console.SetOut(stdout);
        Console.SetError(stderr);
        try
        {
            var exit = CliApp.Build().Parse([verb, "--project", _work]).Invoke();
            return (exit, stderr.ToString());
        }
        finally
        {
            Console.SetOut(originalOut);
            Console.SetError(originalErr);
        }
    }
}
