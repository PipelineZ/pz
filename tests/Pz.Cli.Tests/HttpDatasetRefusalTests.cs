using Pz.Cli;

namespace Pz.Cli.Tests;

/// <summary>An http dataset the connector refuses to parse -- a cross-field rule no JSON Schema can state --
/// is a config error: `pz validate` reports it as PZ0301 naming the connection, dataset and option, and
/// `pz plan` refuses it the same way (exit 2), never as the PZ0500 "bug in pz" catch-all. A connection-level
/// refusal is one error naming the connection.</summary>
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
        Assert.DoesNotContain("'api.items'", line);
        Assert.Contains("hint: fix the options the connector names", line);
        Assert.DoesNotContain("pz validate", line);
    }

    // A connection the connector refuses is one error, attributed to the connection, at validate and at plan alike,
    // however many datasets read it.
    [Theory]
    [InlineData("validate")]
    [InlineData("plan")]
    public void A_refused_connection_is_one_error_naming_the_connection(string verb)
    {
        WriteProject("        path: /items\n", "        path: /other\n", baseUrl: "not-a-url");
        File.WriteAllText(Path.Combine(_work, "pipelines", "other_log.sql"),
            "INSERT INTO {{ sink('lake', 'other_log', format: 'parquet', strategy: 'append', duplicates: 'accept') }}\n" +
            "select * from {{ source('api', 'other') }}\n");

        var (exit, stderr) = Invoke(verb);

        Assert.Equal(ExitCodes.ConfigError, exit);
        var line = Assert.Single(stderr.Split('\n'), l => l.Contains("base_url", StringComparison.Ordinal));
        Assert.StartsWith("error PZ0301: connection 'api'", line);
        Assert.DoesNotContain("dataset '", line);
    }

    // gcs reads only with hmac auth: a source on any other auth is refused by `pz validate`, as by `pz plan`.
    [Theory]
    [InlineData("validate")]
    [InlineData("plan")]
    public void A_gcs_source_without_hmac_auth_is_refused_at_validate_and_plan(string verb)
    {
        Directory.CreateDirectory(Path.Combine(_work, "pipelines"));
        File.WriteAllText(Path.Combine(_work, "project.yml"), "name: refusals\nversion: \"1.0\"\n");
        File.WriteAllText(Path.Combine(_work, "connections.yml"),
            "gcs:\n  connector: gcs\n  auth: adc\n  root: my-bucket\n  entities:\n    events:\n      read:\n" +
            "        format: parquet\n        columns: { id: bigint }\n" +
            "\nlake:\n  connector: localfiles\n  root: out\n");
        File.WriteAllText(Path.Combine(_work, "pipelines", "events_copy.sql"),
            "INSERT INTO {{ sink('lake', 'events_copy', format: 'parquet') }}\n" +
            "select * from {{ source('gcs', 'events') }}\n");

        var (exit, stderr) = Invoke(verb);

        Assert.Equal(ExitCodes.ConfigError, exit);
        var line = Assert.Single(stderr.Split('\n'), l => l.Contains("auth: hmac", StringComparison.Ordinal));
        Assert.StartsWith("error PZ0301: connection 'gcs': gcs sources require 'auth: hmac'", line);
    }

    // A plan failure maps to its exit code by the planner's own verdict: a config refusal is exit 2, a connector
    // that failed rather than refused means pz could not run on its own terms, exit 3 (builtins only refuse
    // offline, so no builtin project can reach the fatal case; the engine tests cover the classification).
    [Fact]
    public void A_plan_failure_exits_fatal_only_when_the_planner_says_so()
    {
        var error = new Pz.Core.Validation.PzError("PZ0330", "connection 'api': connector 'x' failed while planning", null, null, null);

        Assert.Equal(ExitCodes.ConfigError, ExitCodes.ForPlanFailure(new Pz.Core.Validation.PzValidationException([error])));
        Assert.Equal(ExitCodes.Fatal,
            ExitCodes.ForPlanFailure(new Pz.Core.Validation.PzValidationException([error]) { IsFatal = true }));
    }

    // A relative path in the connection resolves against the project, not the shell's directory: `pz validate`
    // run from elsewhere must agree with `pz plan` run from the same place.
    [Fact]
    public void Validate_from_another_directory_resolves_relative_paths_against_the_project_like_plan()
    {
        WriteProject("        path: /items\n", tls: "  tls: { ca_file: certs/ca.pem }\n");
        Directory.CreateDirectory(Path.Combine(_work, "certs"));
        using (var key = System.Security.Cryptography.RSA.Create(2048))
        {
            var request = new System.Security.Cryptography.X509Certificates.CertificateRequest("CN=pz-test-ca", key,
                System.Security.Cryptography.HashAlgorithmName.SHA256, System.Security.Cryptography.RSASignaturePadding.Pkcs1);
            using var cert = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
            File.WriteAllText(Path.Combine(_work, "certs", "ca.pem"), cert.ExportCertificatePem());
        }

        var elsewhere = Directory.CreateTempSubdirectory("pz-elsewhere-").FullName;
        var original = Directory.GetCurrentDirectory();
        try
        {
            Directory.SetCurrentDirectory(elsewhere);
            var (validateExit, validateErr) = Invoke("validate");
            var (planExit, planErr) = Invoke("plan");

            Assert.Equal(ExitCodes.Ok, planExit);
            Assert.True(validateExit == ExitCodes.Ok, validateErr);
        }
        finally
        {
            Directory.SetCurrentDirectory(original);
            Directory.Delete(elsewhere, recursive: true);
        }
    }

    private void WriteProject(string itemsRead, string? otherRead = null, string baseUrl = "https://api.example.com",
        string tls = "")
    {
        Directory.CreateDirectory(Path.Combine(_work, "pipelines"));
        File.WriteAllText(Path.Combine(_work, "project.yml"), "name: refusals\nversion: \"1.0\"\n");
        var other = otherRead is null ? "" : "    other:\n      read:\n" + otherRead;
        File.WriteAllText(Path.Combine(_work, "connections.yml"),
            $"api:\n  connector: http\n  base_url: {baseUrl}\n{tls}  entities:\n" +
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
