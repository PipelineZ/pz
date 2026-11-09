using Pz.Cli;

namespace Pz.Cli.Tests;

/// <summary>`pz validate` refuses what `pz plan` refuses, with the same coded line: one representative refusal
/// per kind a builtin connector makes while the plan probes it offline -- a file-format option on the wrong
/// format (the shared FileFormatCatalog, sink and source side), an object-store output with nowhere to write, a
/// read-only catalog, a missing TLS file.</summary>
// See the "console-and-env-serialized" collection definition in RestoreCommandTests.cs.
[Collection("console-and-env-serialized")]
public sealed class ValidatePlanParityTests : IDisposable
{
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-parity-tests", Guid.NewGuid().ToString("N"));

    private const string Csv =
        "raw:\n  connector: localfiles\n  entities:\n    a:\n" +
        "      read: { path: data/a.csv, format: csv, columns: { id: bigint } }\n\n";

    private const string Lake = "\nlake:\n  connector: localfiles\n  root: out\n";

    public static TheoryData<string, string, string, string> Refusals => new()
    {
        {
            "a format option on the wrong format, at a localfiles sink",
            Csv + Lake,
            "INSERT INTO {{ sink('lake', 'o', format: 'parquet', delimiter: ';') }}\nselect * from {{ source('raw', 'a') }}\n",
            "'delimiter' is not an option of format 'parquet'"
        },
        {
            "a format option on the wrong format, at an azureblob source",
            "az:\n  connector: azureblob\n  auth: credential_chain\n  account_name: acct\n  entities:\n    e:\n" +
            "      read: { container: c, path: x.parquet, format: parquet, delimiter: ';', columns: { id: bigint } }\n" + Lake,
            "INSERT INTO {{ sink('lake', 'o', format: 'parquet') }}\nselect * from {{ source('az', 'e') }}\n",
            "'delimiter' is not an option of format 'parquet'"
        },
        {
            "an s3 output with neither root nor bucket",
            Csv + "b:\n  connector: s3\n  access_key: a\n  secret_key: s\n",
            "INSERT INTO {{ sink('b', 'o', format: 'parquet') }}\nselect * from {{ source('raw', 'a') }}\n",
            "s3 needs a 'root' on the connection or a 'bucket' option"
        },
        {
            "an output on a read-only iceberg catalog",
            Csv + "ice:\n  connector: iceberg\n  catalog: files\n  root: wh\n",
            "INSERT INTO {{ sink('ice', 'ns.t') }}\nselect * from {{ source('raw', 'a') }}\n",
            "catalog 'files' is read-only"
        },
        {
            "a missing http tls.ca_file",
            "api:\n  connector: http\n  base_url: https://api.example.com\n  tls: { ca_file: certs/absent.pem }\n" +
            "  entities:\n    items:\n      read: { path: /items }\n" + Lake,
            "INSERT INTO {{ sink('lake', 'o', format: 'parquet') }}\nselect * from {{ source('api', 'items') }}\n",
            "tls.ca_file"
        },
    };

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Theory]
    [MemberData(nameof(Refusals))]
    public void Validate_and_plan_refuse_with_the_same_error(string _, string connections, string pipeline, string expected)
    {
        Directory.CreateDirectory(Path.Combine(_work, "pipelines"));
        Directory.CreateDirectory(Path.Combine(_work, "data"));
        File.WriteAllText(Path.Combine(_work, "project.yml"), "name: parity\nversion: \"1.0\"\n");
        File.WriteAllText(Path.Combine(_work, "data", "a.csv"), "id\n1\n");
        File.WriteAllText(Path.Combine(_work, "connections.yml"), connections);
        File.WriteAllText(Path.Combine(_work, "pipelines", "p.sql"), pipeline);

        var (validateExit, validateErr) = Invoke("validate");
        var (planExit, planErr) = Invoke("plan");

        Assert.Equal(ExitCodes.ConfigError, planExit);
        Assert.Equal(ExitCodes.ConfigError, validateExit);
        var planned = Assert.Single(planErr.Split('\n'), l => l.Contains(expected, StringComparison.Ordinal));
        var validated = Assert.Single(validateErr.Split('\n'), l => l.Contains(expected, StringComparison.Ordinal));
        Assert.Equal(planned, validated);
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
