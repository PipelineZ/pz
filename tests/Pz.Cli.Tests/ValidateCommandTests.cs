using Pz.Cli;

namespace Pz.Cli.Tests;

/// <summary>`pz validate`: tiers 1-2 (load+compile) -> tier 3 (connector connection/dataset config
/// schemas + cross-field ValidateAsync). Uses the same `hello-pz` fixture (connector "localfiles") as
/// <see cref="RunCommandTests"/>/<see cref="PlanCommandTests"/> for the clean-project case, and a
/// hand-written broken project (postgres source missing `host`, s3 sink missing credentials)
/// for the aggregated-errors case.</summary>
// See the "console-and-env-serialized" collection definition in RestoreCommandTests.cs: this class
// redirects Console.Out/Error to assert on CLI output and mutates the process-global DATA_DIR/OUT_DIR
// env vars, both of which must serialize against every other Console/env-swapping class in the assembly.
[Collection("console-and-env-serialized")]
public sealed class ValidateCommandTests : IDisposable
{
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-validate-tests", Guid.NewGuid().ToString("N"));

    public ValidateCommandTests()
    {
        Environment.SetEnvironmentVariable("DATA_DIR", "/tmp/pz-data");
        Environment.SetEnvironmentVariable("OUT_DIR", "/tmp/pz-out");
        CopyTree(Path.Combine(AppContext.BaseDirectory, "Fixtures", "hello-pz"), _work);
    }

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public void Validate_ok_on_hello_pz_fixture()
    {
        var stdout = RunCapturingStdout(["validate", "--project", _work]);

        Assert.Contains("validation passed", stdout);
    }

    [Fact]
    public void Validate_reports_all_config_errors()
    {
        var brokenDir = Path.Combine(Path.GetTempPath(), "pz-validate-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(brokenDir);
        try
        {
            WriteBrokenProject(brokenDir);

            var stderr = new StringWriter();
            var originalErr = Console.Error;
            Console.SetError(stderr);
            int exit;
            try
            {
                exit = CliApp.Build().Parse(["validate", "--project", brokenDir]).Invoke();
            }
            finally
            {
                Console.SetError(originalErr);
            }

            Assert.Equal(ExitCodes.ConfigError, exit);
            var output = stderr.ToString();
            Assert.Contains("PZ0301", output);
            Assert.Contains("connection 'db'", output);
            Assert.Contains("host", output);
            Assert.Contains("connection 'store'", output);
            Assert.Contains("access_key", output);
        }
        finally
        {
            Directory.Delete(brokenDir, recursive: true);
        }
    }

    [Fact]
    public void Validate_writes_no_artifacts()
    {
        var exit = CliApp.Build().Parse(["validate", "--project", _work]).Invoke();

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.False(Directory.Exists(Path.Combine(_work, ".pz", "target")));
    }

    /// <summary>Tier 4 (SQL dry-compile): the hello-pz fixture's `crm.orders` dataset has no declared
    /// `columns:` contract, so a typo introduced there would only ever land in SkippedPipelines, not
    /// Errors -- see <c>SqlDryCompilerTests</c>. To exercise a genuine PZ0401 through the CLI, this typos a
    /// column in the pipeline that ALREADY reads `crm.customers`, which DOES declare columns
    /// (`id`, `email`) -- PZ0349 allows only one reader per source dataset.</summary>
    [Fact]
    public void Validate_catches_sql_typo_before_any_run()
    {
        // PZ0349: a source dataset is read by exactly one pipeline, and hello-pz's crm.customers is
        // already read by orders_enriched. So the probe gets its own dataset -- same CSV, same declared
        // contract -- keeping one reader each. It must read a SOURCE directly: a pipeline over ref()
        // has no offline schema for the dry compiler to typo-check against. It gets its own CONNECTION
        // rather than a second entity under crm because both directions share one file, so appending
        // mid-file is not an option -- and a connection to the same place is what the probe needs.
        File.AppendAllText(Path.Combine(_work, "connections.yml"), """

            crm_probe:
              connector: localfiles
              entities:
                customers_probe:
                  read:
                    path: data/customers.csv
                    format: csv
                    columns:
                      id: bigint
                      email: varchar
            """);
        File.WriteAllText(Path.Combine(_work, "pipelines", "typo_check.sql"),
            "select id, emailx from {{ source('crm_probe', 'customers_probe') }}\n");

        var stderr = new StringWriter();
        var originalErr = Console.Error;
        Console.SetError(stderr);
        int exit;
        try
        {
            exit = CliApp.Build().Parse(["validate", "--project", _work]).Invoke();
        }
        finally
        {
            Console.SetError(originalErr);
        }

        Assert.Equal(ExitCodes.ConfigError, exit);
        var output = stderr.ToString();
        Assert.Contains("PZ0401", output);
        Assert.Contains("emailx", output);
        Assert.Contains("typo_check.sql", output);
    }

    /// <summary>Tier 5 (`--connect`) against a mutated COPY of the shipped `templates/sample` tree
    /// (content-linked into this test project as "TemplatesSample" -- see the .csproj). Every dataset
    /// the sample ships declares a `columns:` contract, so this test strips the one it declares for
    /// `customers` in `connections.yml` back out, reconstructing the contract-less csv/json case:
    /// `pz run`'s native-scan tier would handle it fine via DuckDB's own `auto_detect`, but tier 5's
    /// `ConnectivityValidator` calls `ISource.GetSchemaAsync`, which is the universal-tier schema path
    /// and unconditionally requires a full contract for csv -- so `customers`' probe fails with PZ0330,
    /// while `orders` and `products` (contracts untouched) probe cleanly. That is an accepted tier-5 gap
    /// for a contract-less csv/json dataset: `--connect` cannot pre-flight-check it, so a real shape
    /// problem there only surfaces at `pz run` time. `.pz/target/schemas.json` is still written
    /// (byte-stably) even though the command exits non-zero -- empty, since no dataset ends up in
    /// `ConnectivityResult.FetchedSchemas` (that dictionary is populated only for a dataset whose schema
    /// fetch actually SUCCEEDED with no declared contract, which never happens for csv).</summary>
    [Fact]
    public void Validate_connect_reports_undeclared_csv_gap_on_sample()
    {
        var sampleDir = Path.Combine(Path.GetTempPath(), "pz-validate-tests", Guid.NewGuid().ToString("N"));
        CopyTree(Path.Combine(AppContext.BaseDirectory, "TemplatesSample"), sampleDir);
        try
        {
            var connectionsPath = Path.Combine(sampleDir, "connections.yml");
            var connectionsYml = File.ReadAllText(connectionsPath).Replace(
                "    customers:\n      read:\n        path: data/customers.csv\n        format: csv\n" +
                "        columns:\n          id: bigint\n          email: varchar\n",
                "    customers:\n      read:\n        path: data/customers.csv\n        format: csv\n");
            File.WriteAllText(connectionsPath, connectionsYml);

            var stdout = new StringWriter();
            var stderr = new StringWriter();
            var originalOut = Console.Out;
            var originalErr = Console.Error;
            Console.SetOut(stdout);
            Console.SetError(stderr);
            int exit;
            try
            {
                exit = CliApp.Build().Parse(["validate", "--project", sampleDir, "--connect"]).Invoke();
            }
            finally
            {
                Console.SetOut(originalOut);
                Console.SetError(originalErr);
            }

            Assert.Equal(ExitCodes.ConfigError, exit);
            Assert.Contains("note: dataset 'raw.customers' has no columns: contract", stdout.ToString());
            var output = stderr.ToString();
            Assert.Contains("PZ0330", output);
            Assert.Contains("dataset 'customers'", output);
            Assert.Contains("requires a declared columns: contract", output);

            var schemasPath = Path.Combine(sampleDir, ".pz", "target", "schemas.json");
            Assert.True(File.Exists(schemasPath));
            var schemasJson = File.ReadAllText(schemasPath);
            Assert.Contains("\"version\": 1", schemasJson);
            Assert.Contains("\"schemas\": {}", schemasJson);
        }
        finally
        {
            Directory.Delete(sampleDir, recursive: true);
        }
    }

    /// <summary>A dataset may declare its `columns:` contract at its source() CALL SITE rather than in
    /// `connections.yml`, so tiers 3-5 must validate the EFFECTIVE connections, not the loaded ones:
    /// reading `PzProject.Connections` instead of `dag.Connections` skips a call-site-declared entity
    /// entirely and its drift goes unnoticed. The shipped sample's own `products` (read at its
    /// `source()` call site in `pipelines/product_catalog.sql`) already IS that call-site-declared
    /// entity, so this test only needs to corrupt a COPY of its CSV header to create drift; no pipeline
    /// edit is needed.</summary>
    [Fact]
    public void Validate_connect_reports_drift_for_call_site_declared_contract()
    {
        var sampleDir = Path.Combine(Path.GetTempPath(), "pz-validate-tests", Guid.NewGuid().ToString("N"));
        CopyTree(Path.Combine(AppContext.BaseDirectory, "TemplatesSample"), sampleDir);
        try
        {
            var productsPath = Path.Combine(sampleDir, "data", "products.csv");
            var lines = File.ReadAllLines(productsPath);
            lines[0] = "id,name"; // drop the call-site-declared "price" column from the real header
            File.WriteAllLines(productsPath, lines);

            var stderr = new StringWriter();
            var originalErr = Console.Error;
            Console.SetError(stderr);
            int exit;
            try
            {
                exit = CliApp.Build().Parse(["validate", "--project", sampleDir, "--connect"]).Invoke();
            }
            finally
            {
                Console.SetError(originalErr);
            }

            Assert.Equal(ExitCodes.ConfigError, exit);
            var output = stderr.ToString();
            Assert.Contains("PZ0331", output);
            Assert.Contains("price", output);
        }
        finally
        {
            Directory.Delete(sampleDir, recursive: true);
        }
    }

    private static void WriteBrokenProject(string dir)
    {
        File.WriteAllText(Path.Combine(dir, "project.yml"), """
            name: broken
            version: 0.1.0
            """);

        File.WriteAllText(Path.Combine(dir, "connections.yml"), """
            db:
              connector: postgres
              database: mydb

            store:
              connector: s3
              region: us-east-1
            """);
    }

    private static string RunCapturingStdout(string[] args)
    {
        var stdout = new StringWriter();
        var originalOut = Console.Out;
        Console.SetOut(stdout);
        try
        {
            var exit = CliApp.Build().Parse(args).Invoke();
            Assert.Equal(ExitCodes.Ok, exit);
        }
        finally
        {
            Console.SetOut(originalOut);
        }

        return stdout.ToString();
    }

    private static void CopyTree(string from, string to)
    {
        Directory.CreateDirectory(to);
        foreach (var file in Directory.EnumerateFiles(from, "*", SearchOption.AllDirectories))
        {
            var dest = Path.Combine(to, Path.GetRelativePath(from, file));
            Directory.CreateDirectory(Path.GetDirectoryName(dest)!);
            File.Copy(file, dest, overwrite: true);
        }
    }
}
