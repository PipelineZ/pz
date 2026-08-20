using System.Text.Json;
using Pz.Cli;
using Pz.DuckDb;

namespace Pz.EndToEnd.Tests;

/// <summary>End-to-end: `pz run` moves real data — LocalFiles CSV source -> DuckDB
/// staging -> SQL transform -> parquet sink. Every test copies the fixture into a throwaway temp dir
/// (never runs against the repo tree) before invoking the real CLI entry point.
///
/// Joins "console-redirection" (see <c>JsonLogFormatTests.ConsoleRedirectionCollection</c>): this class
/// redirects <see cref="Console.Error"/> in one test, and <see cref="Console.Out"/>/<see cref="Console.Error"/>
/// are process-global statics that other e2e test classes also redirect -- without a shared collection,
/// xunit's default cross-class parallelism lets those redirections race.</summary>
[Collection("console-redirection")]
public sealed class HelloRunTests : IDisposable
{
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-e2e-tests", Guid.NewGuid().ToString("N"));

    public HelloRunTests() => CopyTree(Path.Combine(AppContext.BaseDirectory, "Fixtures", "csv-to-parquet"), _work);

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task Run_moves_csv_through_transform_to_parquet()
    {
        var exit = CliApp.Build().Parse(["run", "--project", _work]).Invoke();
        Assert.Equal(ExitCodes.Ok, exit);

        var parquetPath = Path.Combine(_work, "out", "customer_totals.parquet");
        Assert.True(File.Exists(parquetPath));

        await using var duck = DuckSession.Open(Path.Combine(Path.GetTempPath(), $"pz-e2e-readback-{Guid.NewGuid():N}.duckdb"));
        var quoted = parquetPath.Replace("'", "''");
        var rowCount = await duck.ScalarAsync<long>($"select count(*) from read_parquet('{quoted}')");
        var sumTotal = await duck.ScalarAsync<double>($"select sum(total) from read_parquet('{quoted}')");

        Assert.Equal(3, rowCount);
        Assert.Equal(468.50, sumTotal, precision: 2);

        var runResults = ReadRunResults(_work);
        Assert.Equal("success", runResults.RootElement.GetProperty("status").GetString());

        var nodes = runResults.RootElement.GetProperty("nodes");
        Assert.Equal(3, nodes.GetArrayLength());

        var byKind = new Dictionary<string, JsonElement>();
        foreach (var node in nodes.EnumerateArray())
        {
            byKind[node.GetProperty("kind").GetString()!] = node;
        }

        Assert.Equal("success", byKind["SourceLoad"].GetProperty("status").GetString());
        Assert.Equal(12, byKind["SourceLoad"].GetProperty("rows").GetInt64());

        Assert.Equal("success", byKind["Pipeline"].GetProperty("status").GetString());
        Assert.Equal(3, byKind["Pipeline"].GetProperty("rows").GetInt64());

        Assert.Equal("success", byKind["SinkWrite"].GetProperty("status").GetString());
        Assert.Equal(3, byKind["SinkWrite"].GetProperty("rows").GetInt64());
    }

    /// <summary>LocalFiles offers a native tier for both edges of this fixture (the 'orders' dataset
    /// declares a columns: contract; the sink's format is 'parquet') — a default run (no
    /// engine.force_universal) must plan and use native_scan/native_copy, landing the exact same rows
    /// the universal path produces.</summary>
    [Fact]
    public async Task Run_defaults_to_native_scan_and_native_copy_for_localfiles()
    {
        var exit = CliApp.Build().Parse(["run", "--project", _work]).Invoke();
        Assert.Equal(ExitCodes.Ok, exit);

        var planPath = Path.Combine(_work, ".pz", "target", "plan.json");
        using var plan = JsonDocument.Parse(File.ReadAllText(planPath));

        var byKind = new Dictionary<string, JsonElement>();
        foreach (var node in plan.RootElement.GetProperty("nodes").EnumerateArray())
        {
            byKind[node.GetProperty("kind").GetString()!] = node;
        }

        Assert.Equal("native_scan", byKind["SourceLoad"].GetProperty("strategy").GetString());
        Assert.Equal("native_copy", byKind["SinkWrite"].GetProperty("strategy").GetString());

        var parquetPath = Path.Combine(_work, "out", "customer_totals.parquet");
        Assert.True(File.Exists(parquetPath));

        await using var duck = DuckSession.Open(Path.Combine(Path.GetTempPath(), $"pz-e2e-readback-{Guid.NewGuid():N}.duckdb"));
        var quoted = parquetPath.Replace("'", "''");
        var rowCount = await duck.ScalarAsync<long>($"select count(*) from read_parquet('{quoted}')");
        var sumTotal = await duck.ScalarAsync<double>($"select sum(total) from read_parquet('{quoted}')");

        Assert.Equal(3, rowCount);
        Assert.Equal(468.50, sumTotal, precision: 2);
    }

    /// <summary>engine.force_universal must flip every edge back to the universal path (arrow_stream)
    /// and produce identical output rows to the native default above — proving the two tiers are
    /// behaviorally interchangeable.</summary>
    [Fact]
    public async Task Force_universal_variant_produces_identical_rows()
    {
        await File.WriteAllTextAsync(Path.Combine(_work, "project.yml"), """
            name: csv_to_parquet
            version: 0.1.0

            connectors:
              - package: Pz.Connector.LocalFiles
                version: 0.1.0

            engine:
              threads: 2
              force_universal: true
            """);

        var exit = CliApp.Build().Parse(["run", "--project", _work]).Invoke();
        Assert.Equal(ExitCodes.Ok, exit);

        var planPath = Path.Combine(_work, ".pz", "target", "plan.json");
        using var plan = JsonDocument.Parse(File.ReadAllText(planPath));

        var byKind = new Dictionary<string, JsonElement>();
        foreach (var node in plan.RootElement.GetProperty("nodes").EnumerateArray())
        {
            byKind[node.GetProperty("kind").GetString()!] = node;
        }

        Assert.Equal("arrow_stream", byKind["SourceLoad"].GetProperty("strategy").GetString());
        Assert.Equal("arrow_stream", byKind["SinkWrite"].GetProperty("strategy").GetString());

        var parquetPath = Path.Combine(_work, "out", "customer_totals.parquet");
        Assert.True(File.Exists(parquetPath));

        await using var duck = DuckSession.Open(Path.Combine(Path.GetTempPath(), $"pz-e2e-readback-{Guid.NewGuid():N}.duckdb"));
        var quoted = parquetPath.Replace("'", "''");
        var rowCount = await duck.ScalarAsync<long>($"select count(*) from read_parquet('{quoted}')");
        var sumTotal = await duck.ScalarAsync<double>($"select sum(total) from read_parquet('{quoted}')");

        Assert.Equal(3, rowCount);
        Assert.Equal(468.50, sumTotal, precision: 2);
    }

    /// <summary>`customer_totals.sql`'s `orders` dataset declares a `columns:` contract (id, customer,
    /// amount, created), so the implicit tier-4 SQL dry-compile that `pz run` performs before opening
    /// any real staging session catches this typo pre-run: it never reaches node execution, and no
    /// `.pz/runs/` directory is created at all.</summary>
    [Fact]
    public async Task Broken_sql_is_caught_by_dry_compile_before_any_run()
    {
        var pipelinePath = Path.Combine(_work, "pipelines", "customer_totals.sql");
        await File.WriteAllTextAsync(pipelinePath,
            "INSERT INTO {{ sink('lake', 'customer_totals') }}\n"
            + "select customer, sum(no_such_column) as total\nfrom {{ source('files', 'orders') }}\ngroup by customer\n");

        var stderr = new StringWriter();
        var originalErr = Console.Error;
        Console.SetError(stderr);
        int exit;
        try
        {
            exit = CliApp.Build().Parse(["run", "--project", _work]).Invoke();
        }
        finally
        {
            Console.SetError(originalErr);
        }

        Assert.Equal(ExitCodes.ConfigError, exit);
        var output = stderr.ToString();
        Assert.Contains("PZ0401", output);
        Assert.Contains("no_such_column", output);
        Assert.False(Directory.Exists(Path.Combine(_work, ".pz", "runs")));
    }

    /// <summary>Tier-4 dry-compile only catches SQL that is type-illegal against the contract's
    /// declared column *types* -- it materializes
    /// the pipeline over an EMPTY contract-shaped table (`limit 0`), so a cast that is syntactically and
    /// type-legal (varchar -> integer) sails through dry-compile with zero rows to choke on, then blows up
    /// once the real 'alice'/'bob'/'carol' strings from the fixture hit it at actual node execution. This
    /// is the one remaining path to a genuine PZ0501 node failure with a downstream skip cascade --
    /// `Broken_sql_is_caught_by_dry_compile_before_any_run` above proves the pre-run interception path;
    /// this proves the run-time execution path is still reachable and still reported correctly end to
    /// end (run_results statuses, exit code, no sink output).</summary>
    [Fact]
    public async Task Runtime_failure_after_clean_dry_compile_fails_node_and_skips_sink()
    {
        var pipelinePath = Path.Combine(_work, "pipelines", "customer_totals.sql");
        await File.WriteAllTextAsync(pipelinePath,
            "INSERT INTO {{ sink('lake', 'customer_totals') }}\n"
            + "select customer, count(*) as orders, sum(amount) as total, cast(customer as integer) as customer_code\n"
            + "from {{ source('files', 'orders') }}\ngroup by customer, customer_code\n");

        var exit = CliApp.Build().Parse(["run", "--project", _work]).Invoke();
        Assert.Equal(ExitCodes.NodeFailures, exit);

        var runResults = ReadRunResults(_work);
        Assert.Equal("completed_with_failures", runResults.RootElement.GetProperty("status").GetString());

        var byKind = new Dictionary<string, JsonElement>();
        foreach (var node in runResults.RootElement.GetProperty("nodes").EnumerateArray())
        {
            byKind[node.GetProperty("kind").GetString()!] = node;
        }

        Assert.Equal("success", byKind["SourceLoad"].GetProperty("status").GetString());

        Assert.Equal("failed", byKind["Pipeline"].GetProperty("status").GetString());
        Assert.Equal("PZ0501", byKind["Pipeline"].GetProperty("error").GetProperty("code").GetString());
        // PipelineExecutor lets ctx.Duck.ExecuteAsync's raw DuckDBException propagate uncaught (it is
        // never wrapped into a Pz exception before reaching KindDispatchingExecutor's terminal catch),
        // so this is a foreign exception and MessageRedaction.Redact(Exception) redacts it: the
        // offending 'alice' value is masked to '***'. "Conversion Error: Could not convert string" is
        // DuckDB's real runtime cast failure wording (never produced by dry-compile's zero-row
        // empty-table check), so it proves this is the runtime path, not the pre-run dry-compile
        // interception `Broken_sql_is_caught_by_dry_compile_before_any_run` pins.
        var pipelineMessage = byKind["Pipeline"].GetProperty("error").GetProperty("message").GetString();
        Assert.Contains("Conversion Error: Could not convert string '***'", pipelineMessage);
        Assert.DoesNotContain("alice", pipelineMessage);

        Assert.Equal("skipped", byKind["SinkWrite"].GetProperty("status").GetString());

        Assert.False(File.Exists(Path.Combine(_work, "out", "customer_totals.parquet")));
    }

    /// <summary>A declared `columns:` contract column absent from the actual CSV file's header. The
    /// SQL never references the phantom column, so tier-4 dry-compile
    /// (which builds an empty table shaped by the FULL contract, then only type-checks columns the SQL
    /// actually touches) sails through -- the failure surfaces only once `CsvSource` opens the real file
    /// at SourceLoad. `force_universal: true` is required to pin the SPECIFIC interplay traced in
    /// <c>CsvSource.GetSchemaAsync</c>'s doc comment (that call itself returns a schema that silently
    /// PRUNES the missing column, while <c>CsvPartition.ReadAsync</c> -- built from the FULL contract --
    /// fails the ordinal lookup and throws its own clear "missing declared column" error before yielding
    /// any row): the default native-scan tier never calls <c>GetSchemaAsync</c> at all (DuckDB's
    /// `read_csv` fails on its own, with a different message), so it would not exercise this path.</summary>
    [Fact]
    public async Task Run_with_csv_missing_declared_column_fails_cleanly_at_load()
    {
        await File.WriteAllTextAsync(Path.Combine(_work, "project.yml"), """
            name: csv_to_parquet
            version: 0.1.0

            connectors:
              - package: Pz.Connector.LocalFiles
                version: 0.1.0

            engine:
              threads: 2
              force_universal: true
            """);

        var sourcePath = Path.Combine(_work, "connections.yml");
        await File.WriteAllTextAsync(sourcePath, """
            files:
              connector: localfiles
              entities:
                orders:
                  read:
                    path: data/orders.csv
                    format: csv
                    columns:
                      id: bigint
                      customer: varchar
                      amount: double
                      created: timestamp
                      region: varchar

            lake:
              connector: localfiles
            """);

        var exit = CliApp.Build().Parse(["run", "--project", _work]).Invoke();
        Assert.Equal(ExitCodes.NodeFailures, exit);

        var runResults = ReadRunResults(_work);
        Assert.Equal("completed_with_failures", runResults.RootElement.GetProperty("status").GetString());

        var byKind = new Dictionary<string, JsonElement>();
        foreach (var node in runResults.RootElement.GetProperty("nodes").EnumerateArray())
        {
            byKind[node.GetProperty("kind").GetString()!] = node;
        }

        Assert.Equal("failed", byKind["SourceLoad"].GetProperty("status").GetString());
        var sourceLoadError = byKind["SourceLoad"].GetProperty("error");
        Assert.Equal("PZ0501", sourceLoadError.GetProperty("code").GetString());
        // This failure is a PzConnectorException thrown by CsvSource/CsvPartition (see
        // connectors/Pz.Connector.LocalFiles/CsvSource.cs) -- a Pz-family exception, which
        // MessageRedaction.Redact(Exception) passes through unredacted, its message being
        // developer-authored and safe, so the actionable column name reaches the operator. Only
        // foreign/raw exceptions (which might echo raw SQL/data) are redacted.
        var sourceLoadMessage = sourceLoadError.GetProperty("message").GetString();
        Assert.Contains("missing declared column 'region' in header", sourceLoadMessage);

        Assert.Equal("skipped", byKind["Pipeline"].GetProperty("status").GetString());
        Assert.Equal("skipped", byKind["SinkWrite"].GetProperty("status").GetString());

        Assert.False(File.Exists(Path.Combine(_work, "out", "customer_totals.parquet")));
    }

    /// <summary>End-to-end proof that a `localfiles` CSV dataset with no declared `columns:` at all
    /// runs: the `orders` entity's `columns:` mapping is dropped entirely and the run must still
    /// succeed, via the DEFAULT native-scan tier -- DuckDB's own `read_csv(..., auto_detect = true)`
    /// infers the whole schema as part of the real read, with no separate sampling pass and no
    /// schema-drift-baseline seeding, so `.pz/state/schemas.json` is never written for this dataset
    /// regardless of `on_source_drift`. This test deliberately does NOT set
    /// `engine.force_universal: true`, unlike its sibling
    /// <see cref="Run_with_csv_missing_declared_column_fails_cleanly_at_load"/>: the universal tier
    /// still requires a declared `columns:` contract, so forcing universal here would fail, not
    /// succeed. Only the native-scan tier infers, so this test asserts the plan actually used
    /// it.</summary>
    [Fact]
    public async Task Run_with_csv_dataset_missing_columns_infers_and_succeeds()
    {
        var sourcePath = Path.Combine(_work, "connections.yml");
        await File.WriteAllTextAsync(sourcePath, """
            files:
              connector: localfiles
              entities:
                orders:
                  read:
                    path: data/orders.csv
                    format: csv

            lake:
              connector: localfiles
            """);

        var exit = CliApp.Build().Parse(["run", "--project", _work]).Invoke();
        Assert.Equal(ExitCodes.Ok, exit);

        var runResults = ReadRunResults(_work);
        Assert.Equal("success", runResults.RootElement.GetProperty("status").GetString());

        var parquetPath = Path.Combine(_work, "out", "customer_totals.parquet");
        Assert.True(File.Exists(parquetPath));

        await using var duck = DuckSession.Open(Path.Combine(Path.GetTempPath(), $"pz-e2e-readback-{Guid.NewGuid():N}.duckdb"));
        var quoted = parquetPath.Replace("'", "''");
        var rowCount = await duck.ScalarAsync<long>($"select count(*) from read_parquet('{quoted}')");
        var sumTotal = await duck.ScalarAsync<double>($"select sum(total) from read_parquet('{quoted}')");

        Assert.Equal(3, rowCount);
        Assert.Equal(468.50, sumTotal, precision: 2);

        var planPath = Path.Combine(_work, ".pz", "target", "plan.json");
        using var plan = JsonDocument.Parse(File.ReadAllText(planPath));
        var sourceLoadNode = plan.RootElement.GetProperty("nodes").EnumerateArray()
            .Single(n => n.GetProperty("kind").GetString() == "SourceLoad");
        Assert.Equal("native_scan", sourceLoadNode.GetProperty("strategy").GetString());

        Assert.False(File.Exists(Path.Combine(_work, ".pz", "state", "schemas.json")));
    }

    [Fact]
    public void Run_results_written_incrementally()
    {
        var exit = CliApp.Build().Parse(["run", "--project", _work]).Invoke();
        Assert.Equal(ExitCodes.Ok, exit);

        var runResults = ReadRunResults(_work);
        Assert.Equal(3, runResults.RootElement.GetProperty("nodes").GetArrayLength());
    }

    private static JsonDocument ReadRunResults(string projectDir)
    {
        var runsDir = Path.Combine(projectDir, ".pz", "runs");
        var runDir = Directory.EnumerateDirectories(runsDir).Single();
        var path = Path.Combine(runDir, "run_results.json");
        Assert.True(File.Exists(path));
        Assert.False(File.Exists(path + ".tmp"));
        return JsonDocument.Parse(File.ReadAllBytes(path));
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
