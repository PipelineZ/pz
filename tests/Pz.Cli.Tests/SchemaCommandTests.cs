using Pz.Cli;
using Pz.DuckDb;

namespace Pz.Cli.Tests;

/// <summary><c>pz schema accept</c>. Exercises the CLI verb against a
/// real, small, working project (Fixtures/schema-accept-basic: two contract-less parquet datasets over
/// localfiles) so node ids -- and therefore the mapping from a run's <c>observed_schema</c> back to
/// `&lt;connection&gt;.&lt;entity&gt;` -- are genuine content hashes, exactly the same shape
/// <c>RetryCommandTests</c> relies on for `pz retry`. The datasets are parquet because a dataset must
/// read without a declared <c>columns:</c> contract for <c>SchemaDriftGate</c> to run at all; data
/// files are generated per test via <see cref="DuckSession"/> rather than checked in, so each test
/// controls its own drift shape. Contract-less csv (localfiles) and csv/json (azureblob) reach the same
/// gate -- see <see cref="Contract_less_csv_dataset_is_covered_by_schema_drift_under_warn"/> below for
/// the real-`pz run` proof.</summary>
// See the "console-and-env-serialized" collection definition in RestoreCommandTests.cs.
[Collection("console-and-env-serialized")]
public sealed class SchemaCommandTests : IDisposable
{
    private readonly List<string> _workDirs = [];

    public void Dispose()
    {
        foreach (var dir in _workDirs)
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    private string NewWork(string fixture)
    {
        var work = Path.Combine(Path.GetTempPath(), "pz-schema-accept-cli-tests", Guid.NewGuid().ToString("N"));
        CopyTree(Path.Combine(AppContext.BaseDirectory, "Fixtures", fixture), work);
        _workDirs.Add(work);
        return work;
    }

    [Fact]
    public async Task Accept_updates_baseline_lists_changes_and_a_later_run_reports_no_more_drift()
    {
        var work = NewWork("schema-accept-basic");
        await WriteOrdersBaseline(work);
        await WriteItemsBaseline(work);

        Assert.Equal(ExitCodes.Ok, CliApp.Build().Parse(["run", "--project", work, "--all"]).Invoke());

        await WriteOrdersDrifted(work);

        var driftStdout = CaptureOut(
            () => CliApp.Build().Parse(["run", "--project", work, "--all", "--log-format", "json"]).Invoke(), out var driftExit);
        Assert.Equal(ExitCodes.Ok, driftExit);
        Assert.Contains("\"event\":\"source_drift_detected\"", driftStdout);
        Assert.Contains("\"connection\":\"pg\"", driftStdout);
        Assert.Contains("\"entity\":\"orders\"", driftStdout);

        var acceptStdout = CaptureOut(
            () => CliApp.Build().Parse(["schema", "accept", "--project", work]).Invoke(), out var acceptExit);

        Assert.Equal(ExitCodes.Ok, acceptExit);
        Assert.Contains("pg.orders: column 'amount' retyped BIGINT -> VARCHAR", acceptStdout);
        Assert.Contains("accepted 1 schema change(s)", acceptStdout);

        var laterStdout = CaptureOut(
            () => CliApp.Build().Parse(["run", "--project", work, "--all", "--log-format", "json"]).Invoke(), out var laterExit);
        Assert.Equal(ExitCodes.Ok, laterExit);
        Assert.DoesNotContain("source_drift_detected", laterStdout);
    }

    [Fact]
    public async Task Selective_accept_leaves_the_other_drifted_dataset_baseline_alone()
    {
        var work = NewWork("schema-accept-basic");
        await WriteOrdersBaseline(work);
        await WriteItemsBaseline(work);
        Assert.Equal(ExitCodes.Ok, CliApp.Build().Parse(["run", "--project", work, "--all"]).Invoke());

        await WriteOrdersDrifted(work);
        await WriteItemsDrifted(work);
        Assert.Equal(ExitCodes.Ok, CliApp.Build().Parse(["run", "--project", work, "--all"]).Invoke());

        var acceptStdout = CaptureOut(
            () => CliApp.Build().Parse(["schema", "accept", "pg.orders", "--project", work]).Invoke(),
            out var acceptExit);

        Assert.Equal(ExitCodes.Ok, acceptExit);
        Assert.Contains("pg.orders: column 'amount' retyped BIGINT -> VARCHAR", acceptStdout);
        Assert.DoesNotContain("pg.items", acceptStdout);
        Assert.Contains("accepted 1 schema change(s)", acceptStdout);

        // A subsequent run must still warn for items (its stale baseline was left alone) but not orders.
        var laterStdout = CaptureOut(
            () => CliApp.Build().Parse(["run", "--project", work, "--all", "--log-format", "json"]).Invoke(), out var laterExit);
        Assert.Equal(ExitCodes.Ok, laterExit);
        Assert.Contains("\"entity\":\"items\"", laterStdout);
        Assert.DoesNotContain("\"entity\":\"orders\"", laterStdout);
    }

    [Fact]
    public async Task Nothing_drifted_exits_ok_reports_nothing_to_accept_and_writes_nothing()
    {
        var work = NewWork("schema-accept-basic");
        await WriteOrdersBaseline(work);
        await WriteItemsBaseline(work);
        Assert.Equal(ExitCodes.Ok, CliApp.Build().Parse(["run", "--project", work, "--all"]).Invoke());

        var schemasPath = Path.Combine(work, ".pz", "state", "schemas.json");
        var before = File.ReadAllBytes(schemasPath);

        var stdout = CaptureOut(
            () => CliApp.Build().Parse(["schema", "accept", "--project", work]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Contains("nothing to accept", stdout);
        Assert.Equal(before, File.ReadAllBytes(schemasPath));
    }

    [Fact]
    public async Task Named_dataset_with_no_recorded_observed_schema_is_a_PZ_coded_error()
    {
        var work = NewWork("schema-accept-ignore");
        await WriteParquetAsync(Path.Combine(work, "data", "orders.parquet"),
            "* from (values (1::bigint,'alice',10::bigint),(2,'bob',20)) as t(id, customer, amount)");

        Assert.Equal(ExitCodes.Ok, CliApp.Build().Parse(["run", "--project", work, "--all"]).Invoke());

        var stderr = Capture(
            () => CliApp.Build().Parse(["schema", "accept", "pg.orders", "--project", work]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0127", stderr);
        Assert.Contains("pg.orders", stderr);
        Assert.Contains("no recorded observed schema", stderr);
        Assert.Contains("on_source_drift: warn|fail", stderr);
    }

    [Fact]
    public async Task Accept_never_opens_a_connector_even_when_the_source_is_now_unreachable()
    {
        var work = NewWork("schema-accept-basic");
        await WriteOrdersBaseline(work);
        await WriteItemsBaseline(work);
        Assert.Equal(ExitCodes.Ok, CliApp.Build().Parse(["run", "--project", work, "--all"]).Invoke());

        await WriteOrdersDrifted(work);
        Assert.Equal(ExitCodes.Ok, CliApp.Build().Parse(["run", "--project", work, "--all"]).Invoke());

        // Point the connection at a location nothing can reach -- if accept ever tried to actually read
        // this dataset (a connector open/connect), it would fail immediately (no such path). It must not
        // even try: it only reads the latest run's recorded observed_schema and the current baseline.
        var connectionsPath = Path.Combine(work, "connections.yml");
        var unreachableRoot = Path.Combine(Path.GetTempPath(), $"pz-unreachable-host-proof-{Guid.NewGuid():N}");
        Assert.False(Directory.Exists(unreachableRoot));
        File.WriteAllText(connectionsPath, File.ReadAllText(connectionsPath)
            .Replace("root: data", $"root: {unreachableRoot}", StringComparison.Ordinal));

        var acceptStdout = CaptureOut(
            () => CliApp.Build().Parse(["schema", "accept", "--project", work]).Invoke(), out var acceptExit);

        Assert.Equal(ExitCodes.Ok, acceptExit);
        Assert.Contains("pg.orders: column 'amount' retyped BIGINT -> VARCHAR", acceptStdout);
        Assert.Contains("accepted 1 schema change(s)", acceptStdout);
    }

    /// <summary>Proves, against two real `pz run`s (not a hand-built <c>NodeResult</c> the way
    /// <c>SchemaDriftGateTests</c> unit-tests the gate), that a contract-less `localfiles` csv dataset
    /// IS covered by <c>on_source_drift</c> under `warn` -- the gate is connector-agnostic, and csv/json
    /// reach it because they can run without a `columns:` contract at all. Reuses the
    /// `schema-accept-basic` fixture's `pg`/`lake` localfiles connectors and its already-
    /// `on_source_drift: warn` project.yml, but drops the `items` pipeline (unused here) and repoints
    /// `orders` at a contract-less csv file instead of the fixture's own declared-format
    /// parquet.</summary>
    [Fact]
    public async Task Contract_less_csv_dataset_is_covered_by_schema_drift_under_warn()
    {
        var work = NewWork("schema-accept-basic");
        File.Delete(Path.Combine(work, "pipelines", "items_out.sql"));
        await File.WriteAllTextAsync(Path.Combine(work, "connections.yml"), """
            pg:
              connector: localfiles
              root: data
              entities:
                orders:
                  read:
                    path: orders.csv
                    format: csv

            lake:
              connector: localfiles
              root: out
            """);

        await WriteCsvAsync(Path.Combine(work, "data", "orders.csv"),
            "id,customer,amount\n1,alice,10\n2,bob,20\n3,alice,30\n4,carol,5\n");

        Assert.Equal(ExitCodes.Ok, CliApp.Build().Parse(["run", "--project", work, "--all"]).Invoke());

        var schemasPath = Path.Combine(work, ".pz", "state", "schemas.json");
        Assert.True(File.Exists(schemasPath)); // baseline seeded from DuckDB's auto-detected shape
        Assert.Contains("\"amount\"", await File.ReadAllTextAsync(schemasPath));
        var seededBytes = await File.ReadAllBytesAsync(schemasPath);

        // Genuinely change the file's shape: amount becomes non-numeric, so DuckDB's auto_detect infers
        // a different type on the second run -- real drift, not a hand-constructed one.
        await WriteCsvAsync(Path.Combine(work, "data", "orders.csv"),
            "id,customer,amount\n1,alice,N/A\n2,bob,N/A\n3,alice,N/A\n4,carol,N/A\n");

        var driftStdout = CaptureOut(
            () => CliApp.Build().Parse(["run", "--project", work, "--all", "--log-format", "json"]).Invoke(),
            out var driftExit);

        Assert.Equal(ExitCodes.Ok, driftExit); // warn never fails a run
        Assert.Contains("\"event\":\"source_drift_detected\"", driftStdout);
        Assert.Contains("\"connection\":\"pg\"", driftStdout);
        Assert.Contains("\"entity\":\"orders\"", driftStdout);
        Assert.Contains("retyped", driftStdout);

        // warn never advances the baseline -- the whole point of the warn-until-accept loop.
        Assert.Equal(seededBytes, await File.ReadAllBytesAsync(schemasPath));
    }

    private static Task WriteCsvAsync(string path, string content)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        return File.WriteAllTextAsync(path, content);
    }

    private static Task WriteOrdersBaseline(string work) => WriteParquetAsync(
        Path.Combine(work, "data", "orders.parquet"),
        "* from (values (1::bigint,'alice',10::bigint),(2,'bob',20),(3,'alice',30),(4,'carol',5)) " +
        "as t(id, customer, amount)");

    private static Task WriteOrdersDrifted(string work) => WriteParquetAsync(
        Path.Combine(work, "data", "orders.parquet"),
        "* from (values (1::bigint,'alice','N/A'),(2,'bob','N/A'),(3,'alice','N/A'),(4,'carol','N/A')) " +
        "as t(id, customer, amount)");

    private static Task WriteItemsBaseline(string work) => WriteParquetAsync(
        Path.Combine(work, "data", "items.parquet"),
        "* from (values ('A'::varchar,5::bigint),('B',7)) as t(sku, qty)");

    private static Task WriteItemsDrifted(string work) => WriteParquetAsync(
        Path.Combine(work, "data", "items.parquet"),
        "* from (values ('A'::varchar,'five'),('B','seven')) as t(sku, qty)");

    private static async Task WriteParquetAsync(string outputPath, string selectSql)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(outputPath)!);
        var scratchDb = Path.Combine(Path.GetTempPath(), "pz-schema-accept-scratch", Guid.NewGuid().ToString("N") + ".duckdb");
        Directory.CreateDirectory(Path.GetDirectoryName(scratchDb)!);
        try
        {
            await using var duck = DuckSession.Open(scratchDb);
            var quoted = outputPath.Replace("'", "''", StringComparison.Ordinal);
            await duck.ExecuteAsync($"COPY (select {selectSql}) TO '{quoted}' (FORMAT PARQUET)");
        }
        finally
        {
            try { File.Delete(scratchDb); } catch { /* best-effort cleanup */ }
        }
    }

    private static string Capture(Func<int> action, out int exit)
    {
        var stderr = new StringWriter();
        var original = Console.Error;
        Console.SetError(stderr);
        try
        {
            exit = action();
        }
        finally
        {
            Console.SetError(original);
        }

        return stderr.ToString();
    }

    private static string CaptureOut(Func<int> action, out int exit)
    {
        var stdout = new StringWriter();
        var original = Console.Out;
        Console.SetOut(stdout);
        try
        {
            exit = action();
        }
        finally
        {
            Console.SetOut(original);
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
