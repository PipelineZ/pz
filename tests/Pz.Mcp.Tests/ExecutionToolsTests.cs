using System.Text.Json;
using Pz.Mcp;
using Pz.Mcp.Handlers;

namespace Pz.Mcp.Tests;

/// <summary>pz_run / pz_retry / pz_run_results -- runs the real TempProject end to end
/// through DuckDB + the localfiles connector (no docker, no network). <see cref="RealServices"/> is
/// literally <c>Pz.Cli.Commands.McpCommand.BuildServices()</c> -- the same wiring `pz mcp` itself uses --
/// rather than a duplicated set of fields, so the CLI verb and these tests share exactly one
/// construction.</summary>
public class ExecutionToolsTests
{
    private static CliServices RealServices() => Pz.Cli.Commands.McpCommand.BuildServices();

    [Fact]
    public async Task Run_executes_the_temp_project_end_to_end_and_returns_node_results()
    {
        using var p = new TempProject();
        var doc = JsonDocument.Parse(await ExecutionTools.RunAsync(
            p.Dir, flowNames: ["orders_out"], all: false, fullRefresh: false, RealServices(), CancellationToken.None));

        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.Equal("success", doc.RootElement.GetProperty("result").GetProperty("status").GetString());
        Assert.Equal(0, doc.RootElement.GetProperty("result").GetProperty("exit_code").GetInt32());
        var nodes = doc.RootElement.GetProperty("result").GetProperty("nodes");
        Assert.True(nodes.GetArrayLength() > 0);
        Assert.True(Directory.EnumerateFiles(Path.Combine(p.Dir, "out"), "*", SearchOption.AllDirectories).Any());
    }

    [Fact]
    public async Task Run_with_broken_pipeline_sql_is_refused_before_any_run_dir_is_created()
    {
        // Fix round 1, Finding 1: the tier-4 SqlDryCompiler pre-flight must run BEFORE ExecuteRun opens
        // a run dir/staging DB/connectors -- broken pipeline SQL must never reach real execution. A
        // declared `columns:` contract on the source is required for SqlDryCompiler to actually evaluate
        // downstream SQL at all (a contract-less dataset is "undeclared" -> unavailable -> the dependent
        // pipeline is recorded as skipped, never errored -- see SqlDryCompiler's own doc comment), so this
        // test declares one rather than relying on TempProject's default contract-less `raw.orders`.
        using var p = new TempProject();
        File.WriteAllText(Path.Combine(p.Dir, "connections.yml"),
            """
            raw:
              connector: localfiles
              entities:
                orders:
                  read:
                    path: data/orders.csv
                    format: csv
                    columns:
                      id: bigint
                      amount: double

            out:
              connector: localfiles
              root: out
            """ + "\n");
        p.WritePipeline("stg_orders", "select id, no_such_column\nfrom {{ source('raw', 'orders') }}\n");

        var doc = JsonDocument.Parse(await ExecutionTools.RunAsync(
            p.Dir, ["orders_out"], false, false, RealServices(), CancellationToken.None));

        Assert.False(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.Equal("PZ0401", doc.RootElement.GetProperty("errors")[0].GetProperty("code").GetString());

        var runsDir = Path.Combine(p.Dir, ".pz", "runs");
        Assert.True(!Directory.Exists(runsDir) || !Directory.EnumerateDirectories(runsDir).Any());
    }

    // `on_source_drift: warn` drift warnings are RUN-TIME events --
    // `pz run` prints them via the console renderer, but an MCP caller has no console, so they must
    // ride the envelope's `warnings` array or agent-driven operation gets silent drift. First run
    // seeds the baseline silently; the second run, against a reshaped file, must warn.
    [Fact]
    public async Task Run_surfaces_run_time_drift_warnings_in_the_result()
    {
        using var p = new TempProject();
        File.WriteAllText(Path.Combine(p.Dir, "project.yml"),
            "name: mcp_test\nversion: \"0.1.0\"\non_source_drift: warn\n");

        var first = JsonDocument.Parse(await ExecutionTools.RunAsync(
            p.Dir, ["orders_out"], false, false, RealServices(), CancellationToken.None));
        Assert.True(first.RootElement.GetProperty("ok").GetBoolean());
        Assert.Equal(0, first.RootElement.GetProperty("result").GetProperty("warnings").GetArrayLength());

        File.WriteAllText(Path.Combine(p.Dir, "data", "orders.csv"), "id,amount,extra\n1,10,x\n2,20,y\n");

        var second = JsonDocument.Parse(await ExecutionTools.RunAsync(
            p.Dir, ["orders_out"], false, false, RealServices(), CancellationToken.None));

        Assert.True(second.RootElement.GetProperty("ok").GetBoolean());
        Assert.Equal("success", second.RootElement.GetProperty("result").GetProperty("status").GetString());
        var warnings = second.RootElement.GetProperty("result").GetProperty("warnings").EnumerateArray().ToList();
        var drift = Assert.Single(warnings);
        Assert.Equal("PZ0331", drift.GetProperty("code").GetString());
        Assert.Contains("schema drift", drift.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.Contains("extra", drift.GetProperty("message").GetString(), StringComparison.Ordinal);
    }

    // A contract-less csv whose integer column
    // exceeds int64 is auto-detected as DOUBLE and silently loses digits; the run must surface the
    // PZ0523 lossy-integer warning in the envelope's `warnings` array, same channel as drift/PZ0522.
    [Fact]
    public async Task Run_surfaces_lossy_integer_inference_warnings_in_the_result()
    {
        using var p = new TempProject();
        File.WriteAllText(Path.Combine(p.Dir, "data", "orders.csv"),
            "id,amount\n12345678901234567890,10\n98765432109876543210,20\n");

        var doc = JsonDocument.Parse(await ExecutionTools.RunAsync(
            p.Dir, ["orders_out"], false, false, RealServices(), CancellationToken.None));

        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
        var warnings = doc.RootElement.GetProperty("result").GetProperty("warnings").EnumerateArray().ToList();
        var lossy = Assert.Single(warnings);
        Assert.Equal("PZ0523", lossy.GetProperty("code").GetString());
        Assert.Contains("id", lossy.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.Contains("columns:", lossy.GetProperty("hint").GetString(), StringComparison.Ordinal);
    }

    // A contract-less csv date column whose every
    // value is day/month-ambiguous is parsed with an assumed field order; the run must surface the
    // PZ0524 ambiguous-date warning in the envelope's `warnings` array.
    [Fact]
    public async Task Run_surfaces_ambiguous_date_inference_warnings_in_the_result()
    {
        using var p = new TempProject();
        File.WriteAllText(Path.Combine(p.Dir, "data", "orders.csv"),
            "id,amount\n1,01/02/2024\n2,03/04/2024\n");

        var doc = JsonDocument.Parse(await ExecutionTools.RunAsync(
            p.Dir, ["orders_out"], false, false, RealServices(), CancellationToken.None));

        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
        var warnings = doc.RootElement.GetProperty("result").GetProperty("warnings").EnumerateArray().ToList();
        var ambiguous = Assert.Single(warnings);
        Assert.Equal("PZ0524", ambiguous.GetProperty("code").GetString());
        Assert.Contains("amount", ambiguous.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.Contains("ISO 8601", ambiguous.GetProperty("hint").GetString(), StringComparison.Ordinal);
    }

    // A failed node's PZ#### code and message are in run_results.json and must also ride the
    // envelope's `nodes[]` -- otherwise an MCP caller sees only status "failed" with no way to learn
    // why without shell access to the artifact file.
    [Fact]
    public async Task Run_surfaces_node_error_details_in_the_result_nodes()
    {
        using var p = new TempProject();
        File.WriteAllText(Path.Combine(p.Dir, "connections.yml"),
            """
            raw:
              connector: localfiles
              entities:
                orders:
                  read:
                    path: data/orders.csv
                    format: csv
                    columns:
                      id: bigint
                      amount: double

            out:
              connector: localfiles
              root: out
            """ + "\n");
        File.WriteAllText(Path.Combine(p.Dir, "data", "orders.csv"), "id,amount\nnot_a_number,10\n");

        var doc = JsonDocument.Parse(await ExecutionTools.RunAsync(
            p.Dir, ["orders_out"], false, false, RealServices(), CancellationToken.None));

        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.Equal("completed_with_failures",
            doc.RootElement.GetProperty("result").GetProperty("status").GetString());
        var failed = doc.RootElement.GetProperty("result").GetProperty("nodes").EnumerateArray()
            .Single(n => n.GetProperty("status").GetString() == "failed");
        var error = failed.GetProperty("error");
        Assert.StartsWith("PZ", error.GetProperty("code").GetString(), StringComparison.Ordinal);
        Assert.False(string.IsNullOrEmpty(error.GetProperty("message").GetString()));
    }

    [Fact]
    public async Task Run_surfaces_compile_notices_and_warnings_in_the_result()
    {
        // Fix round 1, Finding 2: a YAML-declared incremental cursor (`sync: {mode: incremental,
        // cursor: …}`) with no `columns:` contract on its source produces DagCompiler's "cursor
        // unverified until --connect / first run" compile notice -- `pz run` prints it as a `note:`
        // line; this proves the MCP result envelope carries the same information instead of silently
        // discarding it. (The SQL-declared route, i.e. `watermark()` calls in pipeline SQL, is
        // deliberately NOT used here -- it produces no such notice; type ambiguity there is
        // resolved from the stored watermark itself, which is the whole point of that route
        // needing no contract at all.)
        using var p = new TempProject();
        File.WriteAllText(Path.Combine(p.Dir, "connections.yml"),
            """
            raw:
              connector: localfiles
              entities:
                orders:
                  read:
                    path: data/orders.csv
                    format: csv
                    sync:
                      mode: incremental
                      cursor: amount

            out:
              connector: localfiles
              root: out
            """ + "\n");
        // Marking `raw.orders` incremental makes the existing `strategy: 'replace'` sink a PZ0335
        // (delivery-guarantees: replace fed by an incremental dataset would discard prior rows) --
        // switch to the append + explicit-consent surface that combination requires (PZ0214's escape
        // hatch), matching this project's own delivery-guarantees rules rather than fighting them.
        p.WritePipeline("orders_out",
            "INSERT INTO {{ sink('out', 'orders_out', format: 'csv', strategy: 'append', duplicates: 'accept') }}\n" +
            "select * from {{ ref('stg_orders') }}\n");

        var doc = JsonDocument.Parse(await ExecutionTools.RunAsync(
            p.Dir, flowNames: ["orders_out"], all: false, fullRefresh: false, RealServices(), CancellationToken.None));

        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
        var notices = doc.RootElement.GetProperty("result").GetProperty("notices");
        Assert.True(notices.GetArrayLength() > 0);
        Assert.Contains(notices.EnumerateArray(), n => n.GetString()!.Contains("unverified until"));
        // warnings is always present (possibly empty) matching pz_compile's own result shape.
        Assert.True(doc.RootElement.GetProperty("result").TryGetProperty("warnings", out _));
    }

    [Fact]
    public async Task Run_while_lock_held_returns_pz0604()
    {
        using var p = new TempProject();
        var runDir = Path.Combine(p.Dir, ".pz", "runs", "fake");
        using var held = Pz.Engine.Execution.RunDirLock.Acquire(runDir);

        var doc = JsonDocument.Parse(await ExecutionTools.RunAsync(
            p.Dir, ["orders_out"], false, false, RealServices(), CancellationToken.None));

        Assert.False(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.Equal("PZ0604", doc.RootElement.GetProperty("errors")[0].GetProperty("code").GetString());
    }

    [Fact]
    public async Task Retry_with_no_prior_run_is_enveloped_as_no_prior_run()
    {
        using var p = new TempProject();

        var doc = JsonDocument.Parse(await ExecutionTools.RetryAsync(
            p.Dir, fullRefresh: false, RealServices(), CancellationToken.None));

        Assert.False(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.Equal("PZ0502", doc.RootElement.GetProperty("errors")[0].GetProperty("code").GetString());
    }

    [Fact]
    public async Task Retry_after_a_successful_run_reports_nothing_to_retry()
    {
        using var p = new TempProject();
        var runDoc = JsonDocument.Parse(await ExecutionTools.RunAsync(
            p.Dir, ["orders_out"], false, false, RealServices(), CancellationToken.None));
        Assert.True(runDoc.RootElement.GetProperty("ok").GetBoolean());

        var doc = JsonDocument.Parse(await ExecutionTools.RetryAsync(
            p.Dir, fullRefresh: false, RealServices(), CancellationToken.None));

        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.Contains("nothing to retry", doc.RootElement.GetProperty("result").GetProperty("note").GetString());
    }

    [Fact]
    public async Task Retry_carries_the_changed_node_notices_the_cli_prints()
    {
        // `pz retry` prints "note: <node> changed since the failed run" per stale node. They are the
        // entire explanation for a "nothing to retry (project changed)" outcome, so the MCP adapter
        // must not discard them -- they ride the envelope's notices[].
        using var p = new TempProject();
        // Contract-less source => SqlDryCompiler skips the dependent pipeline (see
        // Run_with_broken_pipeline_sql_is_refused... for the contrasting contract case), so this bad
        // column survives the tier-4 pre-flight and fails at execution -- giving a failed prior run.
        p.WritePipeline("stg_orders", "select id, no_such_column\nfrom {{ source('raw', 'orders') }}\n");
        var runDoc = JsonDocument.Parse(await ExecutionTools.RunAsync(
            p.Dir, ["orders_out"], false, false, RealServices(), CancellationToken.None));
        Assert.NotEqual(0, runDoc.RootElement.GetProperty("result").GetProperty("exit_code").GetInt32());

        // Editing the failed pipeline re-hashes its content-addressed node id, so retry drops it with
        // the changed-since-the-failed-run note rather than re-running it.
        p.WritePipeline("stg_orders", "select id, still_no_such_column\nfrom {{ source('raw', 'orders') }}\n");

        var doc = JsonDocument.Parse(await ExecutionTools.RetryAsync(
            p.Dir, fullRefresh: false, RealServices(), CancellationToken.None));

        var notices = doc.RootElement.GetProperty("result").GetProperty("notices");
        Assert.Contains(notices.EnumerateArray(), n => n.GetString()!.Contains("changed since the failed run"));
    }

    [Fact]
    public async Task RunResults_with_no_id_reads_the_latest_run()
    {
        using var p = new TempProject();
        var runDoc = JsonDocument.Parse(await ExecutionTools.RunAsync(
            p.Dir, ["orders_out"], false, false, RealServices(), CancellationToken.None));
        Assert.True(runDoc.RootElement.GetProperty("ok").GetBoolean());
        var runId = runDoc.RootElement.GetProperty("result").GetProperty("run_id").GetString();

        var doc = JsonDocument.Parse(ExecutionTools.RunResults(p.Dir, null, RealServices()));

        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.Equal(runId, doc.RootElement.GetProperty("result").GetProperty("run_id").GetString());
        var nodes = doc.RootElement.GetProperty("result").GetProperty("nodes");
        Assert.True(nodes.GetArrayLength() > 0);
        Assert.Contains(
            nodes.EnumerateArray(), n => n.GetProperty("status").GetString() == "success");
    }

    [Fact]
    public async Task RunResults_by_explicit_id_finds_that_run_on_the_local_store()
    {
        using var p = new TempProject();
        var runDoc = JsonDocument.Parse(await ExecutionTools.RunAsync(
            p.Dir, ["orders_out"], false, false, RealServices(), CancellationToken.None));
        var runId = runDoc.RootElement.GetProperty("result").GetProperty("run_id").GetString()!;

        var doc = JsonDocument.Parse(ExecutionTools.RunResults(p.Dir, runId, RealServices()));

        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.Equal(runId, doc.RootElement.GetProperty("result").GetProperty("run_id").GetString());
    }

    [Fact]
    public void RunResults_for_an_unknown_id_is_an_enveloped_error()
    {
        using var p = new TempProject();

        var doc = JsonDocument.Parse(ExecutionTools.RunResults(p.Dir, "no-such-run", RealServices()));

        Assert.False(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.Equal("PZ0502", doc.RootElement.GetProperty("errors")[0].GetProperty("code").GetString());
    }
}
