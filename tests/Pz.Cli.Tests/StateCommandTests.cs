namespace Pz.Cli.Tests;

/// <summary>The `pz state` verb end to end. Entirely offline — `pz state` loads no
/// project, opens no connectors, and runs none of the eight phases, so these need no fixture beyond a
/// project.yml plus fabricated state files and run dirs.
///
/// Joins "console-and-env-serialized" (defined in RestoreCommandTests.cs) because it redirects the
/// process-global Console.Out/Error and would otherwise race the other classes that do.</summary>
[Collection("console-and-env-serialized")]
public sealed class StateCommandTests : IDisposable
{
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-state-cli-tests", Guid.NewGuid().ToString("N"));

    public StateCommandTests()
    {
        Directory.CreateDirectory(_work);
        File.WriteAllText(Path.Combine(_work, "project.yml"), "name: state_test\nversion: 1\n");
        Directory.CreateDirectory(StateDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private string StateDir => Path.Combine(_work, ".pz", "state");

    private void WriteWatermarks(string body) =>
        File.WriteAllText(Path.Combine(StateDir, "watermarks.json"), body);

    /// <summary>The real byte shape KeyedJsonStateStore writes, so these tests exercise the parser rather
    /// than a convenient fiction.</summary>
    private void WriteOneWatermark(string key = "erp.orders", string cursor = "updated_at",
        string type = "timestamp", string value = "2026-07-29T02:00:00.000000",
        string runId = "20260729T020013422Z-a91c") =>
        WriteWatermarks($$"""
            {
              "version": 1,
              "watermarks": {
                "{{key}}": {
                  "cursor": "{{cursor}}",
                  "type": "{{type}}",
                  "value": "{{value}}",
                  "runId": "{{runId}}"
                }
              }
            }
            """);

    private void MakeRun(string runId, string status, string nodeName, string value,
        string cursor = "updated_at", string type = "timestamp")
    {
        var dir = Path.Combine(_work, ".pz", "runs", runId);
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "run_results.json"), $$"""
            {
              "status": "{{status}}",
              "nodes": [
                { "id": "n1", "kind": "SourceLoad", "name": "{{nodeName}}", "status": "success",
                  "watermark": { "cursor": "{{cursor}}", "type": "{{type}}", "value": "{{value}}" } }
              ]
            }
            """);
    }

    [Fact]
    public void Show_lists_every_watermark_with_its_cursor_type_and_run()
    {
        WriteOneWatermark();

        var stdout = CaptureOut(() => CliApp.Build().Parse(["state", "show", "--project", _work]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Contains("erp.orders", stdout);
        Assert.Contains("updated_at", stdout);
        Assert.Contains("timestamp", stdout);
        Assert.Contains("2026-07-29T02:00:00.000000", stdout);
        Assert.Contains("20260729T020013422Z-a91c", stdout);
    }

    [Fact]
    public void Show_reports_sync_state_in_its_own_section()
    {
        File.WriteAllText(Path.Combine(StateDir, "sync-state.json"), """
            {
              "version": 1,
              "syncState": {
                "erp.shipments": { "token": "0/1A2B3C4D", "runId": "20260729T020013422Z-a91c" }
              }
            }
            """);

        var stdout = CaptureOut(() => CliApp.Build().Parse(["state", "show", "--project", _work]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Contains("sync-state", stdout);
        Assert.Contains("erp.shipments", stdout);
        Assert.Contains("0/1A2B3C4D", stdout);
    }

    [Fact]
    public void Show_with_no_state_at_all_says_so_and_exits_ok()
    {
        var stdout = CaptureOut(() => CliApp.Build().Parse(["state", "show", "--project", _work]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Contains("no watermark state", stdout);
    }

    [Fact]
    public void Show_flags_a_corrupt_state_file_and_exits_one()
    {
        WriteWatermarks("{ not json");

        var stderr = CaptureErr(() => CliApp.Build().Parse(["state", "show", "--project", _work]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.NodeFailures, exit);
        Assert.Contains("corrupt", stderr);
    }

    [Fact]
    public void Show_all_exits_one_when_only_sync_state_is_corrupt()
    {
        WriteOneWatermark();
        File.WriteAllText(Path.Combine(StateDir, "sync-state.json"), "{ not json");

        var stderr = CaptureErr(() => CliApp.Build().Parse(["state", "show", "--project", _work]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.NodeFailures, exit);
        Assert.Contains("sync-state.json", stderr);
    }

    [Fact]
    public void Show_of_one_key_on_a_corrupt_file_reports_corruption_not_PZ0513()
    {
        WriteWatermarks("{ not json");

        var stderr = CaptureErr(
            () => CliApp.Build().Parse(["state", "show", "erp.orders", "--project", _work]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.NodeFailures, exit);
        Assert.DoesNotContain("PZ0513", stderr);
    }

    [Fact]
    public void Show_of_one_key_lists_its_run_history_newest_first()
    {
        WriteOneWatermark();
        MakeRun("20260727T020009111Z-3f2e", "completed_with_failures", "src_erp__orders", "2026-07-27T01:59:00.000000");
        MakeRun("20260729T020013422Z-a91c", "success", "src_erp__orders", "2026-07-29T02:00:00.000000");

        var stdout = CaptureOut(
            () => CliApp.Build().Parse(["state", "show", "erp.orders", "--project", _work]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Contains("history", stdout);
        Assert.True(
            stdout.IndexOf("20260729T020013422Z-a91c", StringComparison.Ordinal)
            < stdout.IndexOf("20260727T020009111Z-3f2e", StringComparison.Ordinal),
            "history must be newest-first");
        Assert.Contains("completed_with_failures", stdout);
    }

    [Fact]
    public void Show_of_an_unknown_key_is_refused_with_PZ0513()
    {
        WriteOneWatermark();

        var stderr = CaptureErr(
            () => CliApp.Build().Parse(["state", "show", "nope.nothing", "--project", _work]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0513", stderr);
    }

    [Fact]
    public void Show_flags_an_unknown_cursor_type_rather_than_crashing()
    {
        // The one entry state WindowMath cannot touch at all. Reaching an unhandled throw
        // here would be exit 3 on the repair tool.
        WriteOneWatermark(type: "weirdtype", value: "whatever");

        var stdout = CaptureOut(
            () => CliApp.Build().Parse(["state", "show", "erp.orders", "--project", _work]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Contains("unknown type", stdout);
    }

    [Fact]
    public void Show_outside_a_project_is_refused()
    {
        var empty = Path.Combine(_work, "not-a-project");
        Directory.CreateDirectory(empty);

        var stderr = CaptureErr(
            () => CliApp.Build().Parse(["state", "show", "--project", empty]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("project.yml", stderr);
    }

    // ---- set ----

    private sealed class FixedClock(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }

    private static readonly FixedClock Clock = new(DateTimeOffset.Parse("2026-07-30T14:22:05.123Z"));

    private string AuditPath => Path.Combine(StateDir, "audit.jsonl");

    private string StoredValue(string key = "erp.orders") =>
        Pz.Engine.State.WatermarkStore.Local(StateDir).Get(key)!.Value;

    [Fact]
    public void Set_writes_the_canonical_value_and_the_manual_marker()
    {
        WriteOneWatermark();

        var stdout = CaptureOut(() => Commands.StateCommand.Write(
            _work, Pz.Engine.State.StateEditAction.Set, "erp.orders", null, "2026-07-01",
            "late-arriving rows", dryRun: false, yes: true, Clock), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        var stored = Pz.Engine.State.WatermarkStore.Local(StateDir).Get("erp.orders")!;
        Assert.Equal("2026-07-01T00:00:00.000000", stored.Value);
        Assert.Equal("manual", stored.RunId);
        Assert.Equal("updated_at", stored.Cursor);
        Assert.Contains("re-extracts", stdout);
    }

    [Fact]
    public void Set_appends_one_audit_line_carrying_the_replaced_value()
    {
        WriteOneWatermark();

        Commands.StateCommand.Write(_work, Pz.Engine.State.StateEditAction.Set, "erp.orders", null,
            "2026-07-01", "late-arriving rows", dryRun: false, yes: true, Clock);

        var line = Assert.Single(File.ReadAllLines(AuditPath));
        Assert.Contains("\"ts\":\"2026-07-30T14:22:05.123Z\"", line);
        Assert.Contains("\"action\":\"set\"", line);
        Assert.Contains("\"from\":\"2026-07-29T02:00:00.000000\"", line);
        Assert.Contains("\"fromRunId\":\"20260729T020013422Z-a91c\"", line);
        Assert.Contains("\"to\":\"2026-07-01T00:00:00.000000\"", line);
        Assert.Contains("\"target\":\"value\"", line);
        Assert.Contains("\"reason\":\"late-arriving rows\"", line);
    }

    [Fact]
    public void Set_forward_says_rows_will_be_skipped_not_re_extracted()
    {
        WriteOneWatermark(cursor: "id", type: "bigint", value: "500");

        var stdout = CaptureOut(() => Commands.StateCommand.Write(
            _work, Pz.Engine.State.StateEditAction.Set, "erp.orders", null, "9000", null,
            dryRun: false, yes: true, Clock), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Contains("skips", stdout);
        Assert.DoesNotContain("re-extracts", stdout);
    }

    [Fact]
    public void Set_dry_run_writes_nothing_at_all()
    {
        WriteOneWatermark();
        var before = File.ReadAllBytes(Path.Combine(StateDir, "watermarks.json"));

        var stdout = CaptureOut(() => Commands.StateCommand.Write(
            _work, Pz.Engine.State.StateEditAction.Set, "erp.orders", null, "2026-07-01", null,
            dryRun: true, yes: true, Clock), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Equal(before, File.ReadAllBytes(Path.Combine(StateDir, "watermarks.json")));
        Assert.False(File.Exists(AuditPath));
        Assert.Contains("dry-run", stdout);
    }

    [Fact]
    public void Set_refuses_while_a_run_holds_its_lock()
    {
        WriteOneWatermark();
        var runDir = Path.Combine(_work, ".pz", "runs", "20260730T140000000Z-aaaa");
        Directory.CreateDirectory(runDir);
        using var heldLock = Pz.Engine.Execution.RunDirLock.Acquire(runDir);

        var stderr = CaptureErr(() => Commands.StateCommand.Write(
            _work, Pz.Engine.State.StateEditAction.Set, "erp.orders", null, "2026-07-01", null,
            dryRun: false, yes: true, Clock), out var exit);

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0517", stderr);
        Assert.Contains("20260730T140000000Z-aaaa", stderr);
        Assert.Equal("2026-07-29T02:00:00.000000", StoredValue());
    }

    [Fact]
    public void Set_refuses_when_a_run_starts_during_the_confirmation_prompt()
    {
        // The race this guards: the first LiveRunId probe passes, the report prints, and the operator
        // is sitting at the prompt when a scheduled run starts. Acquiring the lock inside the confirm
        // callback reproduces that window deterministically, with no sleep.
        WriteOneWatermark();
        var runDir = Path.Combine(_work, ".pz", "runs", "20260730T140000000Z-bbbb");
        Directory.CreateDirectory(runDir);
        Pz.Engine.Execution.RunDirLock? heldLock = null;

        try
        {
            var stderr = CaptureErr(() => Commands.StateCommand.Write(
                _work, Pz.Engine.State.StateEditAction.Set, "erp.orders", null, "2026-07-01", null,
                dryRun: false, yes: false, Clock, isInteractive: () => true, confirm: () =>
                {
                    heldLock = Pz.Engine.Execution.RunDirLock.Acquire(runDir);
                    return true;
                }), out var exit);

            Assert.Equal(ExitCodes.ConfigError, exit);
            Assert.Contains("PZ0517", stderr);
            Assert.Contains("20260730T140000000Z-bbbb", stderr);
            Assert.Equal("2026-07-29T02:00:00.000000", StoredValue());
            Assert.False(File.Exists(AuditPath));
        }
        finally
        {
            heldLock?.Dispose();
        }
    }

    [Fact]
    public void Set_succeeds_once_the_lock_is_released()
    {
        WriteOneWatermark();
        var runDir = Path.Combine(_work, ".pz", "runs", "20260730T140000000Z-aaaa");
        Directory.CreateDirectory(runDir);
        Pz.Engine.Execution.RunDirLock.Acquire(runDir).Dispose();

        var exitCode = Commands.StateCommand.Write(_work, Pz.Engine.State.StateEditAction.Set, "erp.orders",
            null, "2026-07-01", null, dryRun: false, yes: true, Clock);

        Assert.Equal(ExitCodes.Ok, exitCode);
        Assert.Equal("2026-07-01T00:00:00.000000", StoredValue());
    }

    [Fact]
    public void Set_without_yes_off_a_tty_is_refused_with_PZ0516()
    {
        WriteOneWatermark();

        var stderr = CaptureErr(() => Commands.StateCommand.Write(
            _work, Pz.Engine.State.StateEditAction.Set, "erp.orders", null, "2026-07-01", null,
            dryRun: false, yes: false, Clock, isInteractive: () => false), out var exit);

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0516", stderr);
        Assert.Contains("--yes", stderr);
        Assert.Equal("2026-07-29T02:00:00.000000", StoredValue());
    }

    [Fact]
    public void Set_declined_at_the_prompt_changes_nothing_and_exits_ok()
    {
        WriteOneWatermark();

        var exitCode = Commands.StateCommand.Write(_work, Pz.Engine.State.StateEditAction.Set, "erp.orders",
            null, "2026-07-01", null, dryRun: false, yes: false, Clock,
            isInteractive: () => true, confirm: () => false);

        Assert.Equal(ExitCodes.Ok, exitCode);
        Assert.Equal("2026-07-29T02:00:00.000000", StoredValue());
        Assert.False(File.Exists(AuditPath));
    }

    [Fact]
    public void Set_confirmed_at_the_prompt_writes()
    {
        WriteOneWatermark();

        var exitCode = Commands.StateCommand.Write(_work, Pz.Engine.State.StateEditAction.Set, "erp.orders",
            null, "2026-07-01", null, dryRun: false, yes: false, Clock,
            isInteractive: () => true, confirm: () => true);

        Assert.Equal(ExitCodes.Ok, exitCode);
        Assert.Equal("2026-07-01T00:00:00.000000", StoredValue());
    }

    [Fact]
    public void Set_of_a_missing_entry_is_refused_with_PZ0513()
    {
        var stderr = CaptureErr(() => Commands.StateCommand.Write(
            _work, Pz.Engine.State.StateEditAction.Set, "nope.nothing", null, "5", null,
            dryRun: false, yes: true, Clock), out var exit);

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0513", stderr);
    }

    [Fact]
    public void Set_without_a_value_is_refused_with_PZ0516()
    {
        WriteOneWatermark();

        var stderr = CaptureErr(
            () => CliApp.Build().Parse(["state", "set", "erp.orders", "--project", _work, "--yes"]).Invoke(),
            out var exit);

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0516", stderr);
    }

    [Fact]
    public void Set_never_touches_sync_state_runs_target_or_packages()
    {
        WriteOneWatermark();
        File.WriteAllText(Path.Combine(StateDir, "sync-state.json"), """
            { "version": 1, "syncState": { "erp.shipments": { "token": "t", "runId": "r" } } }
            """);
        MakeRun("20260729T020013422Z-a91c", "success", "src_erp__orders", "2026-07-29T02:00:00.000000");
        Directory.CreateDirectory(Path.Combine(_work, ".pz", "target"));
        File.WriteAllText(Path.Combine(_work, ".pz", "target", "manifest.json"), "{}");
        Directory.CreateDirectory(Path.Combine(_work, ".pz", "packages"));
        File.WriteAllText(Path.Combine(_work, ".pz", "packages", "marker"), "x");

        var sync = File.ReadAllBytes(Path.Combine(StateDir, "sync-state.json"));
        var run = File.ReadAllBytes(Path.Combine(_work, ".pz", "runs", "20260729T020013422Z-a91c", "run_results.json"));
        var target = File.ReadAllBytes(Path.Combine(_work, ".pz", "target", "manifest.json"));
        var packages = File.ReadAllBytes(Path.Combine(_work, ".pz", "packages", "marker"));

        Commands.StateCommand.Write(_work, Pz.Engine.State.StateEditAction.Set, "erp.orders", null,
            "2026-07-01", null, dryRun: false, yes: true, Clock);

        Assert.Equal(sync, File.ReadAllBytes(Path.Combine(StateDir, "sync-state.json")));
        Assert.Equal(run, File.ReadAllBytes(Path.Combine(_work, ".pz", "runs", "20260729T020013422Z-a91c", "run_results.json")));
        Assert.Equal(target, File.ReadAllBytes(Path.Combine(_work, ".pz", "target", "manifest.json")));
        Assert.Equal(packages, File.ReadAllBytes(Path.Combine(_work, ".pz", "packages", "marker")));
    }

    [Fact]
    public void Set_on_a_non_canonical_stored_value_reports_unknown_direction()
    {
        // "0500" has a known type (bigint) but is not the canonical form ("500") WindowMath produces, so
        // Compare cannot be called on it — the consequence sentence must say the direction is unknown
        // rather than guessing (and must NOT print the backward sentence's duplication warning).
        WriteOneWatermark(cursor: "id", type: "bigint", value: "0500");

        var stdout = CaptureOut(() => Commands.StateCommand.Write(
            _work, Pz.Engine.State.StateEditAction.Set, "erp.orders", null, "9000", null,
            dryRun: false, yes: true, Clock), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Contains("cannot compare", stdout);
        Assert.DoesNotContain("duplicates those rows", stdout);
    }

    [Fact]
    public void Set_warns_loudly_and_exits_one_when_the_ledger_append_fails()
    {
        WriteOneWatermark();
        // Held with FileShare.None -- the same cross-platform pattern RunDirLock uses -- so
        // StateAudit.Append's File.AppendAllText fails on both ubuntu and windows CI.
        using var held = new FileStream(AuditPath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None);

        var stderr = CaptureErr(() => Commands.StateCommand.Write(
            _work, Pz.Engine.State.StateEditAction.Set, "erp.orders", null, "2026-07-01", null,
            dryRun: false, yes: true, Clock), out var exit);

        Assert.Equal(ExitCodes.NodeFailures, exit);
        Assert.Equal("2026-07-01T00:00:00.000000", StoredValue());
        Assert.Contains("warning:", stderr);
        Assert.Contains("\"action\":\"set\"", stderr);
    }

    // ---- rollback ----

    [Fact]
    public void Rollback_takes_the_value_the_named_run_advanced_to()
    {
        WriteOneWatermark();
        MakeRun("20260727T020009111Z-3f2e", "success", "src_erp__orders", "2026-07-27T01:59:00.000000");
        MakeRun("20260729T020013422Z-a91c", "success", "src_erp__orders", "2026-07-29T02:00:00.000000");

        var stdout = CaptureOut(() => CliApp.Build().Parse([
            "state", "rollback", "erp.orders", "--to-run", "20260727T020009111Z-3f2e",
            "--project", _work, "--yes", "--reason", "late rows"]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Equal("2026-07-27T01:59:00.000000", StoredValue());
        Assert.Equal("manual", Pz.Engine.State.WatermarkStore.Local(StateDir).Get("erp.orders")!.RunId);
        Assert.Contains("re-extracts", stdout);

        var line = Assert.Single(File.ReadAllLines(AuditPath));
        Assert.Contains("\"action\":\"rollback\"", line);
        Assert.Contains("\"target\":\"run:20260727T020009111Z-3f2e\"", line);
        Assert.Contains("\"reason\":\"late rows\"", line);
    }

    [Fact]
    public void Rollback_to_a_purged_run_is_refused_with_PZ0514()
    {
        WriteOneWatermark();
        MakeRun("20260729T020013422Z-a91c", "success", "src_erp__orders", "2026-07-29T02:00:00.000000");

        var stderr = CaptureErr(() => CliApp.Build().Parse([
            "state", "rollback", "erp.orders", "--to-run", "20260101T000000000Z-gone",
            "--project", _work, "--yes"]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0514", stderr);
        Assert.Equal("2026-07-29T02:00:00.000000", StoredValue());
    }

    [Fact]
    public void Rollback_forward_is_refused_and_names_set()
    {
        WriteOneWatermark(cursor: "id", type: "bigint", value: "500");
        MakeRun("20260729T020013422Z-a91c", "success", "src_erp__orders", "9000", cursor: "id", type: "bigint");

        var stderr = CaptureErr(() => CliApp.Build().Parse([
            "state", "rollback", "erp.orders", "--to-run", "20260729T020013422Z-a91c",
            "--project", _work, "--yes"]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0515", stderr);
        Assert.Contains("pz state set", stderr);
        Assert.Equal("500", StoredValue());
    }

    [Fact]
    public void Rollback_to_a_non_success_run_warns_and_proceeds()
    {
        WriteOneWatermark();
        MakeRun("20260727T020009111Z-3f2e", "completed_with_failures", "src_erp__orders", "2026-07-27T01:59:00.000000");

        var stdout = CaptureOut(() => CliApp.Build().Parse([
            "state", "rollback", "erp.orders", "--to-run", "20260727T020009111Z-3f2e",
            "--project", _work, "--yes"]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Contains("did not fully succeed", stdout);
        Assert.Equal("2026-07-27T01:59:00.000000", StoredValue());
    }

    [Fact]
    public void Rollback_of_a_dotted_dataset_name_resolves_the_folded_node_name()
    {
        WriteOneWatermark(key: "erp.dbo.orders");
        MakeRun("20260727T020009111Z-3f2e", "success", "src_erp__dbo_orders", "2026-07-27T01:59:00.000000");

        var exitCode = CliApp.Build().Parse([
            "state", "rollback", "erp.dbo.orders", "--to-run", "20260727T020009111Z-3f2e",
            "--project", _work, "--yes"]).Invoke();

        Assert.Equal(ExitCodes.Ok, exitCode);
        Assert.Equal("2026-07-27T01:59:00.000000", StoredValue("erp.dbo.orders"));
    }

    [Fact]
    public void Rollback_without_to_run_is_refused_with_PZ0516()
    {
        WriteOneWatermark();

        var stderr = CaptureErr(
            () => CliApp.Build().Parse(["state", "rollback", "erp.orders", "--project", _work, "--yes"]).Invoke(),
            out var exit);

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0516", stderr);
    }

    [Fact]
    public void Rollback_dry_run_writes_nothing()
    {
        WriteOneWatermark();
        MakeRun("20260727T020009111Z-3f2e", "success", "src_erp__orders", "2026-07-27T01:59:00.000000");
        var before = File.ReadAllBytes(Path.Combine(StateDir, "watermarks.json"));

        var stdout = CaptureOut(() => CliApp.Build().Parse([
            "state", "rollback", "erp.orders", "--to-run", "20260727T020009111Z-3f2e",
            "--project", _work, "--dry-run"]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Equal(before, File.ReadAllBytes(Path.Combine(StateDir, "watermarks.json")));
        Assert.False(File.Exists(AuditPath));
        Assert.Contains("dry-run", stdout);
    }

    // ---- clear ----

    [Fact]
    public void Clear_removes_the_entry_and_records_it()
    {
        WriteOneWatermark();

        var stdout = CaptureOut(() => CliApp.Build().Parse([
            "state", "clear", "erp.orders", "--project", _work, "--yes", "--reason", "corrupt entry"]).Invoke(),
            out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Null(Pz.Engine.State.WatermarkStore.Local(StateDir).Get("erp.orders"));
        Assert.Contains("IN FULL", stdout);

        var line = Assert.Single(File.ReadAllLines(AuditPath));
        Assert.Contains("\"action\":\"clear\"", line);
        Assert.Contains("\"from\":\"2026-07-29T02:00:00.000000\"", line);
        Assert.DoesNotContain("\"to\"", line);
        Assert.Contains("\"reason\":\"corrupt entry\"", line);
    }

    [Fact]
    public void Clear_works_on_an_unknown_cursor_type_which_nothing_else_can_touch()
    {
        WriteOneWatermark(type: "weirdtype", value: "whatever");

        var exitCode = CliApp.Build().Parse(["state", "clear", "erp.orders", "--project", _work, "--yes"]).Invoke();

        Assert.Equal(ExitCodes.Ok, exitCode);
        Assert.Null(Pz.Engine.State.WatermarkStore.Local(StateDir).Get("erp.orders"));
    }

    [Fact]
    public void Set_on_that_same_unknown_type_is_refused_and_points_at_clear()
    {
        WriteOneWatermark(type: "weirdtype", value: "whatever");

        var stderr = CaptureErr(() => CliApp.Build().Parse([
            "state", "set", "erp.orders", "--value", "5", "--project", _work, "--yes"]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0515", stderr);
        Assert.Contains("pz state clear", stderr);
    }

    [Fact]
    public void Clear_leaves_the_other_entries_alone()
    {
        WriteWatermarks("""
            {
              "version": 1,
              "watermarks": {
                "erp.orders": { "cursor": "updated_at", "type": "timestamp", "value": "2026-07-29T02:00:00.000000", "runId": "r1" },
                "erp.customers": { "cursor": "id", "type": "bigint", "value": "4820193", "runId": "r1" }
              }
            }
            """);

        CliApp.Build().Parse(["state", "clear", "erp.orders", "--project", _work, "--yes"]).Invoke();

        var store = Pz.Engine.State.WatermarkStore.Local(StateDir);
        Assert.Null(store.Get("erp.orders"));
        Assert.Equal("4820193", store.Get("erp.customers")!.Value);
    }

    [Fact]
    public void Clear_of_a_missing_entry_is_refused_with_PZ0513()
    {
        var stderr = CaptureErr(
            () => CliApp.Build().Parse(["state", "clear", "nope.nothing", "--project", _work, "--yes"]).Invoke(),
            out var exit);

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0513", stderr);
    }

    [Fact]
    public void Clear_dry_run_writes_nothing()
    {
        WriteOneWatermark();
        var before = File.ReadAllBytes(Path.Combine(StateDir, "watermarks.json"));

        CliApp.Build().Parse(["state", "clear", "erp.orders", "--project", _work, "--dry-run"]).Invoke();

        Assert.Equal(before, File.ReadAllBytes(Path.Combine(StateDir, "watermarks.json")));
        Assert.False(File.Exists(AuditPath));
    }

    /// <summary>The "no project load, no connectors, no network" promise holds for `backend: local`. A
    /// connections.yml that does not parse is exactly the situation an operator reaches for `pz state`
    /// in -- with no `state:` block and no PZ_STATE_*, that file is never read, so it cannot refuse.</summary>
    [Fact]
    public void Show_works_with_a_connections_file_that_does_not_parse()
    {
        File.WriteAllText(Path.Combine(_work, "connections.yml"), "files:\n  connector: localfiles\n   bad: [\n");
        WriteOneWatermark();

        var stdout = CaptureOut(() => CliApp.Build().Parse(["state", "show", "--project", _work]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Contains("erp.orders", stdout);
    }

    /// <summary>The write path resolves the backend through the same narrow loader, so it inherits the
    /// same property -- a broken connections.yml must not block repairing a watermark either.</summary>
    [Fact]
    public void Set_works_with_a_connections_file_that_does_not_parse()
    {
        File.WriteAllText(Path.Combine(_work, "connections.yml"), "files:\n  connector: localfiles\n   bad: [\n");
        WriteOneWatermark();

        var exit = CliApp.Build().Parse(
            ["state", "set", "erp.orders", "--value", "2026-07-30T00:00:00.000000", "--project", _work, "--yes"]).Invoke();

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Equal("2026-07-30T00:00:00.000000",
            Pz.Engine.State.WatermarkStore.Local(StateDir).Get("erp.orders")!.Value);
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

    private static string CaptureErr(Func<int> action, out int exit)
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
}
