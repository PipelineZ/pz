using System.Collections;
using Pz.Cli;
using Pz.Cli.Commands;
using Pz.Cli.Rendering;
using Pz.Core.Dag;
using Pz.Core.Loading;
using Pz.Core.Templating;
using Pz.Core.Validation;
using Pz.Diagnostics.Events;
using Pz.Engine.Artifacts;
using Pz.Engine.Execution;
using Pz.Engine.Validation;

namespace Pz.Cli.Tests;

/// <summary>Coverage for `pz run`: (1) engine.threads &lt;= 0 is a load-time PZ0120
/// config error, never a crash; (2) a snapshot-writer failure inside <see cref="SnapshotRunEvents"/>
/// is loud (one warning) but never fatal to the run.</summary>
// See the "console-and-env-serialized" collection definition in RestoreCommandTests.cs: this class
// redirects Console.Error to assert on CLI output and mutates the process-global DATA_DIR/OUT_DIR env
// vars, both of which must serialize against every other Console/env-swapping class in the assembly.
[Collection("console-and-env-serialized")]
public class RunCommandTests : IDisposable
{
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-run-tests", Guid.NewGuid().ToString("N"));

    public RunCommandTests()
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
    public void Threads_zero_exits_config_error_not_crash()
    {
        File.WriteAllText(Path.Combine(_work, "project.yml"), """
            name: hello_pz
            version: 0.1.0

            connectors:
              - package: Pz.Connector.LocalFiles
                version: 0.1.0
              - package: Pz.Connector.InMemory
                version: 0.1.0

            vars:
              min_amount: 10
              statuses: [shipped, returned]

            engine:
              threads: 0
              duckdb:
                memory_limit: 1GiB
            """);

        var exit = CliApp.Build().Parse(["run", "--project", _work]).Invoke();
        Assert.Equal(ExitCodes.ConfigError, exit);
    }

    /// <summary>`--vars` whose text isn't valid JSON at all must not raise a raw <c>JsonException</c>:
    /// no command handler's `catch (PzValidationException)` would catch it, crashing the process
    /// instead of exiting cleanly. See <see cref="SharedInputHelpers.ParseVars"/>.</summary>
    [Fact]
    public void Vars_non_json_text_is_clean_config_error_not_crash()
    {
        var stderr = new StringWriter();
        var originalErr = Console.Error;
        Console.SetError(stderr);
        int exit;
        try
        {
            exit = CliApp.Build().Parse(["run", "--project", _work, "--vars", "not json"]).Invoke();
        }
        finally
        {
            Console.SetError(originalErr);
        }

        Assert.Equal(ExitCodes.ConfigError, exit);
        var output = stderr.ToString();
        Assert.Contains("PZ0102", output);
        Assert.Contains("--vars must be a JSON object", output);
    }

    /// <summary>`--vars` whose JSON parses fine but whose root isn't an object (e.g. an array) must not
    /// raise a raw <c>InvalidCastException</c>, for the same reason as above.</summary>
    [Fact]
    public void Vars_json_array_root_is_clean_config_error_not_crash()
    {
        var stderr = new StringWriter();
        var originalErr = Console.Error;
        Console.SetError(stderr);
        int exit;
        try
        {
            exit = CliApp.Build().Parse(["run", "--project", _work, "--vars", "[1,2]"]).Invoke();
        }
        finally
        {
            Console.SetError(originalErr);
        }

        Assert.Equal(ExitCodes.ConfigError, exit);
        var output = stderr.ToString();
        Assert.Contains("PZ0102", output);
        Assert.Contains("--vars must be a JSON object", output);
    }

    /// <summary>Tier 4 (implicit pre-run dry-compile): a broken pipeline over a dataset
    /// with a declared `columns:` contract (`crm.customers`: `id`, `email`) must be rejected BEFORE any
    /// real run starts -- no `.pz/runs/` directory at all, not a node failure inside a run.</summary>
    [Fact]
    public void Run_refuses_project_with_broken_sql_pre_execution()
    {
        // PZ0349: a source dataset is read by exactly one pipeline, and hello-pz's crm.customers is
        // already read by orders_enriched. So the probe gets its own dataset -- same
        // CSV, same declared contract -- keeping one reader each. It must read a SOURCE directly: a
        // pipeline over ref() has no offline schema for the dry compiler to typo-check against.
        // It gets its own CONNECTION rather than a second entity under crm because both directions
        // share one file, so appending mid-file is not an option -- and a connection to the same place is
        // exactly what the probe needs.
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
            exit = CliApp.Build().Parse(["run", "--project", _work]).Invoke();
        }
        finally
        {
            Console.SetError(originalErr);
        }

        Assert.Equal(ExitCodes.ConfigError, exit);
        var output = stderr.ToString();
        Assert.Contains("PZ0401", output);
        Assert.Contains("emailx", output);
        Assert.False(Directory.Exists(Path.Combine(_work, ".pz", "runs")));
    }

    /// <summary><see cref="SnapshotRunEvents"/> touches stdout only via the shared warning latch, never
    /// for progress lines -- those are <c>ConsoleRenderer</c>'s job, covered separately in
    /// Rendering/ConsoleRendererTests.cs -- so only the warning latch is asserted here.</summary>
    [Fact]
    public void Snapshot_write_failure_warns_once_and_does_not_stop_further_snapshot_attempts()
    {
        // Least-invasive writer-failure seam: point RunResultsWriter (underneath LocalRunArtifactStore)
        // at a path that is a directory, not a file, so every WriteSnapshot call's File.Move throws —
        // without needing to predict pz run's randomly generated run id from outside the process.
        var paths = new RunPaths(_work, "fixed-run-id");
        Directory.CreateDirectory(paths.RunDir);
        Directory.CreateDirectory(paths.RunResultsPath); // shadow the results file with a directory

        var artifacts = new LocalRunArtifactStore(_work);
        var events = new SnapshotRunEvents(artifacts, "fixed-run-id",
            DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"));

        var stderr = new StringWriter();
        var originalErr = Console.Error;
        Console.SetError(stderr);
        try
        {
            var node1 = MakeResult("load_a", NodeStatus.Success);
            var node2 = MakeResult("load_b", NodeStatus.Success);
            var node3 = MakeResult("load_c", NodeStatus.Failed);

            events.NodeCompleted(node1);
            events.NodeCompleted(node2);
            events.NodeCompleted(node3);
            // Final terminal-status write, same call site RunCommand.ExecuteRun uses — shares the
            // same latch, so it must not print a second warning.
            events.TryWriteSnapshot([node1, node2, node3], "completed_with_failures");
        }
        finally
        {
            Console.SetError(originalErr);
        }

        var stderrText = stderr.ToString();
        var warningCount = CountOccurrences(stderrText, "could not write run_results.json");
        Assert.Equal(1, warningCount);
        Assert.Contains("resume/retry data may be stale", stderrText);
    }

    /// <summary>A renderer whose <c>Render</c> never
    /// returns must never hang `pz run` — <see cref="RunCommand.ExecuteRun"/>'s drain race
    /// (<c>Task.WhenAny(pump.Completion, Task.Delay(drainTimeout))</c>) already bounds this in
    /// production with a 5s timeout; this test proves the race actually works by injecting a
    /// near-instant timeout through the internal <c>drainTimeout</c>/<c>rendererFactory</c> seams added
    /// for exactly this purpose, so the test itself completes in well under 5 seconds instead of
    /// waiting out the real default.</summary>
    [Fact]
    public async Task Stuck_renderer_does_not_hang_the_run()
    {
        // The Fixtures/hello-pz copy in _work is a validation-error-path fixture with no real data
        // files; this test needs a run that actually executes, so it uses samples/hello-pz (copied
        // into the test output as "SamplesHelloPz", same as TestCommandTests) in its own directory.
        var work = Path.Combine(Path.GetTempPath(), "pz-run-tests", Guid.NewGuid().ToString("N"));
        CopyTree(Path.Combine(AppContext.BaseDirectory, "SamplesHelloPz"), work);

        var env = new Dictionary<string, string>();
        foreach (DictionaryEntry entry in Environment.GetEnvironmentVariables())
        {
            if (entry.Key is string key)
            {
                env[key] = entry.Value?.ToString() ?? string.Empty;
            }
        }

        var project = ProjectLoader.Load(work, env, null);
        project = InjectLocalFilesBaseDir(project, work);
        var renderCtx = new RenderContext(project, Guid.NewGuid().ToString("N"), DateTimeOffset.UtcNow) { Env = env };
        var fullDag = Pz.Core.Dag.DagCompiler.Compile(project, renderCtx);
        var selection = RunSelection.Resolve(fullDag, null);

        var stderr = new StringWriter();
        var originalErr = Console.Error;
        Console.SetError(stderr);

        int exit;
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            exit = await RunCommand.ExecuteRun(project, fullDag, work, selection, failFast: false,
                noLockCheck: false, logFormat: "text", CancellationToken.None,
                drainTimeout: TimeSpan.FromMilliseconds(50),
                rendererFactory: () => new NeverReturningRenderer());
        }
        finally
        {
            Console.SetError(originalErr);
            try { Directory.Delete(work, recursive: true); } catch { /* best-effort cleanup */ }
        }

        stopwatch.Stop();

        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(5),
            $"expected the injected drain timeout to bound wall time; the run took {stopwatch.Elapsed}");
        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Equal(1,
            CountOccurrences(stderr.ToString(), "warning: renderer did not finish draining run events within"));
    }

    /// <summary>Golden-adjacent — values are timing-dependent, so shape only, never byte-golden: a
    /// real, stall-free run writes NO `timings` key for Pipeline/Check nodes and
    /// a well-formed two-field `timings` object for SourceLoad/SinkWrite nodes, with `version` still
    /// 1. `engine.force_universal` pins every load/sink to the Arrow-channel (universal) tier — the
    /// sample project would otherwise plan native_scan/native_copy, which bypass the channel entirely
    /// and legitimately carry no timings. `customers` is contract-less in the shipped sample, and
    /// `force_universal` + no contract is a hard error — so this test re-declares a `columns:` contract
    /// on the local COPY's `customers` call site purely so a forced universal run has something to
    /// succeed with; the real sample file is untouched.</summary>
    [Fact]
    public void Run_results_carries_timings_for_loads_and_sinks_only()
    {
        var work = Path.Combine(Path.GetTempPath(), "pz-run-tests", Guid.NewGuid().ToString("N"));
        CopyTree(Path.Combine(AppContext.BaseDirectory, "SamplesHelloPz"), work);
        try
        {
            var projectYml = Path.Combine(work, "project.yml");
            File.WriteAllText(projectYml,
                File.ReadAllText(projectYml).Replace("engine:\n", "engine:\n  force_universal: true\n"));

            var pipelinePath = Path.Combine(work, "pipelines", "orders_enriched.sql");
            var pipelineSql = File.ReadAllText(pipelinePath).Replace(
                "source('crm', 'customers', path: 'data/customers.csv', format: 'csv')",
                "source('crm', 'customers', path: 'data/customers.csv', format: 'csv', " +
                "columns: { id: 'bigint', email: 'varchar' })");
            File.WriteAllText(pipelinePath, pipelineSql);

            var exit = CliApp.Build().Parse(["run", "--project", work]).Invoke();
            Assert.Equal(ExitCodes.Ok, exit);

            var runDir = Directory.GetDirectories(Path.Combine(work, ".pz", "runs")).Single();
            using var doc = System.Text.Json.JsonDocument.Parse(
                File.ReadAllBytes(Path.Combine(runDir, "run_results.json")));
            Assert.Equal(1, doc.RootElement.GetProperty("version").GetInt32());

            var sawChannelNode = false;
            foreach (var node in doc.RootElement.GetProperty("nodes").EnumerateArray())
            {
                var kind = node.GetProperty("kind").GetString();
                var hasTimings = node.TryGetProperty("timings", out var timings);
                if (kind is "SourceLoad" or "SinkWrite")
                {
                    sawChannelNode = true;
                    Assert.True(hasTimings, $"{kind} node '{node.GetProperty("name")}' should carry timings");
                    Assert.True(timings.GetProperty("producerStallMs").GetInt64() >= 0);
                    Assert.True(timings.GetProperty("consumerStallMs").GetInt64() >= 0);
                }
                else
                {
                    Assert.False(hasTimings, $"{kind} node '{node.GetProperty("name")}' must have no timings key");
                }
            }

            Assert.True(sawChannelNode, "expected the sample project to contain at least one load/sink node");
        }
        finally
        {
            try { Directory.Delete(work, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    /// <summary>The terminal run status must be the LAST durable thing a run writes. Stamping
    /// run_results.json terminal when the orchestrator returns puts it seconds ahead of the retention
    /// sweep, a renderer drain of up to <c>RendererDrainTimeout</c>, renderer disposal, and both state
    /// advancements. A process death anywhere in that window (SIGKILL, OOM kill, host eviction) would
    /// then leave a durable artifact reading "success" for a run whose watermarks never advanced, and
    /// `pz retry` would answer "nothing to retry (run … succeeded)" with exit 0 — reported successful
    /// and unrecoverable at once.
    ///
    /// Probed deterministically here — no signals, no sleeps — through the existing <c>rendererFactory</c>
    /// seam: renderer disposal happens inside that same window, strictly before watermark advancement and
    /// the terminal write, so a renderer that reads the artifact from its own <c>Dispose</c> observes
    /// exactly what a crash at that instant would have left on disk. Fixtures/watermark-basic rather than
    /// hello-pz because the ordering only means anything for a dataset that actually writes a
    /// watermark.</summary>
    [Fact]
    public async Task Terminal_run_status_is_written_after_watermark_advancement()
    {
        var work = Path.Combine(Path.GetTempPath(), "pz-run-tests", Guid.NewGuid().ToString("N"));
        CopyTree(Path.Combine(AppContext.BaseDirectory, "Fixtures", "watermark-basic"), work);
        var probe = new FinalizeProbeRenderer(work);
        int exit;
        string? statusAtEnd;
        bool watermarksAtEnd;
        try
        {
            exit = await ExecuteFixtureRun(work, runtimeNotices: null, rendererFactory: () => probe);
            // Sampled before cleanup — the work directory is gone by the time the assertions run.
            statusAtEnd = probe.SampleStatus();
            watermarksAtEnd = probe.SampleWatermarksExist();
        }
        finally
        {
            try { Directory.Delete(work, recursive: true); } catch { /* best-effort cleanup */ }
        }

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.True(probe.Disposed, "the run must dispose the injected renderer — the probe never fired");
        // The crash-window observation: node results already durable, terminal status deliberately not yet.
        Assert.Equal("running", probe.StatusAtDispose);
        Assert.False(probe.WatermarksExistedAtDispose,
            "watermark advancement must still lie ahead at renderer disposal — otherwise this probe is " +
            "no longer observing the finalize window the fix is about");
        // ...and by the time ExecuteRun returns, both have happened, in that order.
        Assert.Equal("success", statusAtEnd);
        Assert.True(watermarksAtEnd);
    }

    /// <summary>Run-time notices must be collectable by a caller that has no console. `pz mcp` parks
    /// Console.Out onto stderr before its stdio transport starts, so a "note: " line
    /// <see cref="RunCommand.ExecuteRun"/> merely printed would be invisible to a connected agent — if
    /// <c>ToolEnvelope</c>'s <c>notices</c> array carried compile notices only, a failed watermark write
    /// (or a corrupt watermark file, which silently re-extracts the whole source) would envelope as
    /// ok:true / status:success / exit_code:0 / notices:[], byte-identical to a clean run.
    ///
    /// Uses the same deterministic persistence failure
    /// <c>WatermarkCliTests.Watermark_persistence_failure_does_not_fail_an_otherwise_successful_run</c>
    /// relies on — a DIRECTORY at the exact path <c>WatermarkStore</c> writes to, so <c>File.Move</c>
    /// onto it throws — rather than real filesystem permissions, which aren't reliably controllable in
    /// CI/sandboxes. Collected entries carry NO "note: " prefix: that prefix belongs to console
    /// rendering, and the envelope's existing compile-notice entries are bare text too.</summary>
    [Fact]
    public async Task Run_time_notices_are_collected_for_callers_that_have_no_console()
    {
        var work = Path.Combine(Path.GetTempPath(), "pz-run-tests", Guid.NewGuid().ToString("N"));
        CopyTree(Path.Combine(AppContext.BaseDirectory, "Fixtures", "watermark-basic"), work);
        Directory.CreateDirectory(Path.Combine(work, ".pz", "state", "watermarks.json"));

        var notices = new List<string>();
        var stdout = new StringWriter();
        var originalOut = Console.Out;
        Console.SetOut(stdout);
        int exit;
        try
        {
            exit = await ExecuteFixtureRun(work, notices, rendererFactory: null);
        }
        finally
        {
            Console.SetOut(originalOut);
            try { Directory.Delete(work, recursive: true); } catch { /* best-effort cleanup */ }
        }

        // The persist failure stays advisory: it must not flip an otherwise-successful run (B3's rule).
        Assert.Equal(ExitCodes.Ok, exit);
        var collected = Assert.Single(notices, n => n.StartsWith("could not persist watermarks", StringComparison.Ordinal));
        Assert.Contains("the next run will re-extract from the previous watermark", collected);
        Assert.DoesNotContain("note: ", collected, StringComparison.Ordinal);
        // The console line is unchanged for CLI users — the seam adds a second destination, not a move.
        Assert.Contains("note: could not persist watermarks", stdout.ToString(), StringComparison.Ordinal);
    }

    /// <summary>Shared driver for the two finalize tests above: the same load → inject base_dir → compile
    /// → resolve selection → <see cref="RunCommand.ExecuteRun"/> sequence
    /// <see cref="Stuck_renderer_does_not_hang_the_run"/> performs, factored out because both new tests
    /// need the internal seams (<c>runtimeNotices</c>, <c>rendererFactory</c>) rather than the
    /// <see cref="CliApp"/> surface.</summary>
    private static async Task<int> ExecuteFixtureRun(
        string work, ICollection<string>? runtimeNotices, Func<IEventRenderer>? rendererFactory)
    {
        var env = new Dictionary<string, string>();
        foreach (DictionaryEntry entry in Environment.GetEnvironmentVariables())
        {
            if (entry.Key is string key)
            {
                env[key] = entry.Value?.ToString() ?? string.Empty;
            }
        }

        var project = ProjectLoader.Load(work, env, null);
        project = InjectLocalFilesBaseDir(project, work);
        var renderCtx = new RenderContext(project, Guid.NewGuid().ToString("N"), DateTimeOffset.UtcNow) { Env = env };
        var fullDag = DagCompiler.Compile(project, renderCtx);
        var selection = RunSelection.Resolve(fullDag, null);

        return await RunCommand.ExecuteRun(project, fullDag, work, selection, failFast: false,
            noLockCheck: false, logFormat: "text", CancellationToken.None,
            rendererFactory: rendererFactory, runtimeNotices: runtimeNotices);
    }

    /// <summary>Test-only <see cref="IEventRenderer"/> that samples the on-disk run state from its own
    /// <c>Dispose</c> — the one injectable point inside <c>ExecuteRun</c>'s finalize window (see
    /// <see cref="Terminal_run_status_is_written_after_watermark_advancement"/>). Reads are best-effort
    /// by design: a probe must never be able to fail the run it is observing.</summary>
    private sealed class FinalizeProbeRenderer(string projectDir) : IEventRenderer, IDisposable
    {
        public bool Disposed { get; private set; }
        public string? StatusAtDispose { get; private set; }
        public bool WatermarksExistedAtDispose { get; private set; }

        /// <summary>The same two reads the probe takes at disposal, callable by the test for the
        /// after-the-run comparison — it must sample before its own cleanup deletes the project.</summary>
        public string? SampleStatus() => ReadStatus(projectDir);

        public bool SampleWatermarksExist() => WatermarksExist(projectDir);

        public void Render(RunEvent evt) { }

        public void Dispose()
        {
            Disposed = true;
            StatusAtDispose = ReadStatus(projectDir);
            WatermarksExistedAtDispose = WatermarksExist(projectDir);
        }

        private static bool WatermarksExist(string projectDir) =>
            File.Exists(Path.Combine(projectDir, ".pz", "state", "watermarks.json"));

        private static string? ReadStatus(string projectDir)
        {
            try
            {
                var runDir = Directory.GetDirectories(Path.Combine(projectDir, ".pz", "runs")).Single();
                using var doc = System.Text.Json.JsonDocument.Parse(
                    File.ReadAllBytes(Path.Combine(runDir, "run_results.json")));
                return doc.RootElement.GetProperty("status").GetString();
            }
            catch
            {
                return null;
            }
        }
    }

    /// <summary>Same `localfiles` `base_dir` injection <see cref="RunCommand"/> performs internally
    /// (private there) — duplicated here (mirroring <c>TestCommand</c>'s own copy) since the test drives
    /// <see cref="RunCommand.ExecuteRun"/> directly rather than through <see cref="RunCommand.Execute"/>.</summary>
    private static Pz.Core.Model.PzProject InjectLocalFilesBaseDir(Pz.Core.Model.PzProject project, string projectDir)
    {
        var connections = project.Connections
            .Select(s => s.Connector == "localfiles" ? s with { Connection = WithBaseDir(s.Connection, projectDir) } : s)
            .ToList();
        return project with { Connections = connections };
    }

    private static IReadOnlyDictionary<string, object?> WithBaseDir(
        IReadOnlyDictionary<string, object?> connection, string projectDir)
    {
        var merged = new Dictionary<string, object?>(connection) { ["base_dir"] = projectDir };
        return merged;
    }

    /// <summary>Test-only <see cref="IEventRenderer"/> whose <see cref="Render"/> blocks forever,
    /// simulating a wedged terminal/renderer for <see cref="Stuck_renderer_does_not_hang_the_run"/>.</summary>
    private sealed class NeverReturningRenderer : IEventRenderer
    {
        public void Render(RunEvent evt) => Thread.Sleep(Timeout.Infinite);
    }

    [Fact]
    public void Invalid_log_format_exits_config_error_not_crash()
    {
        var stderr = new StringWriter();
        var originalErr = Console.Error;
        Console.SetError(stderr);
        int exit;
        try
        {
            exit = CliApp.Build().Parse(["run", "--project", _work, "--log-format", "xml"]).Invoke();
        }
        finally
        {
            Console.SetError(originalErr);
        }

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("invalid --log-format value", stderr.ToString());
    }

    [Fact]
    public void Invalid_otel_endpoint_exits_config_error_not_crash()
    {
        var stderr = new StringWriter();
        var originalErr = Console.Error;
        Console.SetError(stderr);
        int exit;
        try
        {
            exit = CliApp.Build().Parse(["run", "--project", _work, "--otel-endpoint", "not-a-url"]).Invoke();
        }
        finally
        {
            Console.SetError(originalErr);
        }

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("--otel-endpoint", stderr.ToString());
    }

    private static NodeResult MakeResult(string name, NodeStatus status) =>
        new(new NodeId(name), NodeKind.SourceLoad, name, status, 5, TimeSpan.FromMilliseconds(10),
            status == NodeStatus.Failed ? new PzError(PzErrorCode.NodeFailed, "boom", null, null, null) : null);

    private static int CountOccurrences(string haystack, string needle)
    {
        var count = 0;
        var index = 0;
        while ((index = haystack.IndexOf(needle, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += needle.Length;
        }

        return count;
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

    /// <summary>Fabricates a prior run dir under <paramref name="work"/>. Ids are ordinal-sortable, so a
    /// higher suffix is a newer run -- and every fabricated id sorts BELOW the real one `pz run` will mint
    /// (2026 > 2025), which is what makes these the "older" runs the sweep is expected to take.</summary>
    private static string MakePriorRun(string work, int ordinal)
    {
        var runId = $"20250101T00000{ordinal:D3}Z-{ordinal:x4}";
        var dir = Path.Combine(work, ".pz", "runs", runId);
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "run_results.json"), "{}");
        File.WriteAllBytes(Path.Combine(dir, "staging.duckdb"), new byte[4096]);
        return runId;
    }

    [Fact]
    public void Run_sweeps_older_staging_and_keeps_run_results()
    {
        var work = Path.Combine(Path.GetTempPath(), "pz-run-tests", Guid.NewGuid().ToString("N"));
        CopyTree(Path.Combine(AppContext.BaseDirectory, "SamplesHelloPz"), work);
        try
        {
            var priors = Enumerable.Range(1, 12).Select(i => MakePriorRun(work, i)).ToList();
            // hello-pz has no incremental datasets, so pz run never creates watermarks.json on its own --
            // stub one here (as a real project with prior runs would already have) so this assertion
            // actually exercises "retention never touches .pz/state" instead of throwing on a missing file.
            var stateDir = Path.Combine(work, ".pz", "state");
            Directory.CreateDirectory(stateDir);
            var watermarksPath = Path.Combine(stateDir, "watermarks.json");
            File.WriteAllText(watermarksPath, "{}\n");
            var stateBefore = File.ReadAllBytes(watermarksPath);

            var exit = CliApp.Build().Parse(["run", "--project", work]).Invoke();
            Assert.Equal(ExitCodes.Ok, exit);

            // keep_last defaults to 10 and the run that just finished occupies one of those slots, so
            // exactly 9 of the 12 priors keep their staging DB and the 3 oldest lose theirs.
            var survivingStaging = priors
                .Count(id => File.Exists(Path.Combine(work, ".pz", "runs", id, "staging.duckdb")));
            Assert.Equal(9, survivingStaging);

            // Run history is never swept -- that is the whole point of staging-only.
            Assert.All(priors, id =>
                Assert.True(File.Exists(Path.Combine(work, ".pz", "runs", id, "run_results.json"))));

            // The run that just finished keeps its own staging DB.
            var newest = Directory.GetDirectories(Path.Combine(work, ".pz", "runs"))
                .Select(Path.GetFileName)
                .Where(id => !priors.Contains(id!))
                .Single()!;
            Assert.True(File.Exists(Path.Combine(work, ".pz", "runs", newest, "staging.duckdb")));

            // State is never enumerated by any retention code path. This assertion is a real
            // regression net for RunSweeper (it structurally never touches .pz/state) -- but it only holds
            // because hello-pz has no incremental datasets, so WatermarkAdvancement.Advance never calls
            // store.Set and never rewrites this file on its own. If hello-pz ever gains an incremental
            // dataset, a failure here would be fixture drift (the file legitimately changing), not a
            // retention regression -- re-derive stateBefore from a run that already produced watermarks
            // in that case, don't chase this test.
            Assert.Equal(stateBefore, File.ReadAllBytes(Path.Combine(work, ".pz", "state", "watermarks.json")));
        }
        finally
        {
            try { Directory.Delete(work, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    [Fact]
    public void Retention_off_sweeps_nothing_and_prints_nothing()
    {
        var work = Path.Combine(Path.GetTempPath(), "pz-run-tests", Guid.NewGuid().ToString("N"));
        CopyTree(Path.Combine(AppContext.BaseDirectory, "SamplesHelloPz"), work);
        try
        {
            var projectYml = Path.Combine(work, "project.yml");
            File.WriteAllText(projectYml, File.ReadAllText(projectYml) + "\nretention: off\n");
            var priors = Enumerable.Range(1, 12).Select(i => MakePriorRun(work, i)).ToList();

            var stdout = new StringWriter();
            var original = Console.Out;
            int exit;
            try
            {
                Console.SetOut(stdout);
                exit = CliApp.Build().Parse(["run", "--project", work]).Invoke();
            }
            finally
            {
                Console.SetOut(original);
            }

            Assert.Equal(ExitCodes.Ok, exit);
            Assert.All(priors, id =>
                Assert.True(File.Exists(Path.Combine(work, ".pz", "runs", id, "staging.duckdb"))));
            Assert.DoesNotContain("cleaned", stdout.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            try { Directory.Delete(work, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    [Fact]
    public void Retention_reports_what_it_freed()
    {
        var work = Path.Combine(Path.GetTempPath(), "pz-run-tests", Guid.NewGuid().ToString("N"));
        CopyTree(Path.Combine(AppContext.BaseDirectory, "SamplesHelloPz"), work);
        try
        {
            for (var i = 1; i <= 12; i++)
            {
                MakePriorRun(work, i);
            }

            var stdout = new StringWriter();
            var original = Console.Out;
            try
            {
                Console.SetOut(stdout);
                Assert.Equal(ExitCodes.Ok, CliApp.Build().Parse(["run", "--project", work]).Invoke());
            }
            finally
            {
                Console.SetOut(original);
            }

            Assert.Contains("cleaned 3 staging database(s)", stdout.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            try { Directory.Delete(work, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    /// <summary><c>SweepOutcome.BytesFreed</c> is staging-only -- tmp workdir bytes live in the separate
    /// <c>TmpBytesFreed</c> field. A run with zero sweepable staging DBs (no prior runs at all here) but
    /// one free, unlocked <c>.pz/tmp</c> workdir would report "freed 0 B" if the console line read
    /// <c>BytesFreed</c> alone. This pins the combined total.</summary>
    [Fact]
    public void Retention_reports_tmp_only_bytes_not_zero()
    {
        var work = Path.Combine(Path.GetTempPath(), "pz-run-tests", Guid.NewGuid().ToString("N"));
        CopyTree(Path.Combine(AppContext.BaseDirectory, "SamplesHelloPz"), work);
        try
        {
            var tmpDir = Path.Combine(work, ".pz", "tmp", "stale-restore");
            Directory.CreateDirectory(tmpDir);
            var tmpBytes = new byte[12_345];
            File.WriteAllBytes(Path.Combine(tmpDir, "partial.bin"), tmpBytes);

            var stdout = new StringWriter();
            var original = Console.Out;
            try
            {
                Console.SetOut(stdout);
                Assert.Equal(ExitCodes.Ok, CliApp.Build().Parse(["run", "--project", work]).Invoke());
            }
            finally
            {
                Console.SetOut(original);
            }

            var expected = "cleaned 0 staging database(s) and 1 stale workdir(s) — " +
                $"freed {CleanCommand.FormatBytes(tmpBytes.Length)}";
            Assert.Contains(expected, stdout.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            try { Directory.Delete(work, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    /// <summary>The three tests above only assert on disk/console state, so nothing there catches
    /// the publish moving to after <c>bus.Complete()</c> -- <c>retentionOutcome</c> is a local, so the
    /// deletions and the console line stay correct either way and the NDJSON event just silently vanishes.
    /// This drives a real sweep through <c>--log-format json</c> and pins both that <c>retention_swept</c>
    /// exists on stdout AND that it comes after <c>run_completed</c> -- the only order the channel can
    /// produce if the publish still precedes <c>bus.Complete()</c>.</summary>
    [Fact]
    public void Retention_swept_event_is_published_before_bus_completes()
    {
        var work = Path.Combine(Path.GetTempPath(), "pz-run-tests", Guid.NewGuid().ToString("N"));
        CopyTree(Path.Combine(AppContext.BaseDirectory, "SamplesHelloPz"), work);
        try
        {
            for (var i = 1; i <= 12; i++)
            {
                MakePriorRun(work, i);
            }

            var stdout = new StringWriter();
            var original = Console.Out;
            try
            {
                Console.SetOut(stdout);
                Assert.Equal(ExitCodes.Ok,
                    CliApp.Build().Parse(["run", "--project", work, "--log-format", "json"]).Invoke());
            }
            finally
            {
                Console.SetOut(original);
            }

            var lines = stdout.ToString().Split('\n', StringSplitOptions.RemoveEmptyEntries);
            var runCompletedIndex = Array.FindIndex(lines, l => l.Contains("\"event\":\"run_completed\"", StringComparison.Ordinal));
            var retentionSweptIndex = Array.FindIndex(lines, l => l.Contains("\"event\":\"retention_swept\"", StringComparison.Ordinal));

            Assert.True(runCompletedIndex >= 0, "expected a run_completed NDJSON line");
            Assert.True(retentionSweptIndex >= 0,
                "expected a retention_swept NDJSON line -- its absence means the publish moved to after " +
                "bus.Complete(), which drops it silently");
            Assert.True(retentionSweptIndex > runCompletedIndex,
                "retention_swept must be published before bus.Complete() closes the channel, which " +
                "requires it to be enqueued strictly after run_completed");
        }
        finally
        {
            try { Directory.Delete(work, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }
}
