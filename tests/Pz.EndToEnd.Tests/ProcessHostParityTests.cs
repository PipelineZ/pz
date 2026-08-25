using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Text.Json;
using System.Text.RegularExpressions;
using Pz.Cli;
using Pz.PackageManagement.Restore;

namespace Pz.EndToEnd.Tests;

/// <summary>The Phase 1 exit criterion: one project run twice — once against the builtin
/// <c>localfiles</c> connector, once against the SAME connector reached out of process over PCP — must
/// produce the same plan and the same run results, byte for byte after an explicit, minimal
/// normalization. Everything the process path adds (a child process, a control socket, an Arrow IPC
/// data plane, a handshake) has to be invisible in pz's own artifacts.
///
/// <para>The out-of-process peer is <c>tests/fixtures/PcpFakeConnector</c>, which delegates every call
/// to a real <c>LocalFilesConnector</c>: the two runs therefore differ in HOW the connector is reached
/// and in nothing else, which is what makes a byte comparison meaningful rather than a coincidence.</para>
///
/// <para>Linux only: the fixture serves the protocol over unix domain sockets and does not implement
/// the named-pipe transport.</para></summary>
[SupportedOSPlatform("linux")]
public sealed class ProcessHostParityTests : IDisposable
{
    private const string PackageId = "LocalFilesPcp";
    private const string PackageVersion = "1.0.0";
    private const string ProcessConnector = "localfiles-pcp";
    private const string BuiltinConnector = "localfiles";

    private readonly List<string> _dirs = [];

    /// <summary>Default tiers: LocalFiles offers a native path for a <c>columns:</c>-contracted csv read
    /// and a parquet write, and the process host must offer the same two — the connector's native SQL
    /// fragment crosses the wire and DuckDB executes it in the host either way. Strict parity: no
    /// wall-clock stall numbers and no operation-gate counts exist on this tier, so the ONLY
    /// normalizations in play are the run identity and the connector name.</summary>
    [SkippableFact]
    public void Process_hosted_connector_plans_and_runs_identically_on_the_native_tier()
    {
        Skip.If(OperatingSystem.IsWindows(), "PcpFakeConnector serves unix domain sockets only");

        var builtin = RunProject(BuiltinConnector, forceUniversal: false);
        var process = RunProject(ProcessConnector, forceUniversal: false);

        // Guards against the comparison degenerating: if the process host failed to offer a native
        // tier, both sides would still be byte-equal after normalization -- just equally universal.
        Assert.Equal("native_scan", StrategyOf(builtin.Plan, "SourceLoad"));
        Assert.Equal("native_scan", StrategyOf(process.Plan, "SourceLoad"));
        Assert.Equal("native_copy", StrategyOf(builtin.Plan, "SinkWrite"));
        Assert.Equal("native_copy", StrategyOf(process.Plan, "SinkWrite"));

        AssertParity(builtin, process);
    }

    /// <summary>engine.force_universal pushes both edges onto the Arrow <c>RecordBatch</c> path, which
    /// over PCP means every row physically crosses the data-plane socket as Arrow IPC. This is the fact
    /// that actually proves the data plane moves the same rows, not just the same SQL string.</summary>
    [SkippableFact]
    public void Process_hosted_connector_moves_the_same_rows_over_the_universal_tier()
    {
        Skip.If(OperatingSystem.IsWindows(), "PcpFakeConnector serves unix domain sockets only");

        var builtin = RunProject(BuiltinConnector, forceUniversal: true);
        var process = RunProject(ProcessConnector, forceUniversal: true);

        Assert.Equal("arrow_stream", StrategyOf(builtin.Plan, "SourceLoad"));
        Assert.Equal("arrow_stream", StrategyOf(process.Plan, "SourceLoad"));
        Assert.Equal("arrow_stream", StrategyOf(builtin.Plan, "SinkWrite"));
        Assert.Equal("arrow_stream", StrategyOf(process.Plan, "SinkWrite"));

        // Pinned rather than only normalized away, so the exact shape of the one structural difference
        // stays visible: the process shims hold an operation gate unconditionally
        // (IOperationGateAware), so the engine records an ops object for them, and the counts are zero
        // because a connector that never issues a GateAcquire never routes an operation through it.
        // LocalFiles is not gate-aware and records no ops key at all.
        Assert.DoesNotContain("\"ops\":", builtin.Results, StringComparison.Ordinal);
        Assert.Contains(
            "\"ops\":{\"executed\":0,\"retried\":0,\"throttle_wait_ms\":0}",
            process.Results, StringComparison.Ordinal);

        AssertParity(builtin, process, universalTier: true);
    }

    /// <summary>A verb with no run directory still opens connectors — <c>pz validate</c> spawns one to
    /// ask it about the connection config. Its sockets therefore go to a host-owned temp root, which
    /// <c>ConnectorHosts</c> removes when the hosts are disposed, and nothing about the process path
    /// leaves a run directory behind in a project that never ran.</summary>
    [SkippableFact]
    public void Validate_opens_a_process_connector_with_no_run_directory()
    {
        Skip.If(OperatingSystem.IsWindows(), "PcpFakeConnector serves unix domain sockets only");

        var dir = NewProjectDir();
        WriteProject(dir, ProcessConnector, forceUniversal: false);

        Assert.Equal(ExitCodes.Ok, CliApp.Build().Parse(["validate", "--project", dir]).Invoke());
        Assert.False(Directory.Exists(Path.Combine(dir, ".pz", "runs")));
    }

    private static void AssertParity(RunArtifacts builtin, RunArtifacts process, bool universalTier = false)
    {
        Assert.Equal(
            NormalizePlan(builtin.Plan, BuiltinConnector),
            NormalizePlan(process.Plan, ProcessConnector));

        var builtinResults = NormalizeResults(builtin.Results, builtin.RunId);
        var processResults = NormalizeResults(process.Results, process.RunId);
        if (universalTier)
        {
            builtinResults = NormalizeUniversalTier(builtinResults);
            processResults = NormalizeUniversalTier(processResults);
        }

        Assert.Equal(builtinResults, processResults);

        // The artifacts agreeing is only worth something if the data does: the sink's own bytes are
        // compared unnormalized.
        Assert.Equal(builtin.Output, process.Output);
    }

    // --- normalization -------------------------------------------------------------------------
    // Golden-file discipline: every entry below is a field that MUST differ between the two runs, and
    // says why. Nothing is normalized to paper over a difference the process path should not have.

    /// <summary>plan.json. One entry: the connector NAME. The two projects deliberately differ in
    /// exactly one authored value — <c>connector:</c> in connections.yml — and
    /// <c>ExecutionPlanner</c> quotes it in every Reason string it builds. Node ids are unaffected:
    /// <c>DagCompiler</c> hashes the CONNECTION name, never the connector's.
    ///
    /// <para>Matched together with the apostrophes around it — otherwise replacing the builtin's name
    /// would also rewrite the middle of the process connector's. The apostrophes are asked of the same
    /// encoder <c>PlanWriter</c> used rather than spelled out, since it is the encoder that decides
    /// whether they appear escaped in the file.</para></summary>
    private static string NormalizePlan(string json, string connectorName) =>
        json.Replace(
            Quoted(connectorName), Quoted("<connector>"), StringComparison.Ordinal);

    private static string Quoted(string value) => JsonEncodedText.Encode($"'{value}'").ToString();

    /// <summary>run_results.json. Three entries, all run identity or wall clock:
    /// <list type="bullet">
    /// <item><c>runId</c> — one per run, minted from the clock plus randomness.</item>
    /// <item><c>startedAt</c> — wall clock.</item>
    /// <item><c>durationMs</c> — measured elapsed time per node.</item>
    /// </list></summary>
    private static string NormalizeResults(string json, string runId) =>
        DurationPattern.Replace(
            StartedAtPattern.Replace(
                json.Replace(runId, "<run-id>", StringComparison.Ordinal),
                "\"startedAt\":\"<started-at>\""),
            "\"durationMs\":0");

    /// <summary>Two further entries, applied ONLY by the force_universal fact so the native-tier fact
    /// still proves parity without them:
    /// <list type="bullet">
    /// <item><c>timings</c> — producer/consumer channel STALL measurements, i.e. wall clock. Written
    /// only on the universal tier.</item>
    /// <item><c>ops</c> — a difference in the HOST's bookkeeping, not in the data: the process shims
    /// implement <c>IOperationGateAware</c> unconditionally, so the engine gives them a gate and
    /// records its (here all-zero) counters, while the non-gate-aware <c>LocalFilesSource</c> gets
    /// none. The exact object is asserted in the fact above rather than only erased here.</item>
    /// </list></summary>
    private static string NormalizeUniversalTier(string json) =>
        OpsPattern.Replace(TimingsPattern.Replace(json, ""), "");

    private static readonly Regex StartedAtPattern =
        new("\"startedAt\":\"[^\"]*\"", RegexOptions.Compiled);

    private static readonly Regex DurationPattern =
        new("\"durationMs\":[0-9]+", RegexOptions.Compiled);

    private static readonly Regex TimingsPattern =
        new(",\"timings\":\\{[^}]*\\}", RegexOptions.Compiled);

    private static readonly Regex OpsPattern =
        new(",\"ops\":\\{[^}]*\\}", RegexOptions.Compiled);

    // --- project construction ------------------------------------------------------------------

    private static string StrategyOf(string planJson, string kind)
    {
        using var plan = JsonDocument.Parse(planJson);
        return plan.RootElement.GetProperty("nodes").EnumerateArray()
            .Single(n => n.GetProperty("kind").GetString() == kind)
            .GetProperty("strategy").GetString()!;
    }

    private sealed record RunArtifacts(string RunId, string Plan, string Results, byte[] Output);

    /// <summary>Writes one project, runs it through the real CLI entry point, and reads back its
    /// artifacts. The two connector names produce byte-identical projects apart from
    /// <c>connections.yml</c>'s <c>connector:</c> lines and — for the process one — the package
    /// layout/lock the CLI needs to find it at all.</summary>
    private RunArtifacts RunProject(string connectorName, bool forceUniversal)
    {
        var dir = NewProjectDir();
        WriteProject(dir, connectorName, forceUniversal);

        // Asserted here rather than after the artifacts are read: a run that never got as far as
        // producing a plan should fail this test with its own exit code, not with a missing file.
        Assert.Equal(ExitCodes.Ok, CliApp.Build().Parse(["run", "--project", dir]).Invoke());

        var runDir = Directory.EnumerateDirectories(Path.Combine(dir, ".pz", "runs")).Single();
        return new RunArtifacts(
            Path.GetFileName(runDir),
            File.ReadAllText(Path.Combine(dir, ".pz", "target", "plan.json")),
            File.ReadAllText(Path.Combine(runDir, "run_results.json")),
            File.ReadAllBytes(Path.Combine(dir, "out", "customer_totals.parquet")));
    }

    private static void WriteProject(string dir, string connectorName, bool forceUniversal)
    {
        var isProcess = connectorName == ProcessConnector;

        File.WriteAllText(Path.Combine(dir, "project.yml"),
            "name: pcp_parity\n" +
            "version: 0.1.0\n" +
            "connectors:\n" +
            (isProcess
                ? $"  - package: {PackageId}\n    version: {PackageVersion}\n"
                : "  - package: Pz.Connector.LocalFiles\n    version: 0.1.0\n") +
            "engine:\n" +
            "  threads: 2\n" +
            (forceUniversal ? "  force_universal: true\n" : ""));

        File.WriteAllText(Path.Combine(dir, "connections.yml"), $"""
            files:
              connector: {connectorName}
              entities:
                orders:
                  read:
                    path: data/orders.csv
                    format: csv
                    columns:
                      id: bigint
                      customer: varchar
                      amount: double

            lake:
              connector: {connectorName}
            """);

        Directory.CreateDirectory(Path.Combine(dir, "pipelines"));
        File.WriteAllText(Path.Combine(dir, "pipelines", "customer_totals.sql"),
            "INSERT INTO {{ sink('lake', 'customer_totals', strategy: 'replace', format: 'parquet', path: 'out/') }}\n"
            + "select customer, count(*) as orders, sum(amount) as total\n"
            + "from {{ source('files', 'orders') }}\n"
            + "group by customer\n"
            + "order by customer\n");

        Directory.CreateDirectory(Path.Combine(dir, "data"));
        File.WriteAllText(Path.Combine(dir, "data", "orders.csv"),
            "id,customer,amount\n1,ann,10.5\n2,bob,20.25\n3,ann,5.25\n4,cy,100.0\n");

        if (isProcess)
        {
            WriteProcessPackage(dir);
        }
    }

    /// <summary>Materializes what `pz restore` would have left behind for a <c>runtime: "process"</c>
    /// connector package: <c>.pz/packages/&lt;id&gt;/&lt;version&gt;/</c> holding a manifest and an
    /// executable entrypoint, plus a matching <c>pz.lock.json</c> (the drift check is not bypassed —
    /// this suite exercises the same lock-verified path a real run takes).
    ///
    /// <para>The entrypoint is a wrapper script rather than a published copy of the fixture: the
    /// manifest→RID→spawn path is what is under test, and a script keeps the package to two files.</para></summary>
    private static void WriteProcessPackage(string projectDir)
    {
        var packageDir = Path.Combine(projectDir, ".pz", "packages", PackageId, PackageVersion);
        var binDir = Path.Combine(packageDir, "bin");
        Directory.CreateDirectory(binDir);

        var entrypoint = Path.Combine(binDir, "connector");
        File.WriteAllText(entrypoint, $"#!/bin/sh\nexec \"{FixtureExecutablePath()}\" \"$@\"\n");
        File.SetUnixFileMode(
            entrypoint,
            UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute |
            UnixFileMode.GroupRead | UnixFileMode.GroupExecute);

        var manifest = new Dictionary<string, object?>
        {
            // Required for a process package (PZ0354 without it): the name is what the connector's own
            // handshake is checked against.
            ["name"] = ProcessConnector,
            ["protocolMajorMin"] = 1,
            ["protocolMajorMax"] = 1,
            // Exactly what LocalFilesConnector declares -- PcpClient refuses any Hello whose capability
            // set differs from the manifest's, and the fixture reports the real connector's.
            ["capabilities"] = new[]
            {
                "NativeScan", "NativeCopy", "ReplaceWrites", "BoundedWindow", "PartitionedRead",
            },
            // The same opt-in the builtin gets from ProjectDirectoryAnchor.BuiltinAnchoredConnectors:
            // without it a relative root:/path: would resolve against the CWD on one side and the
            // project directory on the other, and the two runs would not be comparable.
            ["projectDirectoryAnchor"] = true,
            ["runtime"] = "process",
            ["entrypoints"] = new Dictionary<string, string>
            {
                [RuntimeInformation.RuntimeIdentifier] = "bin/connector",
            },
        };
        File.WriteAllText(
            Path.Combine(packageDir, "pz.connector.json"), JsonSerializer.Serialize(manifest));

        // A process package materializes no lib/native assets, so both asset lists are empty; the
        // sha512 is never re-verified by the drift check, only the id/version/directory triple is.
        LockFileWriter.Write(
            new LockFile(LockFileWriter.CurrentVersion, RuntimeInformation.RuntimeIdentifier, [
                new LockedPackage(PackageId, PackageVersion, "sha512-parity-fixture", new LockedAssets([], [])),
            ]),
            Path.Combine(projectDir, "pz.lock.json"));
    }

    private static string FixtureExecutablePath()
    {
        var baseDir = new DirectoryInfo(AppContext.BaseDirectory);
        var tfm = baseDir.Name;
        var config = baseDir.Parent!.Name;
        var testsDir = baseDir.Parent!.Parent!.Parent!.Parent!.FullName;
        return Path.Combine(testsDir, "fixtures", "PcpFakeConnector", "bin", config, tfm, "PcpFakeConnector");
    }

    /// <summary>Deliberately short and outside the test output tree. The run-scoped socket root lives at
    /// <c>&lt;project&gt;/.pz/runs/&lt;id&gt;/sockets</c>, and a unix socket path is capped around 104
    /// bytes — a conventional temp name here would push the connector's socket past the limit and this
    /// suite would be testing the fallback rather than the run-scoped path.</summary>
    private string NewProjectDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pzp" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        _dirs.Add(dir);
        return dir;
    }

    public void Dispose()
    {
        foreach (var dir in _dirs)
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }
}
