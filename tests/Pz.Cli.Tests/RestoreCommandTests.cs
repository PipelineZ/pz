using Pz.Cli;
using Pz.TestSupport;

namespace Pz.Cli.Tests;

/// <summary>CLI-level coverage of the lockfile contract: `pz restore` writes pz.lock.json and materializes
/// non-builtin connectors; a project whose connectors are all builtin restores nothing; `pz run`
/// enforces the lock (PZ0322 missing, PZ0321 drift) unless <c>--no-lock-check</c> loudly bypasses it.
/// Joins "console-and-env-serialized" (see that collection's definition below) both for its
/// <see cref="CliLocalFeedFixture"/> and because it redirects Console.Out/Error.</summary>
[Collection("console-and-env-serialized")]
public sealed class RestoreCommandTests(CliLocalFeedFixture feed) : IDisposable
{
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-restore-cli-tests", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public void Restore_writes_lock_and_materializes()
    {
        WriteProject(FakeSourceConnectorProject());

        var exit = CliApp.Build().Parse(["restore", "--project", _work, "--feeds", feed.FeedDir]).Invoke();
        var stdout = RunAndCapture(["restore", "--project", _work, "--feeds", feed.FeedDir]);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.True(File.Exists(Path.Combine(_work, "pz.lock.json")));
        Assert.True(Directory.Exists(Path.Combine(_work, ".pz", "packages", "FakeSourceConnector", "1.2.3", "lib")));
        Assert.Contains("restored FakeSourceConnector 1.2.3", stdout);
    }

    [Fact]
    public void Restore_all_builtin_project_writes_no_lock()
    {
        Directory.CreateDirectory(_work);
        File.WriteAllText(Path.Combine(_work, "project.yml"), """
            name: allbuiltin
            version: 0.1.0

            connectors:
              - package: Pz.Connector.LocalFiles
                version: 0.1.0
            """);

        var stdout = RunAndCapture(["restore", "--project", _work, "--feeds", feed.FeedDir]);
        var exit = CliApp.Build().Parse(["restore", "--project", _work, "--feeds", feed.FeedDir]).Invoke();

        Assert.Equal(ExitCodes.Ok, exit);
        // A builtin's declared version: is schema-valid but never consulted, and the notice must say
        // so, not imply it was honored.
        Assert.Contains("Pz.Connector.LocalFiles is builtin in this pz version; declared version ignored", stdout);
        Assert.False(File.Exists(Path.Combine(_work, "pz.lock.json")));
    }

    [Fact]
    public void Run_without_lock_when_required_is_error_PZ0322()
    {
        WriteProject(FakeSourceConnectorProject());

        var stderr = RunAndCaptureStderr(["run", "--project", _work]);
        var exit = CliApp.Build().Parse(["run", "--project", _work]).Invoke();

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0322", stderr);
    }

    [Fact]
    public void Run_with_drifted_lock_is_error_PZ0321_hint_restore()
    {
        WriteProject(FakeSourceConnectorProject());
        Assert.Equal(ExitCodes.Ok, CliApp.Build().Parse(["restore", "--project", _work, "--feeds", feed.FeedDir]).Invoke());

        // Drift the requirement so the committed lock (pinned to 1.2.3) no longer satisfies it.
        WriteProject(FakeSourceConnectorProject(requiredVersion: "9.9.9"));

        var stderr = RunAndCaptureStderr(["run", "--project", _work]);
        var exit = CliApp.Build().Parse(["run", "--project", _work]).Invoke();

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0321", stderr);
        Assert.Contains("restore", stderr);
    }

    // Drift detection covers both `pz run` and `pz plan`. PlanCommand goes through the same
    // ConnectorRegistryFactory, so this mirrors Run_with_drifted_lock_is_error_PZ0321_hint_restore
    // exactly, just through `pz plan`.
    [Fact]
    public void Plan_with_drifted_lock_is_error_PZ0321_hint_restore()
    {
        WriteProject(FakeSourceConnectorProject());
        Assert.Equal(ExitCodes.Ok, CliApp.Build().Parse(["restore", "--project", _work, "--feeds", feed.FeedDir]).Invoke());

        // Drift the requirement so the committed lock (pinned to 1.2.3) no longer satisfies it.
        WriteProject(FakeSourceConnectorProject(requiredVersion: "9.9.9"));

        var stderr = RunAndCaptureStderr(["plan", "--project", _work]);
        var exit = CliApp.Build().Parse(["plan", "--project", _work]).Invoke();

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0321", stderr);
        Assert.Contains("restore", stderr);
    }

    // The bypass is proven by what comes AFTER the skipped drift check: the run gets far enough to
    // refuse the dotnet-runtime package itself (PZ0360, from hosting) instead of stopping at the
    // drifted lock (PZ0321) — plus the loud warning the flag promises.
    [Fact]
    public void No_lock_check_bypasses_with_loud_warning()
    {
        WriteProject(FakeSourceConnectorProject());
        Assert.Equal(ExitCodes.Ok, CliApp.Build().Parse(["restore", "--project", _work, "--feeds", feed.FeedDir]).Invoke());

        WriteProject(FakeSourceConnectorProject(requiredVersion: "9.9.9"));

        var stderr = RunAndCaptureStderr(["run", "--project", _work, "--no-lock-check"]);
        var exit = CliApp.Build().Parse(["run", "--project", _work, "--no-lock-check"]).Invoke();

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("warning", stderr);
        Assert.Contains("--no-lock-check", stderr);
        Assert.Contains("PZ0360", stderr);
        Assert.DoesNotContain("PZ0321", stderr);
    }

    [Fact]
    public void Restore_uses_pz_feeds_env_when_no_flag()
    {
        WriteProject(FakeSourceConnectorProject());
        Environment.SetEnvironmentVariable("PZ_FEEDS", feed.FeedDir);
        try
        {
            var exit = CliApp.Build().Parse(["restore", "--project", _work]).Invoke();
            Assert.Equal(ExitCodes.Ok, exit);
            Assert.True(File.Exists(Path.Combine(_work, "pz.lock.json")));
        }
        finally
        {
            Environment.SetEnvironmentVariable("PZ_FEEDS", null);
        }
    }

    [Fact]
    public void Feeds_in_project_yml_fails_restore_with_PZ0352()
    {
        WriteProject($"""
            name: restore_test
            version: 0.1.0

            feeds:
              - "{feed.FeedDir.Replace("\\", "\\\\")}"

            connectors:
              - package: FakeSourceConnector
                version: 1.2.3
            """);

        var stderr = RunAndCaptureStderr(["restore", "--project", _work, "--feeds", feed.FeedDir]);
        var exit = CliApp.Build().Parse(["restore", "--project", _work, "--feeds", feed.FeedDir]).Invoke();

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0352", stderr);
    }

    private string FakeSourceConnectorProject(string requiredVersion = "1.2.3") => $"""
        name: restore_test
        version: 0.1.0

        connectors:
          - package: FakeSourceConnector
            version: {requiredVersion}
        """;

    private void WriteProject(string yaml)
    {
        Directory.CreateDirectory(_work);
        File.WriteAllText(Path.Combine(_work, "project.yml"), yaml);
    }

    private static string RunAndCapture(string[] args)
    {
        var stdout = new StringWriter();
        var original = Console.Out;
        Console.SetOut(stdout);
        try { CliApp.Build().Parse(args).Invoke(); }
        finally { Console.SetOut(original); }
        return stdout.ToString();
    }

    private static string RunAndCaptureStderr(string[] args)
    {
        var stderr = new StringWriter();
        var original = Console.Error;
        Console.SetError(stderr);
        try { CliApp.Build().Parse(args).Invoke(); }
        finally { Console.SetError(original); }
        return stderr.ToString();
    }
}

/// <summary>Packs the FakeSourceConnector/FakeTransitiveDep fixture projects (via the shared
/// <see cref="Pz.TestSupport.LocalFeed"/> packing helper — see its own doc comment for the hoist
/// rationale) into a local feed directory once per test class, shared across every fact in
/// <see cref="RestoreCommandTests"/>.</summary>
public sealed class CliLocalFeedFixture : IDisposable
{
    public string FeedDir { get; }
    private readonly string _workRoot;

    public CliLocalFeedFixture()
    {
        _workRoot = Path.Combine(Path.GetTempPath(), "pz-cli-tests", "feed-" + Guid.NewGuid().ToString("N"));
        FeedDir = Path.Combine(_workRoot, "feed");
        Directory.CreateDirectory(FeedDir);
        LocalFeed.Pack(FeedDir, "FakeTransitiveDep", version: null);
        LocalFeed.Pack(FeedDir, "FakeSourceConnector", "1.2.3", versionProperty: "FakeSourceConnectorVersion");
    }

    public void Dispose()
    {
        try { Directory.Delete(_workRoot, recursive: true); } catch { /* best effort */ }
    }
}

/// <summary>Serializes every Pz.Cli.Tests class that mutates process-global
/// state -- Console.Out/Error redirection (<see cref="RunCommandTests"/>, <see cref="ValidateCommandTests"/>,
/// <see cref="TestCommandTests"/>, <see cref="PlanCommandTests"/>, <see cref="RetryCommandTests"/>,
/// <see cref="InitCommandTests"/>, <see cref="RestoreCommandTests"/>, <see cref="ConnectorRegistryFactoryTests"/>,
/// <see cref="LsCommandTests"/> and <see cref="ConnectorsCommandTests"/>)
/// and/or the DATA_DIR/OUT_DIR/TMPDIR environment variables (<see cref="RunCommandTests"/>,
/// <see cref="PlanCommandTests"/>, <see cref="CompileCommandTests"/>, <see cref="LsCommandTests"/>,
/// <see cref="ProcessSocketRootTests"/>) -- into ONE xunit collection.
///
/// xunit only serializes test CLASSES within the SAME collection; it still runs different collections
/// (and any uncollected class) concurrently with each other. Everything that swaps this process-global
/// state therefore has to be in the SAME collection, not merely in "a" collection -- otherwise the
/// groups race for Console.Out/Error and DATA_DIR/OUT_DIR and corrupt each other's captured output
/// intermittently. One collection here beats an assembly-wide
/// <c>[CollectionBehavior(DisableTestParallelization = true)]</c>: every other class in the assembly
/// (CliAppTests, PackagingSmokeTests, Rendering/*, Otel/*) -- none of which touch Console or these env
/// vars -- stays parallel-eligible.
///
/// Also carries <see cref="CliLocalFeedFixture"/> (built once, shared by every fact in the
/// collection) for <see cref="RestoreCommandTests"/>, <see cref="ConnectorRegistryFactoryTests"/>,
/// and <see cref="ConnectorsCommandTests"/>.</summary>
[CollectionDefinition("console-and-env-serialized")]
public class ConsoleAndEnvSerializedCollection : ICollectionFixture<CliLocalFeedFixture>;
