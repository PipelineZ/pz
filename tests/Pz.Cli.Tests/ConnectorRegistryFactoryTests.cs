using System.Runtime.InteropServices;
using System.Text.Json;
using Pz.Core.Loading;
using Pz.Core.Validation;
using Pz.PackageManagement.Restore;

namespace Pz.Cli.Tests;

/// <summary>The registry's refusal paths. External connectors are hosted out of process only: a
/// restored package declaring runtime <c>"dotnet"</c> — or shipping no manifest, which means the same —
/// is refused with PZ0360 before any host is constructed. A process-hosted connector whose manifest
/// name collides with a builtin's must be rejected loudly (PZ0305), never silently replace the trusted
/// builtin. Shares <see cref="CliLocalFeedFixture"/> (and the "console-and-env-serialized" collection --
/// see its definition in RestoreCommandTests.cs) with <see cref="RestoreCommandTests"/>; also redirects
/// Console.Error, which is the other reason it must serialize against the rest of that collection.</summary>
[Collection("console-and-env-serialized")]
public sealed class ConnectorRegistryFactoryTests(CliLocalFeedFixture feed) : IDisposable
{
    private readonly string _work = Path.Combine(
        Path.GetTempPath(), "pz-registry-factory-tests", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    /// <summary>Nothing is spawned: the refusal happens at registration, so the process package's
    /// entrypoint only has to exist.</summary>
    [Fact]
    public async Task Hosted_connector_colliding_with_builtin_name_is_error_PZ0305()
    {
        Directory.CreateDirectory(_work);
        File.WriteAllText(Path.Combine(_work, "project.yml"), """
            name: collider_test
            version: 0.1.0

            connectors:
              - package: FakeLocalfilesPcp
                version: 1.0.0
            """);
        WriteProcessPackageClaiming("localfiles");

        var project = ProjectLoader.Load(_work, new Dictionary<string, string>());

        var ex = await Assert.ThrowsAsync<PzValidationException>(() =>
            ConnectorRegistryFactory.CreateAsync(project, _work, noLockCheck: false, CancellationToken.None));

        var error = Assert.Single(ex.Errors);
        Assert.Equal(PzErrorCode.ConnectorNotInstalled, error.Code);
        Assert.Contains("FakeLocalfilesPcp", error.Message);
        Assert.Contains("localfiles", error.Message);
        Assert.NotNull(error.Hint);
    }

    // Same scenario surfaced through the actual CLI verb (`pz run`), proving RunCommand's catch of
    // PzValidationException around ConnectorRegistryFactory.CreateAsync prints a clean error rather than
    // letting an InvalidOperationException escape as an unhandled exception / PZ0500.
    [Fact]
    public void Run_with_hosted_connector_colliding_with_builtin_name_is_error_PZ0305()
    {
        Directory.CreateDirectory(_work);
        File.WriteAllText(Path.Combine(_work, "project.yml"), """
            name: collider_run_test
            version: 0.1.0

            connectors:
              - package: FakeLocalfilesPcp
                version: 1.0.0
            """);
        WriteProcessPackageClaiming("localfiles");

        var stderr = RunAndCaptureStderr(["run", "--project", _work]);
        var exit = CliApp.Build().Parse(["run", "--project", _work]).Invoke();

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0305", stderr);
        Assert.Contains("localfiles", stderr);
    }

    // A restored package shipping no pz.connector.json means runtime "dotnet", and external dotnet
    // connectors are refused (PZ0360) — silently ALC-loading unattested third-party code is exactly
    // what the process-only rule exists to prevent. FakeSourceConnector ships a pz.connector.json (see
    // MaterializerTests), so its manifest is deleted post-restore to exercise the no-manifest wording.
    [Fact]
    public void Run_with_manifest_less_hosted_connector_is_error_PZ0360()
    {
        Directory.CreateDirectory(_work);
        File.WriteAllText(Path.Combine(_work, "project.yml"), """
            name: manifestless_test
            version: 0.1.0

            connectors:
              - package: FakeSourceConnector
                version: 1.2.3
            """);

        Assert.Equal(ExitCodes.Ok, CliApp.Build().Parse(["restore", "--project", _work, "--feeds", feed.FeedDir]).Invoke());

        var manifestPath = Path.Combine(_work, ".pz", "packages", "FakeSourceConnector", "1.2.3", "pz.connector.json");
        Assert.True(File.Exists(manifestPath));
        File.Delete(manifestPath);

        var stderr = RunAndCaptureStderr(["run", "--project", _work]);
        var exit = CliApp.Build().Parse(["run", "--project", _work]).Invoke();

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0360", stderr);
        Assert.Contains("no pz.connector.json", stderr);
    }

    /// <summary>The runtime "dotnet" wording of the same refusal, through the factory directly:
    /// FakeSourceConnector's shipped manifest declares no runtime, which reads as "dotnet".</summary>
    [Fact]
    public async Task Dotnet_runtime_connector_package_is_error_PZ0360()
    {
        Directory.CreateDirectory(_work);
        File.WriteAllText(Path.Combine(_work, "project.yml"), """
            name: dotnet_runtime_test
            version: 0.1.0

            connectors:
              - package: FakeSourceConnector
                version: 1.2.3
            """);

        Assert.Equal(ExitCodes.Ok, CliApp.Build().Parse(["restore", "--project", _work, "--feeds", feed.FeedDir]).Invoke());

        var project = ProjectLoader.Load(_work, new Dictionary<string, string>());

        var ex = await Assert.ThrowsAsync<PzValidationException>(() =>
            ConnectorRegistryFactory.CreateAsync(project, _work, noLockCheck: false, CancellationToken.None));

        var error = Assert.Single(ex.Errors);
        Assert.Equal(PzErrorCode.ExternalConnectorNotOutOfProcess, error.Code);
        Assert.Contains("FakeSourceConnector", error.Message);
        Assert.Contains("runtime 'dotnet'", error.Message);
        Assert.NotNull(error.Hint);
        Assert.Contains("process", error.Hint);
    }

    /// <summary>Materializes what `pz restore` would have left behind for a <c>runtime: "process"</c>
    /// package claiming <paramref name="connectorName"/>, plus the matching pz.lock.json — no feed can
    /// serve a process package yet, and what is under test is the registry, not restore.</summary>
    private void WriteProcessPackageClaiming(string connectorName)
    {
        const string packageId = "FakeLocalfilesPcp";
        const string version = "1.0.0";
        var packageDir = Path.Combine(_work, ".pz", "packages", packageId, version);
        Directory.CreateDirectory(packageDir);
        File.WriteAllText(Path.Combine(packageDir, "connector"), "never executed by this test");
        File.WriteAllText(Path.Combine(packageDir, "pz.connector.json"), JsonSerializer.Serialize(
            new Dictionary<string, object?>
            {
                ["name"] = connectorName,
                ["protocolMajorMin"] = 1,
                ["protocolMajorMax"] = 1,
                ["capabilities"] = Array.Empty<string>(),
                ["runtime"] = "process",
                ["entrypoints"] = new Dictionary<string, string>
                {
                    [RuntimeInformation.RuntimeIdentifier] = "connector",
                },
            }));

        LockFileWriter.Write(
            new LockFile(LockFileWriter.CurrentVersion, RuntimeInformation.RuntimeIdentifier, [
                new LockedPackage(packageId, version, "sha512-collider-fixture", new LockedAssets([], [])),
            ]),
            Path.Combine(_work, "pz.lock.json"));
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

    // ---- connector_log transform helpers: level naming, exception-message folding ----------------

    [Theory]
    [InlineData(0, "trace")]
    [InlineData(1, "debug")]
    [InlineData(2, "info")]
    [InlineData(3, "warn")]
    [InlineData(4, "error")]
    [InlineData(5, "critical")]
    [InlineData(6, "unknown")]
    [InlineData(-1, "unknown")]
    public void LevelName_maps_the_Microsoft_Extensions_Logging_LogLevel_ordinals(int level, string expected) =>
        Assert.Equal(expected, ConnectorRegistryFactory.LevelName(level));

    [Fact]
    public void FoldException_appends_the_exceptionMessage_field_when_present()
    {
        var fields = new Dictionary<string, string> { ["exceptionMessage"] = "connection refused" };

        Assert.Equal("read failed: connection refused", ConnectorRegistryFactory.FoldException("read failed", fields));
    }

    [Fact]
    public void FoldException_leaves_the_message_untouched_with_no_exceptionMessage_field()
    {
        var fields = new Dictionary<string, string> { ["category"] = "Kafka" };

        Assert.Equal("read failed", ConnectorRegistryFactory.FoldException("read failed", fields));
    }

    [Fact]
    public void FoldException_leaves_the_message_untouched_when_exceptionMessage_is_empty()
    {
        var fields = new Dictionary<string, string> { ["exceptionMessage"] = "" };

        Assert.Equal("read failed", ConnectorRegistryFactory.FoldException("read failed", fields));
    }

    // A connector host failing during a connector call (planning opens a hosted connector, which spawns it) keeps
    // the host's own code and hint, exactly as one while the registry is built does -- never PZ0500.
    [Fact]
    public async Task Registry_describes_a_host_failure_with_its_own_code_and_hint()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-registry-host-failure", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var project = new Pz.Core.Model.PzProject("p", "0.1.0", new Pz.Core.Model.EngineConfig(),
                new Dictionary<string, object?>(), [], [], []);
            var (registry, hosts) = await ConnectorRegistryFactory.CreateAsync(project, dir, noLockCheck: true, CancellationToken.None);
            await using var _ = hosts;

            var described = registry.DescribeHostFailure!(new Pz.PackageManagement.Hosting.ConnectorHostException(
                "PZ0355", "could not start connector 'acme'", "check the executable"));

            Assert.Equal(new Pz.Engine.Execution.ConnectorHostFailure("PZ0355", "could not start connector 'acme'",
                "check the executable"), described);
            Assert.Null(registry.DescribeHostFailure(new InvalidOperationException("engine bug")));

            // The one place a host failure's code decides whether it is fatal: a connector process that broke
            // protocol or died while it was in use stops pz on its own terms; one that never started or
            // handshook, or a missing package, is an install problem the author fixes.
            foreach (var (code, fatal) in new[]
            {
                ("PZ0304", false), ("PZ0305", false), ("PZ0306", false), ("PZ0307", false),
                ("PZ0355", false), ("PZ0356", false), ("PZ0357", true), ("PZ0358", true),
            })
            {
                Assert.Equal(fatal, registry.DescribeHostFailure(
                    new Pz.PackageManagement.Hosting.ConnectorHostException(code, "m"))!.IsFatal);
            }
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }
}
