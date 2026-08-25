using System.Runtime.InteropServices;
using System.Text.Json;
using Pz.Core.Loading;
using Pz.Core.Validation;
using Pz.PackageManagement.Restore;

namespace Pz.Cli.Tests;

/// <summary>A hosted (restored) connector whose <c>[assembly: PzConnector]</c> name collides with a
/// builtin's must be rejected loudly (PZ0305), never silently replace the trusted
/// builtin. Shares <see cref="CliLocalFeedFixture"/> (and the "console-and-env-serialized" collection --
/// see its definition in RestoreCommandTests.cs) with <see cref="RestoreCommandTests"/>; also redirects
/// Console.Error, which is the other reason it must serialize against the rest of that collection. It
/// also packs <c>FakeBuiltinCollider</c>, whose registered connector name ("localfiles") deliberately
/// collides with <c>BuiltinConnectors</c>'.</summary>
[Collection("console-and-env-serialized")]
public sealed class ConnectorRegistryFactoryTests(CliLocalFeedFixture feed) : IDisposable
{
    private readonly string _work = Path.Combine(
        Path.GetTempPath(), "pz-registry-factory-tests", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task Hosted_connector_colliding_with_builtin_name_is_error_PZ0305()
    {
        Directory.CreateDirectory(_work);
        File.WriteAllText(Path.Combine(_work, "project.yml"), $"""
            name: collider_test
            version: 0.1.0

            connectors:
              - package: FakeBuiltinCollider
                version: 1.0.0
            """);

        Assert.Equal(ExitCodes.Ok, CliApp.Build().Parse(["restore", "--project", _work, "--feeds", feed.FeedDir]).Invoke());
        Assert.True(File.Exists(Path.Combine(_work, "pz.lock.json")));

        var project = ProjectLoader.Load(_work, new Dictionary<string, string>());

        var ex = await Assert.ThrowsAsync<PzValidationException>(() =>
            ConnectorRegistryFactory.CreateAsync(project, _work, noLockCheck: false, CancellationToken.None));

        var error = Assert.Single(ex.Errors);
        Assert.Equal(PzErrorCode.ConnectorNotInstalled, error.Code);
        Assert.Contains("FakeBuiltinCollider", error.Message);
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
        File.WriteAllText(Path.Combine(_work, "project.yml"), $"""
            name: collider_run_test
            version: 0.1.0

            connectors:
              - package: FakeBuiltinCollider
                version: 1.0.0
            """);

        Assert.Equal(ExitCodes.Ok, CliApp.Build().Parse(["restore", "--project", _work, "--feeds", feed.FeedDir]).Invoke());

        var stderr = RunAndCaptureStderr(["run", "--project", _work]);
        var exit = CliApp.Build().Parse(["run", "--project", _work]).Invoke();

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0305", stderr);
        Assert.Contains("localfiles", stderr);
    }

    // A restored package shipping no pz.connector.json must still load, but the note that it's running
    // without a pre-load handshake has to reach the user (ConnectorHost.LoadFromDirectory's missing-manifest
    // `warn` callback). FakeSourceConnector ships a pz.connector.json (see MaterializerTests), so its
    // manifest is deleted post-restore to exercise that branch without a dedicated no-manifest fixture
    // package.
    [Fact]
    public void Run_with_manifest_less_hosted_connector_succeeds_and_warns_on_stderr()
    {
        Directory.CreateDirectory(_work);
        File.WriteAllText(Path.Combine(_work, "project.yml"), $"""
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

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Contains("warning: note: 'FakeSourceConnector' ships no pz.connector.json", stderr);
    }

    /// <summary>Neither host can see the other's package set, so each one's own PZ0305 duplicate-name
    /// check passes and the collision only exists in the merged registry. Nothing is spawned: the
    /// refusal happens at registration, so the process package's entrypoint only has to exist.</summary>
    [Fact]
    public async Task Connector_name_registered_by_both_hosts_is_error_PZ0305()
    {
        Directory.CreateDirectory(_work);
        File.WriteAllText(Path.Combine(_work, "project.yml"), """
            name: cross_host_collider_test
            version: 0.1.0

            connectors:
              - package: FakeSourceConnector
                version: 1.2.3
            """);

        Assert.Equal(ExitCodes.Ok, CliApp.Build().Parse(["restore", "--project", _work, "--feeds", feed.FeedDir]).Invoke());

        // Declared after the restore that produced the lock, then folded into that lock by hand: no
        // feed can serve a process package yet, and what is under test is the registry's merge, not
        // restore.
        File.AppendAllText(Path.Combine(_work, "project.yml"), """

              - package: FakeSourcePcp
                version: 1.0.0
            """);
        WriteProcessPackageClaiming("fakesource");

        var project = ProjectLoader.Load(_work, new Dictionary<string, string>());

        var ex = await Assert.ThrowsAsync<PzValidationException>(() =>
            ConnectorRegistryFactory.CreateAsync(project, _work, noLockCheck: false, CancellationToken.None));

        var error = Assert.Single(ex.Errors);
        Assert.Equal(PzErrorCode.ConnectorNotInstalled, error.Code);
        Assert.Contains("fakesource", error.Message);
        Assert.Contains("out-of-process", error.Message);
        Assert.NotNull(error.Hint);
    }

    private void WriteProcessPackageClaiming(string connectorName)
    {
        const string packageId = "FakeSourcePcp";
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

        var lockPath = Path.Combine(_work, "pz.lock.json");
        var existing = LockFileWriter.Read(lockPath)!;
        LockFileWriter.Write(
            existing with
            {
                Packages = [.. existing.Packages,
                    new LockedPackage(packageId, version, "sha512-cross-host-fixture", new LockedAssets([], []))],
            },
            lockPath);
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
