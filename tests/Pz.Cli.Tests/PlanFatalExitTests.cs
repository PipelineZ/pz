using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Text.Json;
using Pz.Cli;
using Pz.PackageManagement.Restore;

namespace Pz.Cli.Tests;

/// <summary>A connector that fails while the plan probes it -- a defect, not a refusal of the config -- stops pz on
/// its own terms: `pz plan` and `pz run` exit 3 (fatal) with PZ0330 naming the connector, never exit 2's "fix your
/// config" and never PZ0500. Uses the real out-of-process PcpFakeConnector, whose native-scan probe throws an
/// unhandled exception under <c>--throw-unhandled-at-plan</c>; the SDK codes that as a connector defect. Linux
/// only: the staged package's entrypoint is a <c>#!/bin/sh</c> wrapper.</summary>
// See the "console-and-env-serialized" collection definition in RestoreCommandTests.cs.
[Trait("Category", "Pcp")]
[Collection("console-and-env-serialized")]
public sealed class PlanFatalExitTests : IDisposable
{
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-plan-fatal-tests", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [SkippableTheory]
    [InlineData("plan")]
    [InlineData("run")]
    [SupportedOSPlatform("linux")]
    public void A_connector_defect_while_planning_exits_fatal_with_PZ0330(string verb)
    {
        Skip.If(OperatingSystem.IsWindows(), "this test stages a #!/bin/sh wrapper as the package entrypoint, which is POSIX-only");

        Directory.CreateDirectory(Path.Combine(_work, "pipelines"));
        Directory.CreateDirectory(Path.Combine(_work, "data"));
        File.WriteAllText(Path.Combine(_work, "project.yml"),
            "name: fatal\nversion: 0.1.0\nconnectors:\n  - package: LocalFilesPcp\n    version: 1.0.0\n");
        File.WriteAllText(Path.Combine(_work, "data", "orders.csv"), "id\n1\n");
        File.WriteAllText(Path.Combine(_work, "connections.yml"),
            "pcp:\n  connector: localfiles-pcp\n  entities:\n    orders:\n" +
            "      read: { path: data/orders.csv, format: csv, columns: { id: bigint } }\n" +
            "\nlake:\n  connector: localfiles\n  root: out\n");
        File.WriteAllText(Path.Combine(_work, "pipelines", "orders_out.sql"),
            "INSERT INTO {{ sink('lake', 'orders_out', format: 'parquet') }}\nselect * from {{ source('pcp', 'orders') }}\n");
        WriteSpawnableProcessPackage("--throw-unhandled-at-plan");

        var stderr = new StringWriter();
        var originalErr = Console.Error;
        var originalOut = Console.Out;
        Console.SetError(stderr);
        Console.SetOut(new StringWriter());
        int exit;
        try
        {
            exit = CliApp.Build().Parse([verb, "--project", _work]).Invoke();
        }
        finally
        {
            Console.SetError(originalErr);
            Console.SetOut(originalOut);
        }

        var output = stderr.ToString();
        Assert.True(exit == ExitCodes.Fatal, output);
        Assert.Contains("error PZ0330: connection 'pcp' dataset 'orders': connector 'localfiles-pcp' failed while planning",
            output);
        Assert.DoesNotContain("PZ0500", output);
        Assert.DoesNotContain("PZ0301", output);
    }

    /// <summary>Mirrors <c>ConnectorsCommandTests.WriteSpawnableProcessPackage</c>, with fixture switches baked
    /// into the wrapper script (the host passes the entrypoint only <c>--pz-socket</c>).</summary>
    [SupportedOSPlatform("linux")]
    private void WriteSpawnableProcessPackage(string switches)
    {
        const string packageId = "LocalFilesPcp";
        const string version = "1.0.0";
        var packageDir = Path.Combine(_work, ".pz", "packages", packageId, version);
        var binDir = Path.Combine(packageDir, "bin");
        Directory.CreateDirectory(binDir);

        var entrypoint = Path.Combine(binDir, "connector");
        File.WriteAllText(entrypoint, $"#!/bin/sh\nexec \"{FixtureExecutablePath()}\" {switches} \"$@\"\n");
        File.SetUnixFileMode(entrypoint,
            UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute |
            UnixFileMode.GroupRead | UnixFileMode.GroupExecute);

        File.WriteAllText(Path.Combine(packageDir, "pz.connector.json"), JsonSerializer.Serialize(
            new Dictionary<string, object?>
            {
                ["name"] = "localfiles-pcp",
                ["protocolMajorMin"] = 1,
                ["protocolMajorMax"] = 1,
                ["capabilities"] = new[]
                {
                    "NativeScan", "NativeCopy", "ReplaceWrites", "BoundedWindow", "PartitionedRead",
                },
                ["runtime"] = "process",
                ["entrypoints"] = new Dictionary<string, string>
                {
                    [RuntimeInformation.RuntimeIdentifier] = "bin/connector",
                },
                ["sdk"] = new Dictionary<string, string> { ["name"] = "Pz.Connectors.Sdk", ["version"] = "0.7.0" },
            }));

        LockFileWriter.Write(
            new LockFile(LockFileWriter.CurrentVersion, RuntimeInformation.RuntimeIdentifier, [
                new LockedPackage(packageId, version, "sha512-plan-fatal-fixture", new LockedAssets([], [])),
            ]),
            Path.Combine(_work, "pz.lock.json"));
    }

    private static string FixtureExecutablePath()
    {
        var baseDir = new DirectoryInfo(AppContext.BaseDirectory);
        var tfm = baseDir.Name;
        var config = baseDir.Parent!.Name;
        var testsDir = baseDir.Parent!.Parent!.Parent!.Parent!.FullName;
        return Path.Combine(testsDir, "fixtures", "PcpFakeConnector", "bin", config, tfm, "PcpFakeConnector");
    }
}
