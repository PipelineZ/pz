using System.Runtime.CompilerServices;
using Pz.PackageManagement.ProcessHosting;
using Pz.TestSupport;

namespace Pz.EndToEnd.Tests;

internal static class TestAssemblySetup
{
    /// <summary>The process-hosted runs here spawn a cold .NET connector while the rest of the solution's test hosts
    /// load the machine; the production handshake budget would make their outcome depend on machine speed. A real
    /// hang is still caught by the test run's hang timeout (CI's --blame-hang).</summary>
    [ModuleInitializer]
    internal static void LiftTheHandshakeBudget() => PcpClient.DefaultHandshakeTimeout = Timeout.InfiniteTimeSpan;

    /// <summary>Runs before any test code can parse an image name, so a loaded machine cannot time out
    /// Testcontainers' image-reference regex (see <see cref="TestcontainersRegex"/>).</summary>
    [ModuleInitializer]
    internal static void LiftTheImageRegexTimeout() => TestcontainersRegex.LiftMatchTimeout();
}
