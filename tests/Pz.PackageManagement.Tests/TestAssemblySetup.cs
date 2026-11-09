using System.Runtime.CompilerServices;
using Pz.PackageManagement.ProcessHosting;

namespace Pz.PackageManagement.Tests;

internal static class TestAssemblySetup
{
    /// <summary>Process-spawning tests here dial a cold .NET fixture while the rest of the solution's test hosts load
    /// the machine; the production handshake budget would make their outcome depend on machine speed. A real hang is
    /// still caught by the test run's hang timeout (CI's --blame-hang).</summary>
    [ModuleInitializer]
    internal static void LiftTheHandshakeBudget() => PcpClient.DefaultHandshakeTimeout = Timeout.InfiniteTimeSpan;
}
