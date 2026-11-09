using System.Runtime.CompilerServices;
using Pz.TestSupport;

namespace Pz.Connector.SqlServer.Tests;

internal static class TestAssemblySetup
{
    /// <summary>Runs before any test code can parse an image name, so a loaded machine cannot time out
    /// Testcontainers' image-reference regex (see <see cref="TestcontainersRegex"/>).</summary>
    [ModuleInitializer]
    internal static void LiftTheImageRegexTimeout() => TestcontainersRegex.LiftMatchTimeout();
}
