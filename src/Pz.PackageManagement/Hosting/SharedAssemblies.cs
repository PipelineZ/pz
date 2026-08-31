namespace Pz.PackageManagement.Hosting;

/// <summary>Dependency ids <c>pz restore</c> never resolves or materializes into a connector
/// package's own <c>lib/</c>: they ship with pz itself, so a package pulling its own copy could only
/// ever disagree with the host's.</summary>
public static class SharedAssemblies
{
    public static readonly IReadOnlySet<string> Names = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        { "Pz.Connectors.Abstractions", "Apache.Arrow", "Microsoft.Extensions.Logging.Abstractions" };
}
