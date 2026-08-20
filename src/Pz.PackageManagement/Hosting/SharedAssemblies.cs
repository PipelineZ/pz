namespace Pz.PackageManagement.Hosting;

/// <summary>Assembly names that every connector ALC resolves to the host's (default ALC) copy instead
/// of loading privately. Keeping these unified lets <c>IConnector</c> instances, Arrow batches, and
/// logging abstractions cross the ALC boundary without proxying or reflection gymnastics. Everything
/// else a connector package ships is loaded privately into its own collectible ALC.</summary>
public static class SharedAssemblies
{
    public static readonly IReadOnlySet<string> Names = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        { "Pz.Connectors.Abstractions", "Apache.Arrow", "Microsoft.Extensions.Logging.Abstractions" };
}
