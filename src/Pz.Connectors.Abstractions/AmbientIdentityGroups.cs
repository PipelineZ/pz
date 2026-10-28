namespace Pz.Connectors.Abstractions;

/// <summary>The cloud identity groups an out-of-process connector may declare in its manifest
/// (<c>ambientIdentity</c>), each asking the host to forward the environment variables that cloud's
/// platforms use to tell an SDK where its ambient identity is. The names live here, shared by the SDK
/// that writes a manifest and the host that reads one, so the SDK can refuse a typo at pack time; which
/// variables each group carries is the host's decision alone (<c>Pz.PackageManagement</c>'s
/// <c>AmbientIdentity</c>).</summary>
public static class AmbientIdentityGroups
{
    public const string Aws = "aws";
    public const string Azure = "azure";
    public const string Gcp = "gcp";

    /// <summary>Every group this build knows, ordinal-sorted: the order a manifest lists them in.</summary>
    public static IReadOnlyList<string> All { get; } = [Aws, Azure, Gcp];

    public static bool IsKnown(string group) => All.Contains(group, StringComparer.Ordinal);
}
