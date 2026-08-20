namespace Pz.PackageManagement.Hosting;

/// <summary>Identifies one installed connector package by id and version, as laid out under
/// <c>&lt;packagesRoot&gt;/&lt;PackageId&gt;/&lt;Version&gt;/lib/*.dll</c>.</summary>
public sealed record ConnectorPackageRef(string PackageId, string Version);
