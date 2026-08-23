using System.Reflection;
using System.Runtime.Loader;

namespace Pz.PackageManagement.Hosting;

/// <summary>One collectible <see cref="AssemblyLoadContext"/> per connector package. Assemblies named in
/// <see cref="SharedAssemblies.Names"/> defer to the default ALC (unification); everything else is
/// probed for and loaded privately from this package's <c>lib/</c> directory, so two packages can carry
/// conflicting versions of the same dependency without colliding. Unmanaged libraries are probed for in
/// the sibling <c>native/</c> directory only — which is why <c>PackageMaterializer</c> must flatten a
/// dependency's native assets into the connector package's own directory, not leave them in the
/// dependency's.</summary>
internal sealed class ConnectorLoadContext(string packageId, string libDir) : AssemblyLoadContext(packageId, isCollectible: true)
{
    protected override Assembly? Load(AssemblyName assemblyName)
    {
        if (assemblyName.Name is not null && SharedAssemblies.Names.Contains(assemblyName.Name))
        {
            return null; // defer to the default ALC's copy
        }

        if (assemblyName.Name is null)
        {
            return null;
        }

        var candidate = Path.Combine(libDir, assemblyName.Name + ".dll");
        return File.Exists(candidate) ? LoadFromAssemblyPath(candidate) : null; // framework assemblies fall through
    }

    protected override IntPtr LoadUnmanagedDll(string unmanagedDllName)
    {
        var nativeDir = Path.Combine(libDir, "..", "native");
        if (!Directory.Exists(nativeDir))
        {
            return IntPtr.Zero;
        }

        foreach (var candidateName in NativeCandidateNames(unmanagedDllName))
        {
            var candidatePath = Path.Combine(nativeDir, candidateName);
            if (File.Exists(candidatePath))
            {
                return LoadUnmanagedDllFromPath(candidatePath);
            }
        }

        // Falling through to the default probing logic (which would search the host's own directories)
        // would load a library this connector never asked for, so an unresolved name stays unresolved.
        return IntPtr.Zero;
    }

    private static IEnumerable<string> NativeCandidateNames(string unmanagedDllName)
    {
        yield return unmanagedDllName;

        if (OperatingSystem.IsWindows())
        {
            yield return unmanagedDllName + ".dll";
        }
        else if (OperatingSystem.IsMacOS())
        {
            yield return "lib" + unmanagedDllName + ".dylib";
            yield return unmanagedDllName + ".dylib";
        }
        else
        {
            yield return "lib" + unmanagedDllName + ".so";
            yield return unmanagedDllName + ".so";
        }
    }
}
