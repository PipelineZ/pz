using System.Reflection;
using Pz.Connectors.Abstractions;

namespace Pz.PackageManagement.Hosting;

/// <summary>Loads a fixed set of connector packages, one collectible <see cref="ConnectorLoadContext"/>
/// per package, and exposes the connectors they register via <c>[assembly: PzConnector]</c>. Error codes
/// below are the PZ03xx literals pinned by <c>Pz.Core.Validation.PzErrorCode</c> (see
/// <c>Hosting/HostErrorCodeTests.cs</c>); they are hardcoded here rather than referenced because this
/// assembly must not depend on Pz.Core.</summary>
public sealed class ConnectorHost : IAsyncDisposable
{
    private readonly List<ConnectorLoadContext> _contexts;
    private readonly Dictionary<string, IConnector> _connectorsByName;

    private ConnectorHost(List<ConnectorLoadContext> contexts, Dictionary<string, IConnector> connectorsByName)
    {
        _contexts = contexts;
        _connectorsByName = connectorsByName;
    }

    /// <summary>Test seam only: invoked synchronously with each <see cref="ConnectorLoadContext"/> as it
    /// is created by <see cref="LoadFromDirectory"/>, before any unload-on-failure cleanup runs. Lets
    /// tests capture a <see cref="WeakReference"/> to a context without holding a strong reference of
    /// their own. Not part of the public API; never set outside tests.</summary>
    internal static Action<ConnectorLoadContext>? OnContextCreatedForTests;

    /// <summary>Layout consumed: <c>&lt;packagesRoot&gt;/&lt;PackageId&gt;/&lt;Version&gt;/lib/*.dll</c>
    /// (+ <c>native/*</c> in later phases). Entry assembly: <c>&lt;PackageId&gt;.dll</c> carrying at
    /// least one <c>[assembly: PzConnector]</c>.
    ///
    /// <para>Before any assembly is loaded, each package's <c>&lt;packagesRoot&gt;/&lt;PackageId&gt;/
    /// &lt;Version&gt;/pz.connector.json</c> manifest is read via
    /// <see cref="ManifestReader.TryRead"/>: a manifest whose declared protocol-major range excludes
    /// this host's <see cref="ProtocolVersion.Major"/> raises PZ0306 before any
    /// <see cref="ConnectorLoadContext"/> is even created. A package that ships no manifest at all
    /// invokes <paramref name="warn"/> (if given) and proceeds. The post-load check against the
    /// instantiated connector's <see cref="ConnectorInfo.ProtocolMajor"/> stays as a second line of
    /// defense for connectors that misreport their own manifest.</para></summary>
    public static ConnectorHost LoadFromDirectory(
        string packagesRoot, IReadOnlyList<ConnectorPackageRef> required, Action<string>? warn = null)
    {
        var contexts = new List<ConnectorLoadContext>();
        var connectorsByName = new Dictionary<string, IConnector>(StringComparer.Ordinal);

        try
        {
            foreach (var packageRef in required)
            {
                var libDir = Path.Combine(packagesRoot, packageRef.PackageId, packageRef.Version, "lib");
                if (!Directory.Exists(libDir))
                {
                    throw new ConnectorHostException(
                        "PZ0304",
                        $"connector package '{packageRef.PackageId}' {packageRef.Version} not found under {packagesRoot}",
                        "run 'pz restore' or check the packages layout");
                }

                var entryDll = Path.Combine(libDir, packageRef.PackageId + ".dll");
                if (!File.Exists(entryDll))
                {
                    throw new ConnectorHostException(
                        "PZ0304",
                        $"connector package '{packageRef.PackageId}' {packageRef.Version} is missing its entry assembly '{packageRef.PackageId}.dll' under {libDir}",
                        "run 'pz restore' or check the packages layout");
                }

                var packageDir = Path.Combine(packagesRoot, packageRef.PackageId, packageRef.Version);
                var manifest = ManifestReader.TryRead(packageDir);
                if (manifest is null)
                {
                    warn?.Invoke($"note: '{packageRef.PackageId}' ships no pz.connector.json; loading without a pre-load handshake");
                }
                else if (ProtocolVersion.Major < manifest.ProtocolMajorMin || ProtocolVersion.Major > manifest.ProtocolMajorMax)
                {
                    throw new ConnectorHostException(
                        "PZ0306",
                        $"connector package '{packageRef.PackageId}' supports protocol majors {manifest.ProtocolMajorMin}-{manifest.ProtocolMajorMax} but this pz speaks {ProtocolVersion.Major}",
                        "upgrade pz, or pin an older connector version");
                }

                var context = new ConnectorLoadContext(packageRef.PackageId, libDir);
                contexts.Add(context);
                OnContextCreatedForTests?.Invoke(context);
                var entryAssembly = context.LoadFromAssemblyPath(entryDll);

                var attributes = entryAssembly.GetCustomAttributes<PzConnectorAttribute>().ToArray();
                if (attributes.Length == 0)
                {
                    throw new ConnectorHostException(
                        "PZ0307",
                        $"connector package '{packageRef.PackageId}' {packageRef.Version} declares no connectors (entry assembly '{entryDll}' carries no [assembly: PzConnector] attribute)",
                        "add an [assembly: PzConnector(\"name\", typeof(YourConnector))] attribute to the entry assembly");
                }

                foreach (var attribute in attributes)
                {
                    var connector = CreateConnector(packageRef, attribute);

                    var info = connector.Info;
                    if (info.ProtocolMajor != ProtocolVersion.Major)
                    {
                        throw new ConnectorHostException(
                            "PZ0306",
                            $"connector '{attribute.Name}' speaks protocol major {info.ProtocolMajor} but this pz speaks {ProtocolVersion.Major}",
                            "upgrade pz, or pin an older connector version");
                    }

                    if (!connectorsByName.TryAdd(attribute.Name, connector))
                    {
                        throw new ConnectorHostException(
                            "PZ0305",
                            $"connector name '{attribute.Name}' is registered by more than one package",
                            "remove one of the conflicting packages");
                    }
                }
            }
        }
        catch
        {
            // A partially-loaded set must not leak collectible ALCs: unload every context created so
            // far before the original exception propagates. Cleanup failures are swallowed so they
            // never mask the real error (same exception instance, same code/message).
            foreach (var context in contexts)
            {
                try
                {
                    context.Unload();
                }
                catch
                {
                    // best-effort cleanup; the original exception is what the caller needs to see
                }
            }

            throw;
        }

        return new ConnectorHost(contexts, connectorsByName);
    }

    private static IConnector CreateConnector(ConnectorPackageRef packageRef, PzConnectorAttribute attribute)
    {
        object instance;
        try
        {
            instance = Activator.CreateInstance(attribute.ConnectorType)!;
        }
        catch (Exception ex)
        {
            throw new ConnectorHostException(
                "PZ0307",
                $"connector '{attribute.Name}' in package '{packageRef.PackageId}' could not be instantiated from type '{attribute.ConnectorType.FullName}': {ex.Message}",
                "ensure the connector type has a public parameterless constructor");
        }

        if (instance is not IConnector connector)
        {
            throw new ConnectorHostException(
                "PZ0307",
                $"connector '{attribute.Name}' in package '{packageRef.PackageId}': type '{attribute.ConnectorType.FullName}' does not implement IConnector",
                "ensure the type referenced by [assembly: PzConnector] implements IConnector");
        }

        return connector;
    }

    /// <summary>Looks up an installed connector by its registered name.</summary>
    public IConnector Get(string connectorName)
    {
        if (_connectorsByName.TryGetValue(connectorName, out var connector))
        {
            return connector;
        }

        var installed = string.Join(", ", _connectorsByName.Keys.OrderBy(name => name, StringComparer.Ordinal));
        throw new ConnectorHostException(
            "PZ0305",
            $"no connector named '{connectorName}' is installed",
            installed.Length == 0 ? "no connectors are installed" : $"installed connectors: {installed}");
    }

    /// <summary>All installed connectors' identities, ordered by name.</summary>
    public IReadOnlyList<ConnectorInfo> Installed =>
        _connectorsByName.Values
            .Select(connector => connector.Info)
            .OrderBy(info => info.Name, StringComparer.Ordinal)
            .ToArray();

    /// <summary>Unloads every connector's <see cref="ConnectorLoadContext"/>. Unload completion is
    /// nondeterministic (it depends on the GC collecting the ALC) — the contract is "no further use of
    /// connectors from this host after dispose," not an assertion that memory is reclaimed immediately.</summary>
    public ValueTask DisposeAsync()
    {
        _connectorsByName.Clear();
        foreach (var context in _contexts)
        {
            context.Unload();
        }

        _contexts.Clear();
        return ValueTask.CompletedTask;
    }
}
