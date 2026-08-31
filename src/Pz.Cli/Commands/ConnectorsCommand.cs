using System.CommandLine;
using System.Reflection;
using Pz.Connectors.Abstractions;
using Pz.Core.Loading;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Execution;
using Pz.PackageManagement.Hosting;

namespace Pz.Cli.Commands;

/// <summary>`pz connectors`: builds the registry exactly like `run`/`plan`
/// (<see cref="ConnectorRegistryFactory.CreateAsync"/> -- builtins + restored per lock, drift rules
/// honored; a drifted lock is refused with the same PZ0321 remediation, since this is a read-only
/// reporting verb, not a bypass) and prints one row per registered connector: `name package version
/// tiers capabilities`.</summary>
internal static class ConnectorsCommand
{
    public static Command Create()
    {
        var projectOption = new Option<string?>("--project") { Description = "Project directory (default: current directory)" };
        var varsOption = new Option<string?>("--vars") { Description = "JSON object of var overrides" };
        var command = new Command("connectors", "List every registered connector (builtin + restored) and its capabilities/tiers");
        command.Options.Add(projectOption);
        command.Options.Add(varsOption);
        command.SetAction((parseResult, ct) => Execute(
            parseResult.GetValue(projectOption) ?? Directory.GetCurrentDirectory(),
            parseResult.GetValue(varsOption),
            ct));
        return command;
    }

    private static readonly string PzInformationalVersion =
        typeof(ConnectorsCommand).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()
            ?.InformationalVersion ?? "unknown";

    internal static async Task<int> Execute(string projectDir, string? varsJson, CancellationToken ct)
    {
        try
        {
            var env = SharedInputHelpers.SnapshotEnvironment();
            var overrides = SharedInputHelpers.ParseVars(varsJson);
            var project = ProjectLoader.Load(projectDir, env, overrides);

            var (registry, host) = await ConnectorRegistryFactory.CreateAsync(project, projectDir, noLockCheck: false, ct);
            await using var connectorHost = host;

            var hostedVersions = host?.Installed.ToDictionary(i => i.Name, i => i.Version, StringComparer.Ordinal)
                ?? new Dictionary<string, string>(StringComparer.Ordinal);
            var hostedPackage = DescribeHostedPackage(project, host);

            var names = registry.Sources.Keys
                .Union(registry.Sinks.Keys, StringComparer.Ordinal)
                .OrderBy(n => n, StringComparer.Ordinal);

            Console.WriteLine($"{"name",-14} {"package",-28} {"version",-16} {"tiers",-18} capabilities");
            foreach (var name in names)
            {
                registry.Sources.TryGetValue(name, out var source);
                registry.Sinks.TryGetValue(name, out var sink);
                IConnector? connector = source is not null ? source : sink;
                var capabilities = connector?.Capabilities ?? ConnectorCapabilities.None;

                var (package, version) = hostedVersions.TryGetValue(name, out var hostedVersion)
                    ? (hostedPackage, hostedVersion)
                    : ("pz (builtin)", PzInformationalVersion);

                var tiers = FormatTiers(source, sink, capabilities);
                Console.WriteLine($"{name,-14} {package,-28} {version,-16} {tiers,-18} {FormatCapabilities(capabilities)}");
            }

            return ExitCodes.Ok;
        }
        catch (PzValidationException ex)
        {
            foreach (var error in ex.Errors)
                Console.Error.WriteLine($"error {error}");
            return ExitCodes.ConfigError;
        }
    }

    /// <summary>Tiers rule: source side -- <see cref="ConnectorCapabilities.NativeScan"/>
    /// gives "native+universal" unless the connector is <see cref="INativeOnlySource"/>, in which case
    /// there is no universal fallback at all and the token is "native-only"; the absence of
    /// <c>NativeScan</c> gives "universal" alone. Sink side -- <see
    /// cref="ConnectorCapabilities.NativeCopy"/> gives "native+universal" unless the connector is
    /// <see cref="INativeOnlySink"/>, in which case there is no universal fallback at all and the token
    /// is "native-only"; the absence of <c>NativeCopy</c> gives "universal" alone. Only sides the
    /// connector is actually registered under (source and/or sink) are rendered.</summary>
    private static string FormatTiers(ISourceConnector? source, ISinkConnector? sink, ConnectorCapabilities capabilities)
    {
        var parts = new List<string>();
        if (source is not null)
        {
            var hasNative = capabilities.HasFlag(ConnectorCapabilities.NativeScan);
            var token = hasNative
                ? (source is INativeOnlySource ? "native-only" : "native+universal")
                : "universal";
            parts.Add("src:" + token);
        }

        if (sink is not null)
        {
            var hasNative = capabilities.HasFlag(ConnectorCapabilities.NativeCopy);
            var token = hasNative
                ? (sink is INativeOnlySink ? "native-only" : "native+universal")
                : "universal";
            parts.Add("snk:" + token);
        }

        return string.Join(" ", parts);
    }

    private static string FormatCapabilities(ConnectorCapabilities capabilities)
    {
        if (capabilities == ConnectorCapabilities.None)
        {
            return "-";
        }

        var flags = Enum.GetValues<ConnectorCapabilities>()
            .Where(f => f != ConnectorCapabilities.None && capabilities.HasFlag(f))
            .Select(f => f.ToString());
        return string.Join(",", flags);
    }

    /// <summary>Best-effort package attribution for a hosted connector: the host's <c>Installed</c>
    /// reports each connector's manifest name and package version (see <see
    /// cref="ConnectorRegistryFactory"/>), not which restored package registered it, so this names the
    /// sole declared non-builtin package when there is exactly one (the common case), mirroring
    /// <c>ConnectorRegistryFactory.DescribePackages</c>'s same-ambiguity precedent.</summary>
    private static string DescribeHostedPackage(PzProject project, ConnectorHosts? host)
    {
        if (host is null)
        {
            return string.Empty;
        }

        var nonBuiltin = project.Connectors
            .Where(c => !BuiltinConnectors.PackageIds.Contains(c.Package))
            .Select(c => c.Package)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(p => p, StringComparer.Ordinal)
            .ToArray();

        return nonBuiltin.Length switch
        {
            0 => string.Empty,
            1 => nonBuiltin[0],
            _ => "one of " + string.Join(", ", nonBuiltin),
        };
    }
}
