using System.CommandLine;
using System.Runtime.InteropServices;
using Pz.Connectors.Abstractions;
using Pz.Core.Loading;
using Pz.Core.Validation;
using Pz.PackageManagement.Hosting;
using Pz.PackageManagement.ProcessHosting.Conformance;

namespace Pz.Cli.Commands;

/// <summary>`pz connector test &lt;entrypoint-path-or-package-dir&gt; [--config file.yml]`: runs
/// <see cref="ConformanceSuite"/>'s black-box protocol checks against one out-of-process connector and
/// prints each vector pass/fail/skip by name. Aggregate-all-failures (the error philosophy binding
/// convention): every applicable vector runs regardless of earlier failures, and the report is what
/// decides the exit code, never a first failure short-circuiting the rest.
///
/// <para>Exit codes: 0 every applicable vector passed; 1 one or more vectors failed; 2 a config/usage
/// problem (unknown path, malformed manifest or --config) meant no vector could even be attempted.</para></summary>
internal static class ConnectorTestCommand
{
    public static Command Create()
    {
        var targetArgument = new Argument<string>("target")
        {
            Description = "Path to a connector package directory (containing pz.connector.json) or a bare entrypoint binary",
        };
        var configOption = new Option<string?>("--config")
        {
            Description = "YAML file naming the connection to configure and the read:/write: dataset(s) to probe",
        };
        var test = new Command("test",
            "Run black-box PCP protocol conformance checks against an out-of-process connector");
        test.Arguments.Add(targetArgument);
        test.Options.Add(configOption);
        test.SetAction((parseResult, ct) => Execute(
            parseResult.GetValue(targetArgument)!,
            parseResult.GetValue(configOption),
            ct));

        var connector = new Command("connector", "Out-of-process connector tooling");
        connector.Subcommands.Add(test);
        return connector;
    }

    internal static async Task<int> Execute(string target, string? configPath, CancellationToken ct)
    {
        string socketRoot;
        bool ownsSocketRoot;
        try
        {
            (socketRoot, ownsSocketRoot) = ProcessSocketRoot.Resolve(Directory.GetCurrentDirectory(), runId: null);
        }
        catch (ConnectorHostException ex)
        {
            return ReportConfigError(ex);
        }

        try
        {
            string entrypoint;
            ConnectorManifest? manifest;
            string packageName;
            ConnectorConfig connection;
            ConformanceReadProbe? readProbe;
            ConformanceWriteProbe? writeProbe;
            try
            {
                (entrypoint, manifest, packageName) = ResolveTarget(target);
                (connection, readProbe, writeProbe) = LoadProbeConfig(configPath);
            }
            catch (ConnectorHostException ex)
            {
                return ReportConfigError(ex);
            }
            catch (PzConfigException ex)
            {
                Console.Error.WriteLine($"error {ex.Error}");
                return ExitCodes.ConfigError;
            }

            var request = new ConformanceRequest(entrypoint, packageName, manifest, "conformance", connection, readProbe, writeProbe);

            ConformanceReport report;
            try
            {
                report = await ConformanceSuite.RunAsync(request, socketRoot, ct).ConfigureAwait(false);
            }
            catch (ConnectorHostException ex)
            {
                // Raised only for a setup failure that precedes every vector (entrypoint missing / not
                // executable, socket-root exhausted) -- a handshake-discipline failure is reported as
                // the "handshake" vector instead and never reaches this catch.
                return ReportConfigError(ex);
            }

            foreach (var vector in report.Vectors)
            {
                var status = vector.Outcome switch
                {
                    ConformanceOutcome.Passed => "PASS",
                    ConformanceOutcome.Failed => "FAIL",
                    ConformanceOutcome.Skipped => "SKIP",
                    _ => "????",
                };
                Console.WriteLine(vector.Detail is null
                    ? $"{status} {vector.Name}"
                    : $"{status} {vector.Name}: {vector.Detail}");
            }

            return report.AnyFailed ? ExitCodes.NodeFailures : ExitCodes.Ok;
        }
        finally
        {
            if (ownsSocketRoot)
            {
                try
                {
                    Directory.Delete(socketRoot, recursive: true);
                }
                catch
                {
                    // best-effort; a leftover temp directory is a cleanliness issue, not a correctness one
                }
            }
        }
    }

    private static int ReportConfigError(ConnectorHostException ex)
    {
        Console.Error.WriteLine(ex.Hint is null
            ? $"error {ex.Code}: {ex.Message}"
            : $"error {ex.Code}: {ex.Message} — hint: {ex.Hint}");
        return ExitCodes.ConfigError;
    }

    /// <summary>A directory must ship a <c>pz.connector.json</c> declaring <c>runtime: "process"</c> --
    /// exactly what a restored package layout provides, and what <see cref="PcpClient"/>'s identity/
    /// capability handshake gates check the connector against once spawned. A bare file is taken
    /// literally as the entrypoint, with no manifest -- those gates simply do not run for it.</summary>
    private static (string Entrypoint, ConnectorManifest? Manifest, string PackageName) ResolveTarget(string target)
    {
        if (Directory.Exists(target))
        {
            var manifest = ManifestReader.TryRead(target) ?? throw new ConnectorHostException(
                PzErrorCode.ProcessEntrypointMissing,
                $"'{target}' is a directory but ships no pz.connector.json",
                "point at a connector package directory containing pz.connector.json, or at the entrypoint binary itself");

            if (manifest.Runtime != "process")
            {
                throw new ConnectorHostException(
                    PzErrorCode.ProcessEntrypointMissing,
                    $"'{target}' declares runtime '{manifest.Runtime ?? "dotnet"}', which is not hosted out of process",
                    "pz connector test only conformance-checks a package declaring runtime: \"process\"");
            }

            var rid = RuntimeInformation.RuntimeIdentifier;
            var entrypoint = ManifestReader.ResolveEntrypoint(manifest, target, rid);
            if (!File.Exists(entrypoint))
            {
                throw new ConnectorHostException(
                    PzErrorCode.ProcessEntrypointMissing,
                    $"'{target}' declares an entrypoint for RID '{rid}' that does not exist: '{entrypoint}'",
                    "rebuild the connector package for this platform");
            }

            var packageName = manifest.Name is { Length: > 0 } name ? name : Path.GetFileName(target.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar));
            return (entrypoint, manifest, packageName);
        }

        if (File.Exists(target))
        {
            return (target, null, Path.GetFileNameWithoutExtension(target));
        }

        throw new ConnectorHostException(
            PzErrorCode.ProcessEntrypointMissing,
            $"'{target}' does not exist",
            "pass a connector package directory (containing pz.connector.json) or an entrypoint binary path");
    }

    /// <summary>--config's shape: <c>connection:</c> (Configure RPC values), optional <c>read:</c>
    /// (<c>dataset:</c> plus dataset options) and/or <c>write:</c> (<c>output:</c>, <c>mode:</c>,
    /// <c>schema_policy:</c>, plus output options) -- whichever of the two is present is what tells the
    /// suite which direction(s) it has anything to probe, per <see cref="ConformanceSuite"/>'s own
    /// direction-gating doc.</summary>
    private static (ConnectorConfig Connection, ConformanceReadProbe? Read, ConformanceWriteProbe? Write) LoadProbeConfig(
        string? configPath)
    {
        if (configPath is null)
        {
            return (ConnectorConfig.Empty, null, null);
        }

        var root = YamlMapper.LoadFile(configPath, configPath);
        var connection = new ConnectorConfig(AsMap(root.GetValueOrDefault("connection")));

        ConformanceReadProbe? read = null;
        var readMap = AsMap(root.GetValueOrDefault("read"));
        if (readMap.Count > 0)
        {
            if (readMap.GetValueOrDefault("dataset") is not string dataset || dataset.Length == 0)
            {
                throw ConfigShapeError(configPath, "--config's read: block needs a non-empty 'dataset' name");
            }

            read = new ConformanceReadProbe(dataset, Without(readMap, "dataset"));
        }

        ConformanceWriteProbe? write = null;
        var writeMap = AsMap(root.GetValueOrDefault("write"));
        if (writeMap.Count > 0)
        {
            if (writeMap.GetValueOrDefault("output") is not string output || output.Length == 0)
            {
                throw ConfigShapeError(configPath, "--config's write: block needs a non-empty 'output' name");
            }

            var mode = writeMap.GetValueOrDefault("mode") as string ?? "replace";
            var schemaPolicy = writeMap.GetValueOrDefault("schema_policy") as string ?? "match";
            write = new ConformanceWriteProbe(output, mode, schemaPolicy, Without(writeMap, "output", "mode", "schema_policy"));
        }

        return (connection, read, write);
    }

    private static PzConfigException ConfigShapeError(string configPath, string message) =>
        new(new PzError(PzErrorCode.YamlShape, message, configPath, null, "see the --config file's read:/write: shape"));

    private static Dictionary<string, object?> AsMap(object? value) =>
        value as Dictionary<string, object?> ?? new Dictionary<string, object?>(StringComparer.Ordinal);

    private static Dictionary<string, object?> Without(Dictionary<string, object?> map, params string[] keys) =>
        map.Where(kv => !keys.Contains(kv.Key, StringComparer.Ordinal))
            .ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal);
}
