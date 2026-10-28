using Pz.Connectors.Abstractions;

namespace Pz.Connectors.Sdk;

internal abstract record HostCommand;

internal sealed record ServeCommand(string SocketPath) : HostCommand;

internal sealed record ManifestCommand(
    string OutPath, SortedDictionary<string, string> Entrypoints, bool ProjectDirectoryAnchor = false,
    SortedSet<string>? AmbientIdentity = null) : HostCommand;

internal sealed record InvalidCommand(string Message) : HostCommand;

/// <summary>The only argv the SDK understands. Connector configuration never travels here -- it
/// reaches the connector through the Configure RPC and no other way -- so anything unrecognized is
/// refused rather than ignored: a typo that passed silently would be a config option that never
/// arrived.</summary>
internal static class HostArguments
{
    public const string Usage =
        "usage: <connector> --pz-socket <path>\n" +
        "       <connector> --pz-manifest --out <file> [--entrypoint <rid>=<package-relative path>]...\n" +
        "                   [--project-directory-anchor] [--ambient-identity <group>]...";

    public static HostCommand Parse(string[] args)
    {
        string? socket = null;
        string? outPath = null;
        var manifest = false;
        var projectDirectoryAnchor = false;
        var ambientIdentity = new SortedSet<string>(StringComparer.Ordinal);
        var entrypoints = new SortedDictionary<string, string>(StringComparer.Ordinal);

        for (var i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--pz-socket":
                    if (++i >= args.Length)
                    {
                        return new InvalidCommand("--pz-socket needs a socket path");
                    }

                    // An empty/whitespace value passed the "there is an arg" check above but names no
                    // real socket; refused here with a clear message instead of reaching Kestrel's own
                    // ListenUnixSocket, whose failure on "" does not say what was actually wrong.
                    if (string.IsNullOrWhiteSpace(args[i]))
                    {
                        return new InvalidCommand("--pz-socket needs a non-empty socket path");
                    }

                    socket = args[i];
                    break;
                case "--pz-manifest":
                    manifest = true;
                    break;
                case "--out":
                    if (++i >= args.Length)
                    {
                        return new InvalidCommand("--out needs a file path");
                    }

                    outPath = args[i];
                    break;
                case "--entrypoint":
                    if (++i >= args.Length)
                    {
                        return new InvalidCommand("--entrypoint needs <rid>=<path>");
                    }

                    var split = args[i].IndexOf('=', StringComparison.Ordinal);
                    if (split <= 0 || split == args[i].Length - 1)
                    {
                        return new InvalidCommand($"--entrypoint '{args[i]}' is not <rid>=<path>");
                    }

                    entrypoints[args[i][..split]] = args[i][(split + 1)..];
                    break;
                case "--project-directory-anchor":
                    projectDirectoryAnchor = true;
                    break;
                case "--ambient-identity":
                    if (++i >= args.Length)
                    {
                        return new InvalidCommand("--ambient-identity needs a group name");
                    }

                    // Refused here, at pack time, rather than written: the host only warns about a
                    // group it does not know (a newer SDK's, on an older pz), so a typo that reached
                    // the manifest would ship a connector that silently never gets its identity.
                    if (!AmbientIdentityGroups.IsKnown(args[i]))
                    {
                        return new InvalidCommand(
                            $"--ambient-identity '{args[i]}' is not a known group ({string.Join(", ", AmbientIdentityGroups.All)})");
                    }

                    ambientIdentity.Add(args[i]);
                    break;
                default:
                    return new InvalidCommand($"unrecognized argument '{args[i]}'");
            }
        }

        var manifestOnlyOption =
            outPath is not null || entrypoints.Count > 0 || projectDirectoryAnchor || ambientIdentity.Count > 0;
        if (socket is not null && (manifest || manifestOnlyOption))
        {
            return new InvalidCommand("--pz-socket and --pz-manifest are separate modes");
        }

        if (socket is not null)
        {
            return new ServeCommand(socket);
        }

        if (manifest)
        {
            return outPath is null
                ? new InvalidCommand("--pz-manifest needs --out <file>")
                : new ManifestCommand(outPath, entrypoints, projectDirectoryAnchor, ambientIdentity);
        }

        return new InvalidCommand(manifestOnlyOption
            ? "--out/--entrypoint/--project-directory-anchor/--ambient-identity belong to --pz-manifest"
            : "one of --pz-socket <path> or --pz-manifest is required");
    }
}
