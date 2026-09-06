namespace Pz.Connectors.Sdk;

internal abstract record HostCommand;

internal sealed record ServeCommand(string SocketPath) : HostCommand;

internal sealed record ManifestCommand(string OutPath, SortedDictionary<string, string> Entrypoints) : HostCommand;

internal sealed record InvalidCommand(string Message) : HostCommand;

/// <summary>The only argv the SDK understands. Connector configuration never travels here -- it
/// reaches the connector through the Configure RPC and no other way -- so anything unrecognized is
/// refused rather than ignored: a typo that passed silently would be a config option that never
/// arrived.</summary>
internal static class HostArguments
{
    public const string Usage =
        "usage: <connector> --pz-socket <path>\n" +
        "       <connector> --pz-manifest --out <file> [--entrypoint <rid>=<package-relative path>]...";

    public static HostCommand Parse(string[] args)
    {
        string? socket = null;
        string? outPath = null;
        var manifest = false;
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
                default:
                    return new InvalidCommand($"unrecognized argument '{args[i]}'");
            }
        }

        if (socket is not null && (manifest || outPath is not null || entrypoints.Count > 0))
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
                : new ManifestCommand(outPath, entrypoints);
        }

        return new InvalidCommand(outPath is not null || entrypoints.Count > 0
            ? "--out/--entrypoint belong to --pz-manifest"
            : "one of --pz-socket <path> or --pz-manifest is required");
    }
}
