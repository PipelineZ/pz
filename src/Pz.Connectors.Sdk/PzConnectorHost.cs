using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connectors.Sdk;

/// <summary>The entry point of a C# out-of-process connector. A connector's <c>Program.cs</c> is
/// one line: <c>return await PzConnectorHost.RunAsync(args, new MyConnector());</c>.
///
/// <para>Two modes, chosen by argv: <c>--pz-socket &lt;path&gt;</c> serves the connector over PCP
/// until the host shuts it down; <c>--pz-manifest --out &lt;file&gt;</c> writes the package manifest
/// and exits. Connector configuration never travels on argv.</para></summary>
public static class PzConnectorHost
{
    /// <summary>Serves <paramref name="connector"/>, which must implement
    /// <see cref="ISourceConnector"/>, <see cref="ISinkConnector"/>, or both.</summary>
    public static Task<int> RunAsync(string[] args, IConnector connector)
    {
        ArgumentNullException.ThrowIfNull(connector);
        return RunAsync(args, _ => connector);
    }

    /// <summary>Like <see cref="RunAsync(string[], IConnector)"/>, but constructs the connector with a
    /// <see cref="PzConnectorContext"/> -- the way to obtain a logger whose output reaches the host.</summary>
    public static Task<int> RunAsync(string[] args, Func<PzConnectorContext, IConnector> create)
    {
        ArgumentNullException.ThrowIfNull(create);
        return RunAsync(args, create, hooks: null);
    }

    internal static async Task<int> RunAsync(string[] args, Func<PzConnectorContext, IConnector> create, PcpServerHooks? hooks)
    {
        ArgumentNullException.ThrowIfNull(args);
        var command = HostArguments.Parse(args);
        if (command is InvalidCommand invalid)
        {
            await Console.Error.WriteLineAsync($"pz connector: {invalid.Message}\n{HostArguments.Usage}").ConfigureAwait(false);
            return 2;
        }

        var peer = new HostChannelPeer();
        using var loggerFactory = LoggerFactory.Create(logging =>
        {
            logging.SetMinimumLevel(LogLevel.Trace);
            logging.AddProvider(new HostLoggerProvider(peer));
        });
        var connector = create(new PzConnectorContext(loggerFactory));
        if (connector is not (ISourceConnector or ISinkConnector))
        {
            throw new ArgumentException(
                $"connector type '{connector.GetType().FullName}' implements neither ISourceConnector nor ISinkConnector",
                nameof(create));
        }

        switch (command)
        {
            case ManifestCommand manifest:
                var directory = Path.GetDirectoryName(Path.GetFullPath(manifest.OutPath));
                if (!string.IsNullOrEmpty(directory))
                {
                    Directory.CreateDirectory(directory);
                }

                await File.WriteAllTextAsync(manifest.OutPath, ManifestWriter.Render(connector, manifest.Entrypoints))
                    .ConfigureAwait(false);
                return 0;
            case ServeCommand serve:
                return await PcpServer.ServeAsync(serve.SocketPath, connector, peer, hooks ?? PcpServerHooks.None)
                    .ConfigureAwait(false);
            default:
                throw new InvalidOperationException($"unhandled command {command.GetType().Name}");
        }
    }
}
