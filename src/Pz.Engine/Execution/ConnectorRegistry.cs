using System.Diagnostics.CodeAnalysis;
using Pz.Connectors.Abstractions;
using Pz.Core.Model;

namespace Pz.Engine.Execution;

/// <summary>Host-agnostic name → connector map, filled both with builtins and from the
/// out-of-process connector host. Names are the logical connector names used in source/sink YAML.
/// <see cref="Sources"/>/<see cref="Sinks"/> expose read-only enumeration
/// views of the same backing dictionaries for reporting verbs (e.g. `pz connectors`) that need to
/// list every registered connector rather than look one up by name.</summary>
public sealed class ConnectorRegistry
{
    /// <summary>Reserved connection key carrying the named connection an open belongs to, so an
    /// out-of-process host can name the instance after the connection (<c>pz.instance</c> on its spans)
    /// instead of by ordinal. Set by <see cref="ConfigFor"/> for hosted connectors only; the host strips
    /// it before anything crosses to the connector, and a builtin never sees it. Never authored in
    /// <c>connections.yml</c>: an authored value is overwritten.</summary>
    public const string InstanceIdKey = "__pz_instance";

    private readonly Dictionary<string, ISourceConnector> _sources = [];
    private readonly Dictionary<string, ISinkConnector> _sinks = [];
    private readonly HashSet<string> _hosted = new(StringComparer.Ordinal);

    /// <summary>Registers a source connector under <paramref name="name"/>. Throws
    /// <see cref="InvalidOperationException"/> if a source connector is already registered under that
    /// name — a security invariant: names must never be silently hijacked (e.g. a
    /// restored feed package whose connector happens to share a builtin's name must not quietly replace
    /// the trusted builtin). Sources and sinks are tracked in separate dictionaries, so registering the
    /// same name as both a source and a sink (as builtins do for "localfiles") is unaffected. Callers
    /// must catch this exception (see <c>Pz.Cli.ConnectorRegistryFactory</c>, which translates it into a
    /// user-facing <c>PzValidationException</c>) — kept as a plain BCL exception rather than
    /// <c>PzValidationException</c> so the Engine stays Core-free.
    ///
    /// <para><paramref name="hosted"/> marks a connector served by an out-of-process host, which is what
    /// makes <see cref="ConfigFor"/> thread the connection name in under <see cref="InstanceIdKey"/>.</para></summary>
    public void AddSource(string name, ISourceConnector connector, bool hosted = false)
    {
        if (!_sources.TryAdd(name, connector))
        {
            throw new InvalidOperationException(
                $"a source connector named '{name}' is already registered; refusing to silently replace it");
        }

        if (hosted)
        {
            _hosted.Add(name);
        }
    }

    /// <summary>Registers a sink connector under <paramref name="name"/>. See <see cref="AddSource"/> for
    /// the duplicate-name invariant this enforces and for <paramref name="hosted"/>.</summary>
    public void AddSink(string name, ISinkConnector connector, bool hosted = false)
    {
        if (!_sinks.TryAdd(name, connector))
        {
            throw new InvalidOperationException(
                $"a sink connector named '{name}' is already registered; refusing to silently replace it");
        }

        if (hosted)
        {
            _hosted.Add(name);
        }
    }

    /// <summary>The connection config for one open, validate or check of <paramref name="connection"/>:
    /// its authored values, plus -- when its connector is hosted out of process -- the connection name
    /// under <see cref="InstanceIdKey"/>. Every engine call that hands a connector a
    /// <see cref="ConnectorConfig"/> goes through here, so the instance id a host reports is the
    /// connection name wherever the engine knows one.</summary>
    public ConnectorConfig ConfigFor(ConnectionDef connection)
    {
        if (!_hosted.Contains(connection.Connector))
        {
            return new ConnectorConfig(connection.Connection);
        }

        var values = new Dictionary<string, object?>(connection.Connection, StringComparer.Ordinal)
        {
            [InstanceIdKey] = connection.Name,
        };
        return new ConnectorConfig(values);
    }

    public bool TryGetSource(string name, [NotNullWhen(true)] out ISourceConnector? connector) =>
        _sources.TryGetValue(name, out connector);

    public bool TryGetSink(string name, [NotNullWhen(true)] out ISinkConnector? connector) =>
        _sinks.TryGetValue(name, out connector);

    /// <summary>Read-only enumeration view over the registered source connectors, keyed by name — so
    /// the `pz connectors` verb can list every registered connector without the registry exposing its
    /// backing dictionaries directly.</summary>
    public IReadOnlyDictionary<string, ISourceConnector> Sources => _sources;

    /// <summary>Read-only enumeration view over the registered sink connectors. See <see cref="Sources"/>.</summary>
    public IReadOnlyDictionary<string, ISinkConnector> Sinks => _sinks;
}
