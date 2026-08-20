using System.Diagnostics.CodeAnalysis;
using Pz.Connectors.Abstractions;

namespace Pz.Engine.Execution;

/// <summary>Host-agnostic name → connector map, filled both with builtins and from the ALC
/// ConnectorHost. Names are the logical connector names used in source/sink YAML.
/// <see cref="Sources"/>/<see cref="Sinks"/> expose read-only enumeration
/// views of the same backing dictionaries for reporting verbs (e.g. `pz connectors`) that need to
/// list every registered connector rather than look one up by name.</summary>
public sealed class ConnectorRegistry
{
    private readonly Dictionary<string, ISourceConnector> _sources = [];
    private readonly Dictionary<string, ISinkConnector> _sinks = [];

    /// <summary>Registers a source connector under <paramref name="name"/>. Throws
    /// <see cref="InvalidOperationException"/> if a source connector is already registered under that
    /// name — a security invariant: names must never be silently hijacked (e.g. a
    /// restored feed package whose connector happens to share a builtin's name must not quietly replace
    /// the trusted builtin). Sources and sinks are tracked in separate dictionaries, so registering the
    /// same name as both a source and a sink (as builtins do for "localfiles") is unaffected. Callers
    /// must catch this exception (see <c>Pz.Cli.ConnectorRegistryFactory</c>, which translates it into a
    /// user-facing <c>PzValidationException</c>) — kept as a plain BCL exception rather than
    /// <c>PzValidationException</c> so the Engine stays Core-free.</summary>
    public void AddSource(string name, ISourceConnector connector)
    {
        if (!_sources.TryAdd(name, connector))
        {
            throw new InvalidOperationException(
                $"a source connector named '{name}' is already registered; refusing to silently replace it");
        }
    }

    /// <summary>Registers a sink connector under <paramref name="name"/>. See <see cref="AddSource"/> for
    /// the duplicate-name invariant this enforces.</summary>
    public void AddSink(string name, ISinkConnector connector)
    {
        if (!_sinks.TryAdd(name, connector))
        {
            throw new InvalidOperationException(
                $"a sink connector named '{name}' is already registered; refusing to silently replace it");
        }
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
