using System.Collections.Concurrent;
using Pz.Engine.Execution;

namespace Pz.Cli;

/// <summary>Where a process-hosted connector's log lines go. Every line becomes a <c>connector_log</c>
/// run event. Warn and above is also said to the person watching, through the run's own notice line
/// -- the one channel a connector's words reach a console (and an MCP result) by, so the renderers
/// never print the event and nothing is said twice.
///
/// A connector process is started once per node, so a line about the CONNECTION arrives once per entity
/// read through it: the event stream keeps every one, a person is told each distinct line once per
/// connection. A level this build does not know is said rather than hidden.</summary>
internal sealed class ConnectorLogRelay(Action<string> notice)
{
    private readonly ConcurrentDictionary<(string Connection, string Level, string Message), byte> _said = new();

    /// <summary>Set once the run's event bus exists. The registry that hands connectors this relay is
    /// built before the bus, but no connector process is started until a node runs, which is after.</summary>
    public IRunEvents? Events { get; set; }

    public void Log(string connection, string level, string message)
    {
        Events?.SafeConnectorLog(connection, level, message);
        if (level is not ("trace" or "debug" or "info") && _said.TryAdd((connection, level, message), 0))
        {
            notice($"{connection}: [{level}] {message}");
        }
    }
}
