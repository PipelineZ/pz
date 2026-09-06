using Microsoft.Extensions.Logging;

namespace Pz.Connectors.Sdk;

/// <summary>What the SDK hands a connector at construction. <see cref="LoggerFactory"/> produces
/// loggers whose output reaches the pz host as connector log events; the host renders message and
/// fields verbatim, so never log configuration values or payloads.</summary>
public sealed class PzConnectorContext
{
    internal PzConnectorContext(ILoggerFactory loggerFactory)
    {
        LoggerFactory = loggerFactory;
    }

    public ILoggerFactory LoggerFactory { get; }
}
