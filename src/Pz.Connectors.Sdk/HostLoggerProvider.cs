using Microsoft.Extensions.Logging;
using Pz.Connectors.Protocol.V1;

namespace Pz.Connectors.Sdk;

/// <summary>Forwards every <see cref="ILogger"/> call to the host as a connector log event. The
/// host renders <c>message</c> and <c>fields</c> verbatim, which is why nothing here inspects or
/// redacts: what a connector logs is what the operator sees.</summary>
internal sealed class HostLoggerProvider(HostChannelPeer peer) : ILoggerProvider
{
    public ILogger CreateLogger(string categoryName) => new HostLogger(peer, categoryName);

    public void Dispose()
    {
    }

    private sealed class HostLogger(HostChannelPeer peer, string category) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (!IsEnabled(logLevel))
            {
                return;
            }

            var log = new LogEvent { Level = (int)logLevel, Message = formatter(state, exception) };
            log.Fields["category"] = category;
            if (state is IReadOnlyList<KeyValuePair<string, object?>> properties)
            {
                foreach (var (key, value) in properties)
                {
                    if (key != "{OriginalFormat}")
                    {
                        log.Fields[key] = value?.ToString() ?? string.Empty;
                    }
                }
            }

            if (exception is not null)
            {
                log.Fields["exception"] = exception.GetType().FullName ?? exception.GetType().Name;
            }

            peer.QueueLog(log);
        }
    }
}
