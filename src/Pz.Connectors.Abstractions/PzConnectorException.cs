namespace Pz.Connectors.Abstractions;

/// <summary>The one exception type connectors throw for operational failures. The engine retries
/// transient failures (honoring <see cref="RetryAfter"/>); permanent failures fail the node.</summary>
public sealed class PzConnectorException(string message, bool isTransient, TimeSpan? retryAfter = null,
    Exception? innerException = null) : Exception(message, innerException)
{
    public bool IsTransient { get; } = isTransient;
    public TimeSpan? RetryAfter { get; } = retryAfter;
}
