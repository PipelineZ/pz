namespace Pz.Connectors.Abstractions;

/// <summary>The one exception type connectors throw for operational failures. The engine retries
/// transient failures (honoring <see cref="RetryAfter"/>); permanent failures fail the node.
///
/// <para>THE MESSAGE IS PUBLISHED VERBATIM. The engine treats this type as trusted and does NOT redact
/// <see cref="Exception.Message"/> — it is written unchanged into <c>run_results.json</c>, onto the
/// NDJSON event stream, and into a <c>retry_scheduled</c> event's reason. All three are durable and all
/// three routinely leave the machine. The trust is deliberate: the engine cannot parse what a connector
/// wrapped, and only the connector knows which substrings of its own message are secrets. It is also
/// TOTAL — nothing downstream will catch a credential that reaches this message.</para>
///
/// <para>So a connector that wraps a third-party client's error owns redacting it, and owns knowing the
/// shapes that client actually answers in. An object store's 403 carries the access key id and the
/// signing payload inside XML ELEMENTS, not as <c>name=value</c> pairs — a redactor that only
/// understands one shape passes the other straight through, and an emulator used in tests may not
/// produce the shape the real service does. <c>Pz.Connectors.TestKit</c>'s
/// <c>ErrorRedactionContractTests</c> exists to check exactly this.</para>
///
/// <para>The same trust means storage LOCATIONS a message names — bucket or container, object path,
/// endpoint host and port — also reach run artifacts unchanged. That is intended: a message naming
/// nothing is not a diagnosis. It is worth knowing when shipping run artifacts off the machine.</para></summary>
public sealed class PzConnectorException(string message, bool isTransient, TimeSpan? retryAfter = null,
    Exception? innerException = null) : Exception(message, innerException)
{
    public bool IsTransient { get; } = isTransient;
    public TimeSpan? RetryAfter { get; } = retryAfter;
}
