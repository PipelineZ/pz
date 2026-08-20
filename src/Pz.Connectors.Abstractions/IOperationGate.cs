namespace Pz.Connectors.Abstractions;

/// <summary>Engine-implemented resilience boundary a connector wraps each remote operation in.
/// The engine owns all policy (retry delays, jitter, pacing, breaker state); the connector
/// contributes only the operation boundary and an idempotency declaration. opLabel is a STATIC,
/// connector-authored identifier (e.g. "http.get_page") — never a URL, parameter, or any value
/// derived from config or payloads.</summary>
public interface IOperationGate
{
    /// <summary>Runs one remote operation through the gate: paces it against the instance's
    /// request budget, and — when <paramref name="idempotent"/> is true — retries transient
    /// PzConnectorException failures internally under the node's resolved retry policy.
    /// Non-idempotent operations execute exactly once (still paced). On exhaustion the last
    /// transient exception propagates unchanged, surfacing as ONE transient node failure —
    /// node-level retry stays the backstop.</summary>
    Task<T> ExecuteAsync<T>(string opLabel, bool idempotent,
        Func<CancellationToken, Task<T>> op, CancellationToken ct);

    /// <summary>Proactive throttle hint parsed from provider metadata (rate-limit headers).
    /// remaining == 0 makes the next paced operation wait until resetAt before executing (the
    /// engine bounds a single hint's wait to a sanity cap). Values with remaining &gt; 0 are
    /// recorded but currently have no pacing effect.</summary>
    void ReportBudget(int remaining, DateTimeOffset resetAt);
}

/// <summary>Implemented by an ISource/ISink that routes its remote operations through an
/// engine-supplied IOperationGate. The engine calls UseOperationGate exactly once per opened
/// ISource/ISink, after OpenAsync returns and before any plan/read/write call. A connector that
/// declares ConnectorCapabilities.GatedOperations MUST implement this on its ISource and/or
/// ISink and route every remote operation of the gated path through the gate. Streaming writes
/// over one open connection are NOT discrete operations and must not be wrapped per-batch.</summary>
public interface IOperationGateAware
{
    void UseOperationGate(IOperationGate gate);
}
