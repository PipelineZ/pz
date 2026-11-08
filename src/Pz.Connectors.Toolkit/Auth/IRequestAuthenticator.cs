namespace Pz.Connectors.Toolkit.Auth;

/// <summary>What a response told an authenticator about the credential the request carried.</summary>
public enum AuthResponseOutcome
{
    /// <summary>Nothing changed; the credential stays in use.</summary>
    Unchanged,

    /// <summary>The credential was dropped; a retry with a fresh one is worthwhile.</summary>
    TokenDiscarded,

    /// <summary>A token that never succeeded was rejected; retrying cannot help.</summary>
    FreshTokenRejected,
}

/// <summary>Applies auth to an outgoing request. Implementations hold secret material —
/// they must never surface it in any message; <see cref="SecretQueryParams"/> lets error paths
/// redact URLs that carry a secret as a query parameter. <see cref="ApplyAsync"/> and
/// <see cref="OnResponse"/> are default-implemented so static implementers are unaffected.</summary>
public interface IRequestAuthenticator
{
    void Apply(HttpRequestMessage request);
    IReadOnlyCollection<string> SecretQueryParams { get; }

    ValueTask ApplyAsync(HttpRequestMessage request, CancellationToken ct)
    {
        Apply(request);
        return ValueTask.CompletedTask;
    }

    AuthResponseOutcome OnResponse(HttpRequestMessage request, int status) => AuthResponseOutcome.Unchanged;
}
