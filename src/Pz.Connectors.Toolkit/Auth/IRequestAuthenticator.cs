namespace Pz.Connectors.Toolkit.Auth;

/// <summary>Applies static auth to an outgoing request. Implementations hold secret material —
/// they must never surface it in any message; <see cref="SecretQueryParams"/> lets error paths
/// redact URLs that carry a secret as a query parameter.</summary>
public interface IRequestAuthenticator
{
    void Apply(HttpRequestMessage request);
    IReadOnlyCollection<string> SecretQueryParams { get; }
}
