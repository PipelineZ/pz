using System.Net.Http.Headers;

namespace Pz.Connectors.Toolkit.Auth;

/// <summary>A bearer token and its absolute expiry (null = no expiry known).</summary>
public sealed record FetchedToken(string AccessToken, DateTimeOffset? ExpiresOn);

/// <summary>Caches one bearer token, refreshes it <see cref="RefreshMargin"/> before expiry, and
/// shares a single in-flight fetch between concurrent callers. Each fetched token has a
/// generation stamped on the requests that carry it, so a 401 for an older token cannot discard
/// a newer one. Subclasses must not put the access token in any exception message.</summary>
public abstract class CachedTokenAuthenticator(TimeProvider time) : IRequestAuthenticator, IDisposable
{
    public static readonly TimeSpan RefreshMargin = TimeSpan.FromMinutes(5);

    private static readonly HttpRequestOptionsKey<int> GenerationKey = new("pz.auth.token-generation");

    private readonly SemaphoreSlim _fetchGate = new(1, 1);
    private readonly Lock _lock = new();
    private FetchedToken? _token;
    private int _generation;
    private bool _accepted;

    public IReadOnlyCollection<string> SecretQueryParams => [];

    protected abstract ValueTask<FetchedToken> FetchTokenAsync(CancellationToken ct);

    public void Apply(HttpRequestMessage request)
        => throw new InvalidOperationException("this authenticator fetches tokens asynchronously; use ApplyAsync");

    public async ValueTask ApplyAsync(HttpRequestMessage request, CancellationToken ct)
    {
        if (!TryGetCurrent(out var token, out var generation))
        {
            await _fetchGate.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                // Another caller may have fetched while this one waited for the gate.
                if (!TryGetCurrent(out token, out generation))
                {
                    var fetched = await FetchTokenAsync(ct).ConfigureAwait(false);
                    lock (_lock)
                    {
                        _token = fetched;
                        _accepted = false;
                        generation = ++_generation;
                    }

                    token = fetched.AccessToken;
                }
            }
            finally
            {
                _fetchGate.Release();
            }
        }

        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        request.Options.Set(GenerationKey, generation);
    }

    public AuthResponseOutcome OnResponse(HttpRequestMessage request, int status)
    {
        if (!request.Options.TryGetValue(GenerationKey, out var generation))
        {
            return AuthResponseOutcome.Unchanged;
        }

        lock (_lock)
        {
            var current = generation == _generation && _token is not null;
            if (status is >= 200 and < 300)
            {
                if (current)
                {
                    _accepted = true;
                }

                return AuthResponseOutcome.Unchanged;
            }

            if (status != 401)
            {
                return AuthResponseOutcome.Unchanged;
            }

            if (!current)
            {
                return AuthResponseOutcome.TokenDiscarded;
            }

            var wasAccepted = _accepted;
            _token = null;
            _accepted = false;
            return wasAccepted ? AuthResponseOutcome.TokenDiscarded : AuthResponseOutcome.FreshTokenRejected;
        }
    }

    public virtual void Dispose()
    {
    }

    private bool TryGetCurrent(out string token, out int generation)
    {
        lock (_lock)
        {
            if (_token is { } t && (t.ExpiresOn is not { } expires || time.GetUtcNow() < expires - RefreshMargin))
            {
                token = t.AccessToken;
                generation = _generation;
                return true;
            }
        }

        token = "";
        generation = 0;
        return false;
    }
}
