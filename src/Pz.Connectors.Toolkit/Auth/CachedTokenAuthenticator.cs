using System.Net.Http.Headers;

namespace Pz.Connectors.Toolkit.Auth;

/// <summary>A bearer token and its absolute expiry (null = no expiry known).</summary>
public sealed record FetchedToken(string AccessToken, DateTimeOffset? ExpiresOn);

/// <summary>Caches one bearer token, refreshes it <see cref="RefreshMargin"/> before expiry
/// (at most half its lifetime, so short-lived tokens are still reused), and shares a single
/// in-flight fetch between concurrent callers. The fetch outlives any one caller's cancellation;
/// it is cancelled only by <see cref="Dispose"/>. A failed fetch reaches every current waiter once
/// and is not cached. Each fetched token has a generation stamped on the requests that carry it,
/// so a 401 for an older token cannot discard a newer one. Subclasses must not put the access
/// token in any exception message.</summary>
public abstract class CachedTokenAuthenticator(TimeProvider time) : IRequestAuthenticator, IDisposable
{
    public static readonly TimeSpan RefreshMargin = TimeSpan.FromMinutes(5);

    private static readonly HttpRequestOptionsKey<int> GenerationKey = new("pz.auth.token-generation");

    private readonly Lock _lock = new();
    private readonly CancellationTokenSource _disposed = new();
    private TaskCompletionSource<(string Token, int Generation)>? _inFlight;
    private string? _token;
    private DateTimeOffset? _refreshAt;
    private int _generation;
    private bool _accepted;
    private string? _discardedAccepted;

    public IReadOnlyCollection<string> SecretQueryParams => [];

    protected abstract ValueTask<FetchedToken> FetchTokenAsync(CancellationToken ct);

    public void Apply(HttpRequestMessage request)
        => throw new InvalidOperationException("this authenticator fetches tokens asynchronously; use ApplyAsync");

    public async ValueTask ApplyAsync(HttpRequestMessage request, CancellationToken ct)
    {
        TaskCompletionSource<(string Token, int Generation)> flight;
        var owner = false;
        string token;
        int generation;
        lock (_lock)
        {
            if (_token is { } cached && (_refreshAt is not { } at || time.GetUtcNow() < at))
            {
                token = cached;
                generation = _generation;
                flight = null!;
            }
            else
            {
                if (_inFlight is null)
                {
                    _inFlight = new(TaskCreationOptions.RunContinuationsAsynchronously);
                    owner = true;
                }

                flight = _inFlight;
                token = "";
                generation = 0;
            }
        }

        if (flight is not null)
        {
            if (owner)
            {
                _ = RunFetchAsync(flight);
            }

            (token, generation) = await flight.Task.WaitAsync(ct).ConfigureAwait(false);
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
            var isLatest = generation == _generation;
            if (status is >= 200 and < 300)
            {
                if (isLatest && _token is not null)
                {
                    _accepted = true;
                }

                return AuthResponseOutcome.Unchanged;
            }

            if (status != 401)
            {
                return AuthResponseOutcome.Unchanged;
            }

            if (!isLatest)
            {
                return AuthResponseOutcome.TokenDiscarded;
            }

            // The cache may already be cleared by an earlier 401 for this same generation;
            // whether the token was ever accepted still decides the outcome.
            // An identity cache often hands the same token straight back; it was accepted before, so a
            // repeat 401 on it is still the retryable kind (the engine bounds the retries), not a bad scope.
            if (_token is not null)
            {
                _discardedAccepted = _accepted ? _token : null;
            }

            _token = null;
            return _accepted ? AuthResponseOutcome.TokenDiscarded : AuthResponseOutcome.FreshTokenRejected;
        }
    }

    public virtual void Dispose() => _disposed.Cancel();

    private async Task RunFetchAsync(TaskCompletionSource<(string Token, int Generation)> flight)
    {
        try
        {
            var fetched = await FetchTokenAsync(_disposed.Token).ConfigureAwait(false);
            var now = time.GetUtcNow();
            (string, int) result;
            lock (_lock)
            {
                _token = fetched.AccessToken;
                _refreshAt = fetched.ExpiresOn is { } expires
                    ? expires - TimeSpan.FromTicks(Math.Clamp((expires - now).Ticks / 2, 0, RefreshMargin.Ticks))
                    : null;
                _accepted = _discardedAccepted is not null
                    && string.Equals(fetched.AccessToken, _discardedAccepted, StringComparison.Ordinal);
                _discardedAccepted = null;
                result = (fetched.AccessToken, ++_generation);
                _inFlight = null;
            }

            flight.SetResult(result);
        }
        catch (Exception ex)
        {
            lock (_lock)
            {
                _inFlight = null;
            }

            flight.SetException(ex);
        }
    }
}
