using System.Net.Http.Headers;
using Pz.Connectors.Abstractions;

namespace Pz.Connectors.Toolkit.Auth;

/// <summary>A bearer token and its absolute expiry (null = no expiry known).</summary>
public sealed record FetchedToken(string AccessToken, DateTimeOffset? ExpiresOn);

/// <summary>Caches one bearer token, refreshes it <see cref="RefreshMargin"/> before expiry
/// (at most half its lifetime, so short-lived tokens are still reused), and shares a single
/// in-flight fetch between concurrent callers. The fetch outlives any one caller's cancellation;
/// it is cancelled only by <see cref="Dispose"/>, though a wait for the sign-in gate (below) stops once
/// every caller waiting on it has cancelled. A failed fetch reaches every current waiter once
/// and is not cached. Each fetched token has a generation stamped on the requests that carry it,
/// so a 401 for an older token cannot discard a newer one. Subclasses must not put the access
/// token in any exception message. A subclass that names a <see cref="SignInKey"/> gets two more
/// guarantees: its sign-ins are serialised with every other authenticator in the process that has the same
/// key, and once one of them fails with an exception marked by <see cref="Terminal"/>, every later sign-in
/// with that key fails at once with the same permanent error and sends nothing.</summary>
public abstract class CachedTokenAuthenticator(TimeProvider time) : IRequestAuthenticator, IDisposable
{
    public static readonly TimeSpan RefreshMargin = TimeSpan.FromMinutes(5);

    private static readonly HttpRequestOptionsKey<int> GenerationKey = new("pz.auth.token-generation");

    private readonly Lock _lock = new();
    private readonly CancellationTokenSource _disposed = new();
    private Flight? _inFlight;
    private string? _token;
    private DateTimeOffset? _refreshAt;
    private int _generation;
    private bool _accepted;
    private string? _discardedAccepted;

    /// <summary>Query parameters that carry the token; none for the default bearer header. A subclass that
    /// puts the token in the URL (<see cref="ApplyToken"/>) names the parameter here so messages redact it.</summary>
    public virtual IReadOnlyCollection<string> SecretQueryParams => [];

    protected abstract ValueTask<FetchedToken> FetchTokenAsync(CancellationToken ct);

    /// <summary>A fingerprint of the endpoint and credential (see <see cref="Fingerprint"/>) under which a
    /// <see cref="Terminal"/> refusal is remembered for the process; null (the default) remembers nothing.</summary>
    protected virtual string? SignInKey => null;

    /// <summary>True when a response with <paramref name="status"/> means the token was refused; 401 by default.</summary>
    protected virtual bool IsTokenRejection(int status) => status == 401;

    /// <summary>A SHA-256 fingerprint of <paramref name="parts"/> (endpoint, client id, secrets): safe to keep,
    /// since no part can be recovered from it.</summary>
    protected static string Fingerprint(params string?[] parts) => SignInRefusals.Fingerprint(parts);

    /// <summary>Marks <paramref name="refusal"/> (a permanent error) as a refusal that must not be repeated in
    /// this process, and returns it to be thrown.</summary>
    protected static PzConnectorException Terminal(PzConnectorException refusal) => MarkTerminal(refusal);

    private const string TerminalMark = "pz.auth.terminal";

    internal static PzConnectorException MarkTerminal(PzConnectorException refusal)
    {
        refusal.Data[TerminalMark] = true;
        return refusal;
    }

    internal const string RepeatSuffix =
        " (pz did not try again: this sign-in was already refused once in this run, and pz makes one refused sign-in per connection per run)";

    /// <summary>Puts <paramref name="token"/> on the request; by default as <c>Authorization: Bearer</c>.</summary>
    protected virtual void ApplyToken(HttpRequestMessage request, string token)
        => request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);

    public void Apply(HttpRequestMessage request)
        => throw new InvalidOperationException("this authenticator fetches tokens asynchronously; use ApplyAsync");

    /// <summary>One shared sign-in and the callers waiting on it. When every waiter has cancelled before it
    /// finishes, it is abandoned: a wait for the process-wide sign-in gate stops, and the next caller starts a
    /// new one. A fetch already under way still runs to the end.</summary>
    private sealed class Flight
    {
        public TaskCompletionSource<(string Token, int Generation)> Result { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public CancellationTokenSource Abandoned { get; } = new();

        public int Waiters;
    }

    public async ValueTask ApplyAsync(HttpRequestMessage request, CancellationToken ct)
    {
        Flight flight;
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
                if (_inFlight is null || _inFlight.Abandoned.IsCancellationRequested)
                {
                    _inFlight = new Flight();
                    owner = true;
                }

                flight = _inFlight;
                flight.Waiters++;
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

            try
            {
                (token, generation) = await flight.Result.Task.WaitAsync(ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                // Left here rather than in a token callback: the wait can resume inline inside Cancel and
                // dispose such a registration before it runs.
                Leave(flight);
                throw;
            }
        }

        ApplyToken(request, token);
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

            if (!IsTokenRejection(status))
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

    private void Leave(Flight flight)
    {
        bool abandon;
        lock (_lock)
        {
            abandon = --flight.Waiters == 0 && !flight.Result.Task.IsCompleted;
        }

        if (abandon)
        {
            flight.Abandoned.Cancel();
        }
    }

    private async Task RunFetchAsync(Flight flight)
    {
        try
        {
            var fetched = await FetchOnceAsync(flight.Abandoned.Token).ConfigureAwait(false);
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
                if (_inFlight == flight)
                {
                    _inFlight = null;
                }
            }

            flight.Result.SetResult(result);
        }
        catch (Exception ex)
        {
            lock (_lock)
            {
                if (_inFlight == flight)
                {
                    _inFlight = null;
                }
            }

            flight.Result.SetException(ex);
        }
    }

    private async ValueTask<FetchedToken> FetchOnceAsync(CancellationToken abandoned)
    {
        if (SignInKey is not { } key)
        {
            return await FetchTokenAsync(_disposed.Token).ConfigureAwait(false);
        }

        var gate = SignInRefusals.Gate(key);
        // Waiting behind another sign-in with this key stops as soon as nobody wants this one any more.
        using (var waiting = CancellationTokenSource.CreateLinkedTokenSource(_disposed.Token, abandoned))
        {
            await gate.WaitAsync(waiting.Token).ConfigureAwait(false);
        }

        // A cancelled semaphore wait can still be handed the slot when a release races the cancellation.
        if (abandoned.IsCancellationRequested)
        {
            gate.Release();
            throw new OperationCanceledException(abandoned);
        }

        try
        {
            if (SignInRefusals.TryGet(key, out var refused))
            {
                throw new PzConnectorException(refused + RepeatSuffix, isTransient: false);
            }

            return await FetchTokenAsync(_disposed.Token).ConfigureAwait(false);
        }
        catch (PzConnectorException ex) when (!ex.IsTransient && ex.Data[TerminalMark] is true)
        {
            SignInRefusals.Record(key, ex.Message);
            throw;
        }
        finally
        {
            gate.Release();
        }
    }
}
