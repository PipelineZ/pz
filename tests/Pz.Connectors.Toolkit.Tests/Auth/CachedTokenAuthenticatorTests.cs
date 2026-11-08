using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Auth;

namespace Pz.Connectors.Toolkit.Tests.Auth;

public class CachedTokenAuthenticatorTests
{
    private sealed class CountingAuthenticator(ManualClock clock, TimeSpan? lifetime = null)
        : CachedTokenAuthenticator(clock)
    {
        public int Fetches;
        public TaskCompletionSource? Gate;
        public bool FailNext;

        protected override async ValueTask<FetchedToken> FetchTokenAsync(CancellationToken ct)
        {
            var n = Interlocked.Increment(ref Fetches);
            if (Gate is { } gate)
            {
                await gate.Task;
            }

            if (FailNext)
            {
                FailNext = false;
                throw new PzConnectorException("token endpoint unavailable", isTransient: true);
            }

            return new FetchedToken($"token-{n}", lifetime is { } l ? clock.GetUtcNow() + l : null);
        }
    }

    private static HttpRequestMessage Req() => new(HttpMethod.Get, "http://x/");

    private static string Bearer(HttpRequestMessage r) => r.Headers.Authorization!.ToString();

    [Fact]
    public async Task Reuses_the_token_until_five_minutes_before_expiry()
    {
        var clock = new ManualClock();
        var auth = new CountingAuthenticator(clock, TimeSpan.FromMinutes(60));
        await auth.ApplyAsync(Req(), default);
        await auth.ApplyAsync(Req(), default);
        Assert.Equal(1, auth.Fetches);
        clock.Advance(TimeSpan.FromMinutes(54));
        await auth.ApplyAsync(Req(), default);
        Assert.Equal(1, auth.Fetches);
        clock.Advance(TimeSpan.FromMinutes(2));
        var r = Req();
        await auth.ApplyAsync(r, default);
        Assert.Equal(2, auth.Fetches);
        Assert.Equal("Bearer token-2", Bearer(r));
    }

    [Fact]
    public async Task A_token_without_expiry_is_reused_until_rejected()
    {
        var clock = new ManualClock();
        var auth = new CountingAuthenticator(clock);
        await auth.ApplyAsync(Req(), default);
        clock.Advance(TimeSpan.FromDays(10));
        await auth.ApplyAsync(Req(), default);
        Assert.Equal(1, auth.Fetches);
    }

    [Fact]
    public async Task Concurrent_requests_share_one_in_flight_fetch()
    {
        var auth = new CountingAuthenticator(new ManualClock(), TimeSpan.FromMinutes(60))
        {
            Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously),
        };
        var requests = Enumerable.Range(0, 5).Select(_ => Req()).ToArray();
        var tasks = requests.Select(r => auth.ApplyAsync(r, default).AsTask()).ToArray();
        auth.Gate.SetResult();
        await Task.WhenAll(tasks);
        Assert.All(requests, r => Assert.Equal("Bearer token-1", Bearer(r)));
        Assert.Equal(1, auth.Fetches);
    }

    [Fact]
    public async Task A_401_on_an_accepted_token_discards_it()
    {
        var auth = new CountingAuthenticator(new ManualClock());
        var r1 = Req();
        await auth.ApplyAsync(r1, default);
        Assert.Equal(AuthResponseOutcome.Unchanged, auth.OnResponse(r1, 200));
        var r2 = Req();
        await auth.ApplyAsync(r2, default);
        Assert.Equal(AuthResponseOutcome.TokenDiscarded, auth.OnResponse(r2, 401));
        var r3 = Req();
        await auth.ApplyAsync(r3, default);
        Assert.Equal("Bearer token-2", Bearer(r3));
    }

    [Fact]
    public async Task A_401_on_a_never_accepted_token_is_reported_fresh()
    {
        var auth = new CountingAuthenticator(new ManualClock());
        var r1 = Req();
        await auth.ApplyAsync(r1, default);
        Assert.Equal(AuthResponseOutcome.FreshTokenRejected, auth.OnResponse(r1, 401));
    }

    [Fact]
    public async Task A_401_for_an_older_token_does_not_discard_the_newer_one()
    {
        var auth = new CountingAuthenticator(new ManualClock());
        var r1 = Req();
        var r2 = Req();
        var r3 = Req();
        await auth.ApplyAsync(r1, default);
        Assert.Equal(AuthResponseOutcome.Unchanged, auth.OnResponse(r1, 200));
        await auth.ApplyAsync(r2, default);
        await auth.ApplyAsync(r3, default);
        Assert.Equal(AuthResponseOutcome.TokenDiscarded, auth.OnResponse(r3, 401));
        var r4 = Req();
        await auth.ApplyAsync(r4, default);
        Assert.Equal("Bearer token-2", Bearer(r4));
        Assert.Equal(AuthResponseOutcome.TokenDiscarded, auth.OnResponse(r2, 401));
        var r5 = Req();
        await auth.ApplyAsync(r5, default);
        Assert.Equal("Bearer token-2", Bearer(r5));
        Assert.Equal(2, auth.Fetches);
    }

    [Fact]
    public async Task A_failed_fetch_is_not_cached()
    {
        var auth = new CountingAuthenticator(new ManualClock()) { FailNext = true };
        await Assert.ThrowsAsync<PzConnectorException>(async () => await auth.ApplyAsync(Req(), default));
        var r = Req();
        await auth.ApplyAsync(r, default);
        Assert.Equal("Bearer token-2", Bearer(r));
        Assert.Equal(2, auth.Fetches);
    }

    [Fact]
    public async Task Static_authenticators_keep_their_behaviour_through_the_new_members()
    {
        Assert.True(Authenticators.TryCreate(
            new Dictionary<string, object?> { ["type"] = "bearer", ["token"] = "T" }, out var auth, out _));
        var r = Req();
        await auth!.ApplyAsync(r, default);
        Assert.Equal("Bearer T", Bearer(r));
        Assert.Equal(AuthResponseOutcome.Unchanged, auth.OnResponse(r, 401));
    }
}
