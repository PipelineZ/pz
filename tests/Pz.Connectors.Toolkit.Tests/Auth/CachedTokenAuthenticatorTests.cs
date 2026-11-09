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
        public bool SameToken;

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

            return new FetchedToken(SameToken ? "token-same" : $"token-{n}", lifetime is { } l ? clock.GetUtcNow() + l : null);
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
    public async Task The_identical_token_handed_back_after_a_401_stays_accepted()
    {
        var auth = new CountingAuthenticator(new ManualClock()) { SameToken = true };
        var r1 = Req();
        await auth.ApplyAsync(r1, default);
        Assert.Equal(AuthResponseOutcome.Unchanged, auth.OnResponse(r1, 200));
        var r2 = Req();
        await auth.ApplyAsync(r2, default);
        Assert.Equal(AuthResponseOutcome.TokenDiscarded, auth.OnResponse(r2, 401));

        for (var i = 0; i < 2; i++)
        {
            var again = Req();
            await auth.ApplyAsync(again, default);
            Assert.Equal("Bearer token-same", Bearer(again));
            Assert.Equal(AuthResponseOutcome.TokenDiscarded, auth.OnResponse(again, 401));
        }

        Assert.Equal(3, auth.Fetches);
    }

    [Fact]
    public async Task A_different_token_after_a_401_is_fresh_again()
    {
        var auth = new CountingAuthenticator(new ManualClock());
        var r1 = Req();
        await auth.ApplyAsync(r1, default);
        auth.OnResponse(r1, 200);
        var r2 = Req();
        await auth.ApplyAsync(r2, default);
        Assert.Equal(AuthResponseOutcome.TokenDiscarded, auth.OnResponse(r2, 401));
        var r3 = Req();
        await auth.ApplyAsync(r3, default);
        Assert.Equal(AuthResponseOutcome.FreshTokenRejected, auth.OnResponse(r3, 401));
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

    private static TaskCompletionSource NewGate() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    [Fact]
    public async Task A_token_shorter_than_the_refresh_margin_is_still_reused_and_shared()
    {
        var clock = new ManualClock();
        var auth = new CountingAuthenticator(clock, TimeSpan.FromSeconds(60)) { Gate = NewGate() };
        var requests = Enumerable.Range(0, 5).Select(_ => Req()).ToArray();
        var tasks = requests.Select(r => auth.ApplyAsync(r, default).AsTask()).ToArray();
        auth.Gate.SetResult();
        await Task.WhenAll(tasks);
        Assert.Equal(1, auth.Fetches);
        clock.Advance(TimeSpan.FromSeconds(29));
        await auth.ApplyAsync(Req(), default);
        Assert.Equal(1, auth.Fetches);
        clock.Advance(TimeSpan.FromSeconds(1));
        var r = Req();
        await auth.ApplyAsync(r, default);
        Assert.Equal(2, auth.Fetches);
        Assert.Equal("Bearer token-2", Bearer(r));
    }

    [Fact]
    public async Task A_failed_fetch_reaches_every_waiter_once_and_is_not_cached()
    {
        var auth = new CountingAuthenticator(new ManualClock()) { Gate = NewGate(), FailNext = true };
        var tasks = Enumerable.Range(0, 5).Select(_ => auth.ApplyAsync(Req(), default).AsTask()).ToArray();
        auth.Gate.SetResult();
        var errors = new List<PzConnectorException>();
        foreach (var t in tasks)
        {
            errors.Add(await Assert.ThrowsAsync<PzConnectorException>(() => t));
        }

        Assert.Equal(1, auth.Fetches);
        Assert.All(errors, e => Assert.Same(errors[0], e));
        var r = Req();
        await auth.ApplyAsync(r, default);
        Assert.Equal(2, auth.Fetches);
        Assert.Equal("Bearer token-2", Bearer(r));
    }

    [Fact]
    public async Task One_callers_cancellation_does_not_cancel_the_shared_fetch()
    {
        var auth = new CountingAuthenticator(new ManualClock()) { Gate = NewGate() };
        using var cts = new CancellationTokenSource();
        var first = auth.ApplyAsync(Req(), cts.Token).AsTask();
        var others = Enumerable.Range(0, 4).Select(_ => Req()).ToArray();
        var tasks = others.Select(r => auth.ApplyAsync(r, default).AsTask()).ToArray();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first);
        auth.Gate.SetResult();
        await Task.WhenAll(tasks);
        Assert.All(others, r => Assert.Equal("Bearer token-1", Bearer(r)));
        Assert.Equal(1, auth.Fetches);
    }

    [Fact]
    public async Task A_second_401_for_the_same_never_accepted_token_is_also_fresh()
    {
        var auth = new CountingAuthenticator(new ManualClock());
        var r1 = Req();
        var r2 = Req();
        await auth.ApplyAsync(r1, default);
        await auth.ApplyAsync(r2, default);
        Assert.Equal(AuthResponseOutcome.FreshTokenRejected, auth.OnResponse(r1, 401));
        Assert.Equal(AuthResponseOutcome.FreshTokenRejected, auth.OnResponse(r2, 401));
    }

    private sealed class HeaderTokenAuthenticator(ManualClock clock) : CachedTokenAuthenticator(clock)
    {
        public override IReadOnlyCollection<string> SecretQueryParams => ["session"];

        protected override ValueTask<FetchedToken> FetchTokenAsync(CancellationToken ct)
            => ValueTask.FromResult(new FetchedToken("tok", null));

        protected override void ApplyToken(HttpRequestMessage request, string token)
            => request.Headers.TryAddWithoutValidation("X-Session", token);
    }

    [Fact]
    public async Task A_subclass_chooses_where_the_token_goes_and_keeps_the_401_rule()
    {
        var auth = new HeaderTokenAuthenticator(new ManualClock());
        var r = Req();
        await auth.ApplyAsync(r, default);
        Assert.Null(r.Headers.Authorization);
        Assert.Equal("tok", Assert.Single(r.Headers.GetValues("X-Session")));
        Assert.Equal(["session"], ((IRequestAuthenticator)auth).SecretQueryParams);
        Assert.Equal(AuthResponseOutcome.Unchanged, auth.OnResponse(r, 200));
        Assert.Equal(AuthResponseOutcome.TokenDiscarded, auth.OnResponse(r, 401));
    }
}
