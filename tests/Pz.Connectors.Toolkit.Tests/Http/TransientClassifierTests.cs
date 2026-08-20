using System.Net;
using System.Net.Sockets;
using Pz.Connectors.Toolkit.Http;

namespace Pz.Connectors.Toolkit.Tests.Http;

public class TransientClassifierTests
{
    [Theory]
    [InlineData(408, true)]
    [InlineData(429, true)]
    [InlineData(500, true)]
    [InlineData(502, true)]
    [InlineData(503, true)]
    [InlineData(504, true)]
    [InlineData(501, false)]   // Not Implemented: retrying never helps
    [InlineData(400, false)]
    [InlineData(401, false)]
    [InlineData(403, false)]
    [InlineData(404, false)]
    [InlineData(422, false)]
    public void Status_set_matches_azure_precedent(int status, bool transient)
        => Assert.Equal(transient, TransientClassifier.IsTransientStatus(status));

    [Fact]
    public void Network_exceptions_are_transient()
    {
        Assert.True(TransientClassifier.IsTransientException(new IOException()));
        Assert.True(TransientClassifier.IsTransientException(new TimeoutException()));
        Assert.True(TransientClassifier.IsTransientException(new SocketException()));
        Assert.True(TransientClassifier.IsTransientException(
            new HttpRequestException("x", new SocketException())));
        Assert.True(TransientClassifier.IsTransientException(
            new HttpRequestException("x", null, HttpStatusCode.ServiceUnavailable)));
        Assert.False(TransientClassifier.IsTransientException(new InvalidOperationException()));
        Assert.False(TransientClassifier.IsTransientException(
            new HttpRequestException("x", null, HttpStatusCode.NotFound)));
    }

    [Fact]
    public void Retry_after_parses_delta_seconds()
    {
        var now = new DateTimeOffset(2026, 7, 17, 12, 0, 0, TimeSpan.Zero);
        Assert.Equal(TimeSpan.FromSeconds(30), TransientClassifier.ParseRetryAfter("30", now));
    }

    [Fact]
    public void Retry_after_parses_http_date()
    {
        var now = new DateTimeOffset(2026, 7, 17, 12, 0, 0, TimeSpan.Zero);
        Assert.Equal(TimeSpan.FromSeconds(60),
            TransientClassifier.ParseRetryAfter("Fri, 17 Jul 2026 12:01:00 GMT", now));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("soon")]
    public void Retry_after_unparsable_is_null(string? value)
        => Assert.Null(TransientClassifier.ParseRetryAfter(
            value, new DateTimeOffset(2026, 7, 17, 12, 0, 0, TimeSpan.Zero)));

    [Fact]
    public void Retry_after_in_the_past_clamps_to_zero()
    {
        var now = new DateTimeOffset(2026, 7, 17, 12, 0, 0, TimeSpan.Zero);
        Assert.Equal(TimeSpan.Zero,
            TransientClassifier.ParseRetryAfter("Fri, 17 Jul 2026 11:59:00 GMT", now));
    }

    [Fact]
    public void Retry_after_overflow_delta_seconds_returns_null()
    {
        var now = new DateTimeOffset(2026, 7, 17, 12, 0, 0, TimeSpan.Zero);
        // A value that parses as long but exceeds TimeSpan.MaxValue.TotalSeconds
        var result = TransientClassifier.ParseRetryAfter("999999999999999", now);
        Assert.Null(result);
    }

    [Fact]
    public void Retry_after_negative_delta_seconds_returns_zero()
    {
        var now = new DateTimeOffset(2026, 7, 17, 12, 0, 0, TimeSpan.Zero);
        var result = TransientClassifier.ParseRetryAfter("-5", now);
        Assert.Equal(TimeSpan.Zero, result);
    }
}
