using Azure;

namespace Pz.Connector.AzureBlob.Tests;

/// <summary>Offline, no-docker unit tests for <see cref="AzureTransient.IsTransient"/> -- pure and
/// deterministic, so every status-code branch is directly testable without a live Azurite/Storage
/// account (see AzureNativeEndToEndTests/AzureUniversalSinkEndToEndTests/AzureSchemaPeekEndToEndTests for
/// the docker-gated e2e proof that these boundaries actually wrap in practice).</summary>
public sealed class AzureTransientTests
{
    [Theory]
    [InlineData(503)]
    [InlineData(429)]
    [InlineData(500)]
    [InlineData(408)]
    [InlineData(502)]
    [InlineData(504)]
    public void RequestFailedException_with_transient_status_is_transient(int status)
    {
        var ex = new RequestFailedException(status, "transient failure");
        Assert.True(AzureTransient.IsTransient(ex));
    }

    [Theory]
    [InlineData(404)]
    [InlineData(403)]
    [InlineData(400)]
    public void RequestFailedException_with_permanent_status_is_not_transient(int status)
    {
        var ex = new RequestFailedException(status, "permanent failure");
        Assert.False(AzureTransient.IsTransient(ex));
    }

    [Fact]
    public void IOException_is_transient()
    {
        Assert.True(AzureTransient.IsTransient(new IOException("connection reset")));
    }

    [Fact]
    public void TimeoutException_is_transient()
    {
        Assert.True(AzureTransient.IsTransient(new TimeoutException("timed out")));
    }

    [Fact]
    public void Unrelated_exception_is_not_transient()
    {
        Assert.False(AzureTransient.IsTransient(new InvalidOperationException("not a network failure")));
    }

    [Fact]
    public void SocketException_is_transient()
    {
        Assert.True(AzureTransient.IsTransient(new System.Net.Sockets.SocketException()));
    }
}
