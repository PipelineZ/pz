using System.Net;
using System.Net.Sockets;
using NuGet.Protocol.Core.Types;
using Pz.PackageManagement.Restore;

namespace Pz.PackageManagement.Tests.Restore;

public class RestoreFailureMapperTests
{
    private static readonly string[] Feeds = ["https://user:s3cr3t@feed.example/v3/index.json?sig=abc123"];

    [Fact]
    public void A_refused_request_names_the_status_and_never_the_wrapped_text()
    {
        var ex = new FatalProtocolException("Unable to load https://user:s3cr3t@feed.example/v3/index.json?sig=abc123",
            new HttpRequestException("Response status code does not indicate success: 401", null, HttpStatusCode.Unauthorized));

        var mapped = RestoreFailureMapper.TryMap(ex, Feeds, "/p/.pz/packages");

        Assert.NotNull(mapped);
        Assert.Equal("PZ0328", mapped.Code);
        Assert.Contains("401", mapped.Message, StringComparison.Ordinal);
        Assert.Contains("feed.example", mapped.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("s3cr3t", mapped.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("abc123", mapped.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_connection_failure_anywhere_in_the_chain_is_a_feed_failure()
    {
        var ex = new InvalidOperationException("wrapper", new SocketException((int)SocketError.ConnectionRefused));

        Assert.Equal("PZ0328", RestoreFailureMapper.TryMap(ex, Feeds, "/p/.pz/packages")?.Code);
    }

    [Fact]
    public void A_local_io_failure_is_a_disk_failure()
    {
        var mapped = RestoreFailureMapper.TryMap(new UnauthorizedAccessException("Access to '/p/.pz/packages/x' is denied."),
            Feeds, "/p/.pz/packages");

        Assert.Equal("PZ0329", mapped?.Code);
    }

    [Fact]
    public void A_response_that_broke_mid_stream_is_a_feed_failure_not_a_disk_failure()
    {
        var ex = new HttpIOException(HttpRequestError.ResponseEnded, "The response ended prematurely.");

        Assert.Equal("PZ0328", RestoreFailureMapper.TryMap(ex, Feeds, "/p/.pz/packages")?.Code);
    }

    // A defect inside pz is not a feed problem: blaming the network for it would send the reader to check
    // connectivity and hide the stack trace the fatal path prints.
    [Fact]
    public void An_exception_that_is_neither_is_not_mapped()
    {
        Assert.Null(RestoreFailureMapper.TryMap(new InvalidOperationException("bug"), Feeds, "/p/.pz/packages"));
        Assert.Null(RestoreFailureMapper.TryMap(new NullReferenceException(), Feeds, "/p/.pz/packages"));
    }
}
