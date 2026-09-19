using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using NuGet.Protocol.Core.Types;

namespace Pz.PackageManagement.Restore;

/// <summary>Maps a restore failure that escaped every coded surface (<see cref="RestoreException"/>,
/// <see cref="NuGetResolver"/>'s own PZ032x throws) onto one of two: an unreachable feed or one that
/// refused the request (an HTTP 401/403), or a local disk failure (permission denied, disk full, a file
/// locked by another process) under <c>.pz</c>.
///
/// <para>A feed-side failure never echoes the triggering exception's message: a feed URL, or NuGet's own
/// wrapped exception text, can carry a credential or a SAS token in its query string, so only the feed
/// list (userinfo and query stripped) and, where an <see cref="HttpRequestException"/> is found in the
/// exception chain, its status code are named. A local I/O failure carries no such risk -- its message is
/// a file path and an OS error -- so it is passed through unredacted, the same as every other path-naming
/// PZ error in this codebase.</para></summary>
public static class RestoreFailureMapper
{
    private const string FeedUnreachable = "PZ0328";
    private const string DiskFailure = "PZ0329";

    /// <summary>True for the two local-filesystem exception types any write under <c>.pz</c> can raise:
    /// a permission refusal, or any other I/O failure (disk full, a file locked by another process, a
    /// path too long, ...). A response that broke mid-stream is an <see cref="IOException"/> too, but it is
    /// the feed's failure, not the disk's.</summary>
    public static bool IsDiskFailure(Exception ex) =>
        ex is UnauthorizedAccessException or (IOException and not HttpIOException);

    /// <summary>True when the feed client, an HTTP request or a socket failed anywhere in the chain --
    /// the feed client wraps the transport failure, so the outermost type alone says little.</summary>
    public static bool IsFeedFailure(Exception ex)
    {
        for (var current = ex; current is not null; current = current.InnerException)
        {
            if (current is NuGetProtocolException or HttpRequestException or HttpIOException
                or SocketException or TimeoutException)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>The coded restore failure for <paramref name="ex"/>, or null when it is neither a disk
    /// nor a feed failure: a defect inside pz must stay a fatal error with its stack trace, not send
    /// the reader to check their network.</summary>
    public static RestoreException? TryMap(Exception ex, IReadOnlyList<string> feeds, string packagesDir)
    {
        if (IsDiskFailure(ex))
        {
            return new RestoreException(
                DiskFailure,
                $"could not restore package files under '{packagesDir}': {ex.Message}",
                "check permissions and free disk space under that path, then run 'pz restore' again");
        }

        return IsFeedFailure(ex)
            ? new RestoreException(
                FeedUnreachable,
                $"could not resolve packages from the configured feed(s) ({string.Join(", ", feeds.Select(RedactFeedUrl))}): " +
                DescribeFeedFailure(ex),
                "check network connectivity to the feed and, for a private feed, its credentials (--feeds / PZ_FEEDS)")
            : null;
    }

    private static string DescribeFeedFailure(Exception ex)
    {
        for (var current = ex; current is not null; current = current.InnerException)
        {
            if (current is HttpRequestException { StatusCode: { } status })
            {
                return status is HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden
                    ? $"the feed rejected the request ({(int)status} {status})"
                    : $"the feed returned {(int)status} {status}";
            }
        }

        return $"the feed could not be reached ({ex.GetType().Name})";
    }

    /// <summary>Strips userinfo and the query string from a feed URL before it reaches an error message
    /// -- a private feed routinely carries a SAS token or an API key there. A local folder feed (every
    /// test feed, and any on-disk feed a host configures) is not a URL at all and passes through
    /// unchanged; it names a path, not a credential.</summary>
    private static string RedactFeedUrl(string feed) =>
        Uri.TryCreate(feed, UriKind.Absolute, out var uri) && uri.Scheme is "http" or "https"
            ? new UriBuilder(uri) { UserName = "", Password = "", Query = "" }.Uri.ToString()
            : feed;
}
