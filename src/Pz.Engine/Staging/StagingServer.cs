using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using Pz.Core.Validation;
using Pz.DuckDb;
using Pz.Engine.Execution;

namespace Pz.Engine.Staging;

/// <summary><c>pz staging serve</c>: a finished run's <c>staging.duckdb</c>, served read-only over the
/// Quack protocol so any DuckDB client can attach it and query the run's intermediate tables.
///
/// Nothing a client reaches is writable: the served database is a small read-only database of views
/// over the staging file, itself attached read-only (see <see cref="DuckSession.OpenForServing"/>). Once
/// the server is listening the session gives up filesystem and network access and locks its
/// configuration, as the read-only preview session does. A client holding the token runs SQL inside this process, so that lockdown is what keeps
/// it from reading other files, writing any, installing extensions or attaching other databases; no
/// client statement can lift it. The token is fresh per serve and never stored. The run directory's lock
/// is held for as long as the server runs, so retention cannot delete the file and a run cannot start in
/// that directory.</summary>
public sealed class StagingServer : IAsyncDisposable
{
    private const int BindAttempts = 5;

    private readonly DuckSession _session;
    private readonly RunDirLock _lock;
    private readonly string _scratchDir;

    private StagingServer(DuckSession session, RunDirLock held, string scratchDir, string uri, string token,
        IReadOnlyList<string> tables)
    {
        _session = session;
        _lock = held;
        _scratchDir = scratchDir;
        Uri = uri;
        Token = token;
        Tables = tables;
    }

    /// <summary>The address clients attach: <c>quack:host:port</c>.</summary>
    public string Uri { get; }

    /// <summary>The bearer token a client presents, in a <c>type quack</c> secret scoped to <see cref="Uri"/>.</summary>
    public string Token { get; }

    /// <summary>The run's staged tables and views, <c>schema.name</c>, ordered; pz's own bookkeeping is left out.</summary>
    public IReadOnlyList<string> Tables { get; }

    /// <summary>Starts serving <paramref name="runDir"/>'s staging on <paramref name="host"/>. A null
    /// <paramref name="port"/> picks a free one. Every way it cannot start is PZ0543.</summary>
    public static Task<StagingServer> StartAsync(string runDir, string host, int? port, CancellationToken ct) =>
        Task.Run(() => Start(runDir, host, port, ct), ct);

    private static StagingServer Start(string runDir, string host, int? port, CancellationToken ct)
    {
        if (!IsValidHost(host))
        {
            throw new ArgumentException($"'{host}' is not a host name or IP address", nameof(host));
        }

        if (!Directory.Exists(runDir))
        {
            throw Unavailable($"run directory '{runDir}' does not exist");
        }

        var staging = Path.Combine(runDir, "staging.duckdb");
        if (!File.Exists(staging))
        {
            throw Unavailable($"run directory '{runDir}' has no staging.duckdb (it was cleaned)");
        }

        var held = RunDirLock.TryAcquire(runDir)
            ?? throw Unavailable($"the run in '{runDir}' is still in progress");
        var scratchDir = Path.Combine(Path.GetTempPath(), "pz-staging-serve-" + Guid.NewGuid().ToString("N"));
        try
        {
            ct.ThrowIfCancellationRequested();
            var token = Convert.ToHexStringLower(RandomNumberGenerator.GetBytes(24));
            var (session, relations, uri) = Open(staging, scratchDir, host, port, token);
            return new StagingServer(session, held, scratchDir, uri, token, relations);
        }
        catch
        {
            held.Dispose();
            TryDelete(scratchDir);
            throw;
        }
    }

    /// <summary>Opens the file and starts the server in one session. A free port can be taken between
    /// choosing it and binding it, so a bind failure on a chosen port retries with another; a bind failure
    /// on the port the caller asked for is reported.</summary>
    private static (DuckSession Session, IReadOnlyList<string> Relations, string Uri) Open(string staging,
        string scratchDir, string host, int? port, string token)
    {
        for (var attempt = 1; ; attempt++)
        {
            var uri = $"quack:{FormatHost(host)}:{port ?? FreePort(host)}";
            var serve = $"CALL quack_serve('{uri}', token = '{token}'" +
                (IsLoopback(host) ? ")" : ", allow_other_hostname = true)");
            try
            {
                ResetScratch(scratchDir);
                // quack's token handling needs the secure crypto module httpfs provides; the serving
                // session turns extension autoloading off, so it is loaded explicitly.
                var (session, relations) = DuckSession.OpenForServing(staging, scratchDir,
                    ["INSTALL httpfs", "LOAD httpfs", "INSTALL quack", "LOAD quack", serve]);
                return (session, relations, uri);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                var bindFailed = ex.Message.Contains("Failed to bind", StringComparison.Ordinal);
                if (bindFailed && port is null && attempt < BindAttempts)
                {
                    continue;
                }

                throw bindFailed
                    ? Unavailable($"could not listen on {uri}: the address is in use or not available here")
                    : Unavailable($"could not open '{staging}' or start the quack server: {FirstLine(ex.Message)}");
            }
        }
    }

    private static void ResetScratch(string scratchDir)
    {
        if (Directory.Exists(scratchDir))
        {
            Directory.Delete(scratchDir, recursive: true);
        }

        Directory.CreateDirectory(scratchDir);
    }

    /// <summary>A host name, an IPv4 address, or an IPv6 address (bracketed or not). The host is spliced
    /// into the quack URI inside a SQL string literal, so nothing else is accepted.</summary>
    public static bool IsValidHost(string host) =>
        host.Length is > 0 and <= 253 &&
        (host.All(c => char.IsAsciiLetterOrDigit(c) || c is '.' or '-') ||
         (IPAddress.TryParse(host.Trim('[', ']'), out var address) && address.AddressFamily == AddressFamily.InterNetworkV6 &&
          host.All(c => char.IsAsciiHexDigit(c) || c is ':' or '.' or '[' or ']')));

    /// <summary>Whether <paramref name="host"/> is reachable only from this machine.</summary>
    public static bool IsLoopback(string host) =>
        host.Equals("localhost", StringComparison.OrdinalIgnoreCase) ||
        (IPAddress.TryParse(host.Trim('[', ']'), out var address) && IPAddress.IsLoopback(address));

    /// <summary>An IPv6 literal is bracketed in a quack URI, as in a URL.</summary>
    private static string FormatHost(string host) =>
        host.Contains(':', StringComparison.Ordinal) && !host.StartsWith('[') ? $"[{host}]" : host;

    private static int FreePort(string host)
    {
        var address = IPAddress.TryParse(host.Trim('[', ']'), out var parsed) ? parsed : IPAddress.Loopback;
        var listener = new TcpListener(address, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static string FirstLine(string message) => message.Split('\n')[0].Trim();

    private static PzValidationException Unavailable(string reason) =>
        new([new PzError(PzErrorCode.StagingServeUnavailable, $"pz staging serve: {reason}.", null, null,
            "pick a finished run from `pz runs`; without internet access, install the quack extension first")]);

    private static void TryDelete(string dir)
    {
        try
        {
            if (Directory.Exists(dir))
            {
                Directory.Delete(dir, recursive: true);
            }
        }
        catch (IOException)
        {
            // best-effort: a scratch view database in the temp directory
        }
        catch (UnauthorizedAccessException)
        {
            // best-effort, as above
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _session.DisposeAsync().ConfigureAwait(false);
        _lock.Dispose();
        TryDelete(_scratchDir);
    }
}
