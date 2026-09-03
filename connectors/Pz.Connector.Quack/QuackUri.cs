namespace Pz.Connector.Quack;

/// <summary>Parses a server URI: <c>quack:host</c>, <c>quack:host:port</c> or <c>quack://host[:port]</c>.
/// The port defaults to the quack server's own default. Kept in lockstep with the ducklake
/// connector's catalog URI parser.</summary>
internal static class QuackUri
{
    internal const int DefaultPort = 9494;

    internal static bool TryParse(string uri, out string host, out int port)
    {
        host = "";
        port = DefaultPort;
        if (!uri.StartsWith("quack:", StringComparison.Ordinal))
        {
            return false;
        }

        var rest = uri["quack:".Length..].TrimStart('/');
        if (rest.Length == 0)
        {
            return false;
        }

        var colon = rest.LastIndexOf(':');
        if (colon < 0)
        {
            host = rest;
            return true;
        }

        host = rest[..colon];
        return host.Length > 0 && int.TryParse(rest[(colon + 1)..], out port) && port is > 0 and <= 65535;
    }
}
