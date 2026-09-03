namespace Pz.Connector.Quack;

/// <summary>Parses a server URI: <c>quack:host</c>, <c>quack:host:port</c> or <c>quack://host[:port]</c>,
/// where host is a name, an IPv4 literal, or a bracketed IPv6 literal (<c>quack:[::1]:9494</c>). The
/// port defaults to the quack server's own default. Kept in lockstep with the ducklake connector's
/// catalog URI parser.</summary>
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

        var rest = uri["quack:".Length..].TrimStart('/').TrimEnd('/');
        if (rest.Length == 0 || rest.Contains('/'))
        {
            return false;
        }

        // An IPv6 literal must be bracketed (`quack:[::1]:9494`): the brackets stay part of the host,
        // since the server's own canonical form keeps them, and everything after `]` is the optional
        // port. An unbracketed host may not contain a colon at all -- `quack:::1` is ambiguous, not
        // an address.
        string portPart;
        if (rest[0] == '[')
        {
            var close = rest.IndexOf(']', StringComparison.Ordinal);
            if (close < 2)
            {
                return false;
            }

            host = rest[..(close + 1)];
            portPart = rest[(close + 1)..];
            if (portPart.Length == 0)
            {
                return true;
            }

            if (portPart[0] != ':')
            {
                return false;
            }

            portPart = portPart[1..];
        }
        else
        {
            var colon = rest.IndexOf(':', StringComparison.Ordinal);
            if (colon < 0)
            {
                host = rest;
                return true;
            }

            host = rest[..colon];
            portPart = rest[(colon + 1)..];
        }

        return host.Length > 0 && int.TryParse(portPart, out port) && port is > 0 and <= 65535;
    }
}
