using System.Net.Sockets;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Iceberg;

/// <summary>Zero-driver connectivity checks for <c>pz validate --connect</c>: a REST catalog by
/// TCP reachability only — credentials cannot be verified without a driver and are exercised by
/// the first run's attach, whose failure is a redacted PZ0311 — and a local <c>files</c> root by
/// its directory existing.</summary>
internal static class IcebergProbe
{
    internal static ValueTask<ConnectionCheck> CheckDirectory(string path)
    {
        if (!Directory.Exists(path))
        {
            return new(new ConnectionCheck(false,
                "permanent: the 'root' directory does not exist -- reads cannot create it; fix the connection's 'root'"));
        }

        return new(new ConnectionCheck(true, "root directory exists; tables are verified at run time"));
    }
}
