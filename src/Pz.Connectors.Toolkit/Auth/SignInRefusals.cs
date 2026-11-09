using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;

namespace Pz.Connectors.Toolkit.Auth;

/// <summary>Sign-ins the far side refused for good (a rejected password, a revoked or rotated refresh token),
/// remembered for the life of the process. Every node opens its own authenticator, so without this each node
/// would repeat the refused sign-in, and a few nodes are enough to lock an account or to trip a provider's
/// refresh-token reuse detection. Keys are SHA-256 fingerprints of the endpoint and the credential: a secret is
/// never stored. A changed credential has a new fingerprint and is tried again. Sign-ins with the same key are
/// also serialised, so nodes starting together send one attempt between them, not one each.</summary>
internal static class SignInRefusals
{
    private static readonly ConcurrentDictionary<string, string> Refused = new(StringComparer.Ordinal);
    private static readonly ConcurrentDictionary<string, SemaphoreSlim> Gates = new(StringComparer.Ordinal);

    /// <summary>A fingerprint of <paramref name="parts"/>; each part is length-prefixed so no two lists collide.</summary>
    public static string Fingerprint(IEnumerable<string?> parts)
    {
        var text = new StringBuilder();
        foreach (var part in parts)
        {
            var value = part ?? "\0null";
            text.Append(value.Length).Append(':').Append(value).Append('|');
        }

        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(text.ToString())));
    }

    public static SemaphoreSlim Gate(string key) => Gates.GetOrAdd(key, _ => new SemaphoreSlim(1, 1));

    public static bool TryGet(string key, out string message) => Refused.TryGetValue(key, out message!);

    public static void Record(string key, string message) => Refused.TryAdd(key, message);
}
