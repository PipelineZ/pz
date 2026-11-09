using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;

namespace Pz.Connectors.Toolkit.Auth;

/// <summary>Sign-ins the far side refused for good (a rejected password, a revoked or rotated refresh token),
/// remembered for the run that saw them. Every node opens its own authenticator, so without this each node
/// would repeat the refused sign-in, and a few nodes are enough to lock an account or to trip a provider's
/// refresh-token reuse detection. Keys are SHA-256 fingerprints of the endpoint and the credential: a secret is
/// never stored. A changed credential has a new fingerprint and is tried again. A refusal is recorded under the
/// key and the run (<see cref="CachedTokenAuthenticator.RunScope"/>), so the next run in a long-lived process
/// (<c>pz mcp</c>) makes its own one attempt; only refusals are kept, so the map grows by refusal, not by run.
/// Sign-ins with the same key are also serialised (across runs too: the gate is keyed by the credential alone),
/// so nodes starting together send one attempt between them, not one each.</summary>
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

    public static bool TryGet(string key, string? run, out string message)
        => Refused.TryGetValue(Scoped(key, run), out message!);

    public static void Record(string key, string? run, string message) => Refused.TryAdd(Scoped(key, run), message);

    // No run (a host that never names one) is a scope of its own, kept for the process.
    private static string Scoped(string key, string? run) => run is null ? key : Fingerprint([key, "run", run]);
}
