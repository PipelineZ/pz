using System.Text.Json;
using System.Text.RegularExpressions;
using Pz.Core.Validation;

namespace Pz.Mcp.Handlers;

/// <summary>Step 1 of the connection-authoring mutation pipeline: refuses a
/// proposed connection block that carries what looks like a literal credential typed directly into
/// YAML, rather than an env var reference (<c>${VAR}</c>). Runs before anything else in
/// <see cref="AuthoringTools"/>'s pipeline — no project load, no registry resolution, no file I/O — so
/// a caller gets PZ0601 back with zero side effects and the secret itself never leaves this process
/// (the returned <see cref="PzError.Message"/> names the offending KEY only, never the value).
///
/// Credential shape is two signals: (a) the floor — a key at ANY depth (nested maps such as an http
/// <c>auth:</c>, <c>proxy:</c>, <c>tls:</c> or <c>headers:</c>, and maps inside lists) whose name
/// (case-insensitive) CONTAINS "pass", "pwd", "secret", "token", "key", "credential", "connection_string",
/// "authorization" or "cookie", unless it ends in <c>_url</c>, <c>_uri</c>, <c>_pointer</c>, <c>_path</c>,
/// <c>_file</c> or <c>_fingerprint</c> (where a thing is, not the thing); strings in a list under such a key; a
/// session <c>login.body</c> written as one form or JSON string, read field by field; and a URL-valued key
/// (<c>url</c>, <c>_url</c>, <c>_uri</c>) checked for a typed password or secret query parameter — and (b) a property the connector's own
/// <c>ConnectionConfigSchema</c> flags <c>writeOnly</c>/<c>format: password</c>, offered by the
/// <c>connectionSchemaJson</c> overload for forward compatibility. **Only (a) runs in the shipped
/// surface**: <see cref="AuthoringTools"/> calls the name-heuristic overload and nothing calls the other
/// one, which is why the docs (mcp-contract.md's PZ0601 row, use-with-an-ai-agent.md's Secrets section)
/// describe the name heuristic alone. Signal (b) is also vacuous against the first-party connectors:
/// neither <c>Pz.Connector.Postgres</c> nor <c>Pz.Connector.LocalFiles</c>
/// marks a connection property <c>writeOnly</c> or <c>format: password</c> (plain
/// <c>{"type":"string"}</c> for <c>password</c> either way). Either signal is only consulted for a
/// *string* value holding a literal: a value made only of <c>${VAR}</c> references, separators between them and
/// one leading scheme word (<c>Bearer ${TOKEN}</c>) is always accepted, matching the point of the guard. The
/// same guard runs over an entity's <c>read:</c>/<c>write:</c> options.</summary>
public static class CredentialGuard
{
    private static readonly Regex CredentialShapedKeyName =
        new("pass|pwd|secret|token|key|credential|connection_string|authorization|cookie",
            RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Regex NotASecret =
        new("(_|^)(url|uri|pointer|path|file|fingerprint)$", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Regex EnvRef = new(@"\$\{[^}]+\}", RegexOptions.Compiled);

    /// <summary>Authorization scheme words that may stand beside a reference (<c>Bearer ${TOKEN}</c>): they are
    /// fixed protocol text, not part of the secret.</summary>
    private static readonly string[] SchemeWords = ["Bearer", "Basic", "Token", "Digest", "ApiKey", "Bot"];

    /// <summary>The floor: key-name heuristic only. This is the entry point
    /// <see cref="AuthoringTools"/> calls first, before it has resolved (or even knows whether it CAN
    /// resolve) the target connector.</summary>
    public static IReadOnlyList<PzError> FindLiteralCredentials(Dictionary<string, object?> connection) =>
        FindLiteralCredentials(connection, connectionSchemaJson: null);

    /// <summary>Same guard, plus signal (b) above when the connector's own connection schema is
    /// available (already resolved for step 2's pre-validate) — a defensive extra pass a caller MAY run
    /// once the connector is known, on top of the always-first plain overload.</summary>
    public static IReadOnlyList<PzError> FindLiteralCredentials(
        Dictionary<string, object?> connection, string? connectionSchemaJson)
    {
        var schemaFlagged = connectionSchemaJson is null
            ? []
            : CredentialShapedPropertiesFromSchema(connectionSchemaJson);

        var errors = new List<PzError>();
        new Walker("connection option", schemaFlagged, errors).Map(connection, prefix: "", parentKey: null, root: true);
        return errors;
    }

    /// <summary>The guard over an entity's <c>read:</c> or <c>write:</c> options, named under
    /// <paramref name="pathPrefix"/> (<c>entities.orders.read</c>): a query parameter, header or body field of a
    /// dataset carries a credential as easily as a connection does.</summary>
    public static IReadOnlyList<PzError> FindLiteralCredentialsUnder(Dictionary<string, object?> options, string pathPrefix)
    {
        var errors = new List<PzError>();
        new Walker("option", [], errors).Map(options, pathPrefix, parentKey: null, root: false);
        return errors;
    }

    /// <summary>True when <paramref name="text"/> holds a literal beside its <c>${...}</c> references. A value made
    /// only of references, separators between them (<c>${USER}:${PASSWORD}</c>) and one leading scheme word
    /// (<c>Bearer ${TOKEN}</c>) is not: the secret lives in the environment.</summary>
    internal static bool HasLiteral(string text)
    {
        var rest = EnvRef.Replace(text, " ").Trim();
        foreach (var word in SchemeWords)
        {
            if (rest.StartsWith(word, StringComparison.OrdinalIgnoreCase)
                && (rest.Length == word.Length || !char.IsLetterOrDigit(rest[word.Length])))
            {
                rest = rest[word.Length..];
                break;
            }
        }

        return rest.Any(char.IsLetterOrDigit);
    }

    /// <summary>Checks every string at any depth: nested maps (<c>auth:</c>, <c>proxy:</c>, <c>tls:</c>,
    /// <c>headers:</c>), lists, a login body written as one form or JSON string, and URLs, each named by its
    /// dotted path (<c>auth.login.body.password</c>, <c>accounts[1].secret</c>). The schema signal applies to
    /// top-level connection properties only, where the schema names them.</summary>
    private sealed class Walker(string label, HashSet<string> schemaFlagged, List<PzError> errors)
    {
        public void Map(Dictionary<string, object?> map, string prefix, string? parentKey, bool root)
        {
            foreach (var (key, value) in map)
            {
                var path = prefix.Length == 0 ? key : $"{prefix}.{key}";
                switch (value)
                {
                    case Dictionary<string, object?> nested:
                        Map(nested, path, key, root: false);
                        break;
                    case List<object?> list:
                        List(list, path, key);
                        break;
                    case string text when key == "body" && parentKey == "login":
                        LoginBody(text, path);
                        break;
                    case string text:
                        Scalar(text, path, key, flagged: IsCredentialShaped(key) || (root && schemaFlagged.Contains(key)));
                        break;
                }
            }
        }

        private void List(List<object?> list, string path, string key)
        {
            for (var i = 0; i < list.Count; i++)
            {
                switch (list[i])
                {
                    case Dictionary<string, object?> nested:
                        Map(nested, $"{path}[{i}]", key, root: false);
                        break;
                    case List<object?> inner:
                        List(inner, $"{path}[{i}]", key);
                        break;
                    case string text:
                        Scalar(text, $"{path}[{i}]", key, flagged: IsCredentialShaped(key));
                        break;
                }
            }
        }

        private void Scalar(string text, string path, string key, bool flagged)
        {
            if (flagged ? HasLiteral(text) : IsUrlKey(key) && UrlCarriesLiteralSecret(text))
            {
                errors.Add(Refusal(label, path, key));
            }
        }

        /// <summary>A session login body written as one string: JSON text is walked like a map, anything else
        /// is read as a form (<c>user=me&amp;password=...</c>), each field under its own decoded name.</summary>
        private void LoginBody(string text, string path)
        {
            var trimmed = text.TrimStart();
            if (trimmed.StartsWith('{') && TryParseJsonObject(trimmed, out var json))
            {
                Map(json, path, "body", root: false);
                return;
            }

            foreach (var pair in text.Split('&', StringSplitOptions.RemoveEmptyEntries))
            {
                var parts = pair.Split('=', 2);
                var name = Decode(parts[0]);
                Scalar(parts.Length > 1 ? Decode(parts[1]) : "", $"{path}.{name}", name,
                    flagged: IsCredentialShaped(name));
            }
        }
    }

    private static bool IsUrlKey(string key)
        => key.Equals("url", StringComparison.OrdinalIgnoreCase) || key.Equals("uri", StringComparison.OrdinalIgnoreCase)
            || key.EndsWith("_url", StringComparison.OrdinalIgnoreCase) || key.EndsWith("_uri", StringComparison.OrdinalIgnoreCase);

    /// <summary>A URL typed with a literal password in its userinfo (<c>https://user:pass@host</c>) or a literal
    /// value in a credential-shaped query parameter (<c>?client_secret=abc</c>). Read from the raw text, so a
    /// <c>${...}</c> part is seen as written.</summary>
    private static bool UrlCarriesLiteralSecret(string url)
    {
        var scheme = url.IndexOf("://", StringComparison.Ordinal);
        if (scheme < 0)
        {
            return false;
        }

        var authorityStart = scheme + 3;
        var authorityEnd = url.IndexOfAny(['/', '?', '#'], authorityStart);
        var authority = authorityEnd < 0 ? url[authorityStart..] : url[authorityStart..authorityEnd];
        var at = authority.LastIndexOf('@');
        if (at > 0 && authority[..at].Split(':', 2) is [_, var password] && HasLiteral(Decode(password)))
        {
            return true;
        }

        var query = url.IndexOf('?');
        if (query < 0)
        {
            return false;
        }

        var fragment = url.IndexOf('#', query);
        var text = fragment < 0 ? url[(query + 1)..] : url[(query + 1)..fragment];
        foreach (var pair in text.Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            var parts = pair.Split('=', 2);
            if (IsCredentialShaped(Decode(parts[0])) && parts.Length > 1 && HasLiteral(Decode(parts[1])))
            {
                return true;
            }
        }

        return false;
    }

    private static string Decode(string text)
    {
        try
        {
            return Uri.UnescapeDataString(text.Replace('+', ' '));
        }
        catch (UriFormatException)
        {
            return text;
        }
    }

    private static bool TryParseJsonObject(string text, out Dictionary<string, object?> map)
    {
        map = [];
        try
        {
            using var doc = JsonDocument.Parse(text);
            if (doc.RootElement.ValueKind != JsonValueKind.Object)
            {
                return false;
            }

            map = (Dictionary<string, object?>)FromJson(doc.RootElement)!;
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static object? FromJson(JsonElement element) => element.ValueKind switch
    {
        JsonValueKind.Object => element.EnumerateObject().ToDictionary(p => p.Name, p => FromJson(p.Value)),
        JsonValueKind.Array => element.EnumerateArray().Select(FromJson).ToList(),
        JsonValueKind.String => element.GetString(),
        _ => null,
    };

    // A path, URL, JSON pointer or fingerprint names where a thing is, never the secret itself, even when its
    // name says key or token (tls key_path, auth token_url, token_pointer, sftp host_key_fingerprint). A URL is
    // still checked for a password or a secret query parameter typed into it.
    private static bool IsCredentialShaped(string key)
        => CredentialShapedKeyName.IsMatch(key) && !NotASecret.IsMatch(key);

    private static PzError Refusal(string label, string path, string key) => new(PzErrorCode.McpLiteralCredential,
        $"{label} '{path}' looks like a literal credential typed directly into YAML.",
        null, null,
        $"write '{path}' as an env var reference instead, e.g. \"${{{EnvName(key)}}}\" (a scheme word may stay: " +
        "\"Bearer ${TOKEN}\"), and set that environment variable out of band rather than typing the value here -- " +
        "see https://pipelinez.dev/how-to/secure-connection-config/");

    private static string EnvName(string key) => Regex.Replace(key, "[^A-Za-z0-9]+", "_").ToUpperInvariant();

    /// <summary>Property names a JSON Schema marks <c>"writeOnly": true</c> or <c>"format": "password"</c>
    /// — the two conventional JSON-Schema-vocabulary ways to flag a secret-shaped field. Malformed
    /// schema text is swallowed here (falls back to the name heuristic alone): step 2's own
    /// <c>ConnectorConfigValidator</c> call parses the same schema text and reports a malformed schema
    /// in its own terms, so this guard does not need to duplicate that diagnostic.</summary>
    private static HashSet<string> CredentialShapedPropertiesFromSchema(string schemaJson)
    {
        var flagged = new HashSet<string>(StringComparer.Ordinal);
        try
        {
            using var doc = JsonDocument.Parse(schemaJson);
            if (doc.RootElement.ValueKind != JsonValueKind.Object ||
                !doc.RootElement.TryGetProperty("properties", out var properties) ||
                properties.ValueKind != JsonValueKind.Object)
            {
                return flagged;
            }

            foreach (var property in properties.EnumerateObject())
            {
                var schema = property.Value;
                if (schema.ValueKind != JsonValueKind.Object)
                {
                    continue;
                }

                var isWriteOnly = schema.TryGetProperty("writeOnly", out var writeOnly)
                    && writeOnly.ValueKind == JsonValueKind.True;
                var isPasswordFormat = schema.TryGetProperty("format", out var format)
                    && format.ValueKind == JsonValueKind.String
                    && string.Equals(format.GetString(), "password", StringComparison.OrdinalIgnoreCase);

                if (isWriteOnly || isPasswordFormat)
                {
                    flagged.Add(property.Name);
                }
            }
        }
        catch (JsonException)
        {
            // malformed schema text -- fall back to the name heuristic alone.
        }

        return flagged;
    }
}
