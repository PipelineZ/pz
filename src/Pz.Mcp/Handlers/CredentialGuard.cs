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
/// <c>auth:</c>, <c>proxy:</c>, <c>tls:</c> or <c>headers:</c>, maps and strings inside lists) whose name, split into
/// whole segments (<c>client_secret</c>, <c>X-Api-Key</c>, <c>clientSecret</c>), holds a secret word (password,
/// pass, pwd, secret, token, credential, authorization, cookie, apikey, sig, connection string) or a <c>key</c> that
/// is not a column (<c>primary_key</c>, merge <c>keys</c> are not), unless its last segment says where or what a thing
/// is (<c>_url</c>, <c>_pointer</c>, <c>_path</c>, <c>_file</c>, <c>_header</c>, <c>_type</c>, ...); a request
/// <c>body</c> written as one form or JSON string, read field by field; and any URL-shaped value, checked for a
/// typed userinfo password or a literal secret query parameter — and (b) a property the connector's own
/// <c>ConnectionConfigSchema</c> flags <c>writeOnly</c>/<c>format: password</c>, offered by the
/// <c>connectionSchemaJson</c> overload for forward compatibility. **Only (a) runs in the shipped
/// surface**: <see cref="AuthoringTools"/> calls the name-heuristic overload and nothing calls the other
/// one. Under a secret key a number is refused, and a string is refused when it holds a literal: a value made of
/// <c>${VAR}</c> references with structure around them (<c>Bearer ${TOKEN}</c>, <c>session=${S}</c>,
/// <c>${USER}:${PASSWORD}</c>) is accepted. The same guard runs over an entity's <c>read:</c>/<c>write:</c>
/// options.</summary>
public static class CredentialGuard
{
    /// <summary>Name segments that make a key secret-bearing wherever they appear (<c>client_secret</c>,
    /// <c>refresh_token</c>, <c>x-api-token</c>, <c>sig</c> of an Azure SAS).</summary>
    private static readonly HashSet<string> SecretWords = new(StringComparer.OrdinalIgnoreCase)
    {
        "password", "passwd", "pwd", "pass", "passphrase", "secret", "secrets", "token", "tokens", "credential",
        "credentials", "authorization", "cookie", "apikey", "sig", "signature", "connectionstring",
    };

    /// <summary>A last segment that says the value is where or what a thing is, never the secret itself
    /// (<c>token_url</c>, <c>token_pointer</c>, <c>key_path</c>, <c>idempotency_key_header</c>,
    /// <c>host_key_fingerprint</c>, <c>auth_type</c>).</summary>
    private static readonly HashSet<string> NotASecretSuffixes = new(StringComparer.OrdinalIgnoreCase)
    {
        "url", "uri", "pointer", "path", "file", "fingerprint", "header", "param", "name", "type", "ttl", "status",
        "method", "id", "prefix", "format", "location", "kind", "mode", "scope", "rotation", "inject", "user",
        "username", "email",
    };

    /// <summary>What a <c>key</c> segment means when one of these comes right before it: a column, not a
    /// credential (<c>primary_key</c>, <c>merge_key</c>, <c>partition_key</c>).</summary>
    private static readonly HashSet<string> NonSecretKeyQualifiers = new(StringComparer.OrdinalIgnoreCase)
    {
        "primary", "partition", "sort", "merge", "foreign", "unique", "join", "natural", "cluster", "clustering",
        "dedupe", "cursor", "idempotency", "cache", "lookup", "row", "shard", "hash", "group", "order",
        "distribution", "surrogate", "business", "composite", "item", "list", "map", "object", "s3",
    };

    /// <summary>Header-name segments that carry a credential in a <c>headers:</c> map (<c>X-Auth</c>,
    /// <c>X-Session</c>), on top of <see cref="SecretWords"/>.</summary>
    private static readonly HashSet<string> SecretHeaderWords = new(StringComparer.OrdinalIgnoreCase) { "auth", "session" };

    /// <summary>Maps whose keys are names of data (contract columns, their pointers, a for_each item's fields), not
    /// options: a column called <c>key</c> or <c>password_hash</c> is not a credential.</summary>
    private static readonly HashSet<string> NameMaps = new(StringComparer.Ordinal) { "columns", "column_pointers", "fields" };

    /// <summary>A pz template binding (<c>{{ watermark }}</c>, <c>{{ run_date }}</c>): rendered by pz, never a
    /// literal secret.</summary>
    private static readonly Regex Binding = new(@"\{\{[^}]*\}\}", RegexOptions.Compiled);

    // A name=value pair inside a value: an identifier name and a value that is not only base64 padding.
    private static readonly Regex Pair = new(@"^\s*(?<name>[A-Za-z0-9_.\-]+)\s*=(?<value>(?!=*\s*$).*)$", RegexOptions.Compiled);

    // name=value settings separated by whitespace or ';' in a free-form string (a libpq or ADO connection string).
    private static readonly Regex Setting = new(@"(?:^|[\s;:])(?<name>[A-Za-z][A-Za-z0-9_]*)\s*=\s*(?<value>[^\s;]+)",
        RegexOptions.Compiled);

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

    /// <summary>True when <paramref name="text"/>, a value under a secret-bearing key, holds a literal secret. With
    /// no <c>${...}</c> reference, any letter or digit is one. With references, the value may be structure around
    /// them: <c>name=${VAR}</c> pairs separated by <c>&amp;</c> or <c>;</c> (<c>session=${S}</c>), separators between
    /// references (<c>${USER}:${PASSWORD}</c>) and one leading scheme word (<c>Bearer ${TOKEN}</c>); a literal value
    /// anywhere in it (<c>session=abc</c>, <c>Bearer ${X} abc</c>, <c>${P}hunter2</c>) is a literal secret.</summary>
    internal static bool HasLiteral(string text, string? key = null)
    {
        var templated = EnvRef.IsMatch(text) || Binding.IsMatch(text);
        var cookie = key is not null && Segments(key).Contains("cookie");
        if (!templated && !cookie)
        {
            return text.Any(char.IsLetterOrDigit);
        }

        foreach (var part in text.Split(['&', ';'], StringSplitOptions.RemoveEmptyEntries))
        {
            // name=value: the name is structure; a literal value is a secret only under a secret-bearing name
            // (theme=dark is a preference, session=abc a credential).
            if (Pair.Match(part) is { Success: true } pair)
            {
                if (IsSecretKey(pair.Groups["name"].Value, "headers") && Unreferenced(pair.Groups["value"].Value).Any(char.IsLetterOrDigit))
                {
                    return true;
                }

                continue;
            }

            if (StripSchemeWord(Unreferenced(part).Trim()).Any(char.IsLetterOrDigit))
            {
                return true;
            }
        }

        return false;
    }

    private static string Unreferenced(string text) => Binding.Replace(EnvRef.Replace(text, " "), " ");

    private static string StripSchemeWord(string rest)
    {
        foreach (var word in SchemeWords)
        {
            if (rest.StartsWith(word, StringComparison.OrdinalIgnoreCase)
                && (rest.Length == word.Length || !char.IsLetterOrDigit(rest[word.Length])))
            {
                return rest[word.Length..];
            }
        }

        return rest;
    }

    /// <summary>A key name split into lowercase segments at <c>_</c>, <c>-</c>, <c>.</c>, spaces and camelCase
    /// boundaries (<c>clientSecret</c> → client, secret).</summary>
    private static List<string> Segments(string key)
    {
        var parts = new List<string>();
        var current = new System.Text.StringBuilder();
        for (var i = 0; i < key.Length; i++)
        {
            var c = key[i];
            if (c is '_' or '-' or '.' or ' ' || (char.IsUpper(c) && i > 0 && char.IsLower(key[i - 1])))
            {
                if (current.Length > 0)
                {
                    parts.Add(current.ToString().ToLowerInvariant());
                    current.Clear();
                }

                if (c is '_' or '-' or '.' or ' ')
                {
                    continue;
                }
            }

            current.Append(c);
        }

        if (current.Length > 0)
        {
            parts.Add(current.ToString().ToLowerInvariant());
        }

        return parts;
    }

    /// <summary>Whether a key names a secret, by whole name segments rather than substrings: <c>password</c>,
    /// <c>client_secret</c>, <c>api_key</c>, <c>key</c>, <c>X-Api-Key</c>, <c>sig</c> do; merge <c>keys</c>,
    /// <c>primary_key</c>, proxy <c>bypass</c>, <c>token_url</c>, <c>idempotency_key_header</c> do not. Inside a
    /// <c>headers:</c> map, auth and session headers (<c>X-Auth</c>, <c>X-Session</c>) count too.</summary>
    internal static bool IsSecretKey(string key, string? parentKey = null)
    {
        var segments = Segments(key);
        if (segments.Count == 0 || NotASecretSuffixes.Contains(segments[^1]))
        {
            return false;
        }

        var inHeaders = string.Equals(parentKey, "headers", StringComparison.OrdinalIgnoreCase);
        for (var i = 0; i < segments.Count; i++)
        {
            var segment = segments[i];
            if (SecretWords.Contains(segment) || (inHeaders && SecretHeaderWords.Contains(segment)))
            {
                return true;
            }

            // key, api_key, key_json, api_keys: a credential, unless a column qualifier precedes it (primary_key,
            // merge_key) or it is the bare plural (merge keys).
            if (segment is "key" or "keys")
            {
                var qualifier = i > 0 ? segments[i - 1] : null;
                if (qualifier is null ? segment == "key" : !NonSecretKeyQualifiers.Contains(qualifier))
                {
                    return true;
                }
            }
        }

        return string.Concat(segments).Contains("connectionstring", StringComparison.Ordinal);
    }

    /// <summary>Checks every string at any depth: nested maps (<c>auth:</c>, <c>proxy:</c>, <c>tls:</c>,
    /// <c>headers:</c>), lists, a login body written as one form or JSON string, and URLs, each named by its
    /// dotted path (<c>auth.login.body.password</c>, <c>accounts[1].secret</c>). The schema signal applies to
    /// top-level connection properties only, where the schema names them.</summary>
    private sealed class Walker(string label, HashSet<string> schemaFlagged, List<PzError> errors)
    {
        public void Map(Dictionary<string, object?> map, string prefix, string? parentKey, bool root,
            bool keysAreNames = false)
        {
            foreach (var (key, value) in map)
            {
                var path = prefix.Length == 0 ? key : $"{prefix}.{key}";
                // A name map's keys are data names; only its values are checked (for a URL carrying a secret).
                var secret = !keysAreNames && (IsSecretKey(key, parentKey) || (root && schemaFlagged.Contains(key)));
                switch (value)
                {
                    case Dictionary<string, object?> nested:
                        Map(nested, path, key, root: false, keysAreNames: !keysAreNames && NameMaps.Contains(key));
                        break;
                    case List<object?> list:
                        List(list, path, key, secret);
                        break;
                    case string text when key == "body" && !secret:
                        Body(text, path);
                        break;
                    case string text:
                        Scalar(text, path, key, secret);
                        break;
                    case long or int or double or decimal or float when secret:
                        // A number typed under a secret key (password: 123456) is as literal as a string.
                        errors.Add(Refusal(label, path, key));
                        break;
                }
            }
        }

        private void List(List<object?> list, string path, string key, bool secret)
        {
            for (var i = 0; i < list.Count; i++)
            {
                switch (list[i])
                {
                    case Dictionary<string, object?> nested:
                        Map(nested, $"{path}[{i}]", key, root: false);
                        break;
                    case List<object?> inner:
                        List(inner, $"{path}[{i}]", key, secret);
                        break;
                    case string text:
                        Scalar(text, $"{path}[{i}]", key, secret);
                        break;
                    case long or int or double or decimal or float when secret:
                        errors.Add(Refusal(label, $"{path}[{i}]", key));
                        break;
                }
            }
        }

        private void Scalar(string text, string path, string key, bool secret)
        {
            if (secret ? HasLiteral(text, key) : CarriesLiteralSecret(text))
            {
                errors.Add(Refusal(label, path, key));
            }
        }

        /// <summary>A request body written as one string (a session login's or a dataset's): JSON text is walked
        /// like a map, form text (<c>user=me&amp;password=...</c>) field by field under its decoded names; any other
        /// text (a GraphQL query) is only checked for a URL carrying a secret.</summary>
        private void Body(string text, string path)
        {
            var trimmed = text.TrimStart();
            if (trimmed.StartsWith('{') && TryParseJsonObject(trimmed, out var json))
            {
                Map(json, path, "body", root: false);
                return;
            }

            if (!FormText.IsMatch(text))
            {
                Scalar(text, path, "body", secret: false);
                return;
            }

            foreach (var pair in text.Split('&', StringSplitOptions.RemoveEmptyEntries))
            {
                var parts = pair.Split('=', 2);
                var name = Decode(parts[0]);
                var value = parts.Length > 1 ? Decode(parts[1]) : "";
                Scalar(value, $"{path}.{name}", name, IsSecretKey(name));
            }
        }
    }

    private static readonly Regex FormText = new(@"^[^=&\s{}]+=[^&]*(&[^=&\s{}]+=[^&]*)*$", RegexOptions.Compiled);

    /// <summary>A literal secret typed into a URL-shaped value under any key: a password in its userinfo
    /// (<c>https://user:pass@host</c>), or a literal value in a secret-bearing query parameter (<c>?api_key=abc</c>,
    /// an Azure SAS <c>sig=</c>), also in a relative <c>path</c> (<c>/r?api_key=abc</c>). Read from the raw text, so a
    /// <c>${...}</c> part is seen as written.</summary>
    private static bool CarriesLiteralSecret(string text)
    {
        var scheme = text.IndexOf("://", StringComparison.Ordinal);
        if (scheme >= 0)
        {
            var authorityStart = scheme + 3;
            var authorityEnd = text.IndexOfAny(['/', '?', '#'], authorityStart);
            var authority = authorityEnd < 0 ? text[authorityStart..] : text[authorityStart..authorityEnd];
            var at = authority.LastIndexOf('@');
            if (at > 0 && authority[..at].Split(':', 2) is [_, var password] && HasLiteral(Decode(password)))
            {
                return true;
            }
        }

        // A connection string's settings (libpq password=..., ADO AccountKey=...), anywhere in the value.
        foreach (System.Text.RegularExpressions.Match setting in Setting.Matches(text))
        {
            if (IsSecretKey(setting.Groups["name"].Value) && HasLiteral(setting.Groups["value"].Value))
            {
                return true;
            }
        }

        var query = text.IndexOf('?');
        if (query < 0)
        {
            return false;
        }

        var fragment = text.IndexOf('#', query);
        var queryText = fragment < 0 ? text[(query + 1)..] : text[(query + 1)..fragment];
        foreach (var pair in queryText.Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            var parts = pair.Split('=', 2);
            if (parts.Length > 1 && IsSecretKey(Decode(parts[0])) && HasLiteral(Decode(parts[1])))
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
        JsonValueKind.Number => element.TryGetInt64(out var l) ? l : element.GetDouble(),
        _ => null,
    };

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
