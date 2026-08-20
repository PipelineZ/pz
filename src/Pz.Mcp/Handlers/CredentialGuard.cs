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
/// Credential shape is two signals: (a) the floor — a key whose name (case-insensitive) CONTAINS
/// "password", "secret", "token", "key", or "connection_string" — and (b) a property the connector's own
/// <c>ConnectionConfigSchema</c> flags <c>writeOnly</c>/<c>format: password</c>, offered by the
/// <c>connectionSchemaJson</c> overload for forward compatibility. **Only (a) runs in the shipped
/// surface**: <see cref="AuthoringTools"/> calls the name-heuristic overload and nothing calls the other
/// one, which is why the docs (mcp-contract.md's PZ0601 row, use-with-an-ai-agent.md's Secrets section)
/// describe the name heuristic alone. Signal (b) is also vacuous against the first-party connectors:
/// neither <c>Pz.Connector.Postgres</c> nor <c>Pz.Connector.LocalFiles</c>
/// marks a connection property <c>writeOnly</c> or <c>format: password</c> (plain
/// <c>{"type":"string"}</c> for <c>password</c> either way). Either signal is only consulted for a
/// *string* value that is not itself an env var reference: <c>${VAR}</c> (the whole value, nothing else)
/// is always accepted, matching the point of the guard.</summary>
public static class CredentialGuard
{
    private static readonly Regex CredentialShapedKeyName =
        new("password|secret|token|key|connection_string", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Regex EnvRefPattern = new(@"^\$\{[^}]+\}$", RegexOptions.Compiled);

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
        foreach (var (key, value) in connection)
        {
            if (value is not string text)
            {
                continue; // only a literal string can carry a credential -- numbers/bools/maps cannot
            }

            if (EnvRefPattern.IsMatch(text))
            {
                continue; // "${VAR}" (and nothing else) -- an env var reference, not a literal
            }

            if (!CredentialShapedKeyName.IsMatch(key) && !schemaFlagged.Contains(key))
            {
                continue;
            }

            errors.Add(new PzError(PzErrorCode.McpLiteralCredential,
                $"connection option '{key}' looks like a literal credential typed directly into YAML.",
                null, null,
                $"write '{key}' as an env var reference instead, e.g. \"${{{key.ToUpperInvariant()}}}\", " +
                "and set that environment variable out of band rather than typing the value here -- " +
                "see https://pipelinez.dev/how-to/secure-connection-config/"));
        }

        return errors;
    }

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
