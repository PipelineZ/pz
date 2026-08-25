using System.Text.Json;
using System.Text.RegularExpressions;
using Json.Pointer;
using Json.Schema;
using Pz.Connectors.Abstractions;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Execution;

namespace Pz.Engine.Validation;

/// <summary>Tier 3 of `pz validate`: connector-published JSON Schema validation of every source/sink
/// connection block and every source dataset's options, plus each resolved connector's own
/// cross-field <see cref="IConnector.ValidateAsync"/>. Never throws for a config violation -- every
/// finding becomes one aggregated <see cref="PzError"/> naming the yml file and the offending path.
/// Two rules keep the aggregate readable: (1) when the connection schema flags a key as MISSING
/// (a `required` violation) and the connector's own <see cref="IConnector.ValidateAsync"/> separately
/// reports, in our connectors' own standard phrasing, that it "requires" that exact same key, only the
/// schema's line is kept -- the cross-field duplicate is suppressed (see
/// <see cref="ValidateCrossFieldAsync"/>'s <c>requiredFlaggedKeys</c> parameter and
/// <see cref="ReferencesFlaggedKey"/>). Suppression requires BOTH that the schema violation was a
/// required/missing-property one AND that the connector's message uses the "requires '&lt;key&gt;'"
/// phrasing, so genuinely distinct cross-field problems about the same key (e.g. a value-shape complaint
/// from a third-party connector) still survive; (2) a schema violation whose
/// <c>InstanceLocation</c> is the object root must not render a dangling "`: :`" (see the
/// <c>location</c> computation in <see cref="ValidateSchema"/>).</summary>
public static class ConnectorConfigValidator
{
    private const string Hint = "fix the value to match the connector's schema";
    private static readonly Regex QuotedNamePattern = new("\"([^\"]+)\"", RegexOptions.Compiled);

    /// <summary>Tier 3: every source/sink connection block against the connector's
    /// ConnectionConfigSchema, every source dataset's options against DatasetConfigSchema, then the
    /// connector's own ValidateAsync for cross-field rules. All errors aggregated; never throws for
    /// validation failures. Config is validated as the user wrote it (pre-base_dir-injection).</summary>
    public static async Task<IReadOnlyList<PzError>> ValidateAsync(
        PzProject project, ConnectorRegistry registry, CancellationToken ct)
    {
        var errors = new List<PzError>();
        RefuseReservedProperties(project, registry, errors);
        RefuseUnknownConnectors(project, registry, errors);

        foreach (var source in project.Connections)
        {
            if (!registry.TryGetSource(source.Connector, out var connector))
            {
                // Not a source connector: either sink-only (handled by the second loop below) or
                // unknown (already reported by RefuseUnknownConnectors).
                continue;
            }

            // Keys the connection schema already flagged for THIS source, split into two sets: every
            // flagged key (for the schema call's own intra-evaluation dedup, e.g. a `oneOf` schema like
            // Postgres's table/query exclusivity independently re-reporting the same missing key from
            // each disjunct) versus the subset flagged specifically via a `required`/missing-property
            // violation (passed to ValidateCrossFieldAsync below -- only THIS subset may suppress a
            // cross-field duplicate). Dataset-schema calls get their own throwaway sets: they never
            // participate in the connection-level dedup.
            var connectionFlaggedKeys = new HashSet<string>(StringComparer.Ordinal);
            var requiredFlaggedKeys = new HashSet<string>(StringComparer.Ordinal);
            ValidateSchema(connector.ConnectionConfigSchema, source.Connection, "connection", source.Name,
                "connection", source.FilePath, errors, connectionFlaggedKeys, requiredFlaggedKeys);

            foreach (var dataset in source.Datasets)
            {
                var options = MergeColumns(dataset.Options, dataset.Columns);
                ValidateSchema(connector.DatasetConfigSchema, options, "connection", source.Name,
                    $"dataset '{dataset.Name}'", source.FilePath, errors, new HashSet<string>(StringComparer.Ordinal),
                    new HashSet<string>(StringComparer.Ordinal));
            }

            await ValidateCrossFieldAsync(connector, source.Connection, "connection", source.Name, source.FilePath,
                errors, requiredFlaggedKeys, ct).ConfigureAwait(false);
        }

        // A connection whose connector also reads was config-validated in the loop above, so this one
        // covers write-only connectors -- a postgres connection is not validated, and its every config
        // error not reported, twice.
        foreach (var sink in project.Connections)
        {
            if (registry.TryGetSource(sink.Connector, out _) || !registry.TryGetSink(sink.Connector, out var connector))
            {
                continue;
            }

            var connectionFlaggedKeys = new HashSet<string>(StringComparer.Ordinal);
            var requiredFlaggedKeys = new HashSet<string>(StringComparer.Ordinal);
            ValidateSchema(connector.ConnectionConfigSchema, sink.Connection, "connection", sink.Name,
                "connection", sink.FilePath, errors, connectionFlaggedKeys, requiredFlaggedKeys);

            // Sink OUTPUT options are NOT schema-validated in v0: they are already validated at
            // plan/probe time by the connectors themselves.
            await ValidateCrossFieldAsync(connector, sink.Connection, "connection", sink.Name, sink.FilePath,
                errors, requiredFlaggedKeys, ct).ConfigureAwait(false);
        }

        return errors;
    }

    /// <summary>Nothing upstream checks a connection's connector NAME against the registry (PZ0305's
    /// other raise sites cover missing packages and cross-package collisions), so without this a typo'd
    /// <c>connector:</c> would pass `pz validate` silently and fail only at run time. One aggregated
    /// error per connection, naming the known set.</summary>
    private static void RefuseUnknownConnectors(PzProject project, ConnectorRegistry registry,
        List<PzError> errors)
    {
        foreach (var connection in project.Connections)
        {
            if (registry.TryGetSource(connection.Connector, out _) || registry.TryGetSink(connection.Connector, out _))
            {
                continue;
            }

            var known = registry.Sources.Keys.Union(registry.Sinks.Keys, StringComparer.Ordinal)
                .OrderBy(n => n, StringComparer.Ordinal);
            errors.Add(new PzError(PzErrorCode.ConnectorNotInstalled,
                $"connection '{connection.Name}': unknown connector '{connection.Connector}'.",
                connection.FilePath, null,
                $"available connectors: {string.Join(", ", known)} -- fix the name, or add the " +
                "providing package under project.yml connectors:"));
        }
    }

    /// <summary>The block clause of a config error. Empty when the block IS the connection -- the kind
    /// is already "connection", and "connection 'db' connection:" stutters.</summary>
    private static string Where(string blockLabel) =>
        string.Equals(blockLabel, "connection", StringComparison.Ordinal) ? "" : $" {blockLabel}";

    /// <summary>Keys pz owns at connection level. Kept here rather than referenced from
    /// <c>Pz.Core.Loading.ConnectionsLoader</c> because that type is internal to Pz.Core; the two lists
    /// are pinned equal by <c>ConnectorConfigValidatorTests</c>.</summary>
    private static readonly string[] ReservedConnectionKeys =
        ["connector", "entities", "max_concurrency", "rate_limit", "retry", "allow_unsigned_extensions"];

    /// <summary>A connector whose ConnectionConfigSchema declares a property pz owns could never receive
    /// that key -- the loader strips reserved keys out of connector config before the connector ever
    /// sees them. Refused rather than silently starved.
    ///
    /// This runs at validation tier 3 rather than at connector registration, because
    /// ConnectorRegistry.Add* have no error channel and giving them one ripples through every
    /// registration site for no observable gain. Tier 3 resolves connectors from the lockfile without
    /// connecting and runs before any node executes, so pz still refuses to run, naming both.</summary>
    private static void RefuseReservedProperties(PzProject project, ConnectorRegistry registry,
        List<PzError> errors)
    {
        var checkedConnectors = new HashSet<string>(StringComparer.Ordinal);
        foreach (var connection in project.Connections)
        {
            if (!checkedConnectors.Add(connection.Connector))
            {
                continue;
            }

            var schemaText = registry.TryGetSource(connection.Connector, out var source) ? source.ConnectionConfigSchema
                : registry.TryGetSink(connection.Connector, out var sink) ? sink.ConnectionConfigSchema
                : null;
            if (schemaText is null)
            {
                continue;
            }

            JsonElement root;
            try
            {
                root = JsonSerializer.Deserialize<JsonElement>(schemaText);
            }
            catch (JsonException)
            {
                continue; // malformed schema: JsonSchema.FromText below reports it in its own terms
            }

            if (root.ValueKind != JsonValueKind.Object ||
                !root.TryGetProperty("properties", out var properties) ||
                properties.ValueKind != JsonValueKind.Object)
            {
                continue;
            }

            foreach (var property in properties.EnumerateObject())
            {
                if (ReservedConnectionKeys.Contains(property.Name, StringComparer.Ordinal))
                {
                    errors.Add(new PzError(PzErrorCode.ReservedConnectionKey,
                        $"connector '{connection.Connector}' declares connection property " +
                        $"'{property.Name}', which pz owns at connection level.",
                        connection.FilePath, null,
                        "rename the connector's property -- pz reserves connector, entities, " +
                        "max_concurrency, rate_limit, retry, and allow_unsigned_extensions"));
                }
            }
        }
    }

    private static void ValidateSchema(string schemaText, IReadOnlyDictionary<string, object?> values,
        string kind, string name, string blockLabel, string filePath, List<PzError> errors,
        HashSet<string> flaggedKeys, HashSet<string> requiredFlaggedKeys)
    {
        // An empty schema text is "this connector declares no schema for this block", not a malformed
        // one: an out-of-process connector carries its schemas in its handshake, so one that has not been
        // spawned yet has none to offer, and JsonSchema.FromText would fail on the empty document rather
        // than say so. Deliberately NOT a reason to spawn a connector during validation -- the
        // connector's own ValidateAsync (ValidateCrossFieldAsync below) is where it still gets its say
        // about a config it has actually been handed.
        if (string.IsNullOrEmpty(schemaText))
        {
            return;
        }

        var schema = JsonSchema.FromText(schemaText);
        var node = YamlToJson.Convert(new Dictionary<string, object?>(values));
        var element = JsonSerializer.Deserialize<JsonElement>(node);
        var result = schema.Evaluate(element, new EvaluationOptions { OutputFormat = OutputFormat.List });

        if (result.IsValid)
        {
            return;
        }

        foreach (var detail in result.Details ?? [])
        {
            if (detail.Errors is not { Count: > 0 } detailErrors)
            {
                continue;
            }

            var location = detail.InstanceLocation == JsonPointer.Empty ? "" : $"{detail.InstanceLocation}: ";

            // "required" gets its own branch: JsonSchema.Net reports every missing property of a
            // violated `required` keyword as ONE combined message ('Required properties ["a","b"] are
            // not present") -- splitting it into one PzError per property name gives each missing key
            // its own line and lets flaggedKeys name each one individually for the cross-field dedup
            // below. Also naturally de-dupes a `oneOf`
            // schema (e.g. Postgres's table/query exclusivity) that re-reports the same missing key from
            // each disjunct: HashSet.Add returns false for a key already seen in this same call.
            if (detailErrors.TryGetValue("required", out var requiredMessage))
            {
                foreach (var missingKey in QuotedNamePattern.Matches(requiredMessage).Select(m => m.Groups[1].Value))
                {
                    if (!flaggedKeys.Add(missingKey))
                    {
                        continue;
                    }

                    // This key's violation IS a required/missing-property one -- eligible (subject also
                    // to ReferencesFlaggedKey's phrasing check) to suppress a cross-field duplicate below.
                    requiredFlaggedKeys.Add(missingKey);
                    errors.Add(new PzError(PzErrorCode.ConnectorConfigInvalid,
                        $"{kind} '{name}'{Where(blockLabel)}: {location}'{missingKey}' is required", filePath, null, Hint));
                }

                continue;
            }

            var propertyKey = detail.InstanceLocation == JsonPointer.Empty
                ? ""
                : detail.InstanceLocation.ToString().TrimStart('/');
            if (propertyKey.Length > 0 && !flaggedKeys.Add(propertyKey))
            {
                continue; // already reported this exact property in this same schema evaluation
            }

            // An `additionalProperties: false` violation is the single most common authoring mistake
            // (a misremembered or misspelled option name) and gets the least usable message: the
            // library reports the subschema it failed -- "All values fail against the false schema" --
            // which names neither the offending key nor what was allowed instead. Say both.
            if (TryDescribeUnknownOption(schemaText, detail, out var unknownOption, out var accepted))
            {
                errors.Add(new PzError(PzErrorCode.ConnectorConfigInvalid,
                    $"{kind} '{name}'{Where(blockLabel)}: {ContainerLocation(detail)}unknown option '{unknownOption}'",
                    filePath, null,
                    accepted.Count > 0
                        ? $"remove or rename it -- accepted options: {string.Join(", ", accepted)}"
                        : "remove it -- this block accepts no options"));
                continue;
            }

            var firstError = detailErrors.Values.First();
            errors.Add(new PzError(PzErrorCode.ConnectorConfigInvalid,
                $"{kind} '{name}'{Where(blockLabel)}: {location}{firstError}", filePath, null, Hint));
        }
    }

    /// <summary>The instance path of the object the unknown option sits IN, as a message prefix —
    /// empty for a top-level option, where the option's own name is the whole story and repeating it
    /// as a path would stutter ("/connection_string: unknown option 'connection_string'").</summary>
    private static string ContainerLocation(EvaluationResults detail)
    {
        var segments = SplitPointer(detail.InstanceLocation.ToString());
        return segments.Count <= 1 ? "" : $"/{string.Join('/', segments.Take(segments.Count - 1))}: ";
    }

    /// <summary>Recognizes the "unknown option" violation and names what was allowed instead.
    ///
    /// Detection is structural, never textual: the evaluation path's last segment is
    /// <c>additionalProperties</c>, AND resolving its parent in the schema document finds an
    /// <c>additionalProperties</c> that is literally <see langword="false"/>. Both halves matter —
    /// <c>additionalProperties</c> is equally legal as a SUBSCHEMA (localfiles' <c>columns</c> maps
    /// every column name to a type enum that way), and a violation under one of those means "this
    /// value is wrong", not "this key is unknown". Matching the library's message text instead would
    /// conflate the two and break on any rewording upstream.</summary>
    private static bool TryDescribeUnknownOption(
        string schemaText, EvaluationResults detail, out string option, out IReadOnlyList<string> accepted)
    {
        option = "";
        accepted = [];

        var schemaSegments = SplitPointer(detail.EvaluationPath.ToString());
        if (schemaSegments.Count == 0 ||
            !string.Equals(schemaSegments[^1], "additionalProperties", StringComparison.Ordinal))
        {
            return false;
        }

        JsonElement root;
        try
        {
            root = JsonSerializer.Deserialize<JsonElement>(schemaText);
        }
        catch (JsonException)
        {
            return false; // malformed schema: the generic branch reports it in the library's terms
        }

        if (!TryResolve(root, schemaSegments.Take(schemaSegments.Count - 1), out var container) ||
            !container.TryGetProperty("additionalProperties", out var additional) ||
            additional.ValueKind != JsonValueKind.False)
        {
            return false;
        }

        var instanceSegments = SplitPointer(detail.InstanceLocation.ToString());
        if (instanceSegments.Count == 0)
        {
            return false; // the root object itself, with no key to name
        }

        option = instanceSegments[^1];
        accepted = container.TryGetProperty("properties", out var properties) &&
                properties.ValueKind == JsonValueKind.Object
            ? [.. properties.EnumerateObject().Select(p => p.Name).OrderBy(n => n, StringComparer.Ordinal)]
            : [];
        return true;
    }

    /// <summary>JSON Pointer segments, unescaped per RFC 6901 (<c>~1</c> before <c>~0</c>, or a literal
    /// <c>~1</c> would decode twice). Works off the pointer's string form rather than the library's
    /// indexer, which has been renamed across major versions of JsonPointer.Net.</summary>
    private static List<string> SplitPointer(string pointer) =>
    [
        .. pointer.Split('/', StringSplitOptions.RemoveEmptyEntries)
            .Select(s => s.Replace("~1", "/", StringComparison.Ordinal).Replace("~0", "~", StringComparison.Ordinal)),
    ];

    /// <summary>Walks <paramref name="segments"/> through <paramref name="root"/>, stepping into array
    /// elements by index (an <c>allOf</c>/<c>anyOf</c> branch puts one in the evaluation path).</summary>
    private static bool TryResolve(JsonElement root, IEnumerable<string> segments, out JsonElement resolved)
    {
        resolved = root;
        foreach (var segment in segments)
        {
            if (resolved.ValueKind == JsonValueKind.Array && int.TryParse(segment, out var index) &&
                index >= 0 && index < resolved.GetArrayLength())
            {
                resolved = resolved[index];
                continue;
            }

            if (resolved.ValueKind != JsonValueKind.Object || !resolved.TryGetProperty(segment, out resolved))
            {
                return false;
            }
        }

        return resolved.ValueKind == JsonValueKind.Object;
    }

    private static async Task ValidateCrossFieldAsync(IConnector connector,
        IReadOnlyDictionary<string, object?> connection, string kind, string name, string filePath,
        List<PzError> errors, HashSet<string> requiredFlaggedKeys, CancellationToken ct)
    {
        var result = await connector.ValidateAsync(new ConnectorConfig(connection), ct).ConfigureAwait(false);
        if (result.IsValid)
        {
            return;
        }

        foreach (var message in result.Errors)
        {
            if (ReferencesFlaggedKey(message, requiredFlaggedKeys))
            {
                continue; // the connection schema already flagged this exact key
            }

            errors.Add(new PzError(PzErrorCode.ConnectorConfigInvalid,
                $"{kind} '{name}' connection: {message}", filePath, null, Hint));
        }
    }

    /// <summary>True only when BOTH (a) <paramref name="message"/> uses every connector's own
    /// <c>ValidateAsync</c> message convention of "requires '&lt;key&gt;'" and (b) that key's ONLY schema
    /// violation was a `required`/missing-property one (<paramref name="requiredFlaggedKeys"/>, populated
    /// exclusively by <see cref="ValidateSchema"/>'s `required` branch -- never by its general
    /// per-property branch, e.g. a type-mismatch violation). Both halves are load-bearing: a rule that
    /// merely matched an already-flagged key's name anywhere in the message would also suppress
    /// cross-field errors describing a genuinely different problem about that key (e.g. a value-shape
    /// complaint in a third-party connector's own free-text phrasing). Suppression is therefore confined
    /// to the "same missing key, re-reported in our own connectors' standard phrasing" duplicate.</summary>
    private static bool ReferencesFlaggedKey(string message, HashSet<string> requiredFlaggedKeys) =>
        requiredFlaggedKeys.Any(key => message.Contains($"requires '{key}'", StringComparison.Ordinal));

    /// <summary>Mirrors <c>SpecBuilder.ForSourceLoad</c>'s options/columns merge so tier 3 validates the
    /// exact same dataset config shape the executor/planner will see -- the loader stores a dataset's
    /// declared <c>columns:</c> contract separately (<see cref="DatasetDef.Columns"/>), not inside
    /// <see cref="DatasetDef.Options"/>, so without this merge a DatasetConfigSchema's <c>columns</c>
    /// property could never be exercised.</summary>
    private static Dictionary<string, object?> MergeColumns(
        IReadOnlyDictionary<string, object?> options, IReadOnlyDictionary<string, string>? columns)
    {
        var merged = new Dictionary<string, object?>(options);
        if (columns is not null)
        {
            merged["columns"] = columns.ToDictionary(kv => kv.Key, kv => (object?)kv.Value);
        }

        return merged;
    }
}
