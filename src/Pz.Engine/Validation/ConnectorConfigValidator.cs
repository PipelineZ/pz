using System.Text.Json;
using System.Text.RegularExpressions;
using Json.Pointer;
using Json.Schema;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Execution;

namespace Pz.Engine.Validation;

/// <summary>Tier 3 of `pz validate`: connector-published JSON Schema validation of every source/sink
/// connection block, every source dataset's options, every sink output's <c>write:</c> options (when
/// the sink offers <see cref="IOutputConfigSchema"/> -- optional, so a sink that does not is left
/// unchecked exactly as before that capability existed), plus each resolved connector's own
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

    /// <summary>Applicator keywords whose failure only summarizes failing children ("Some properties
    /// did not match the required schema"). The failing child is reported at its own instance location
    /// with the actual cause, so the summary would be a second, vaguer error for the same mistake -- and,
    /// arriving first, would claim the property in the per-evaluation dedup and hide the real one.</summary>
    private static readonly HashSet<string> RollUpKeywords = new(StringComparer.Ordinal)
    {
        "properties", "additionalProperties", "patternProperties", "propertyNames",
        "unevaluatedProperties", "items", "unevaluatedItems",
    };

    /// <summary>YAML 1.1 boolean/null spellings the loader deliberately does NOT type (see
    /// <c>YamlMapper.ConvertScalar</c>, which only recognizes a plain, lowercase <c>true</c>/<c>false</c>):
    /// capitalized/other-cased forms, the <c>yes</c>/<c>no</c>/<c>on</c>/<c>off</c> synonyms, and
    /// <c>null</c>/<c>~</c>. An author who typed one of these expecting it to be typed gets a schema
    /// "expected boolean/integer, got string" error that never says why -- this recognizes the specific
    /// shape and names the rule instead.</summary>
    private static readonly string[] YamlLikeLiterals =
        ["true", "false", "yes", "no", "on", "off", "y", "n", "null", "~"];

    /// <summary>Tier 3: every source/sink connection block against the connector's
    /// ConnectionConfigSchema, every source dataset's options against DatasetConfigSchema, then the
    /// connector's own ValidateAsync for cross-field rules. All errors aggregated; never throws for
    /// validation failures. Config is validated as the user wrote it (pre-base_dir-injection).
    /// <paramref name="warnings"/> is optional (additive parameter, default null) and, when given,
    /// collects every connector's own non-blocking <see cref="ValidationResult.Warnings"/> -- a caller
    /// that does not pass it simply does not see them, unchanged from before this parameter
    /// existed. <paramref name="anchored"/> is the same project with its connections anchored to the project
    /// directory (`base_dir`), as the plan opens them: the offline probes open connectors with it, so a relative
    /// path resolves against the project wherever pz runs. Without it the probes use the as-written config.</summary>
    public static async Task<IReadOnlyList<PzError>> ValidateAsync(
        PzProject project, ConnectorRegistry registry, CancellationToken ct, List<PzWarning>? warnings = null,
        PzProject? anchored = null)
    {
        var probeConnections = (anchored ?? project).Connections
            .GroupBy(c => c.Name, StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.First(), StringComparer.Ordinal);
        ConnectionDef ForProbe(ConnectionDef connection) =>
            probeConnections.TryGetValue(connection.Name, out var anchoredConnection)
                ? anchoredConnection with { Datasets = connection.Datasets, Outputs = connection.Outputs }
                : connection;

        var errors = new List<PzError>();
        RefuseReservedProperties(project, registry, errors);
        RefuseUnknownConnectors(project, registry, errors);
        WarnOnMotherDuckTokenMismatch(project, warnings);

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
            var connectionErrors = errors.Count;
            ValidateSchema(connector.ConnectionConfigSchema, source.Connection, "connection", source.Name,
                "connection", source.FilePath, errors, connectionFlaggedKeys, requiredFlaggedKeys);
            connectionErrors = errors.Count - connectionErrors;

            // The options each dataset's schema flagged, so the connector's own dataset check (which sees every
            // dataset) does not repeat a finding about them; and the datasets with any finding at all, which the
            // offline probe below leaves alone.
            var schemaFlagged = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
            var datasetsWithFindings = new HashSet<string>(StringComparer.Ordinal);
            foreach (var dataset in source.Datasets)
            {
                var errorsBefore = errors.Count;
                var flagged = new HashSet<string>(StringComparer.Ordinal);
                var options = MergeColumns(dataset.Options, dataset.Columns);
                // pz wrote this key, not the user: its refusal comes after the cross-field check below
                // (whose spawn gives an external connector its schema), never as an "unknown option".
                if (!ColumnPointerSupport.IsListedIn(connector.DatasetConfigSchema))
                {
                    options.Remove(ColumnPointerSupport.Option);
                }

                ValidateSchema(connector.DatasetConfigSchema, options, "connection", source.Name,
                    $"dataset '{dataset.Name}'", source.FilePath, errors, flagged,
                    new HashSet<string>(StringComparer.Ordinal));
                schemaFlagged[dataset.Name] = flagged;
                if (errors.Count > errorsBefore)
                {
                    datasetsWithFindings.Add(dataset.Name);
                }
            }

            var crossFieldBefore = errors.Count;
            await ValidateCrossFieldAsync(connector, registry.ConfigFor(source), "connection", source.Name, source.FilePath,
                errors, requiredFlaggedKeys, warnings, ct).ConfigureAwait(false);
            connectionErrors += errors.Count - crossFieldBefore;

            if (connector is IDatasetConfigValidator datasetValidator)
            {
                await ValidateDatasetsAsync(datasetValidator, registry.ConfigFor(source), source, schemaFlagged,
                    datasetsWithFindings, errors, ct).ConfigureAwait(false);
            }

            // Outputs on a connection whose connector also reads are checked here; the loop below covers
            // write-only connectors.
            var outputsWithFindings = new HashSet<string>(StringComparer.Ordinal);
            if (registry.TryGetSink(source.Connector, out var sinkConnector) && sinkConnector is IOutputConfigSchema readWriteSchema)
            {
                ValidateOutputs(readWriteSchema, source, errors, outputsWithFindings);
            }

            if (connectionErrors == 0 && !registry.IsHosted(source.Connector))
            {
                await ProbeAsync(registry, ForProbe(source), connector, sinkConnector, datasetsWithFindings, outputsWithFindings,
                        project.Engine.ForceUniversal, errors, ct)
                    .ConfigureAwait(false);
            }

            foreach (var dataset in source.Datasets)
            {
                if (ColumnPointerSupport.Refusal(dataset.Options, connector.DatasetConfigSchema, source.Connector,
                        source.Name, dataset.Name, source.FilePath) is { } refusal)
                {
                    errors.Add(refusal);
                }
            }
        }

        // A connection whose connector also reads was config-validated in the loop above (its outputs
        // included), so this one covers write-only connectors -- a postgres connection is not validated, and its every config
        // error not reported, twice.
        foreach (var sink in project.Connections)
        {
            if (registry.TryGetSource(sink.Connector, out _) || !registry.TryGetSink(sink.Connector, out var connector))
            {
                continue;
            }

            var connectionFlaggedKeys = new HashSet<string>(StringComparer.Ordinal);
            var requiredFlaggedKeys = new HashSet<string>(StringComparer.Ordinal);
            var connectionErrors = errors.Count;
            ValidateSchema(connector.ConnectionConfigSchema, sink.Connection, "connection", sink.Name,
                "connection", sink.FilePath, errors, connectionFlaggedKeys, requiredFlaggedKeys);
            connectionErrors = errors.Count - connectionErrors;

            var outputsWithFindings = new HashSet<string>(StringComparer.Ordinal);
            if (connector is IOutputConfigSchema outputSchema)
            {
                ValidateOutputs(outputSchema, sink, errors, outputsWithFindings);
            }

            var crossFieldBefore = errors.Count;
            await ValidateCrossFieldAsync(connector, registry.ConfigFor(sink), "connection", sink.Name, sink.FilePath,
                errors, requiredFlaggedKeys, warnings, ct).ConfigureAwait(false);
            connectionErrors += errors.Count - crossFieldBefore;

            // Output-schema findings are about the outputs, not the connection, so they do not stop the probe.
            if (connectionErrors == 0 && !registry.IsHosted(sink.Connector))
            {
                await ProbeAsync(registry, ForProbe(sink), null, connector, [], outputsWithFindings, project.Engine.ForceUniversal,
                    errors, ct).ConfigureAwait(false);
            }
        }

        return errors;
    }

    /// <summary>Every output's <c>write:</c> options against the sink's <see cref="IOutputConfigSchema"/>. Optional:
    /// a sink that does not offer it has its output options reach it unchecked. <see cref="ConnectionDef.Outputs"/> is
    /// DagCompiler's EFFECTIVE, already-merged output list -- whichever of entities:write: / sink() kwargs declared an
    /// output, it lands here once (PZ0341 refuses declaring both).</summary>
    private static void ValidateOutputs(IOutputConfigSchema outputSchema, ConnectionDef connection, List<PzError> errors,
        HashSet<string> outputsWithFindings)
    {
        foreach (var output in connection.Outputs)
        {
            var before = errors.Count;
            // partition_by is set aside first. It stays among an output's options because the
            // sinks that partition read it, but its shape is the compiler's to check and whether
            // this connector can honour it is the planner's, answered from its capabilities. A
            // schema that does not list it must not turn that answer into "unknown option".
            var options = output.Options.ContainsKey(PartitionColumns.OptionName)
                ? output.Options.Where(o => o.Key != PartitionColumns.OptionName)
                    .ToDictionary(o => o.Key, o => o.Value, StringComparer.Ordinal)
                : output.Options;
            ValidateSchema(outputSchema.OutputConfigSchema, options, "connection", connection.Name,
                $"output '{output.Name}'", connection.FilePath, errors, new HashSet<string>(StringComparer.Ordinal),
                new HashSet<string>(StringComparer.Ordinal));
            if (errors.Count > before)
            {
                outputsWithFindings.Add(output.Name);
            }
        }
    }

    /// <summary>The planner's own offline probes of a builtin connection whose connection-level checks passed, so
    /// `pz validate` refuses whatever the plan would: opening the source (when the connection has datasets) and the
    /// sink (when it has outputs), then each dataset's read shape (<see cref="INaturalReadShapeSource"/>), and --
    /// unless engine.force_universal is set, as in the plan -- each dataset's native scan and each output's native
    /// copy, skipping datasets and outputs that already have findings. A connector failure at open or read shape
    /// is classified like the plan's (<see cref="ConnectorFailures"/>); a connection refused at open is reported
    /// once. A native-path refusal is reported as the plan reports it (PZ0353) only when the connector codes it
    /// <see cref="PzConnectorErrorCodes.Config"/>: the others depend on what is on disk now (a database file or
    /// table directory not written yet, a csv header), which the plan rechecks for the nodes a run executes and
    /// deliberately defers for the rest. Never for a hosted connector: its OpenAsync spawns a process and runs the
    /// connector's own configure, which the ABI does not promise is offline.</summary>
    private static async Task ProbeAsync(ConnectorRegistry registry, ConnectionDef connection,
        ISourceConnector? sourceConnector, ISinkConnector? sinkConnector, HashSet<string> datasetsWithFindings,
        HashSet<string> outputsWithFindings, bool forceUniversal, List<PzError> errors, CancellationToken ct)
    {
        var config = registry.ConfigFor(connection);
        var subject = $"connection '{connection.Name}'";
        if (sourceConnector is not null && connection.Datasets.Count > 0)
        {
            ISource source;
            try
            {
                source = await sourceConnector.OpenAsync(config, ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException &&
                                       Classify(ex, connection, subject, registry) is { } refusal)
            {
                errors.Add(refusal);
                return;
            }

            await using (source.ConfigureAwait(false))
            {
                foreach (var dataset in connection.Datasets.Where(d => !datasetsWithFindings.Contains(d.Name)))
                {
                    var spec = SpecBuilder.ForSourceLoad(new SourceDatasetDef(connection, dataset));
                    try
                    {
                        (source as INaturalReadShapeSource)?.GetNaturalReadShape(spec);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException &&
                                               Classify(ex, connection, $"{subject} dataset '{dataset.Name}'",
                                                   registry) is { } refusal)
                    {
                        errors.Add(refusal);
                        continue;
                    }

                    if (!forceUniversal)
                    {
                        try
                        {
                            source.TryGetNativeScan(spec, out _);
                        }
                        catch (PzConnectorException ex) when (IsConfigRefusal(ex))
                        {
                            errors.Add(new PzError(PzErrorCode.NativePathContractMismatch, ex.Message, connection.FilePath,
                                null, null));
                        }
                        catch (PzConnectorException)
                        {
                            // Depends on what is on disk now: the plan's to report, for the nodes a run executes.
                        }
                    }
                }
            }
        }

        if (sinkConnector is not null && connection.Outputs.Count > 0)
        {
            ISink sink;
            try
            {
                sink = await sinkConnector.OpenAsync(config, ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException &&
                                       Classify(ex, connection, subject, registry) is { } refusal)
            {
                errors.Add(refusal);
                return;
            }

            await using (sink.ConfigureAwait(false))
            {
                foreach (var output in connection.Outputs.Where(o => !forceUniversal && !outputsWithFindings.Contains(o.Name)))
                {
                    try
                    {
                        sink.TryGetNativeCopy(SpecBuilder.ForSinkOutput(new SinkOutputDef(connection, output)), out _);
                    }
                    catch (PzConnectorException ex) when (IsConfigRefusal(ex))
                    {
                        errors.Add(new PzError(PzErrorCode.NativePathContractMismatch,
                            $"sink '{connection.Name}' output '{output.Name}': {ex.Message}", connection.FilePath, null, null));
                    }
                    catch (PzConnectorException)
                    {
                        // Depends on what is on disk now: the plan's to report, for the nodes a run executes.
                    }
                }
            }
        }
    }

    private static bool IsConfigRefusal(PzConnectorException ex) =>
        !ex.IsTransient && ex.Code == PzConnectorErrorCodes.Config;

    private static PzError? Classify(Exception ex, ConnectionDef connection, string subject, ConnectorRegistry registry) =>
        ConnectorFailures.Classify(ex, connection.Connector, subject, connection.FilePath, "validating",
            registry.DescribeHostFailure);

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

    /// <summary>DuckDB's motherduck extension accepts <c>set motherduck_token</c> only before its FIRST
    /// attach in a session, and the engine runs one DuckDB session per run. So a RUN that touches two
    /// motherduck connections with different tokens fails at its second attach (PZ0311) -- but the
    /// project itself is sound when every run selects only one of them, which is why this is a warning
    /// under that same code and not a refusal. The tokens are compared, never printed. `motherduck` is a
    /// well-known first-party connector name, not something read from the registry: this is a property
    /// of DuckDB's extension, not of whichever package is installed under that name.</summary>
    private static void WarnOnMotherDuckTokenMismatch(PzProject project, List<PzWarning>? warnings)
    {
        if (warnings is null)
        {
            return;
        }

        string? firstName = null;
        string? firstToken = null;
        foreach (var connection in project.Connections)
        {
            if (!string.Equals(connection.Connector, "motherduck", StringComparison.Ordinal))
            {
                continue;
            }

            // A missing token is the connector's own ValidateAsync's problem to report; this only
            // compares tokens that are present.
            var token = new ConnectorConfig(connection.Connection).GetString("token");
            if (token is null)
            {
                continue;
            }

            if (firstToken is null)
            {
                firstName = connection.Name;
                firstToken = token;
                continue;
            }

            if (!string.Equals(firstToken, token, StringComparison.Ordinal))
            {
                warnings.Add(new PzWarning(PzErrorCode.NativeSetupFailed,
                    $"motherduck connections '{firstName}' and '{connection.Name}' declare different " +
                    "tokens -- DuckDB accepts 'set motherduck_token' only before the first attach in a " +
                    "run, so a run that touches both fails at the second one.",
                    connection.FilePath, null,
                    "use one token for every motherduck connection, or keep the two in separate runs"));
            }
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
                using var schemaDoc = JsonDocument.Parse(schemaText);
                root = schemaDoc.RootElement.Clone();
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
        using var valuesDoc = JsonDocument.Parse(node?.ToJsonString() ?? "null");
        var result = schema.Evaluate(valuesDoc.RootElement, new EvaluationOptions { OutputFormat = OutputFormat.List });

        if (result.IsValid)
        {
            return;
        }

        foreach (var detail in result.Details ?? [])
        {
            if (detail.Errors is not { Count: > 0 } detailErrors ||
                detailErrors.Keys.All(RollUpKeywords.Contains))
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
                var nearMiss = NearMiss.Find(accepted, unknownOption);
                errors.Add(new PzError(PzErrorCode.ConnectorConfigInvalid,
                    $"{kind} '{name}'{Where(blockLabel)}: {ContainerLocation(detail)}unknown option '{unknownOption}'",
                    filePath, null,
                    accepted.Count > 0
                        ? $"remove or rename it -- accepted options: {string.Join(", ", accepted)}" +
                          (nearMiss is null ? "" : $" -- did you mean '{nearMiss}'")
                        : "remove it -- this block accepts no options"));
                continue;
            }

            if (TryDescribeYamlLikeScalar(schemaText, detail, valuesDoc.RootElement, out var literal))
            {
                errors.Add(new PzError(PzErrorCode.ConnectorConfigInvalid,
                    $"{kind} '{name}'{Where(blockLabel)}: {location}'{literal}' was read as text, not a " +
                    "boolean or number here",
                    filePath, null,
                    literal is "null" or "~"
                        ? "leave the option out instead of writing null"
                        : "write true/false in lower case, unquoted"));
                continue;
            }

            if (TryDescribeTypedWhereTextExpected(schemaText, detail, valuesDoc.RootElement, out var readAs))
            {
                // The value is not echoed: an all-digit value where text is expected is a password or
                // an account id at least as often as it is a mistake.
                errors.Add(new PzError(PzErrorCode.ConnectorConfigInvalid,
                    $"{kind} '{name}'{Where(blockLabel)}: {location}the value was read as {readAs}, but " +
                    "this option is text",
                    filePath, null,
                    "quote it -- \"12345\", or \"${VAR}\" when it comes from the environment -- so it stays text"));
                continue;
            }

            var firstError = detailErrors.First(e => !RollUpKeywords.Contains(e.Key)).Value;
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
            using var schemaDoc = JsonDocument.Parse(schemaText);
            root = schemaDoc.RootElement.Clone();
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

    /// <summary>Recognizes a "type" violation whose offending value is a string one of the YAML
    /// boolean/null lookalikes the loader deliberately leaves untyped (<see cref="YamlLikeLiterals"/>),
    /// against a schema property that declares boolean/integer/number. Detection is structural: the
    /// violated property's own resolved schema node must declare one of those types, not a textual match
    /// on the library's message, matching <see cref="TryDescribeUnknownOption"/>'s pattern.</summary>
    private static bool TryDescribeYamlLikeScalar(
        string schemaText, EvaluationResults detail, JsonElement valuesRoot, out string literal)
    {
        literal = "";

        if (detail.Errors is not { } detailErrors || !detailErrors.ContainsKey("type"))
        {
            return false;
        }

        if (!TryResolve(valuesRoot, SplitPointer(detail.InstanceLocation.ToString()), out var instance,
                requireObject: false) || instance.ValueKind != JsonValueKind.String)
        {
            return false;
        }

        var raw = instance.GetString() ?? "";
        if (!YamlLikeLiterals.Contains(raw, StringComparer.OrdinalIgnoreCase))
        {
            return false;
        }

        JsonElement schemaRoot;
        try
        {
            using var schemaDoc = JsonDocument.Parse(schemaText);
            schemaRoot = schemaDoc.RootElement.Clone();
        }
        catch (JsonException)
        {
            return false;
        }

        if (!TryResolve(schemaRoot, SplitPointer(detail.EvaluationPath.ToString()), out var propertySchema,
                requireObject: true) ||
            !propertySchema.TryGetProperty("type", out var declaredType) ||
            !DeclaresBooleanOrNumeric(declaredType))
        {
            return false;
        }

        literal = raw;
        return true;
    }

    /// <summary>The mirror of <see cref="TryDescribeYamlLikeScalar"/>: a "type" violation where the
    /// option is declared <c>string</c> and the value is a number or boolean -- what a plain YAML scalar
    /// (or a bare <c>${VAR}</c> whose value is all digits) becomes. Structural, like its sibling.</summary>
    private static bool TryDescribeTypedWhereTextExpected(
        string schemaText, EvaluationResults detail, JsonElement valuesRoot, out string readAs)
    {
        readAs = "";

        if (detail.Errors is not { } detailErrors || !detailErrors.ContainsKey("type"))
        {
            return false;
        }

        if (!TryResolve(valuesRoot, SplitPointer(detail.InstanceLocation.ToString()), out var instance,
                requireObject: false))
        {
            return false;
        }

        var kind = instance.ValueKind switch
        {
            JsonValueKind.Number => "a number",
            JsonValueKind.True or JsonValueKind.False => "a boolean",
            _ => null,
        };
        if (kind is null)
        {
            return false;
        }

        JsonElement schemaRoot;
        try
        {
            using var schemaDoc = JsonDocument.Parse(schemaText);
            schemaRoot = schemaDoc.RootElement.Clone();
        }
        catch (JsonException)
        {
            return false;
        }

        if (!TryResolve(schemaRoot, SplitPointer(detail.EvaluationPath.ToString()), out var propertySchema,
                requireObject: true) ||
            !propertySchema.TryGetProperty("type", out var declaredType) ||
            declaredType.ValueKind != JsonValueKind.String || declaredType.GetString() != "string")
        {
            return false;
        }

        readAs = kind;
        return true;
    }

    private static bool DeclaresBooleanOrNumeric(JsonElement declaredType) => declaredType.ValueKind switch
    {
        JsonValueKind.String => declaredType.GetString() is "boolean" or "integer" or "number",
        JsonValueKind.Array => declaredType.EnumerateArray().Any(e =>
            e.ValueKind == JsonValueKind.String && e.GetString() is "boolean" or "integer" or "number"),
        _ => false,
    };

    /// <summary>JSON Pointer segments, unescaped per RFC 6901 (<c>~1</c> before <c>~0</c>, or a literal
    /// <c>~1</c> would decode twice). Works off the pointer's string form rather than the library's
    /// indexer, which has been renamed across major versions of JsonPointer.Net.</summary>
    private static List<string> SplitPointer(string pointer) =>
    [
        .. pointer.Split('/', StringSplitOptions.RemoveEmptyEntries)
            .Select(s => s.Replace("~1", "/", StringComparison.Ordinal).Replace("~0", "~", StringComparison.Ordinal)),
    ];

    /// <summary>Walks <paramref name="segments"/> through <paramref name="root"/>, stepping into array
    /// elements by index (an <c>allOf</c>/<c>anyOf</c> branch puts one in the evaluation path).
    /// <paramref name="requireObject"/> is false for an instance-value lookup (a JSON Pointer into the
    /// user's own config, which may land on a scalar) and true for a schema-node lookup (every schema
    /// container -- and every leaf subschema like <c>{"type":"boolean"}</c> -- is itself a JSON
    /// object).</summary>
    private static bool TryResolve(JsonElement root, IEnumerable<string> segments, out JsonElement resolved,
        bool requireObject = true)
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

        return !requireObject || resolved.ValueKind == JsonValueKind.Object;
    }

    private static async Task ValidateCrossFieldAsync(IConnector connector,
        ConnectorConfig config, string kind, string name, string filePath,
        List<PzError> errors, HashSet<string> requiredFlaggedKeys, List<PzWarning>? warnings, CancellationToken ct)
    {
        var result = await connector.ValidateAsync(config, ct).ConfigureAwait(false);
        if (warnings is not null)
        {
            foreach (var message in result.Warnings)
            {
                warnings.Add(new PzWarning(PzErrorCode.ConnectorConfigWarning,
                    $"{kind} '{name}' connection: {message}", filePath, null, null));
            }
        }

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

    /// <summary>The connector's own checks of each dataset against its connection
    /// (<see cref="IDatasetConfigValidator"/>, optional), one error per finding, naming the dataset. The options
    /// are shaped as the plan's <see cref="DatasetSpec"/> (<see cref="SpecBuilder.ForSourceLoad(SourceDatasetDef)"/>:
    /// the dataset's options plus its contract columns, and a declared incremental's cursor), so the check sees what
    /// the plan will. Every dataset is checked, a schema-rejected one too, so validation reports every problem at
    /// once; a finding that names an option the dataset's schema already flagged (<paramref name="schemaFlagged"/>)
    /// is the same problem said twice and is dropped. A connector that refuses by throwing
    /// <see cref="PzConnectorException"/> instead of returning its findings is reported the same way. A dataset
    /// with a finding is added to <paramref name="datasetsWithFindings"/>.</summary>
    private static async Task ValidateDatasetsAsync(IDatasetConfigValidator validator, ConnectorConfig config,
        ConnectionDef source, Dictionary<string, HashSet<string>> schemaFlagged, HashSet<string> datasetsWithFindings,
        List<PzError> errors, CancellationToken ct)
    {
        foreach (var dataset in source.Datasets)
        {
            // The plan's own probe spec, declared incremental cursor included: a rule that holds only for an
            // incremental dataset must see the dataset as the plan will.
            var spec = SpecBuilder.ForSourceLoad(new SourceDatasetDef(source, dataset));
            ValidationResult result;
            try
            {
                result = await validator.ValidateDatasetAsync(config, spec, ct).ConfigureAwait(false);
            }
            catch (PzConnectorException ex) when (!ex.IsTransient)
            {
                result = ValidationResult.Failed(ex.Message);
            }

            var flagged = TopLevelOptions(schemaFlagged.GetValueOrDefault(dataset.Name));
            foreach (var message in result.Errors.Where(m => !NamesAnyOption(m, flagged)))
            {
                datasetsWithFindings.Add(dataset.Name);
                errors.Add(new PzError(PzErrorCode.ConnectorConfigInvalid,
                    $"connection '{source.Name}' dataset '{dataset.Name}': {message}", source.FilePath, null, DatasetHint));
            }
        }
    }

    private const string DatasetHint = "fix the dataset's options the message names";

    /// <summary>The top-level option of each schema-flagged key: a flagged key is an instance path such as
    /// <c>for_each/values</c>, or a missing property's name.</summary>
    private static HashSet<string> TopLevelOptions(HashSet<string>? flaggedKeys) =>
        flaggedKeys is null
            ? []
            : flaggedKeys.Select(k => k.Split('/')[0]).Where(k => k.Length > 0).ToHashSet(StringComparer.Ordinal);

    /// <summary>Whether a connector's finding is about one of <paramref name="options"/>: it names it quoted
    /// (<c>'path'</c>, <c>'for_each.max_items'</c>) or starts with it as a path (<c>on_status[0]: ...</c>,
    /// <c>for_each.max_items cannot ...</c>), the two ways connector messages name an option.</summary>
    private static bool NamesAnyOption(string message, HashSet<string> options) =>
        options.Any(o =>
            message.Contains($"'{o}'", StringComparison.Ordinal) ||
            message.Contains($"'{o}.", StringComparison.Ordinal) ||
            message.Contains($"'{o}[", StringComparison.Ordinal) ||
            (message.StartsWith(o, StringComparison.Ordinal) && message.Length > o.Length &&
             message[o.Length] is '.' or '[' or ':' or ' '));

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
