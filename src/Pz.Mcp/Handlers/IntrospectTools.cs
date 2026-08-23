using System.Text.Json;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Validation;

namespace Pz.Mcp.Handlers;

/// <summary>pz_project_overview / pz_connector_reference / pz_state — the read-only introspect tools.
/// Every handler is console-free. Secret/SQL hygiene (the binding convention this
/// whole namespace follows): none of the three ever serializes a <c>ConnectionDef.Connection</c>
/// dictionary — only connection NAMES and connector TYPES cross the wire, never a config value that
/// dictionary might hold (host, root, credentials, ...). pz_connector_reference's schemas are the one
/// deliberate exception: they are the connector's own published JSON Schema *shape*, never a project's
/// filled-in values.</summary>
internal static class IntrospectTools
{
    /// <summary>pz_project_overview: name, flows, connections (names/types/entities only — never
    /// config values), pipelines (refs/sources/sinks), and the compiled DAG's node list. On a compile
    /// failure, falls back to a bare <see cref="ProjectPhases.Load"/> so a broken pipeline doesn't hide
    /// project-level info an agent might still find useful — the compile errors ride the
    /// same envelope's top-level <c>errors</c> array alongside a populated (best-effort) <c>result</c>.
    /// Only when even the bare load fails does this fall back to a plain error envelope.</summary>
    internal static string Overview(string projectDir)
    {
        try
        {
            var (project, dag, _) = ProjectPhases.LoadAndCompile(projectDir);
            return ToolEnvelope.Ok(json => WriteOverview(json, project, dag.Connections, dag));
        }
        catch (PzValidationException compileEx)
        {
            PzProject project;
            try
            {
                project = ProjectPhases.Load(projectDir);
            }
            catch (PzValidationException)
            {
                // Even a bare load fails — nothing project-level survives, so report the original
                // compile-time failure (for a broken project.yml/connections.yml the two exceptions
                // carry the same errors anyway; LoadAndCompile's first step is that same bare load).
                return ToolEnvelope.Errors(compileEx.Errors);
            }

            return ToolEnvelope.Ok(json =>
            {
                WriteErrorsField(json, compileEx.Errors);
                WriteOverview(json, project, project.Connections, dag: null);
            });
        }
    }

    /// <summary>pz_connector_reference: every distinct connector type this project's connections.yml
    /// names, with the registered connector's capability flags and its raw JSON Schemas for the
    /// connection block and per-dataset options — read verbatim off <see cref="IConnector"/>, never
    /// re-derived, so a caller always sees exactly what the connector itself publishes.</summary>
    internal static async Task<string> ConnectorReferenceAsync(string projectDir, CliServices services, CancellationToken ct)
    {
        try
        {
            var project = ProjectPhases.Load(projectDir);
            var (registry, host) = await services.CreateRegistryAsync(project, projectDir, ct).ConfigureAwait(false);
            await using var connectorHost = host;

            var names = project.Connections.Select(c => c.Connector)
                .Distinct(StringComparer.Ordinal).OrderBy(n => n, StringComparer.Ordinal);

            return ToolEnvelope.Ok(json =>
            {
                json.WriteStartObject("result");
                json.WriteStartArray("connectors");
                foreach (var name in names)
                {
                    var hasSource = registry.TryGetSource(name, out var source);
                    var hasSink = registry.TryGetSink(name, out var sink);
                    IConnector? connector = (IConnector?)source ?? sink;
                    if (connector is null)
                    {
                        // Named by a connections.yml entry but never registered — shouldn't happen
                        // once CreateRegistryAsync itself succeeded; skip defensively.
                        continue;
                    }

                    json.WriteStartObject();
                    json.WriteString("name", name);
                    json.WriteBoolean("source", hasSource);
                    json.WriteBoolean("sink", hasSink);
                    json.WriteString("capabilities", connector.Capabilities.ToString());
                    WriteRawSchema(json, "dataset_schema", connector.DatasetConfigSchema);
                    WriteRawSchema(json, "connection_schema", connector.ConnectionConfigSchema);
                    json.WriteEndObject();
                }

                json.WriteEndArray();
                json.WriteEndObject();
            });
        }
        catch (PzValidationException ex)
        {
            return ToolEnvelope.Errors(ex.Errors);
        }
    }

    /// <summary>pz_state: every stored watermark/sync-state/schema-baseline entry, plus the latest
    /// run's summary — the same three keyed-state stores and run-artifact store `pz state show` reads,
    /// reached through whichever backend project.yml resolved to (local/HTTP/SQL Server). A corrupt
    /// state file is reported inline (<c>corrupt: true</c> on that section) rather than failing the
    /// whole tool — mirrors `pz state show`'s lenient-read/notice-callback contract. Bad `state:` config
    /// (e.g. a SQL Server backend that fails schema migration) is the one failure mode that DOES fail
    /// the tool: <see cref="PzConfigException"/> from <see cref="CliServices.CreateStateStores"/>
    /// envelopes as a single error.</summary>
    internal static string State(string projectDir, CliServices services)
    {
        try
        {
            var project = ProjectPhases.Load(projectDir);
            var stores = services.CreateStateStores(project, projectDir);

            var watermarksCorrupt = false;
            var syncStateCorrupt = false;
            var schemasCorrupt = false;
            var watermarks = stores.Watermarks.ListAll(_ => watermarksCorrupt = true);
            var syncState = stores.SyncState.ListAll(_ => syncStateCorrupt = true);
            var schemas = stores.Schemas.ListAll(_ => schemasCorrupt = true);
            var latestRun = stores.Artifacts.ReadLatest();

            return ToolEnvelope.Ok(json =>
            {
                json.WriteStartObject("result");

                json.WriteStartObject("watermarks");
                json.WriteBoolean("corrupt", watermarksCorrupt);
                json.WriteStartArray("entries");
                foreach (var (key, wm) in watermarks ?? [])
                {
                    json.WriteStartObject();
                    json.WriteString("key", key);
                    json.WriteString("cursor", wm.Cursor);
                    json.WriteString("type", wm.TypeName);
                    json.WriteString("value", wm.Value);
                    json.WriteString("run_id", wm.RunId);
                    json.WriteEndObject();
                }

                json.WriteEndArray();
                json.WriteEndObject();

                json.WriteStartObject("sync_state");
                json.WriteBoolean("corrupt", syncStateCorrupt);
                json.WriteStartArray("entries");
                foreach (var (key, sync) in syncState ?? [])
                {
                    json.WriteStartObject();
                    json.WriteString("key", key);
                    json.WriteString("token", sync.Token);
                    json.WriteString("run_id", sync.RunId);
                    json.WriteEndObject();
                }

                json.WriteEndArray();
                json.WriteEndObject();

                json.WriteStartObject("schema_baselines");
                json.WriteBoolean("corrupt", schemasCorrupt);
                json.WriteStartArray("entries");
                foreach (var (key, baseline) in schemas ?? [])
                {
                    json.WriteStartObject();
                    json.WriteString("key", key);
                    json.WriteString("hints_hash", baseline.HintsHash);
                    json.WriteString("run_id", baseline.RunId);
                    json.WriteStartArray("columns");
                    foreach (var col in baseline.Columns)
                    {
                        json.WriteStartObject();
                        json.WriteString("name", col.Name);
                        json.WriteString("type", col.Type);
                        json.WriteEndObject();
                    }

                    json.WriteEndArray();
                    json.WriteEndObject();
                }

                json.WriteEndArray();
                json.WriteEndObject();

                if (latestRun is { } run)
                {
                    json.WriteStartObject("latest_run");
                    json.WriteString("run_id", run.RunId);
                    json.WriteString("status", run.Status);
                    json.WriteStartObject("node_counts");
                    foreach (var group in run.Nodes.GroupBy(n => n.Status).OrderBy(g => g.Key, StringComparer.Ordinal))
                    {
                        json.WriteNumber(group.Key, group.Count());
                    }

                    json.WriteEndObject();
                    json.WriteEndObject();
                }
                else
                {
                    json.WriteNull("latest_run");
                }

                json.WriteEndObject();
            });
        }
        catch (PzValidationException ex)
        {
            return ToolEnvelope.Errors(ex.Errors);
        }
        catch (PzConfigException ex)
        {
            return ToolEnvelope.Errors([ex.Error]);
        }
    }

    /// <summary>pz_entity_schema: live schema fetch for one connection+entity, via the same
    /// <see cref="ConnectivityValidator"/> tier `pz validate --connect` runs — but scoped to exactly
    /// one connection with its <see cref="ConnectionDef.Datasets"/> filtered to the one requested
    /// entity, so only that entity's connection is probed and only that dataset's schema fetched. Never
    /// writes <c>.pz/target/schemas.json</c> — <see cref="ConnectivityValidator.RunAsync"/> itself has
    /// no artifact-writing side effect (that write lives in <c>ValidateCommand</c>, around the
    /// validator, not inside it), so calling the validator directly here keeps this handler read-only
    /// like every other verify/introspect tool (class doc).
    ///
    /// Filters against <see cref="CompiledDag.Connections"/> (the effective connections, including any
    /// entity declared only at a <c>source()</c> call site with no YAML <c>entities:</c> block) — the
    /// same reason <see cref="Overview"/> uses <c>dag.Connections</c> rather than the loaded project's.
    /// An unknown connection or entity (the filter matches nothing) is reported as a plain enveloped
    /// <see cref="PzErrorCode.ConnectionCheckFailed"/> (PZ0330, the closest existing connectivity code)
    /// rather than a new error code — <see cref="ConnectivityValidator"/> never gets a chance to report
    /// it in its own vocabulary because there is nothing left to probe.
    ///
    /// Result: {columns:[{name,type}], source}. <c>source</c> is <c>"fetched"</c> when the columns came
    /// from <see cref="ConnectivityResult.FetchedSchemas"/>'s live probe (a contract-less dataset — the
    /// single rendered "name: type, name: type" string there is parsed back apart; a depth-aware split,
    /// not a bare comma split, is required because <c>ContractTypes.Describe</c> itself embeds commas
    /// inside a type's own parens, e.g. <c>Decimal128(10,2)</c>), or <c>"declared_contract"</c> when the
    /// dataset has a `columns:` contract — <see cref="ConnectivityValidator"/> never populates
    /// <c>FetchedSchemas</c> for a contract-bearing dataset (it only drift-checks it against the fetched
    /// shape), so falling back to the declared contract itself is the only way this handler avoids
    /// returning a misleading <c>ok:true</c> with an empty <c>columns</c> array — indistinguishable from
    /// "this entity genuinely has zero columns" — for the overwhelmingly common case of a dataset that
    /// DOES declare a contract. If neither is available (should not happen when the validator ran clean
    /// on a contract-less dataset, but guarded rather than assumed), this reports the same enveloped
    /// <see cref="PzErrorCode.ConnectionCheckFailed"/> shape as the unknown-connection/entity case below,
    /// rather than a misleading empty success.</summary>
    internal static async Task<string> EntitySchemaAsync(
        string projectDir, string connection, string entity, CliServices services, CancellationToken ct)
    {
        try
        {
            var (project, dag, _) = ProjectPhases.LoadAndCompile(projectDir);

            var matchedConnection = dag.Connections.FirstOrDefault(
                c => string.Equals(c.Name, connection, StringComparison.Ordinal));
            var matchedDataset = matchedConnection?.Datasets.FirstOrDefault(
                d => string.Equals(d.Name, entity, StringComparison.Ordinal));
            if (matchedConnection is null || matchedDataset is null)
            {
                return ToolEnvelope.Errors([new PzError(PzErrorCode.ConnectionCheckFailed,
                    $"no declared read for entity '{entity}' on connection '{connection}'",
                    matchedConnection?.FilePath ?? projectDir, null,
                    "check pz_project_overview for declared connections and entities")]);
            }

            var filteredProject = project with
            {
                Connections = [matchedConnection with { Datasets = [matchedDataset] }],
            };
            filteredProject = ProjectPhases.InjectProjectDirectoryAnchor(filteredProject, projectDir);

            var (registry, host) = await services.CreateRegistryAsync(filteredProject, projectDir, ct)
                .ConfigureAwait(false);
            await using var connectorHost = host;

            var connectivity = await ConnectivityValidator.RunAsync(filteredProject, registry, ct)
                .ConfigureAwait(false);
            if (connectivity.Errors.Count > 0)
            {
                return ToolEnvelope.Errors(connectivity.Errors);
            }

            var key = $"{connection}.{entity}";
            List<(string Name, string Type)> columns;
            string source;
            if (connectivity.FetchedSchemas.TryGetValue(key, out var rendered))
            {
                columns = ParseRenderedSchema(rendered);
                source = "fetched";
            }
            else if (matchedDataset.Columns is { Count: > 0 } contract)
            {
                columns = [.. contract.OrderBy(kv => kv.Key, StringComparer.Ordinal)
                    .Select(kv => (kv.Key, kv.Value))];
                source = "declared_contract";
            }
            else
            {
                return ToolEnvelope.Errors([new PzError(PzErrorCode.ConnectionCheckFailed,
                    $"no schema available for entity '{entity}' on connection '{connection}' " +
                    "(the connector fetched none and no columns: contract is declared)",
                    matchedConnection.FilePath, null,
                    "check pz_project_overview for declared connections and entities")]);
            }

            return ToolEnvelope.Ok(json =>
            {
                json.WriteStartObject("result");
                json.WriteStartArray("columns");
                foreach (var (name, type) in columns)
                {
                    json.WriteStartObject();
                    json.WriteString("name", name);
                    json.WriteString("type", type);
                    json.WriteEndObject();
                }

                json.WriteEndArray();
                json.WriteString("source", source);
                json.WriteEndObject();
            });
        }
        catch (PzValidationException ex)
        {
            return ToolEnvelope.Errors(ex.Errors);
        }
    }

    /// <summary>Splits <see cref="ConnectivityResult.FetchedSchemas"/>'s per-entity value — a single
    /// "name: type, name: type, ..." string, fields ordered by name — back into (name, type) pairs.
    /// A bare <c>", "</c> split is wrong: a type description can itself contain a comma inside parens
    /// (<c>Decimal128(10,2)</c>, <c>Timestamp(Millisecond, UTC)</c>), so this only splits on a
    /// top-level (paren-depth-zero) <c>", "</c>.</summary>
    private static List<(string Name, string Type)> ParseRenderedSchema(string rendered)
    {
        var result = new List<(string, string)>();
        if (rendered.Length == 0)
        {
            return result;
        }

        var depth = 0;
        var start = 0;
        for (var i = 0; i < rendered.Length; i++)
        {
            switch (rendered[i])
            {
                case '(':
                    depth++;
                    break;
                case ')':
                    depth--;
                    break;
                case ',' when depth == 0 && i + 1 < rendered.Length && rendered[i + 1] == ' ':
                    result.Add(SplitEntry(rendered[start..i]));
                    start = i + 2;
                    i++;
                    break;
            }
        }

        result.Add(SplitEntry(rendered[start..]));
        return result;
    }

    private static (string Name, string Type) SplitEntry(string entry)
    {
        var idx = entry.IndexOf(": ", StringComparison.Ordinal);
        return idx < 0 ? (entry, "") : (entry[..idx], entry[(idx + 2)..]);
    }

    /// <summary>Shared by <see cref="Overview"/>'s two success paths (full compile, and the
    /// compile-failed-but-load-succeeded fallback). <paramref name="dag"/> is null on the fallback
    /// path — flows and the per-pipeline refs/sources/sinks need a compiled DAG, so they come back
    /// empty rather than guessed at (no regex/hand-rolled SQL parsing, per the repo's binding
    /// convention). <paramref name="effectiveConnections"/> is <see cref="CompiledDag.Connections"/>
    /// on the compiled path (includes call-site-only entities a sink()/source() kwarg declared with
    /// no YAML `entities:` block) and the bare <see cref="PzProject.Connections"/> otherwise.</summary>
    private static void WriteOverview(
        Utf8JsonWriter json, PzProject project, IReadOnlyList<ConnectionDef> effectiveConnections, CompiledDag? dag)
    {
        json.WriteStartObject("result");
        json.WriteString("name", project.Name);

        json.WriteStartArray("flows");
        if (dag is not null)
        {
            foreach (var flow in FlowComponents.Compute(dag))
            {
                json.WriteStringValue(flow.Label);
            }
        }

        json.WriteEndArray();

        json.WriteStartArray("connections");
        foreach (var connection in effectiveConnections)
        {
            json.WriteStartObject();
            json.WriteString("name", connection.Name);
            json.WriteString("connector", connection.Connector);
            json.WriteStartArray("entities");
            var readNames = connection.Datasets.Select(d => d.Name);
            var writeNames = connection.Outputs.Select(o => o.Name).Concat(connection.EntityWrites.Keys);
            foreach (var entity in readNames.Concat(writeNames).Distinct(StringComparer.Ordinal)
                .OrderBy(n => n, StringComparer.Ordinal))
            {
                json.WriteStartObject();
                json.WriteString("name", entity);
                json.WriteBoolean("has_read", connection.Datasets.Any(d => d.Name == entity));
                json.WriteBoolean("has_write",
                    connection.Outputs.Any(o => o.Name == entity) || connection.EntityWrites.ContainsKey(entity));
                json.WriteEndObject();
            }

            json.WriteEndArray();
            json.WriteEndObject();
        }

        json.WriteEndArray();

        var byId = dag?.Nodes.ToDictionary(n => n.Id);

        json.WriteStartArray("pipelines");
        foreach (var pipeline in project.Pipelines)
        {
            var node = dag?.Nodes.FirstOrDefault(n => n.Kind == NodeKind.Pipeline && n.Name == pipeline.Name);
            json.WriteStartObject();
            json.WriteString("name", pipeline.Name);
            WritePipelineEdges(json, node, byId, dag);
            json.WriteEndObject();
        }

        json.WriteEndArray();

        WriteDagNodes(json, dag);
        json.WriteEndObject();
    }

    /// <summary>refs/sources/sinks for one pipeline node — all three read straight off DependsOn edges
    /// and their upstream/downstream nodes' typed Definition (never the pipeline's own RawSql/rendered
    /// SQL text, per the repo's binding convention against hand-parsed SQL and the class doc's secret
    /// hygiene note). Empty on the compile-failed fallback path (<paramref name="node"/> is null —
    /// there is no DAG to walk) or for an ephemeral pipeline (never gets a DagNode at all).</summary>
    private static void WritePipelineEdges(
        Utf8JsonWriter json, DagNode? node, Dictionary<NodeId, DagNode>? byId, CompiledDag? dag)
    {
        var parents = node is null || byId is null ? [] : node.DependsOn.Select(id => byId[id]).ToList();

        json.WriteStartArray("refs");
        foreach (var parent in parents.Where(p => p.Kind == NodeKind.Pipeline))
        {
            json.WriteStringValue(parent.Name);
        }

        json.WriteEndArray();

        json.WriteStartArray("sources");
        foreach (var parent in parents.Where(p => p.Kind == NodeKind.SourceLoad))
        {
            var def = (SourceDatasetDef)parent.Definition;
            json.WriteStringValue($"{def.Source.Name}.{def.Dataset.Name}");
        }

        json.WriteEndArray();

        json.WriteStartArray("sinks");
        if (node is not null && dag is not null)
        {
            foreach (var sinkNode in dag.Nodes.Where(
                n => n.Kind == NodeKind.SinkWrite && n.DependsOn.Contains(node.Id)))
            {
                var def = (SinkOutputDef)sinkNode.Definition;
                json.WriteStringValue($"{def.Sink.Name}.{def.Output.Name}");
            }
        }

        json.WriteEndArray();
    }

    private static void WriteDagNodes(Utf8JsonWriter json, CompiledDag? dag)
    {
        json.WriteStartObject("dag");
        json.WriteStartArray("nodes");
        if (dag is not null)
        {
            foreach (var node in dag.Nodes)
            {
                json.WriteStartObject();
                json.WriteString("id", node.Id.Value);
                json.WriteString("name", node.Name);
                json.WriteString("kind", node.Kind.ToString());
                json.WriteStartArray("dependsOn");
                foreach (var dep in node.DependsOn)
                {
                    json.WriteStringValue(dep.Value);
                }

                json.WriteEndArray();
                json.WriteEndObject();
            }
        }

        json.WriteEndArray();
        json.WriteEndObject();
    }

    /// <summary>Parses a connector-published JSON Schema string and writes it back out verbatim as a
    /// raw JSON value (never re-derived/re-shaped): connector reference schemas are emitted exactly as
    /// the connector publishes them.</summary>
    private static void WriteRawSchema(Utf8JsonWriter json, string propertyName, string rawJson)
    {
        json.WritePropertyName(propertyName);
        using var doc = JsonDocument.Parse(rawJson);
        doc.RootElement.WriteTo(json);
    }

    /// <summary>The top-level `errors` array, in the exact shape <see cref="ToolEnvelope.Errors"/>
    /// itself writes (code/message/file?/line?/next_step) — duplicated here rather than reused because
    /// <see cref="ToolEnvelope"/> exposes no way to combine <c>ok:true</c> with a populated
    /// <c>errors</c> array (its <c>Errors</c> factory always writes <c>ok:false</c>), which is exactly
    /// what pz_project_overview's best-effort fallback needs.</summary>
    private static void WriteErrorsField(Utf8JsonWriter json, IReadOnlyList<PzError> errors)
    {
        json.WriteStartArray("errors");
        foreach (var e in errors)
        {
            json.WriteStartObject();
            json.WriteString("code", e.Code);
            json.WriteString("message", e.Message);
            if (e.File is { } file) { json.WriteString("file", file); }
            if (e.Line is { } line) { json.WriteNumber("line", line); }
            json.WriteString("next_step", e.Hint);
            json.WriteEndObject();
        }

        json.WriteEndArray();
    }
}
