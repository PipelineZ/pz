using System.Text.Json;
using Pz.Core.Dag;
using Pz.Core.Validation;
using Pz.Engine.Planning;
using Pz.Engine.Validation;

namespace Pz.Mcp.Handlers;

/// <summary>pz_compile / pz_validate / pz_plan — the read-only verify tools. Every handler is
/// console-free (notices/warnings/results ride the returned envelope, never Console.*) and follows
/// the same shape: try the phase composition, catch <see cref="PzValidationException"/>, turn it into
/// <see cref="ToolEnvelope.Errors"/>. Result payloads never carry SQL text or connection config values
/// (secret/SQL hygiene) — node names/kinds/ids only, and planner Reason strings, which are
/// template-only by the same binding convention plan.json itself relies on.
///
/// These tools are read-only and deliberately never write to <c>.pz/target</c> — no
/// <c>manifest.json</c> (compile), no <c>plan.json</c> (plan), no <c>schemas.json</c> (validate
/// --connect's schema-drift cache). Each handler runs the same in-memory validation work its CLI
/// verb counterpart does, but stops short of that verb's own artifact-writing step; a caller that
/// wants an artifact written (e.g. the schema cache warmed for later drift detection) runs the
/// actual `pz` CLI verb, not this tool. Doc comments below describing a handler's tiers/phases as
/// mirroring its CLI verb mean the validation logic, not the verb's console output or file
/// writes.</summary>
internal static class VerifyTools
{
    /// <summary>pz_compile: render pipelines, build the DAG. Result: {nodes:[{id,name,kind,dependsOn}],
    /// notices:[…], warnings:[…]}.</summary>
    internal static Task<string> CompileAsync(string projectDir, CliServices services, CancellationToken ct)
    {
        try
        {
            var (_, dag, notices) = ProjectPhases.LoadAndCompile(projectDir);
            return Task.FromResult(ToolEnvelope.Ok(json =>
            {
                json.WriteStartObject("result");
                WriteNodes(json, dag.Nodes);
                WriteStringArray(json, "notices", notices);
                WriteWarnings(json, dag.Warnings);
                json.WriteEndObject();
            }));
        }
        catch (PzValidationException ex)
        {
            return Task.FromResult(ToolEnvelope.Errors(ex.Errors));
        }
    }

    /// <summary>pz_validate: tiers 1-4 always, tier 5 (live connectivity + schema drift) only when
    /// <paramref name="connect"/> is true — tiers run cheapest-first exactly like `pz validate`, so a
    /// broken project never reaches a network probe. Result on success:
    /// {pipelines: N, connections_checked: N, undeclared_datasets:[…]}.
    ///
    /// Unlike `pz validate --connect`, tier 5 here never calls SchemaCacheWriter — no
    /// <c>.pz/target/schemas.json</c> is written (class doc: read-only). A caller that
    /// wants the schema-drift cache warmed runs `pz validate --connect` itself.</summary>
    internal static async Task<string> ValidateAsync(
        string projectDir, bool connect, CliServices services, CancellationToken ct)
    {
        try
        {
            var (project, dag, _) = ProjectPhases.LoadAndCompile(projectDir);
            // Tiers 3-5 validate the EFFECTIVE connections (project's + any call-site-declared entity),
            // exactly as ValidateCommand does.
            project = project with { Connections = dag.Connections };

            var (registry, host) = await services.CreateRegistryAsync(project, projectDir, ct).ConfigureAwait(false);
            await using var connectorHost = host;

            var tier3Errors = await ConnectorConfigValidator.ValidateAsync(project, registry, ct).ConfigureAwait(false);
            if (tier3Errors.Count > 0)
            {
                return ToolEnvelope.Errors(tier3Errors);
            }

            var dry = await SqlDryCompiler.RunAsync(dag, ct).ConfigureAwait(false);
            if (dry.Errors.Count > 0)
            {
                return ToolEnvelope.Errors(dry.Errors);
            }

            if (connect)
            {
                var connectProject = ProjectPhases.InjectProjectDirectoryAnchor(project, projectDir);
                var connectivity = await ConnectivityValidator.RunAsync(connectProject, registry, ct).ConfigureAwait(false);
                if (connectivity.Errors.Count > 0)
                {
                    return ToolEnvelope.Errors(connectivity.Errors);
                }
            }

            // One count per CONNECTION, not per direction — mirrors ValidateCommand exactly.
            var connectionsChecked = project.Connections.Count(
                c => registry.TryGetSource(c.Connector, out _) || registry.TryGetSink(c.Connector, out _));

            return ToolEnvelope.Ok(json =>
            {
                json.WriteStartObject("result");
                json.WriteNumber("pipelines", project.Pipelines.Count);
                json.WriteNumber("connections_checked", connectionsChecked);
                WriteStringArray(json, "undeclared_datasets", dry.UndeclaredDatasets);
                json.WriteEndObject();
            });
        }
        catch (PzValidationException ex)
        {
            return ToolEnvelope.Errors(ex.Errors);
        }
    }

    /// <summary>pz_plan: the per-node execution strategy the engine would use (native scan/copy vs.
    /// the universal batch path) — Check nodes excluded, exactly like `pz plan`'s console table.
    /// Result: {nodes:[{node,strategy,reason,pushdown?}], memory_budget_bytes}.</summary>
    internal static async Task<string> PlanAsync(string projectDir, CliServices services, CancellationToken ct)
    {
        try
        {
            var (project, dag, _) = ProjectPhases.LoadAndCompile(projectDir);
            // Injected after compile here (PlanCommand injects before compiling instead) — harmless
            // either way: base_dir is a connector-open-time value, never consulted by DagCompiler
            // itself, so which side of Compile() it's added on doesn't change the resulting DAG.
            project = ProjectPhases.InjectProjectDirectoryAnchor(project, projectDir);
            var runDag = new CompiledDag([.. dag.Nodes.Where(n => n.Kind != NodeKind.Check)]);

            var (registry, host) = await services.CreateRegistryAsync(project, projectDir, ct).ConfigureAwait(false);
            await using var connectorHost = host;

            var plan = await new ExecutionPlanner(registry)
                .PlanAsync(runDag, project.Engine.ForceUniversal, ct, project.Engine).ConfigureAwait(false);

            return ToolEnvelope.Ok(json =>
            {
                json.WriteStartObject("result");
                json.WriteStartArray("nodes");
                foreach (var node in plan.Nodes)
                {
                    json.WriteStartObject();
                    json.WriteString("node", node.Name);
                    json.WriteString("strategy", StrategyName(node.Strategy));
                    json.WriteString("reason", node.Reason);
                    if (node.Pushdown is { } pushdown)
                    {
                        json.WriteStartObject("pushdown");
                        if (pushdown.ColumnsPushed is { } columns)
                        {
                            json.WriteNumber("columns_pushed", columns);
                        }

                        json.WriteBoolean("predicate_pushed", pushdown.PredicatePushed);
                        json.WriteEndObject();
                    }

                    json.WriteEndObject();
                }

                json.WriteEndArray();
                json.WriteNumber("memory_budget_bytes", plan.MemoryBudget.TotalBytes);
                json.WriteEndObject();
            });
        }
        catch (PzValidationException ex)
        {
            return ToolEnvelope.Errors(ex.Errors);
        }
    }

    private static void WriteNodes(Utf8JsonWriter json, IReadOnlyList<DagNode> nodes)
    {
        json.WriteStartArray("nodes");
        foreach (var node in nodes)
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

        json.WriteEndArray();
    }

    private static void WriteWarnings(Utf8JsonWriter json, IReadOnlyList<PzWarning> warnings)
    {
        json.WriteStartArray("warnings");
        foreach (var w in warnings)
        {
            json.WriteStartObject();
            json.WriteString("code", w.Code);
            json.WriteString("message", w.Message);
            if (w.File is { } file)
            {
                json.WriteString("file", file);
            }

            if (w.Line is { } line)
            {
                json.WriteNumber("line", line);
            }

            if (w.Hint is { } hint)
            {
                json.WriteString("hint", hint);
            }

            json.WriteEndObject();
        }

        json.WriteEndArray();
    }

    private static void WriteStringArray(Utf8JsonWriter json, string propertyName, IReadOnlyList<string> values)
    {
        json.WriteStartArray(propertyName);
        foreach (var value in values)
        {
            json.WriteStringValue(value);
        }

        json.WriteEndArray();
    }

    // Mirrors PlanWriter.StrategyName (internal to Pz.Engine, not visible across assemblies) so the
    // MCP result uses the exact same strategy names as plan.json and `pz plan`'s console table.
    private static string StrategyName(EdgeStrategy strategy) => strategy switch
    {
        EdgeStrategy.NativeScan => "native_scan",
        EdgeStrategy.NativeCopy => "native_copy",
        EdgeStrategy.ArrowStream => "arrow_stream",
        EdgeStrategy.DuckSql => "duck_sql",
        _ => throw new ArgumentOutOfRangeException(nameof(strategy), strategy, "unknown strategy"),
    };
}
