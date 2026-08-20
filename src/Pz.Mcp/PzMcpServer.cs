using System.Text.Json;
using ModelContextProtocol.Server;
using Pz.Mcp.Docs;
using Pz.Mcp.Handlers;

namespace Pz.Mcp;

/// <summary>Assembles the pz tool surface. Execution tools exist ONLY when allowRun is
/// true — absent from the listing, not present-but-refusing: the capability boundary
/// is the operator's server flag, not the model's choice.
///
/// Wire-schema convention: the MCP C# SDK emits each delegate's C# PARAMETER NAMES
/// verbatim into a tool's JSON input schema (its snake_case policy applies only when deriving a tool
/// NAME from a method name), so the lambda parameters below are deliberately spelled snake_case — the
/// input surface then matches the envelope's own <c>next_step</c>/<c>run_id</c> style and, more
/// importantly, matches <c>https://pipelinez.dev/reference/mcp-contract/</c>, which is an append-only stability
/// contract. For the same reason every genuinely-optional input carries a C# default
/// value: the SDK's schema inference marks a defaulted parameter as NOT required, which is what makes
/// `pz_validate` callable without `connect` and `pz_run` callable with only `all`. Renaming a parameter
/// here, or removing a default, is a breaking change to that contract.</summary>
public static class PzMcpServer
{
    /// <summary>One client for the process, with a timeout short enough that an unreachable docs site
    /// fails fast into PZ0607 instead of stalling an agent behind the default 100-second wait.</summary>
    private static readonly Lazy<HttpClient> SharedDocsHttp = new(() => new HttpClient
    {
        Timeout = TimeSpan.FromSeconds(15),
    });

    /// <summary>The schema the SDK infers for a <see cref="JsonElement"/> parameter is the boolean
    /// schema <c>true</c> — "any value", which tells a reader nothing about the one shape the handler
    /// accepts. Every such parameter here (<c>connection</c>, <c>read</c>, <c>write</c>) is a mapping
    /// of YAML option keys, so republish it as a typed object: an agent choosing its first call reads
    /// the schema, and "any value" is the one answer that cannot guide it.
    ///
    /// Deliberately narrow — it rewrites a node only when that node is the untyped one and the
    /// parameter really is a JsonElement, so no other tool's schema is touched.</summary>
    private const string OptionMapDescription =
        "YAML option keys and values for this block, nested exactly as they appear in " +
        "connections.yml (see pz_connector_reference for the connector's own schema).";

    private static readonly Microsoft.Extensions.AI.AIJsonSchemaCreateOptions OptionMapSchema = new()
    {
        TransformSchemaNode = static (context, node) =>
        {
            if (context.TypeInfo.Type != typeof(JsonElement) && context.TypeInfo.Type != typeof(JsonElement?))
            {
                return node;
            }

            // Two shapes reach here: `true` (a required JsonElement) arrives as a JsonValue, while an
            // optional JsonElement? arrives as an object carrying only its `default`. Keep whatever
            // the SDK put there — dropping `default` would make the parameter read as required.
            var schema = node as System.Text.Json.Nodes.JsonObject ?? [];
            schema["type"] = "object";
            schema["description"] = OptionMapDescription;
            return schema;
        },
    };

    /// <param name="docsHttp">Overrides the HTTP client the documentation tools use. Production passes
    /// nothing; tests pass a client over a stub handler so the doc tools are exercised without a
    /// network.</param>
    public static McpServerOptions CreateOptions(
        string projectDir, CliServices services, bool allowRun, HttpClient? docsHttp = null)
    {
        var tools = new List<McpServerTool>
        {
            McpServerTool.Create(
                (CancellationToken ct) => VerifyTools.CompileAsync(projectDir, services, ct),
                new McpServerToolCreateOptions
                {
                    Name = "pz_compile",
                    Description = "Compile the project: render pipelines, build the DAG. Returns the DAG or aggregate PZ errors.",
                }),
            McpServerTool.Create(
                (CancellationToken ct) => VerifyTools.PlanAsync(projectDir, services, ct),
                new McpServerToolCreateOptions
                {
                    Name = "pz_plan",
                    Description = "Show the per-node execution strategy (native scan/copy vs. the universal batch " +
                        "path) the engine would use, and why. Read-only — does not write plan.json.",
                }),
            McpServerTool.Create(
                (CancellationToken ct, bool connect = false) =>
                    VerifyTools.ValidateAsync(projectDir, connect, services, ct),
                new McpServerToolCreateOptions
                {
                    Name = "pz_validate",
                    Description = "Validate config, connector option schemas, and SQL. Set connect=true to also " +
                        "probe live connections and fetch schemas.",
                }),
            McpServerTool.Create(
                () => IntrospectTools.Overview(projectDir),
                new McpServerToolCreateOptions
                {
                    Name = "pz_project_overview",
                    Description = "Summarize the project: flows, connections and their entities (names/types " +
                        "only, never config values), pipelines and their refs/sources/sinks, and the compiled DAG.",
                }),
            McpServerTool.Create(
                (CancellationToken ct) => IntrospectTools.ConnectorReferenceAsync(projectDir, services, ct),
                new McpServerToolCreateOptions
                {
                    Name = "pz_connector_reference",
                    Description = "List every connector type this project uses, with its capability flags and " +
                        "raw JSON Schemas for the connection block and per-dataset options.",
                }),
            McpServerTool.Create(
                () => IntrospectTools.State(projectDir, services),
                new McpServerToolCreateOptions
                {
                    Name = "pz_state",
                    Description = "Report stored watermarks, sync-state, and schema baselines, plus the latest run's summary.",
                }),
            McpServerTool.Create(
                (string connection, string entity, CancellationToken ct) =>
                    IntrospectTools.EntitySchemaAsync(projectDir, connection, entity, services, ct),
                new McpServerToolCreateOptions
                {
                    Name = "pz_entity_schema",
                    Description = "Live schema fetch for one connection+entity: opens the connection and " +
                        "fetches its columns/types. Read-only — does not write .pz/target/schemas.json.",
                }),
            McpServerTool.Create(
                (string name, string connector, JsonElement connection, CancellationToken ct) =>
                    AuthoringTools.AddConnectionAsync(
                        projectDir, name, connector, ToConnectionOptions(connection), services, ct),
                new McpServerToolCreateOptions
                {
                    SchemaCreateOptions = OptionMapSchema,
                    Name = "pz_add_connection",
                    Description = "Add a new connection to connections.yml. Validates before writing, refuses " +
                        "literal credentials (use ${VAR}), and self-verifies after applying.",
                }),
            McpServerTool.Create(
                (string name, string connector, JsonElement connection, CancellationToken ct) =>
                    AuthoringTools.UpdateConnectionAsync(
                        projectDir, name, connector, ToConnectionOptions(connection), services, ct),
                new McpServerToolCreateOptions
                {
                    SchemaCreateOptions = OptionMapSchema,
                    Name = "pz_update_connection",
                    Description = "Replace an existing connection's connector + options wholesale -- this DROPS " +
                        "the connection's entities: block (re-add with pz_add_entity; the result reports " +
                        "dropped_entities). Pass every option the connection should keep, not just the changed " +
                        "one. Validates before writing, refuses literal credentials (use ${VAR}), and " +
                        "self-verifies after applying.",
                }),
            McpServerTool.Create(
                (string name, CancellationToken ct) =>
                    AuthoringTools.RemoveConnectionAsync(projectDir, name, services, ct),
                new McpServerToolCreateOptions
                {
                    Name = "pz_remove_connection",
                    Description = "Remove a connection from connections.yml. Stays applied even if a pipeline " +
                        "still reads from it -- self-verify reports the resulting errors.",
                }),
            McpServerTool.Create(
                (string connection, string entity, CancellationToken ct,
                        JsonElement? read = null, JsonElement? write = null) =>
                    AuthoringTools.AddEntityAsync(
                        projectDir, connection, entity, ToConnectionOptionsOrNull(read), ToConnectionOptionsOrNull(write),
                        services, ct),
                new McpServerToolCreateOptions
                {
                    SchemaCreateOptions = OptionMapSchema,
                    Name = "pz_add_entity",
                    Description = "Add a new entity (read:/write: options) under an existing connection in " +
                        "connections.yml. Self-verifies after applying.",
                }),
            McpServerTool.Create(
                (string connection, string entity, CancellationToken ct,
                        JsonElement? read = null, JsonElement? write = null) =>
                    AuthoringTools.SetEntityOptionsAsync(
                        projectDir, connection, entity, ToConnectionOptionsOrNull(read), ToConnectionOptionsOrNull(write),
                        services, ct),
                new McpServerToolCreateOptions
                {
                    SchemaCreateOptions = OptionMapSchema,
                    Name = "pz_set_entity_options",
                    Description = "Replace an existing entity's read:/write: options wholesale. Self-verifies " +
                        "after applying.",
                }),
            McpServerTool.Create(
                (string connection, string entity, CancellationToken ct) =>
                    AuthoringTools.RemoveEntityAsync(projectDir, connection, entity, services, ct),
                new McpServerToolCreateOptions
                {
                    Name = "pz_remove_entity",
                    Description = "Remove an entity from a connection. Stays applied even if a pipeline still " +
                        "reads/writes it -- self-verify reports the resulting errors.",
                }),
            McpServerTool.Create(
                (string name, string sql, CancellationToken ct, string? checks_yaml = null) =>
                    AuthoringTools.WritePipelineAsync(projectDir, name, sql, checks_yaml, services, ct),
                new McpServerToolCreateOptions
                {
                    Name = "pz_write_pipeline",
                    Description = "Create or replace pipelines/<name>.sql, plus an optional " +
                        "pipelines/configs/<name>.yml checks sidecar (written verbatim). Self-verifies after " +
                        "writing.",
                }),
            McpServerTool.Create(
                (string name, CancellationToken ct) =>
                    AuthoringTools.RemovePipelineAsync(projectDir, name, services, ct),
                new McpServerToolCreateOptions
                {
                    Name = "pz_remove_pipeline",
                    Description = "Delete a pipeline's .sql file and its checks sidecar, if any. Stays applied " +
                        "even if another pipeline still ref()s it -- self-verify reports the resulting errors.",
                }),
            McpServerTool.Create(
                (string template = "minimal") => AuthoringTools.InitProject(projectDir, template, services),
                new McpServerToolCreateOptions
                {
                    Name = "pz_init_project",
                    Description = "Scaffold a new pz project into this server's own project directory, and " +
                        "list the files written. minimal (default true, same as `pz init`): project.yml + " +
                        "connections.yml only, ready to author against. Pass minimal=false for the runnable " +
                        "four-pipeline sample (`pz init --sample`) -- only when the user asked to see a " +
                        "worked example, since its demo pipelines compile and would run. Refuses if the " +
                        "directory already exists and is not empty.",
                }),
        };

        // The documentation tools. Alone among the surface, these three read from the network — the
        // docs are published on the site rather than embedded, so an agent gets what is currently
        // true instead of what shipped with this build. Also alone in taking no project directory:
        // documentation is worth consulting before a project exists.
        var docs = new DocsCatalog(docsHttp ?? SharedDocsHttp.Value);
        tools.Add(McpServerTool.Create(
            (CancellationToken ct) => DocsTools.ListAsync(docs, ct),
            new McpServerToolCreateOptions
            {
                Name = "pz_docs_list",
                Description = "List every published pz documentation page: slug, title, one-line summary, " +
                    "and URL. Needs network access; PZ0607 if the docs site is unreachable.",
            }));
        tools.Add(McpServerTool.Create(
            (string query, CancellationToken ct, int limit = 10) => DocsTools.SearchAsync(docs, query, limit, ct),
            new McpServerToolCreateOptions
            {
                Name = "pz_docs_search",
                Description = "Search the pz documentation by keyword, best matches first, with matching " +
                    "excerpts. Good for error codes (PZ0214), option names (force_universal), and concepts. " +
                    "Needs network access; PZ0607 if the docs site is unreachable.",
            }));
        tools.Add(McpServerTool.Create(
            (string slug, CancellationToken ct) => DocsTools.GetAsync(docs, slug, ct),
            new McpServerToolCreateOptions
            {
                Name = "pz_docs_get",
                Description = "Fetch one documentation page's full markdown by slug, as reported by " +
                    "pz_docs_list or pz_docs_search. Needs network access; PZ0607 if unreachable.",
            }));

        if (allowRun)
        {
            tools.Add(McpServerTool.Create(
                (CancellationToken ct, string[]? flow_names = null, bool all = false, bool full_refresh = false) =>
                    ExecutionTools.RunAsync(projectDir, flow_names ?? [], all, full_refresh, services, ct),
                new McpServerToolCreateOptions
                {
                    Name = "pz_run",
                    Description = "Execute a named flow. Moves real data and advances watermarks. " +
                        "flow_names: the flow(s) to run (each runs that node plus every ancestor/descendant); " +
                        "all: run the whole project; neither (in a 2+-flow project) is PZ0215 -- name a flow " +
                        "or pass all=true.",
                }));
            tools.Add(McpServerTool.Create(
                (CancellationToken ct, bool full_refresh = false) =>
                    ExecutionTools.RetryAsync(projectDir, full_refresh, services, ct),
                new McpServerToolCreateOptions
                {
                    Name = "pz_retry",
                    Description = "Re-run the last failed run, reusing staged data where safe.",
                }));
            tools.Add(McpServerTool.Create(
                (string? run_id = null) => ExecutionTools.RunResults(projectDir, run_id, services),
                new McpServerToolCreateOptions
                {
                    Name = "pz_run_results",
                    Description = "Full structured results for a run id (default: latest).",
                }));
        }
        return new McpServerOptions
        {
            ServerInfo = new() { Name = "pz", Version = typeof(PzMcpServer).Assembly.GetName().Version?.ToString() ?? "0.0.0" },
            // Every tool answers detailed invalid-params errors for argument-shape mistakes instead of
            // the SDK binder's generic text — see ArgumentValidatingTool.
            ToolCollection = [.. tools.Select(tool => new ArgumentValidatingTool(tool))],
        };
    }

    /// <summary>Converts the wire-level JSON object a client sends for a tool's <c>connection</c>
    /// parameter into the closed value shape every downstream consumer (CanonicalYaml,
    /// ConnectorConfigValidator's JSON-Schema evaluation) expects: <see cref="Dictionary{TKey,TValue}"/>/
    /// <see cref="List{T}"/> containers and long/double/bool/string/null scalars -- the same shape
    /// <c>Pz.Core.Loading.YamlMapper</c> itself produces, never a boxed <see cref="JsonElement"/>. A JSON
    /// integer that fits in a <see langword="long"/> stays a <see langword="long"/> (matching the YAML
    /// loader's own int-vs-double split); anything larger becomes a <see langword="double"/>.</summary>
    private static Dictionary<string, object?> ToConnectionOptions(JsonElement connection)
    {
        if (connection.ValueKind != JsonValueKind.Object)
        {
            throw new ArgumentException(
                $"'connection' must be a JSON object, got {connection.ValueKind}.", nameof(connection));
        }

        return ToDictionary(connection);
    }

    /// <summary>Same conversion as <see cref="ToConnectionOptions"/>, but for the entity tools' optional
    /// <c>read</c>/<c>write</c> parameters: an absent or JSON-null argument becomes
    /// <see langword="null"/> (the "omit this half of the entity block" signal) rather than an error.</summary>
    private static Dictionary<string, object?>? ToConnectionOptionsOrNull(JsonElement? element)
    {
        if (element is not { } value || value.ValueKind is JsonValueKind.Undefined or JsonValueKind.Null)
        {
            return null;
        }

        if (value.ValueKind != JsonValueKind.Object)
        {
            throw new ArgumentException(
                $"'read'/'write' must be a JSON object, got {value.ValueKind}.", nameof(element));
        }

        return ToDictionary(value);
    }

    private static object? ToValue(JsonElement element) => element.ValueKind switch
    {
        JsonValueKind.Object => ToDictionary(element),
        JsonValueKind.Array => element.EnumerateArray().Select(ToValue).ToList(),
        JsonValueKind.String => element.GetString(),
        JsonValueKind.Number => element.TryGetInt64(out var l) ? l : element.GetDouble(),
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        JsonValueKind.Null or JsonValueKind.Undefined => null,
        _ => throw new ArgumentException($"unsupported JSON value kind: {element.ValueKind}"),
    };

    private static Dictionary<string, object?> ToDictionary(JsonElement element)
    {
        var result = new Dictionary<string, object?>();
        foreach (var property in element.EnumerateObject())
        {
            result[property.Name] = ToValue(property.Value);
        }

        return result;
    }
}
