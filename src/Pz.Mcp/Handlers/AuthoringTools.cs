using System.Text.Json;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Execution;
using Pz.Engine.Validation;
using Pz.Mcp.Editing;
using Pz.PackageManagement.Hosting;

namespace Pz.Mcp.Handlers;

/// <summary>pz_add_connection / pz_update_connection / pz_remove_connection — the connection-authoring
/// tools. Every mutation runs the same shared four-step pipeline:
///
/// 1. <see cref="CredentialGuard"/> (add/update only): refuse a literal credential before anything else
///    — no project load, no file write. PZ0601 per offending key, <c>applied:false</c>.
/// 2. Pre-validate: load the real project (so a malformed <c>connections.yml</c> is reported the normal
///    way — <see cref="ProjectPhases.Load"/> aggregates it as PZ0101 rather than this class ever seeing
///    a raw <c>YamlException</c> out of <see cref="YamlSurgeon"/>'s own parse), then run the proposed
///    connection through the exact tier-3 seam <see cref="ConnectorConfigValidator"/> uses — never
///    reimplemented. Errors → <c>applied:false</c>, file untouched.
/// 3. Apply via <see cref="YamlSurgeon"/>. A <see cref="PzConfigException"/> (PZ0602 — mutation target
///    already exists / does not exist) is caught and its hint rewritten to name the sibling tool that
///    IS the right one to call, never the generic YamlSurgeon-level hint.
/// 4. Self-verify: <see cref="VerifyProjectAsync"/> — the same compile + offline validate tiers
///    <c>VerifyTools.ValidateAsync</c> runs. The edit already applied by this point, so <c>applied</c>
///    is <see langword="true"/> regardless of what self-verify finds — a mutation stays applied and
///    reports the errors; <c>ok</c> is <see langword="false"/> only when self-verify itself found
///    something.
///
/// <c>connections.yml</c> has no top-level <c>connections:</c> wrapper — a connection's own name is a
/// top-level key of the file, and every YamlSurgeon call below therefore uses an EMPTY path
/// (the same root-mapping shape <c>Pz.Core.Loading.ConnectionsLoader</c> reads).</summary>
internal static class AuthoringTools
{
    private const string ConnectionsFileName = "connections.yml";

    /// <summary>pz_add_connection: writes a brand-new top-level connection block. PZ0602 (from step 3)
    /// when <paramref name="name"/> already exists — its hint is rewritten to point at
    /// pz_update_connection.</summary>
    internal static Task<string> AddConnectionAsync(
        string projectDir, string name, string connector, Dictionary<string, object?> connection,
        CliServices services, CancellationToken ct) =>
        AddOrUpdateAsync(projectDir, name, connector, connection, services, isUpdate: false, ct);

    /// <summary>pz_update_connection: replaces an existing top-level connection block WHOLESALE — the
    /// block is re-rendered from exactly <paramref name="connector"/> + <paramref
    /// name="connection"/>, so anything else that lived under the old block (an <c>entities:</c> read/
    /// write block, <c>retry:</c>, ...) is not carried forward; those live under separate entity-
    /// authoring tools, out of this tool's scope. PZ0602 when <paramref name="name"/> does not exist —
    /// its hint is rewritten to point at pz_add_connection.</summary>
    internal static Task<string> UpdateConnectionAsync(
        string projectDir, string name, string connector, Dictionary<string, object?> connection,
        CliServices services, CancellationToken ct) =>
        AddOrUpdateAsync(projectDir, name, connector, connection, services, isUpdate: true, ct);

    /// <summary>pz_remove_connection: deletes a top-level connection block. No credential guard / schema
    /// pre-validate (there is no proposed connection block to check) — just the load guard, the
    /// removal, and self-verify. Removing a connection a pipeline still reads from stays
    /// <c>applied:true</c> and reports the resulting compile/validate errors, rather than refusing the
    /// removal.</summary>
    internal static async Task<string> RemoveConnectionAsync(
        string projectDir, string name, CliServices services, CancellationToken ct)
    {
        try
        {
            ProjectPhases.Load(projectDir);
        }
        catch (PzValidationException ex)
        {
            return ToolEnvelope.Errors(ex.Errors, applied: false);
        }

        var connectionsFile = Path.Combine(projectDir, ConnectionsFileName);
        try
        {
            YamlSurgeon.RemoveMappingEntry(connectionsFile, [], name);
        }
        catch (PzConfigException ex)
        {
            var enriched = ex.Error with
            {
                Hint = $"no connection named '{name}' exists -- check pz_project_overview for the " +
                    "declared connection names",
            };
            return ToolEnvelope.Errors([enriched], applied: false);
        }

        return await FinishWithSelfVerifyAsync(projectDir, services, ct, dropComment: null).ConfigureAwait(false);
    }

    // --------------------------------------------------------------------------------------------
    // Entity authoring: pz_add_entity / pz_set_entity_options / pz_remove_entity.
    //
    // An entity lives at path [connectionName, "entities"] in connections.yml (no top-level
    // "connections:" wrapper -- see the class doc above) -- depth 2, one deeper than a connection
    // block itself. The block value is {read: …, write: …}, omitting whichever half is null. There is
    // no credential-shaped input here and no dedicated schema pre-validate step (unlike connections'
    // tier-3 ConnectorConfigValidator seam): a bad connection name or malformed read/write shape is
    // simply caught by self-verify like any other mutation whose OWN input was well-formed but which
    // breaks the wider project -- applied stays true, errors are reported.
    // --------------------------------------------------------------------------------------------

    private const string EntitiesKey = "entities";

    /// <summary>pz_add_entity: writes a brand-new entity block under an existing connection's
    /// <c>entities:</c> mapping (created if this is the connection's first entity). PZ0602 (from
    /// YamlSurgeon) when <paramref name="entity"/> already exists under <paramref name="connection"/>
    /// -- hint rewritten to point at pz_set_entity_options.</summary>
    internal static Task<string> AddEntityAsync(
        string projectDir, string connection, string entity,
        Dictionary<string, object?>? read, Dictionary<string, object?>? write,
        CliServices services, CancellationToken ct) =>
        AddOrSetEntityAsync(projectDir, connection, entity, read, write, services, isSet: false, ct);

    /// <summary>pz_set_entity_options: replaces an existing entity block WHOLESALE with exactly
    /// <paramref name="read"/>/<paramref name="write"/> -- like pz_update_connection, nothing else is
    /// carried forward from the old block. PZ0602 when <paramref name="entity"/> does not exist under
    /// <paramref name="connection"/> -- hint rewritten to point at pz_add_entity.</summary>
    internal static Task<string> SetEntityOptionsAsync(
        string projectDir, string connection, string entity,
        Dictionary<string, object?>? read, Dictionary<string, object?>? write,
        CliServices services, CancellationToken ct) =>
        AddOrSetEntityAsync(projectDir, connection, entity, read, write, services, isSet: true, ct);

    /// <summary>pz_remove_entity: deletes an entity block. Removing an entity a pipeline still
    /// reads/writes stays <c>applied:true</c> and reports the resulting errors.</summary>
    internal static async Task<string> RemoveEntityAsync(
        string projectDir, string connection, string entity, CliServices services, CancellationToken ct)
    {
        try
        {
            ProjectPhases.Load(projectDir);
        }
        catch (PzValidationException ex)
        {
            return ToolEnvelope.Errors(ex.Errors, applied: false);
        }

        var connectionsFile = Path.Combine(projectDir, ConnectionsFileName);
        try
        {
            YamlSurgeon.RemoveMappingEntry(connectionsFile, [connection, EntitiesKey], entity);
        }
        catch (PzConfigException ex)
        {
            var enriched = ex.Error with
            {
                Hint = $"no entity named '{entity}' under connection '{connection}' exists -- check " +
                    "pz_project_overview for the declared entity names",
            };
            return ToolEnvelope.Errors([enriched], applied: false);
        }

        return await FinishWithSelfVerifyAsync(projectDir, services, ct, EntityResult(connection, entity, null))
            .ConfigureAwait(false);
    }

    private static async Task<string> AddOrSetEntityAsync(
        string projectDir, string connection, string entity,
        Dictionary<string, object?>? read, Dictionary<string, object?>? write,
        CliServices services, bool isSet, CancellationToken ct)
    {
        PzProject project;
        try
        {
            project = ProjectPhases.Load(projectDir);
        }
        catch (PzValidationException ex)
        {
            return ToolEnvelope.Errors(ex.Errors, applied: false);
        }

        // Add-only: InsertMappingEntry auto-vivifies every missing path segment, including the
        // connection itself -- a typo'd connection name would otherwise land as a brand-new,
        // connector-less top-level block (caught only later, by self-verify, in violation of the
        // validate-before-writing contract). ReplaceMappingEntry/RemoveMappingEntry (the isSet and
        // RemoveEntityAsync paths) already refuse via ResolveExistingTarget when any path segment is
        // missing, so this check is Add-only.
        if (!isSet && !project.Connections.Any(c => c.Name == connection))
        {
            return ToolEnvelope.Errors([UnknownConnectionForEntityError(connection, projectDir)], applied: false);
        }

        // A proposed path-scoped-connector path escaping the project is refused before anything is
        // written -- see PathGuard for the posture. Connector resolved from the loaded
        // project; an add against an unknown connection was already refused above, so a null here can
        // only be the isSet path, whose missing-target refusal comes from ReplaceMappingEntry below.
        if (project.Connections.FirstOrDefault(c => c.Name == connection)?.Connector is { } entityConnector &&
            PathGuard.IsPathScoped(entityConnector))
        {
            var pathErrors = PathGuard.FindEscapes(entityConnector, connection, entity, read, write, projectDir);
            if (pathErrors.Count > 0)
            {
                return ToolEnvelope.Errors(pathErrors, applied: false);
            }
        }

        var connectionsFile = Path.Combine(projectDir, ConnectionsFileName);
        var path = new[] { connection, EntitiesKey };
        var canonicalBlock = RenderEntityBlock(entity, read, write, indentLevels: path.Length);
        bool? droppedComment = null;
        try
        {
            if (isSet)
            {
                droppedComment = YamlSurgeon.ReplaceMappingEntry(connectionsFile, path, entity, canonicalBlock);
            }
            else
            {
                YamlSurgeon.InsertMappingEntry(connectionsFile, path, entity, canonicalBlock);
            }
        }
        catch (PzConfigException ex)
        {
            var enriched = ex.Error with
            {
                Hint = isSet
                    ? $"no entity named '{entity}' under connection '{connection}' exists -- call " +
                        "pz_add_entity instead"
                    : $"an entity named '{entity}' under connection '{connection}' already exists -- call " +
                        "pz_set_entity_options instead",
            };
            return ToolEnvelope.Errors([enriched], applied: false);
        }

        return await FinishWithSelfVerifyAsync(
            projectDir, services, ct, EntityResult(connection, entity, droppedComment)).ConfigureAwait(false);
    }

    private static Action<Utf8JsonWriter> EntityResult(string connection, string entity, bool? droppedComment) =>
        json =>
        {
            json.WriteStartObject("result");
            json.WriteString("file", ConnectionsFileName);
            json.WriteString("connection", connection);
            json.WriteString("entity", entity);
            if (droppedComment is { } dropped)
            {
                json.WriteBoolean("dropped_comment", dropped);
            }

            json.WriteEndObject();
        };

    /// <summary>Renders the whole entity block: <c>read:</c> then <c>write:</c>, omitting whichever is
    /// <see langword="null"/>.</summary>
    private static string RenderEntityBlock(
        string entity, Dictionary<string, object?>? read, Dictionary<string, object?>? write, int indentLevels)
    {
        var value = new Dictionary<string, object?>();
        if (read is not null)
        {
            value["read"] = read;
        }

        if (write is not null)
        {
            value["write"] = write;
        }

        return CanonicalYaml.MappingEntry(entity, value, indentLevels);
    }

    private static PzError UnknownConnectionForEntityError(string connection, string projectDir) => new(
        PzErrorCode.McpMutationTarget,
        $"no connection named '{connection}' exists",
        Path.Combine(projectDir, ConnectionsFileName), null,
        "call pz_add_connection first, or check pz_project_overview for the declared connection names");

    // --------------------------------------------------------------------------------------------
    // Pipeline authoring: pz_write_pipeline / pz_remove_pipeline.
    // --------------------------------------------------------------------------------------------

    private const string PipelinesDirName = "pipelines";
    private const string ConfigsDirName = "configs";

    /// <summary>pz_write_pipeline: creates or replaces <c>pipelines/&lt;name&gt;.sql</c>, normalized to
    /// LF line endings with a trailing newline, plus an optional <c>pipelines/configs/&lt;name&gt;.yml</c>
    /// sidecar written verbatim (agent-authored YAML -- the self-verify pass below is what parses and
    /// validates it; a malformed sidecar comes back as PZ0111/PZ0113 in the envelope, same as a
    /// hand-edited one). <paramref name="name"/> must be a safe file stem: no path separators, no
    /// <c>..</c> -- refused (PZ0602) rather than risking a write outside <c>pipelines/</c>.</summary>
    internal static async Task<string> WritePipelineAsync(
        string projectDir, string name, string sql, string? checksYaml, CliServices services, CancellationToken ct)
    {
        if (!IsSafeFileStem(name))
        {
            return ToolEnvelope.Errors([InvalidPipelineNameError(name, projectDir)], applied: false);
        }

        try
        {
            ProjectPhases.Load(projectDir);
        }
        catch (PzValidationException ex)
        {
            return ToolEnvelope.Errors(ex.Errors, applied: false);
        }

        var pipelinesDir = Path.Combine(projectDir, PipelinesDirName);
        Directory.CreateDirectory(pipelinesDir);
        var relativeSqlPath = $"{PipelinesDirName}/{name}.sql";
        File.WriteAllText(Path.Combine(pipelinesDir, name + ".sql"), NormalizeSql(sql));

        string? relativeConfigPath = null;
        if (checksYaml is not null)
        {
            var configsDir = Path.Combine(pipelinesDir, ConfigsDirName);
            Directory.CreateDirectory(configsDir);
            relativeConfigPath = $"{PipelinesDirName}/{ConfigsDirName}/{name}.yml";
            File.WriteAllText(Path.Combine(configsDir, name + ".yml"), checksYaml);
        }

        return await FinishWithSelfVerifyAsync(projectDir, services, ct, json =>
        {
            json.WriteStartObject("result");
            json.WriteString("sql_file", relativeSqlPath);
            if (relativeConfigPath is not null)
            {
                json.WriteString("checks_file", relativeConfigPath);
            }

            json.WriteEndObject();
        }).ConfigureAwait(false);
    }

    /// <summary>pz_remove_pipeline: deletes both <c>pipelines/&lt;name&gt;.sql</c> and its sidecar (if
    /// any). Missing pipeline (no <c>.sql</c> file) is PZ0602. Removing a pipeline another pipeline
    /// still <c>ref()</c>s stays <c>applied:true</c> and reports the resulting compile
    /// errors.</summary>
    internal static async Task<string> RemovePipelineAsync(
        string projectDir, string name, CliServices services, CancellationToken ct)
    {
        if (!IsSafeFileStem(name))
        {
            return ToolEnvelope.Errors([InvalidPipelineNameError(name, projectDir)], applied: false);
        }

        try
        {
            ProjectPhases.Load(projectDir);
        }
        catch (PzValidationException ex)
        {
            return ToolEnvelope.Errors(ex.Errors, applied: false);
        }

        var sqlPath = Path.Combine(projectDir, PipelinesDirName, name + ".sql");
        if (!File.Exists(sqlPath))
        {
            return ToolEnvelope.Errors([MissingPipelineError(name, sqlPath)], applied: false);
        }

        File.Delete(sqlPath);
        var configPath = Path.Combine(projectDir, PipelinesDirName, ConfigsDirName, name + ".yml");
        var removedConfig = File.Exists(configPath);
        if (removedConfig)
        {
            File.Delete(configPath);
        }

        return await FinishWithSelfVerifyAsync(projectDir, services, ct, json =>
        {
            json.WriteStartObject("result");
            json.WriteString("sql_file", $"{PipelinesDirName}/{name}.sql");
            json.WriteBoolean("checks_file_removed", removedConfig);
            json.WriteEndObject();
        }).ConfigureAwait(false);
    }

    /// <summary>No path separators, no <c>..</c>, not blank -- <paramref name="name"/> becomes exactly
    /// <c>pipelines/&lt;name&gt;.sql</c> with no further sanitization, so this is the only thing standing
    /// between a caller-supplied name and a write outside <c>pipelines/</c>.</summary>
    private static bool IsSafeFileStem(string name) =>
        !string.IsNullOrWhiteSpace(name)
        && name is not ("." or "..")
        && name.IndexOfAny(['/', '\\']) < 0
        && !name.Contains("..", StringComparison.Ordinal);

    private static PzError InvalidPipelineNameError(string name, string projectDir) => new(
        PzErrorCode.McpMutationTarget,
        $"pipeline name '{name}' is not a valid file name -- it must contain no path separators or '..'",
        Path.Combine(projectDir, PipelinesDirName), null,
        "use a plain identifier; pz writes it to pipelines/<name>.sql directly");

    private static PzError MissingPipelineError(string name, string filePath) => new(
        PzErrorCode.McpMutationTarget,
        $"no pipeline named '{name}' exists",
        filePath, null,
        "check pz_project_overview for the declared pipeline names");

    /// <summary>Normalizes to LF line endings with exactly one trailing newline (the repo's byte-stable
    /// writer convention, applied to caller-supplied SQL text).</summary>
    private static string NormalizeSql(string sql)
    {
        var normalized = sql.Replace("\r\n", "\n").Replace("\r", "\n");
        return normalized.EndsWith('\n') ? normalized : normalized + "\n";
    }

    // --------------------------------------------------------------------------------------------
    // Project init: pz_init_project.
    // --------------------------------------------------------------------------------------------

    /// <summary>pz_init_project: scaffolds a brand-new project into <paramref name="projectDir"/> itself
    /// (the server's own project directory context -- the tool takes no target-directory argument) via
    /// <see cref="CliServices.InitProject"/>, which wraps <c>InitCommand</c>'s real scaffolding logic.
    /// A non-empty <paramref name="projectDir"/> comes back as PZ0603 with nothing written -- checked by
    /// the <see cref="CliServices.InitProject"/> implementation itself before any file is touched.
    /// Synchronous (no I/O here is awaited) but named/shaped like every other tool handler.
    ///
    /// The result lists every file written. Reporting only <c>created: true</c> left the caller
    /// needing a second round trip to learn what it had just been handed -- and, with the sample
    /// template, what it would have to delete.</summary>
    internal static string InitProject(string projectDir, bool minimal, CliServices services)
    {
        // Trim trailing separators ONCE, up front, and use the trimmed path consistently everywhere
        // below -- McpCommand.InitProject derives its InitCommand.Execute workingDir from this same
        // `dir` via Path.GetDirectoryName, which does NOT strip a trailing separator itself
        // (GetDirectoryName("/foo/bar/") is "/foo/bar", not "/foo"), so passing the untrimmed path
        // through would scaffold one level too deep (targetDir "/foo/bar/bar").
        var trimmed = projectDir.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        var name = Path.GetFileName(trimmed);
        var errors = services.InitProject(trimmed, name, minimal);
        if (errors.Count > 0)
        {
            return ToolEnvelope.Errors(errors, applied: false);
        }

        return ToolEnvelope.Ok(json =>
        {
            json.WriteStartObject("result");
            json.WriteBoolean("created", true);
            json.WriteString("dir", trimmed);
            json.WriteString("template", minimal ? "minimal" : "sample");
            json.WriteStartArray("files");
            foreach (var file in ScaffoldedFiles(trimmed))
            {
                json.WriteStringValue(file);
            }

            json.WriteEndArray();
            json.WriteEndObject();
        }, applied: true);
    }

    /// <summary>Every file the scaffold just wrote, project-relative with forward slashes and
    /// ordinal-sorted so the envelope is byte-stable across filesystems. Read back off disk rather
    /// than predicted from the template: the manifest is whatever actually landed.</summary>
    private static IReadOnlyList<string> ScaffoldedFiles(string projectDir)
    {
        if (!Directory.Exists(projectDir))
        {
            return [];
        }

        return
        [
            .. Directory.EnumerateFiles(projectDir, "*", SearchOption.AllDirectories)
                .Select(f => Path.GetRelativePath(projectDir, f).Replace(Path.DirectorySeparatorChar, '/'))
                .OrderBy(f => f, StringComparer.Ordinal),
        ];
    }

    private static async Task<string> AddOrUpdateAsync(
        string projectDir, string name, string connector, Dictionary<string, object?> connection,
        CliServices services, bool isUpdate, CancellationToken ct)
    {
        // Step 1 -- before anything else: no project load, no registry, no file touched yet.
        var credentialErrors = CredentialGuard.FindLiteralCredentials(connection);
        if (credentialErrors.Count > 0)
        {
            return ToolEnvelope.Errors(credentialErrors, applied: false);
        }

        // Step 1b: a proposed path-scoped root/base_dir/path escaping the project is
        // refused before anything is written -- see PathGuard for the posture.
        if (PathGuard.IsPathScoped(connector))
        {
            var pathErrors = PathGuard.FindEscapes(connector, name, connection, projectDir);
            if (pathErrors.Count > 0)
            {
                return ToolEnvelope.Errors(pathErrors, applied: false);
            }
        }

        // Step 2a: load the real project first, so a malformed connections.yml is reported the normal
        // aggregated way rather than surfacing as a raw YamlException out of YamlSurgeon's own parse
        // in step 3.
        PzProject project;
        try
        {
            project = ProjectPhases.Load(projectDir);
        }
        catch (PzValidationException ex)
        {
            return ToolEnvelope.Errors(ex.Errors, applied: false);
        }

        var connectionsFile = Path.Combine(projectDir, ConnectionsFileName);
        var probeConnection = new ConnectionDef(name, connector, connection, [], connectionsFile);
        // A one-connection project: keeps project.Connectors (so an already-restored non-builtin
        // connector package still resolves) but scopes validation to exactly the proposed connection.
        var probeProject = project with { Connections = [probeConnection] };

        (ConnectorRegistry Registry, ConnectorHost? Host) resolved;
        try
        {
            resolved = await services.CreateRegistryAsync(probeProject, projectDir, ct).ConfigureAwait(false);
        }
        catch (PzValidationException ex)
        {
            return ToolEnvelope.Errors(ex.Errors, applied: false);
        }

        var (registry, host) = resolved;
        await using var connectorHost = host;

        // Step 2b: an unknown connector name never reaches ConnectorConfigValidator with any signal --
        // it silently skips a connection whose connector isn't registered (by design: a source() naming
        // an unregistered connector fails earlier, at compile, in the real CLI flow). Report it here
        // ourselves, in the same vocabulary SourceLoadExecutor/SinkWriteExecutor use at run time.
        if (!registry.TryGetSource(connector, out _) && !registry.TryGetSink(connector, out _))
        {
            return ToolEnvelope.Errors([UnknownConnectorError(connector, connectionsFile)], applied: false);
        }

        // Step 2c: the tier-3 JSON-Schema seam itself -- never reimplemented.
        var schemaErrors = await ConnectorConfigValidator.ValidateAsync(probeProject, registry, ct)
            .ConfigureAwait(false);
        if (schemaErrors.Count > 0)
        {
            return ToolEnvelope.Errors(schemaErrors, applied: false);
        }

        // Step 3: apply.
        var canonicalBlock = RenderConnectionBlock(name, connector, connection);
        bool? droppedComment = null;
        IReadOnlyList<string> droppedEntities = [];
        try
        {
            if (isUpdate)
            {
                // A wholesale replace takes the connection's `entities:` block with it (class doc).
                // Name what went, from the project loaded at step 2a -- an agent that called
                // pz_add_entity and then adjusted one connection option would otherwise be told
                // `ok: true` over the wreckage of its own prior calls, and only find out at compile.
                droppedEntities = DeclaredEntityNames(project, name);
                droppedComment = YamlSurgeon.ReplaceMappingEntry(connectionsFile, [], name, canonicalBlock);
            }
            else
            {
                YamlSurgeon.InsertMappingEntry(connectionsFile, [], name, canonicalBlock);
            }
        }
        catch (PzConfigException ex)
        {
            var enriched = ex.Error with
            {
                Hint = isUpdate
                    ? $"no connection named '{name}' exists -- call pz_add_connection instead"
                    : $"a connection named '{name}' already exists -- call pz_update_connection instead",
            };
            return ToolEnvelope.Errors([enriched], applied: false);
        }

        // Step 4: self-verify.
        return await FinishWithSelfVerifyAsync(projectDir, services, ct, droppedComment, droppedEntities)
            .ConfigureAwait(false);
    }

    /// <summary>The entity names a connection declares, by the same union
    /// <c>IntrospectTools.WriteOverview</c> reports: read declarations (<c>Datasets</c>) plus write
    /// declarations (<c>Outputs</c> and <c>EntityWrites</c>). Ordinal-sorted for a byte-stable
    /// envelope. Empty when the connection declares none, or does not exist.</summary>
    private static IReadOnlyList<string> DeclaredEntityNames(PzProject project, string connectionName)
    {
        var connection = project.Connections.FirstOrDefault(c => c.Name == connectionName);
        if (connection is null)
        {
            return [];
        }

        return
        [
            .. connection.Datasets.Select(d => d.Name)
                .Concat(connection.Outputs.Select(o => o.Name))
                .Concat(connection.EntityWrites.Keys)
                .Distinct(StringComparer.Ordinal)
                .OrderBy(n => n, StringComparer.Ordinal),
        ];
    }

    /// <summary>Step 4, shared by all three tools: the mutation already applied by this point, so
    /// <c>applied</c> is always <see langword="true"/> from here on -- only <c>ok</c>
    /// and the errors array depend on what self-verify finds. <paramref name="dropComment"/> is
    /// <see langword="null"/> for insert/remove (no comment-tracking to report) and the
    /// <see cref="YamlSurgeon.ReplaceMappingEntry"/> result for update. <paramref name="dropEntities"/>
    /// is likewise update-only, and written only when non-empty — a caller that dropped nothing sees
    /// no such field.</summary>
    private static async Task<string> FinishWithSelfVerifyAsync(
        string projectDir, CliServices services, CancellationToken ct, bool? dropComment,
        IReadOnlyList<string>? dropEntities = null)
    {
        var verifyErrors = await VerifyProjectAsync(projectDir, services, ct).ConfigureAwait(false);
        void WriteResult(Utf8JsonWriter json)
        {
            json.WriteStartObject("result");
            json.WriteString("file", ConnectionsFileName);
            if (dropComment is { } dropped)
            {
                json.WriteBoolean("dropped_comment", dropped);
            }

            if (dropEntities is { Count: > 0 })
            {
                json.WriteStartArray("dropped_entities");
                foreach (var entity in dropEntities)
                {
                    json.WriteStringValue(entity);
                }

                json.WriteEndArray();
                json.WriteStartArray("warnings");
                json.WriteStringValue(
                    "pz_update_connection replaced the connection block wholesale, so these entity " +
                    "declarations went with it -- re-add each with pz_add_entity if it is still wanted");
                json.WriteEndArray();
            }

            json.WriteEndObject();
        }

        // The result rides BOTH envelopes: a dropped entity is usually the very reason self-verify
        // now fails (a pipeline still source()s it), so reporting it only on the success path would
        // withhold the explanation exactly when it is needed most.
        return verifyErrors.Count > 0
            ? ToolEnvelope.Errors(verifyErrors, applied: true, WriteResult)
            : ToolEnvelope.Ok(WriteResult, applied: true);
    }

    /// <summary>Overload for the entity/pipeline tools: same self-verify contract as the one above, but
    /// the caller renders its own <c>result</c> object (their shapes differ from connections' fixed
    /// <c>{file, dropped_comment?}</c>) instead of passing a comment-drop flag.</summary>
    private static async Task<string> FinishWithSelfVerifyAsync(
        string projectDir, CliServices services, CancellationToken ct, Action<Utf8JsonWriter> writeResult)
    {
        var verifyErrors = await VerifyProjectAsync(projectDir, services, ct).ConfigureAwait(false);
        if (verifyErrors.Count > 0)
        {
            return ToolEnvelope.Errors(verifyErrors, applied: true);
        }

        return ToolEnvelope.Ok(writeResult, applied: true);
    }

    /// <summary>The same compile + offline-validate-tiers composition <c>VerifyTools.ValidateAsync</c>
    /// runs with <c>connect: false</c> (tiers 1-4) -- reused rather than duplicated. Never throws:
    /// a <see cref="PzValidationException"/> anywhere in this chain (including a compile failure the
    /// mutation itself caused) becomes part of the returned error list, exactly like the errors tier 3/4
    /// already aggregate.</summary>
    private static async Task<IReadOnlyList<PzError>> VerifyProjectAsync(
        string projectDir, CliServices services, CancellationToken ct)
    {
        try
        {
            var (project, dag, _) = ProjectPhases.LoadAndCompile(projectDir);
            project = project with { Connections = dag.Connections };

            var (registry, host) = await services.CreateRegistryAsync(project, projectDir, ct).ConfigureAwait(false);
            await using var connectorHost = host;

            var tier3Errors = await ConnectorConfigValidator.ValidateAsync(project, registry, ct)
                .ConfigureAwait(false);
            if (tier3Errors.Count > 0)
            {
                return tier3Errors;
            }

            var dry = await SqlDryCompiler.RunAsync(dag, ct).ConfigureAwait(false);
            return dry.Errors;
        }
        catch (PzValidationException ex)
        {
            return ex.Errors;
        }
    }

    /// <summary>Renders the whole connection block: <c>connector:</c> first, then every proposed
    /// connection option flattened directly underneath it (no nested <c>connection:</c> key -- connector
    /// config is flat; see <c>Pz.Core.Loading.ConnectionsLoader</c>'s
    /// own doc comment), in the caller-supplied dictionary's own key order (no re-sorting --
    /// CanonicalYaml's contract).</summary>
    private static string RenderConnectionBlock(string name, string connector, Dictionary<string, object?> connection)
    {
        var value = new Dictionary<string, object?> { ["connector"] = connector };
        foreach (var (key, val) in connection)
        {
            value[key] = val;
        }

        return CanonicalYaml.MappingEntry(name, value, indentLevels: 0);
    }

    private static PzError UnknownConnectorError(string connector, string filePath) => new(
        PzErrorCode.ConnectorNotInstalled,
        $"connector '{connector}' is not installed",
        filePath, null,
        "check the connector name against pz_connector_reference, or declare the package under " +
        "project.yml's connectors: and run 'pz restore' first");
}
