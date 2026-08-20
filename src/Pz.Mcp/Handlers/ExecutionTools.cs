using System.Text.Json;
using Pz.Core.Validation;
using Pz.Engine.Artifacts;

namespace Pz.Mcp.Handlers;

/// <summary>pz_run / pz_retry / pz_run_results — the gated execution tools. Registered ONLY under
/// <c>--allow-run</c> (<see cref="PzMcpServer"/>): unlike
/// every other handler in this project, these move real data and advance watermarks/sync state.
///
/// The heavy lifting -- load+compile, selection resolution, <c>RetryReusePlanner.Plan</c>, and the
/// actual <c>RunCommand.ExecuteRun</c> call -- lives in Pz.Cli (<c>McpCommand</c>'s <c>RunAsync</c>/
/// <c>RetryAsync</c> adapters, wired onto <see cref="CliServices"/>) because Pz.Mcp cannot reference
/// Pz.Cli (dependency direction is the other way -- see <see cref="CliServices"/>'s own doc comment).
/// This class is therefore thin: call the adapter, then turn its <see cref="McpRunOutcome"/> into the
/// standard envelope, reading the resulting run's node-level detail from
/// <see cref="CliServices.CreateStateStores"/>'s <see cref="McpStateStores.Artifacts"/> exactly the way
/// `pz retry`/`pz state show` already do.
///
/// Result payload for pz_run/pz_retry on success: <c>{run_id, status, nodes:[{id,name,status,kind,rows}],
/// exit_code, notices:[…], warnings:[…], note?}</c>. <c>nodes[].rows</c> comes straight off
/// <see cref="PriorNode.Rows"/> -- <see cref="PriorNode"/> carries no duration field (and
/// <see cref="PriorRun"/> itself has no StartedAt), so no <c>duration_ms</c> is
/// emitted; a future run-artifact schema change could add one without breaking this append-only shape.
/// <c>notices</c>/<c>warnings</c> are the same DagCompiler compile notices and
/// <see cref="Pz.Core.Dag.CompiledDag.Warnings"/> `pz_compile` already envelopes (identical field shapes
/// to <c>VerifyTools.CompileAsync</c>'s result) -- `pz run`/`pz retry` print both to the console
/// (`SharedInputHelpers.WriteWarnings` / a `note: ` line per compile notice); MCP has no console, so they
/// ride the result instead of being silently dropped. Both are empty arrays (never omitted) when the run
/// never reached a successful compile. <c>note</c> only appears for `pz_retry`'s "nothing to retry"
/// outcome (<see cref="McpRunOutcome.Note"/>) or for `pz_run_results`'s explicit-id fallback on a remote
/// state backend (see <see cref="RunResults"/>).
///
/// A non-empty <see cref="McpRunOutcome.Errors"/> (config/selection/lock failure -- the run never
/// started) envelopes as <c>ok:false</c>. Everything else -- including a run that DID start but ended
/// "completed_with_failures"/"fatal", or a retry that had nothing to do -- is <c>ok:true</c>: the tool
/// call itself succeeded, and the result payload's own <c>status</c>/<c>exit_code</c> report what
/// actually happened, mirroring how `pz run`'s own exit code (0/1/3) is orthogonal to whether the CLI
/// invocation itself was well-formed.</summary>
internal static class ExecutionTools
{
    internal static async Task<string> RunAsync(
        string projectDir, string[] flowNames, bool all, bool fullRefresh, CliServices services, CancellationToken ct)
    {
        // The run delegates to the CLI machinery, which never routes back through
        // ProjectPhases -- so the path-escape guard (see PathGuard) must run here, before anything
        // executes. A load failure would be caught identically inside the run itself; surfacing it
        // through the same envelope shape just happens earlier.
        try
        {
            ProjectPhases.Load(projectDir);
        }
        catch (PzValidationException ex)
        {
            return ToolEnvelope.Errors(ex.Errors);
        }

        var outcome = await services.RunAsync(
            new McpRunRequest(projectDir, flowNames, all, fullRefresh), ct).ConfigureAwait(false);
        return Render(outcome, projectDir, services);
    }

    internal static async Task<string> RetryAsync(
        string projectDir, bool fullRefresh, CliServices services, CancellationToken ct)
    {
        // Same pre-run guard as RunAsync above.
        try
        {
            ProjectPhases.Load(projectDir);
        }
        catch (PzValidationException ex)
        {
            return ToolEnvelope.Errors(ex.Errors);
        }

        var outcome = await services.RetryAsync(projectDir, fullRefresh, ct).ConfigureAwait(false);
        return Render(outcome, projectDir, services);
    }

    /// <summary>pz_run_results(run_id?): <c>ReadLatest()</c> when <paramref name="runId"/> is null/absent
    /// -- the common case, and the same read the RunAsync/RetryAsync envelope above already performs.
    /// <see cref="IRunArtifactStore"/> has no by-id read (only <c>ReadLatest</c>/<c>ReadAllNewestFirst</c>),
    /// so an explicit id against the LOCAL store walks <see cref="RunResultsReader.ReadAllNewestFirst"/>
    /// (the same reader `pz state show`'s rollback menu uses) looking for a match -- still one run's
    /// worth of parsing in the common case (the requested id is recent), degrading to a full scan only
    /// for a very old run id. A remote store has no equivalent
    /// walk-every-run API in v1 -- an explicit id there returns the latest run anyway, with
    /// <c>note</c> saying so plainly rather than silently substituting a different run.</summary>
    internal static string RunResults(string projectDir, string? runId, CliServices services)
    {
        try
        {
            var project = ProjectPhases.Load(projectDir);
            var stores = services.CreateStateStores(project, projectDir);

            string? note = null;
            PriorRun? run;
            if (runId is null)
            {
                run = stores.Artifacts.ReadLatest();
            }
            else if (stores.Artifacts is LocalRunArtifactStore)
            {
                run = RunResultsReader.ReadAllNewestFirst(projectDir)
                    .FirstOrDefault(r => string.Equals(r.RunId, runId, StringComparison.Ordinal));
            }
            else
            {
                run = stores.Artifacts.ReadLatest();
                if (run is not null && !string.Equals(run.RunId, runId, StringComparison.Ordinal))
                {
                    note = "this project's state backend only supports reading the latest run -- " +
                        $"returning run '{run.RunId}' instead of the requested '{runId}'";
                }
            }

            if (run is null)
            {
                return ToolEnvelope.Errors([new PzError(PzErrorCode.NoPriorRun,
                    $"no run found for id '{runId ?? "latest"}' under .pz/runs", null, null,
                    "call pz_run first, or omit run_id to read the latest run")]);
            }

            return ToolEnvelope.Ok(json =>
            {
                json.WriteStartObject("result");
                WriteRun(json, run);
                if (note is not null)
                {
                    json.WriteString("note", note);
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

    private static string Render(McpRunOutcome outcome, string projectDir, CliServices services)
    {
        if (outcome.Errors.Count > 0)
        {
            return ToolEnvelope.Errors(outcome.Errors);
        }

        try
        {
            var project = ProjectPhases.Load(projectDir);
            var stores = services.CreateStateStores(project, projectDir);
            var run = stores.Artifacts.ReadLatest();

            return ToolEnvelope.Ok(json =>
            {
                json.WriteStartObject("result");
                if (run is not null)
                {
                    WriteRun(json, run);
                }

                json.WriteNumber("exit_code", outcome.ExitCode);
                WriteStringArray(json, "notices", outcome.Notices ?? []);
                WriteWarnings(json, outcome.Warnings ?? []);
                if (outcome.Note is not null)
                {
                    json.WriteString("note", outcome.Note);
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

    private static void WriteStringArray(Utf8JsonWriter json, string propertyName, IReadOnlyList<string> values)
    {
        json.WriteStartArray(propertyName);
        foreach (var value in values)
        {
            json.WriteStringValue(value);
        }

        json.WriteEndArray();
    }

    // Field shape matches VerifyTools.CompileAsync's own "warnings" array exactly (code/message/file?/
    // line?/hint?) -- same PzWarning source, same JSON shape, two independent writers because Pz.Mcp's
    // handler files don't share a JSON-writing base.
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

    private static void WriteRun(Utf8JsonWriter json, PriorRun run)
    {
        json.WriteString("run_id", run.RunId);
        json.WriteString("status", run.Status);
        json.WriteStartArray("nodes");
        foreach (var node in run.Nodes)
        {
            json.WriteStartObject();
            json.WriteString("id", node.Id);
            json.WriteString("name", node.Name);
            json.WriteString("status", node.Status);
            json.WriteString("kind", node.Kind);
            json.WriteNumber("rows", node.Rows);
            // Additive: omitted for non-failed nodes so existing consumers' bytes are untouched --
            // the append-only envelope discipline.
            if (node.Error is { } error)
            {
                json.WriteStartObject("error");
                json.WriteString("code", error.Code);
                json.WriteString("message", error.Message);
                json.WriteEndObject();
            }

            json.WriteEndObject();
        }

        json.WriteEndArray();
    }
}
