using Pz.Core.Validation;
using Pz.DuckDb;
using Pz.Engine.Execution;

namespace Pz.Engine.Preview;

/// <summary><c>pz preview --run</c>/<c>--run-dir</c>: a sample of one node as a finished run left it, read
/// from the run directory alone (its <c>staging.duckdb</c> and the preview index inside it). Every way the
/// data can be missing is one PZ0537 that points at <c>--fresh</c>.</summary>
public static class PreviewRunReader
{
    private const string FreshHint = "preview it fresh with `pz preview <node> --fresh`";

    public static async Task<PreviewResult> ReadAsync(string runDir, string? runId, string node, int limit,
        CancellationToken ct)
    {
        if (!Directory.Exists(runDir))
        {
            throw Unavailable($"run directory '{runDir}' does not exist");
        }

        var staging = Path.Combine(runDir, "staging.duckdb");
        if (!File.Exists(staging))
        {
            throw Unavailable($"run directory '{runDir}' has no staging.duckdb (it was cleaned)");
        }

        // Held for the whole read: retention cannot delete the file under us, and a run still writing
        // it is refused at once instead of blocking on DuckDB's own file lock.
        using var held = RunDirLock.TryAcquire(runDir)
            ?? throw Unavailable($"the run in '{runDir}' is still in progress");

        DuckSession duck;
        try
        {
            duck = DuckSession.OpenReadOnly(staging);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw Unavailable($"the run in '{runDir}' is still in progress or its staging.duckdb cannot be opened");
        }

        await using (duck)
        {
            var lookup = await PreviewIndex.LookupAsync(duck, node, ct).ConfigureAwait(false);
            if (!lookup.IndexPresent)
            {
                throw Unavailable($"the run in '{runDir}' was written by an older pz, which kept no preview index");
            }

            if (lookup.ExcludedKind is { } kind)
            {
                throw new PzValidationException([new PzError(PzErrorCode.PreviewNodeNotPreviewable,
                    $"'{node}' is {Describe(kind)}, which has no rows of its own to preview.", null, null,
                    kind == "ephemeral"
                        ? "preview it with `pz preview <node> --fresh`"
                        : "preview the pipeline that feeds it")]);
            }

            if (lookup.Target is not { } target)
            {
                throw new PzValidationException([new PzError(PzErrorCode.SelectorNoMatch,
                    $"no node named '{node}' in the run in '{runDir}'.", null, null,
                    "pick a node name from `pz ls`")]);
            }

            if (!await RelationExistsAsync(duck, target.Relation, ct).ConfigureAwait(false))
            {
                throw Unavailable($"'{node}' did not run, or failed, in the run in '{runDir}'");
            }

            try
            {
                var (columns, rows) = await PreviewSampler
                    .SampleAsync(duck, target.Relation, target.RedactedColumns, limit, ct).ConfigureAwait(false);
                var total = await duck.ScalarAsync<long>($"select count(*) from {target.Relation}", ct)
                    .ConfigureAwait(false);
                return new PreviewResult(node, "run", runId, columns, rows, total, limit, Sliced: false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // A view whose SQL needs something this session lacks (an extension, a dropped table).
                throw Unavailable($"'{node}' could not be read back from the run in '{runDir}'");
            }
        }
    }

    private static async Task<bool> RelationExistsAsync(IDuckSession duck, string relation, CancellationToken ct)
    {
        var name = PzMeta.Escape(relation["staging.".Length..]);
        var count = await duck.ScalarAsync<long>(
            "select count(*) from (select table_name as n, schema_name as s, database_name as d from duckdb_tables() " +
            "union all select view_name, schema_name, database_name from duckdb_views()) " +
            $"where d = current_database() and s = 'staging' and n = '{name}'", ct).ConfigureAwait(false);
        return count > 0;
    }

    private static string Describe(string kind) => kind switch
    {
        "check" => "a check",
        "sink_write" => "a sink",
        _ => "an ephemeral pipeline",
    };

    private static PzValidationException Unavailable(string why) =>
        new([new PzError(PzErrorCode.PreviewUnavailable, $"{why}; nothing to preview from this run.", null, null,
            FreshHint)]);
}
