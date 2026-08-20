using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.DuckDb;
using Pz.Engine.Execution;

namespace Pz.Engine.Validation;

public sealed record DryCompileResult(
    IReadOnlyList<PzError> Errors,            // PZ0401 per failing pipeline
    IReadOnlyList<string> SkippedPipelines,   // unavailable transitive inputs
    IReadOnlyList<string> UndeclaredDatasets); // "source.dataset" without columns:

/// <summary>
/// Tier 4: EXPLAIN-by-materialization against contract-derived empty tables in a
/// throwaway file-backed DuckDB session (the temp file is deleted in a <c>finally</c> before returning,
/// independent of run state -- this never touches <c>.pz/target</c> or any real run's staging database).
/// Walks <see cref="CompiledDag.Nodes"/>, which <see cref="Pz.Core.Dag.DagCompiler"/> already produces in
/// topological order: a <see cref="NodeKind.SourceLoad"/> with a declared non-empty <c>columns:</c>
/// contract gets an empty table built from the contract's DDL types and becomes "available"; one without
/// a contract is recorded as undeclared and stays unavailable (never an error -- v0 does no schema
/// inference, so this is expected for some datasets). A <see cref="NodeKind.Pipeline"/> whose every
/// dependency is available gets dry-materialized (`limit 0`, `create view` for view materializations) --
/// success marks it available for downstream pipelines, a genuine SQL/binder failure is recorded as one
/// PZ0401 naming the pipeline's file and marks it unavailable; a pipeline with any unavailable dependency
/// (whether from an undeclared source or an upstream dry-compile failure) is recorded as skipped, never
/// errored, since dry-compile cannot say anything meaningful about SQL over a schema it doesn't know.
/// Check and SinkWrite nodes are not dry-compiled.
/// </summary>
public static class SqlDryCompiler
{
    /// <param name="tempRoot">Directory for the throwaway session's temp .duckdb file; defaults to the
    /// machine-global <c>%TMP%/pz-dry-compile</c> (safe across concurrent processes — file names are
    /// GUIDs and cleanup is per-file). Injectable so tests can observe leftover-file behavior over an
    /// isolated directory instead of the shared one.</param>
    public static async Task<DryCompileResult> RunAsync(CompiledDag dag, CancellationToken ct, string? tempRoot = null)
    {
        var root = tempRoot ?? Path.Combine(Path.GetTempPath(), "pz-dry-compile");
        var tempDbPath = Path.Combine(root, $"{Guid.NewGuid():N}.duckdb");
        Directory.CreateDirectory(root);

        var errors = new List<PzError>();
        var skipped = new List<string>();
        var undeclared = new List<string>();

        try
        {
            // Nodes available to downstream pipelines: a SourceLoad with a declared contract, or a
            // Pipeline that dry-materialized without error. Keyed by NodeId so it lines up directly with
            // DagNode.DependsOn without any name-based re-derivation.
            var available = new HashSet<NodeId>();

            await using (var duck = DuckSession.Open(tempDbPath))
            {
                await duck.ExecuteAsync("create schema if not exists staging", ct).ConfigureAwait(false);

                foreach (var node in dag.Nodes)
                {
                    ct.ThrowIfCancellationRequested();

                    switch (node.Kind)
                    {
                        case NodeKind.SourceLoad:
                            await HandleSourceLoadAsync(node, duck, available, undeclared, ct).ConfigureAwait(false);
                            break;
                        case NodeKind.Pipeline:
                            await HandlePipelineAsync(node, duck, available, errors, skipped, ct).ConfigureAwait(false);
                            break;
                        case NodeKind.Check:
                        case NodeKind.SinkWrite:
                            break; // not dry-compiled
                        default:
                            throw new ArgumentOutOfRangeException(nameof(dag), node.Kind, "unknown node kind");
                    }
                }
            }
        }
        finally
        {
            TryDeleteFile(tempDbPath);
        }

        return new DryCompileResult(errors, skipped, undeclared);
    }

    private static async Task HandleSourceLoadAsync(
        DagNode node, IDuckSession duck, HashSet<NodeId> available, List<string> undeclared, CancellationToken ct)
    {
        var def = (SourceDatasetDef)node.Definition;
        if (def.Dataset.Columns is not { Count: > 0 } columns)
        {
            undeclared.Add($"{def.Source.Name}.{def.Dataset.Name}");
            return;
        }

        var columnDdl = string.Join(", ",
            columns.Select(kv => $"{QuoteIdentifier(kv.Key)} {ContractTypes.ToDuckDdl(kv.Value)}"));
        await duck.ExecuteAsync($"create table {StagingRelation(node.Name)} ({columnDdl})", ct).ConfigureAwait(false);
        available.Add(node.Id);
    }

    private static async Task HandlePipelineAsync(
        DagNode node, IDuckSession duck, HashSet<NodeId> available, List<PzError> errors, List<string> skipped,
        CancellationToken ct)
    {
        if (!node.DependsOn.All(available.Contains))
        {
            skipped.Add(node.Name);
            return;
        }

        var def = (PipelineDef)node.Definition;
        var isView = string.Equals(def.Materialization, "view", StringComparison.OrdinalIgnoreCase);
        var relationKind = isView ? "view" : "table";
        var sql = $"create {relationKind} {StagingRelation(node.Name)} as select * from ({node.RenderedSql}) q limit 0";

        try
        {
            await duck.ExecuteAsync(sql, ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // The raw DuckDB message is sanitized the same way native scan/copy failures are
            // (NativeStatementRedactor.SanitizeEngineMessage) before it can reach a PzError -- a
            // parser/binder error's "LINE <n>: ..." context block would otherwise echo the dry-compiled
            // SQL verbatim. Only the first summary line of the DuckDB message is kept, which is already
            // enough to name the offending identifier.
            var sanitized = NativeStatementRedactor.SanitizeEngineMessage(ex.Message);
            var firstLine = sanitized.Split('\n', 2)[0];
            errors.Add(new PzError(PzErrorCode.SqlDryCompile, firstLine, def.FilePath, null,
                "fix the SQL or the declared columns: contract"));
            return;
        }

        available.Add(node.Id);
    }

    /// <summary>Quotes a single (dot-free) identifier by reusing <c>ArrowInterop.QuoteQualified</c> --
    /// the engine's one existing identifier-quoting helper (see <c>DuckSession</c>/ingest call sites) --
    /// rather than duplicating quoting logic here. It splits on '.', which is a no-op for a plain name.</summary>
    private static string QuoteIdentifier(string identifier) => ArrowInterop.QuoteQualified(identifier);

    private static string StagingRelation(string name) => $"staging.{QuoteIdentifier(name)}";

    private static void TryDeleteFile(string path)
    {
        try
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
        catch
        {
            // Suppressed by design: best-effort cleanup of the throwaway dry-compile session's temp
            // file must never mask whatever result RunAsync is about to return.
        }
    }
}
