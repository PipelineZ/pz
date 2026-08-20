using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.State;

namespace Pz.Engine.Execution;

/// <summary>Materializes a pipeline's rendered SQL into its staging table or view. Ephemeral pipelines
/// have no DAG nodes, so every node this executor sees is table- or view-materialized.</summary>
public sealed class PipelineExecutor : INodeExecutor
{
    public async Task<NodeResult> ExecuteAsync(DagNode node, RunContext ctx, CancellationToken ct)
    {
        var def = (PipelineDef)node.Definition;
        var isView = string.Equals(def.Materialization, "view", StringComparison.OrdinalIgnoreCase);
        var relationKind = isView ? "VIEW" : "TABLE";

        // Rewrite each watermark() sentinel out of the rendered SQL before executing it --
        // a typed literal from the stored watermark when one exists (and this isn't a full-refresh run),
        // else NULL, which makes the compiler's NULL-guard arm (<expr> IS NULL OR ...) true and passes
        // every row. Empty WatermarkSubstitutions (the common case) leaves the SQL byte-untouched.
        var sql = node.RenderedSql!;
        foreach (var sub in node.WatermarkSubstitutions)
        {
            Watermark? stored = ctx.FullRefresh
                ? null
                : ctx.Watermarks?.Get(WatermarkStore.Key(sub.SourceName, sub.Dataset), ctx.Notice);
            // The declared type is a cross-check, not an input: the stored watermark carries its own
            // TypeName, and a first run (stored is null) renders NULL and needs no type at all. A dataset
            // with no columns: contract has a null CursorType -- nothing to
            // drift from, so the guard is skipped and the store's own type governs the literal.
            if (stored is not null && sub.CursorType is not null
                && !string.Equals(stored.TypeName, sub.CursorType, StringComparison.Ordinal))
            {
                var error = new PzError(PzErrorCode.UnsupportedCursorType,
                    $"pipeline '{node.Name}': stored watermark for '{sub.SourceName}.{sub.Dataset}' has cursor type " +
                    $"'{stored.TypeName}' but the declared cursor type is '{sub.CursorType}'",
                    def.FilePath, null, "run with --full-refresh, or align columns: with the stored watermark's type");
                return new NodeResult(node.Id, node.Kind, node.Name, NodeStatus.Failed, 0, TimeSpan.Zero, error);
            }

            var literal = stored is null ? "NULL" : CursorLiterals.Typed(stored.TypeName, stored.Value);
            sql = sql.Replace($"'{sub.Sentinel}'", literal, StringComparison.Ordinal);
        }

        await ctx.Duck.ExecuteAsync(
            $"CREATE OR REPLACE {relationKind} staging.{node.Name} AS ({sql})", ct).ConfigureAwait(false);

        var rows = isView
            ? 0L
            : await ctx.Duck.ScalarAsync<long>($"select count(*) from staging.{node.Name}", ct).ConfigureAwait(false);

        return new NodeResult(node.Id, node.Kind, node.Name, NodeStatus.Success, rows, TimeSpan.Zero, null);
    }
}
