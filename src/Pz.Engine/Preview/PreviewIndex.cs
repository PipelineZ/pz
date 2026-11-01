using System.Text;
using System.Text.Json;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Execution;

namespace Pz.Engine.Preview;

/// <summary>A node <c>pz preview</c> can sample from a run: its staging relation and the redaction marks
/// the run was compiled with.</summary>
public sealed record PreviewTarget(string Node, string Relation, IReadOnlyList<string> RedactedColumns);

/// <summary>What the index says about one node name. <see cref="IndexPresent"/> is false for a staging
/// file written before the index existed (an older pz).</summary>
public sealed record PreviewLookup(PreviewTarget? Target, string? ExcludedKind, bool IndexPresent);

/// <summary>The preview index every run writes into its own <c>staging.duckdb</c>, so <c>pz preview
/// --run-dir</c> needs nothing but that file: no project, no source. <see cref="NodesTable"/> is part of the
/// contract with PipelineX; <see cref="ExcludedTable"/> is pz-internal and only lets a run-mode preview tell
/// "not previewable" (PZ0538) from "unknown node" without the project.</summary>
public static class PreviewIndex
{
    public const string NodesTable = "pz_meta.preview_nodes";

    /// <summary><c>(node VARCHAR, kind VARCHAR)</c>, kind one of <c>check</c>, <c>sink_write</c>,
    /// <c>ephemeral</c>.</summary>
    public const string ExcludedTable = "pz_meta.preview_excluded";

    /// <summary>Indexes the whole compiled DAG, not only the selection: a node outside the selection is
    /// listed and answered by "relation missing", like a node that failed. <paramref name="pipelines"/>
    /// supplies the ephemeral pipelines, which are inlined and have no DAG node.</summary>
    public static async Task WriteAsync(IDuckSession duck, CompiledDag dag, IReadOnlyList<PipelineDef> pipelines,
        CancellationToken ct)
    {
        await PzMeta.EnsureSchemaAsync(duck, ct).ConfigureAwait(false);

        var nodes = new List<string>();
        var excluded = new List<string>();
        foreach (var node in dag.Nodes)
        {
            switch (node.Kind)
            {
                case NodeKind.SourceLoad:
                    nodes.Add(NodeRow(node.Name, ((SourceDatasetDef)node.Definition).Dataset.RedactedColumns));
                    break;
                case NodeKind.Pipeline:
                    nodes.Add(NodeRow(node.Name, ((PipelineDef)node.Definition).RedactedColumns));
                    break;
                case NodeKind.Check:
                    excluded.Add(ExcludedRow(node.Name, "check"));
                    break;
                case NodeKind.SinkWrite:
                    excluded.Add(ExcludedRow(node.Name, "sink_write"));
                    break;
            }
        }

        excluded.AddRange(pipelines
            .Where(p => string.Equals(p.Materialization, "ephemeral", StringComparison.OrdinalIgnoreCase))
            .Select(p => ExcludedRow(p.Name, "ephemeral")));

        await duck.ExecuteAsync(
            $"create or replace table {NodesTable} (node varchar, relation varchar, redacted_columns varchar[])", ct)
            .ConfigureAwait(false);
        await duck.ExecuteAsync($"create or replace table {ExcludedTable} (node varchar, kind varchar)", ct)
            .ConfigureAwait(false);
        if (nodes.Count > 0)
        {
            await duck.ExecuteAsync($"insert into {NodesTable} values {string.Join(", ", nodes)}", ct)
                .ConfigureAwait(false);
        }

        if (excluded.Count > 0)
        {
            await duck.ExecuteAsync($"insert into {ExcludedTable} values {string.Join(", ", excluded)}", ct)
                .ConfigureAwait(false);
        }
    }

    /// <summary>Looks <paramref name="node"/> up by exact (ordinal) name.</summary>
    public static async Task<PreviewLookup> LookupAsync(IDuckSession duck, string node, CancellationToken ct)
    {
        var present = await duck.ScalarAsync<long>(
            "select count(*) from duckdb_tables() where database_name = current_database() " +
            "and schema_name = 'pz_meta' and table_name in ('preview_nodes', 'preview_excluded')", ct)
            .ConfigureAwait(false);
        if (present < 2)
        {
            return new PreviewLookup(null, null, IndexPresent: false);
        }

        var name = PzMeta.Escape(node);
        var target = await duck.ScalarAsync<string>(
            $"select coalesce((select to_json({{'relation': relation, 'marks': redacted_columns}})::varchar " +
            $"from {NodesTable} where node = '{name}' limit 1), '')", ct).ConfigureAwait(false);
        if (target.Length > 0)
        {
            using var json = JsonDocument.Parse(target);
            var relation = json.RootElement.GetProperty("relation").GetString()!;
            var marks = json.RootElement.GetProperty("marks").EnumerateArray().Select(m => m.GetString()!).ToList();
            return new PreviewLookup(new PreviewTarget(node, relation, marks), null, IndexPresent: true);
        }

        var kind = await duck.ScalarAsync<string>(
            $"select coalesce((select kind from {ExcludedTable} where node = '{name}' limit 1), '')", ct)
            .ConfigureAwait(false);
        return new PreviewLookup(null, kind.Length > 0 ? kind : null, IndexPresent: true);
    }

    private static string NodeRow(string node, IReadOnlyList<string> marks)
    {
        var list = new StringBuilder("[");
        list.AppendJoin(", ", marks.Select(m => $"'{PzMeta.Escape(m)}'"));
        list.Append(']');
        return $"('{PzMeta.Escape(node)}', 'staging.{PzMeta.Escape(node)}', {list}::varchar[])";
    }

    private static string ExcludedRow(string node, string kind) => $"('{PzMeta.Escape(node)}', '{kind}')";
}
