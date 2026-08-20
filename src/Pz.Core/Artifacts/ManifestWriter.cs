using System.Buffers;
using System.Text.Json;
using Pz.Core.Dag;
using Pz.Core.Model;

namespace Pz.Core.Artifacts;

/// <summary>
/// Writes the compile-time output of a <see cref="CompiledDag"/> to disk:
/// <c>&lt;targetDir&gt;/manifest.json</c> (byte-stable format) and
/// <c>&lt;targetDir&gt;/compiled/&lt;pipeline&gt;.sql</c> (rendered, post-ephemeral-inline SQL,
/// LF, one trailing newline — plus a generated binding-header comment for INSERT-form pipelines,
/// see <see cref="ResolveInlineBindingHeader"/>) for every Pipeline node.
/// No wall clock, no environment leakage — every byte written is a pure function of
/// <paramref name="dag"/> and <paramref name="project"/>.
/// </summary>
public static class ManifestWriter
{
    /// <summary>Writes every node in <paramref name="dag"/> (no selection filtering applied).</summary>
    public static void Write(CompiledDag dag, PzProject project, string targetDir) =>
        Write(dag, dag.Nodes, project, targetDir);

    /// <summary>
    /// Writes <paramref name="nodesToWrite"/> (typically a selection-filtered subset of
    /// <paramref name="fullDag"/>'s nodes) while resolving cross-node lookups — such as a check
    /// node's owning pipeline in <see cref="ResolveFile"/> — against the FULL dag. This keeps
    /// selection exact (only the selected nodes appear in the manifest / get .sql emitted) without
    /// crashing when a selected node's dependency was filtered out of <paramref name="nodesToWrite"/>.
    /// </summary>
    public static void Write(CompiledDag fullDag, IReadOnlyList<DagNode> nodesToWrite, PzProject project, string targetDir)
    {
        Directory.CreateDirectory(targetDir);
        var compiledDir = Path.Combine(targetDir, "compiled");
        Directory.CreateDirectory(compiledDir);

        foreach (var node in nodesToWrite.Where(n => n.Kind == NodeKind.Pipeline))
        {
            var sql = (node.RenderedSql ?? string.Empty).TrimEnd('\n') + "\n";
            // Header lines are prepended input-note-first then output-binding: `-- incremental:` lines for
            // every SQL-declared incremental dataset this pipeline reads, then `-- output:` binding lines.
            var incrementalHeader = ResolveIncrementalHeader(node, fullDag);
            var bindingHeader = ResolveInlineBindingHeader(node, fullDag);
            sql = string.Concat(incrementalHeader ?? string.Empty, bindingHeader ?? string.Empty, sql);

            File.WriteAllText(Path.Combine(compiledDir, $"{node.Name}.sql"), sql);
        }

        WriteManifest(fullDag, nodesToWrite, project, Path.Combine(targetDir, "manifest.json"));
    }

    private static void WriteManifest(CompiledDag fullDag, IReadOnlyList<DagNode> nodesToWrite, PzProject project, string path)
    {
        var byId = fullDag.Nodes.ToDictionary(n => n.Id);
        var buffer = new ArrayBufferWriter<byte>();
        var options = new JsonWriterOptions { Indented = true, IndentSize = 2, NewLine = "\n" };
        using (var writer = new Utf8JsonWriter(buffer, options))
        {
            writer.WriteStartObject();
            writer.WriteString("project", project.Name);
            writer.WriteString("version", project.Version);
            writer.WriteString("generatedBy", "pz");
            writer.WriteStartArray("nodes");
            foreach (var node in nodesToWrite)
            {
                WriteNode(writer, node, byId);
            }

            writer.WriteEndArray();
            writer.WriteEndObject();
        }

        using var stream = new FileStream(path, FileMode.Create, FileAccess.Write);
        stream.Write(buffer.WrittenSpan);
        stream.WriteByte((byte)'\n');
    }

    private static void WriteNode(Utf8JsonWriter writer, DagNode node, IReadOnlyDictionary<NodeId, DagNode> byId)
    {
        writer.WriteStartObject();
        writer.WriteString("id", node.Id.Value);
        writer.WriteString("kind", KindName(node.Kind));
        writer.WriteString("name", node.Name);

        writer.WriteStartArray("dependsOn");
        foreach (var dependency in node.DependsOn.Select(d => d.Value).OrderBy(v => v, StringComparer.Ordinal))
        {
            writer.WriteStringValue(dependency);
        }

        writer.WriteEndArray();

        writer.WriteStartArray("tags");
        if (node.Definition is PipelineDef pipeline)
        {
            foreach (var tag in pipeline.Tags)
            {
                writer.WriteStringValue(tag);
            }
        }

        writer.WriteEndArray();

        writer.WriteString("file", ResolveFile(node, byId));
        writer.WriteEndObject();
    }

    /// <summary>
    /// An INSERT-form pipeline's compiled artifact FILE (never
    /// <see cref="DagNode.RenderedSql"/> nor the NodeId canonical it's hashed from — both stay
    /// headerless) carries a generated `-- output: &lt;sink&gt;.&lt;output&gt; (&lt;format,&gt; &lt;mode&gt;)`
    /// header line for EVERY output it binds. Found by scanning the full DAG (not just
    /// <paramref name="fullDag"/>'s selection-filtered subset) for every SinkWrite node depending on
    /// this pipeline node whose binding is inline rather than YAML-declared — a pipeline may bind N
    /// distinct sink outputs through the array `INSERT INTO [{{ sink(...) }}, ...]` fan-out form.
    /// Headers are sorted by `&lt;sink&gt;.&lt;output&gt;` ordinal for byte-stable output; a 1:1
    /// pipeline gets exactly one line.
    /// </summary>
    private static string? ResolveInlineBindingHeader(DagNode pipelineNode, CompiledDag fullDag)
    {
        var headers = fullDag.Nodes
            .Where(n => n.Kind == NodeKind.SinkWrite && n.DependsOn.Contains(pipelineNode.Id))
            .Select(n => (SinkOutputDef)n.Definition)
            .Where(d => d.IsInlineBound)
            .OrderBy(d => $"{d.Sink.Name}.{d.Output.Name}", StringComparer.Ordinal)
            .Select(FormatBindingHeaderLine)
            .ToList();

        return headers.Count == 0 ? null : string.Concat(headers);
    }

    /// <summary>
    /// An INSERT-form pipeline that reads a SQL-declared incremental dataset (its cursor
    /// inferred from a watermark() comparison, never YAML) gets one generated
    /// `-- incremental: &lt;source&gt;.&lt;dataset&gt; (cursor &lt;cursor&gt;, declared in SQL)` header line per such
    /// dataset — the compiled-artifact record that this pipeline's read is watermark-filtered, sourced from
    /// the SourceLoad nodes it depends on whose synthesized <see cref="IncrementalDef.DeclaredInSql"/> is set
    /// (PZ0226 guarantees every SQL-declared dataset a pipeline reads is watermark-filtered by it, so a
    /// DependsOn SourceLoad with DeclaredInSql is exactly a dataset this pipeline watermark-reads). Like the
    /// binding header, this scans the FULL dag (SourceLoad deps may be filtered out of the selection subset)
    /// and sorts by `&lt;source&gt;.&lt;dataset&gt;` ordinal for byte-stable output. Neither this line nor the
    /// synthesized IncrementalDef ever feeds a NodeId hash.
    /// </summary>
    private static string? ResolveIncrementalHeader(DagNode pipelineNode, CompiledDag fullDag)
    {
        var byId = fullDag.Nodes.ToDictionary(n => n.Id);
        var headers = pipelineNode.DependsOn
            .Where(byId.ContainsKey)
            .Select(id => byId[id].Definition)
            .OfType<SourceDatasetDef>()
            .Where(d => d.Dataset.SyncMode?.Incremental is { DeclaredInSql: true })
            .OrderBy(d => $"{d.Source.Name}.{d.Dataset.Name}", StringComparer.Ordinal)
            .Select(d => $"-- incremental: {d.Source.Name}.{d.Dataset.Name} " +
                $"(cursor {d.Dataset.SyncMode!.Incremental!.Cursor}, declared in SQL)\n")
            .ToList();

        return headers.Count == 0 ? null : string.Concat(headers);
    }

    private static string FormatBindingHeaderLine(SinkOutputDef binding)
    {
        var format = binding.Output.Options.TryGetValue("format", out var value) && value is string formatText
            ? formatText
            : null;
        var descriptor = format is null ? binding.Output.Mode : $"{format}, {binding.Output.Mode}";
        return $"-- output: {binding.Sink.Name}.{binding.Output.Name} ({descriptor})\n";
    }

    private static string KindName(NodeKind kind) => kind switch
    {
        NodeKind.SourceLoad => "source_load",
        NodeKind.Pipeline => "pipeline",
        NodeKind.Check => "check",
        NodeKind.SinkWrite => "sink_write",
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "unknown node kind"),
    };

    /// <summary>
    /// Every node's "owning file" comes from its <see cref="DagNode.Definition"/> — except
    /// <see cref="Check"/> nodes, whose <c>Definition</c> (<see cref="CheckNodeDef"/>) has no file
    /// of its own (checks are declared inline in a pipeline's sidecar, but the model only tracks
    /// one <c>FilePath</c> per pipeline). A check node always depends on exactly the pipeline
    /// node it was declared on, so its file is that pipeline's <c>FilePath</c>.
    /// </summary>
    private static string ResolveFile(DagNode node, IReadOnlyDictionary<NodeId, DagNode> byId) => node.Definition switch
    {
        SourceDatasetDef sourceDataset => sourceDataset.Source.FilePath,
        PipelineDef pipeline => pipeline.FilePath,
        SinkOutputDef sinkOutput => sinkOutput.Sink.FilePath,
        CheckNodeDef => byId[node.DependsOn[0]].Definition is PipelineDef ownerPipeline
            ? ownerPipeline.FilePath
            : throw new InvalidOperationException($"check node '{node.Name}' does not depend on a pipeline node"),
        _ => throw new InvalidOperationException(
            $"node '{node.Name}' has an unrecognized Definition type '{node.Definition.GetType()}'"),
    };
}
