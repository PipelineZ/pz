using Pz.Core.Artifacts;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using static Pz.Core.Tests.TestProjects;

namespace Pz.Core.Tests.Dag;

/// <summary>SQL-declared incremental: DagCompiler wires <see cref="WatermarkInference"/>
/// via the <see cref="ISqlAstReader"/> seam. End-to-end through <see cref="DagCompiler.Compile"/> with a
/// stubbed reader (Pz.Core.Tests never touches DuckDB — the DuckDB reader is exercised in
/// <c>Pz.DuckDb.Tests/DuckDbSqlAstReaderTests.cs</c>). Mirrors <see cref="DagCompilerTemplatingTests"/>'
/// project-fixture style.</summary>
public class DagCompilerWatermarkTests
{
    /// <summary>Canned <see cref="ISqlAstReader"/> that, per sentinel it is asked about, returns one
    /// recognized `&lt;cursor&gt; &gt; &lt;sentinel&gt;` comparison whose resolved base table it derives from the
    /// sentinel itself (<c>__pz_watermark__&lt;source&gt;__&lt;dataset&gt;__</c> → <c>src_&lt;source&gt;__&lt;dataset&gt;</c>),
    /// so <see cref="WatermarkInference"/>'s table-tracing check passes without hard-coding a table name.
    /// With <paramref name="emitViolation"/> it instead reports every sentinel as a shape violation (the
    /// PZ0224 path). The rewrite text is fixed and returned verbatim as the pipeline node's RenderedSql.</summary>
    private sealed class StubSqlAstReader(string cursor = "updated_at",
        string rewritten = "select 1 as x -- null-guarded rewrite", bool emitViolation = false) : ISqlAstReader
    {

        // The read-hints half of the seam is exercised against the real parser in Pz.DuckDb.Tests;
        // pushing nothing keeps these facts about watermark inference alone.
        public ReadHintPlan ExtractReadHints(string sql, string baseTable, string? cursorColumn) =>
            ReadHintPlan.None;
        public WatermarkAnalysis Analyze(string sql, IReadOnlyList<string> sentinels)
        {
            if (emitViolation)
            {
                var violations = sentinels
                    .Select(s => new WatermarkShapeViolation(s, "comparison is an upper bound (col < expr)"))
                    .ToList();
                return new WatermarkAnalysis([], violations, sql);
            }

            var comparisons = sentinels.Select(s =>
            {
                // __pz_watermark__<source>__<dataset>__  ->  src_<source>__<dataset>
                var body = s["__pz_watermark__".Length..^2];
                var split = body.IndexOf("__", StringComparison.Ordinal);
                var source = body[..split];
                var dataset = body[(split + 2)..];
                return new WatermarkComparison(s, cursor, $"src_{source}__{dataset}",
                    Inclusive: false, ValueExprSql: $"'{s}'");
            }).ToList();
            return new WatermarkAnalysis(comparisons, [], rewritten);
        }
    }

    /// <summary>crm.orders with a columns: contract (cursor <c>updated_at</c> as timestamp) and NO YAML
    /// incremental — the cursor is declared purely in SQL via watermark(). A single pipeline reads it with
    /// an inline `INSERT INTO {{ sink('lake', 'orders', strategy: 'replace', format: 'parquet') }}` + `where updated_at > {{ watermark(...) }}`,
    /// draining into one <c>lake.orders</c> output whose mode/keys/accept_duplicates the caller picks.
    /// Defaults to <c>mode: merge</c> (with a synthesized <c>id</c> key) because PZ03D hard-refuses an
    /// incremental dataset (SQL-synthesized ones included) feeding mode: replace, and most callers here
    /// only care about the SQL-declared-incremental wiring, not the write mode.</summary>
    private static PzProject WatermarkProject(string sinkMode = "merge", string[]? keys = null,
        bool acceptDuplicates = false)
    {
        var dataset = new DatasetDef("orders",
            new Dictionary<string, object?> { ["path"] = "orders.csv", ["format"] = "csv" },
            new Dictionary<string, string> { ["updated_at"] = "timestamp", ["id"] = "bigint" });
        var source = new ConnectionDef("crm", "localfiles",
            new Dictionary<string, object?> { ["root"] = "/data" }, [dataset], "connections.yml");
        var pipe = Pipe("orders_curated",
            Into("orders", sinkMode, keys ?? (sinkMode == "merge" ? ["id"] : []),
                duplicates: acceptDuplicates ? "accept" : null) +
            "select * from {{ source('crm', 'orders') }} where updated_at > {{ watermark('crm', 'orders') }}");
        return Project([pipe], sources: [source], sinks: [Sink()]);
    }

    /// <summary>Same shape as <see cref="WatermarkProject"/>, but the caller chooses the dataset's
    /// <c>columns:</c> contract: <c>null</c> for "no contract at all" (the cursor's type is discovered
    /// at run time from the stored watermark), or an explicit map for the
    /// contract-declared cases PZ0227 still governs.</summary>
    private static PzProject WatermarkProjectWithColumns(IReadOnlyDictionary<string, string>? columns)
    {
        var dataset = new DatasetDef("orders",
            new Dictionary<string, object?> { ["path"] = "orders.csv", ["format"] = "csv" }, columns);
        var source = new ConnectionDef("crm", "localfiles",
            new Dictionary<string, object?> { ["root"] = "/data" }, [dataset], "connections.yml");
        var pipe = Pipe("orders_curated",
            Into("orders", "merge", ["id"]) +
            "select * from {{ source('crm', 'orders') }} where updated_at > {{ watermark('crm', 'orders') }}");
        return Project([pipe], sources: [source], sinks: [Sink()]);
    }

    [Fact]
    public void Watermark_without_a_columns_contract_synthesizes_an_untyped_incremental()
    {
        // No columns: at all -> nothing is pruned, so the cursor IS extracted and its type is
        // discoverable at run time from the stored watermark. Compile must not demand a contract.
        var project = WatermarkProjectWithColumns(columns: null);

        var dag = DagCompiler.Compile(project, Ctx(project), null, new StubSqlAstReader());

        var sourceNode = dag.Nodes.Single(n => n.Name == "src_crm__orders");
        var incremental = Assert.IsType<SourceDatasetDef>(sourceNode.Definition).Dataset.SyncMode?.Incremental;
        Assert.NotNull(incremental);
        Assert.True(incremental!.DeclaredInSql);
        Assert.Equal("updated_at", incremental.Cursor);

        var sub = Assert.Single(dag.Nodes.Single(n => n.Name == "orders_curated").WatermarkSubstitutions);
        Assert.Null(sub.CursorType);
    }

    [Fact]
    public void Watermark_cursor_missing_from_a_declared_contract_is_still_PZ0227()
    {
        // A declared contract prunes reads to exactly its columns, so a cursor outside it would never
        // be extracted. That stays a compile-time error.
        var project = WatermarkProjectWithColumns(new Dictionary<string, string> { ["id"] = "bigint" });

        var ex = Assert.Throws<PzValidationException>(() =>
            DagCompiler.Compile(project, Ctx(project), null, new StubSqlAstReader()));

        Assert.Single(ex.Errors, e => e.Code == PzErrorCode.WatermarkCursorUndeclared);
    }

    [Fact]
    public void Watermark_pipeline_synthesizes_incremental_rewrites_sql_and_populates_substitutions()
    {
        var project = WatermarkProject();
        var reader = new StubSqlAstReader(rewritten: "select 1 as x -- null-guarded rewrite");

        var dag = DagCompiler.Compile(project, Ctx(project), null, reader);

        var sourceNode = dag.Nodes.Single(n => n.Name == "src_crm__orders");
        var incremental = Assert.IsType<SourceDatasetDef>(sourceNode.Definition).Dataset.SyncMode?.Incremental;
        Assert.NotNull(incremental);
        Assert.True(incremental!.DeclaredInSql);
        Assert.Equal("updated_at", incremental.Cursor);

        var pipelineNode = dag.Nodes.Single(n => n.Name == "orders_curated");
        Assert.Equal("select 1 as x -- null-guarded rewrite", pipelineNode.RenderedSql);
        var sub = Assert.Single(pipelineNode.WatermarkSubstitutions);
        Assert.Equal("crm", sub.SourceName);
        Assert.Equal("orders", sub.Dataset);
        Assert.Equal("timestamp", sub.CursorType);
    }

    /// <summary>Like <see cref="WatermarkProject"/> but the source() and watermark() live inside an
    /// EPHEMERAL pipeline; a single table-materialized consumer reads it via ref() and drains the sink.
    /// The ephemeral has no node, so its inlined sentinel must be attributed to the consumer.</summary>
    private static PzProject EphemeralWatermarkProject()
    {
        var dataset = new DatasetDef("orders",
            new Dictionary<string, object?> { ["path"] = "orders.csv", ["format"] = "csv" },
            new Dictionary<string, string> { ["updated_at"] = "timestamp", ["id"] = "bigint" });
        var source = new ConnectionDef("crm", "localfiles",
            new Dictionary<string, object?> { ["root"] = "/data" }, [dataset], "connections.yml");
        var ephemeral = Pipe("orders_filtered",
            "select * from {{ source('crm', 'orders') }} where updated_at > {{ watermark('crm', 'orders') }}",
            materialization: "ephemeral");
        var consumer = Pipe("orders_curated",
            Into("orders", "merge", ["id"]) + "select * from {{ ref('orders_filtered') }}");
        return Project([ephemeral, consumer], sources: [source], sinks: [Sink()]);
    }

    [Fact]
    public void Watermark_inside_ephemeral_is_attributed_to_consumer()
    {
        var project = EphemeralWatermarkProject();
        var reader = new StubSqlAstReader(rewritten: "select 1 as x -- ephemeral rewrite");

        var dag = DagCompiler.Compile(project, Ctx(project), null, reader);

        // SourceLoad for the (inherited) dataset gets the SQL-synthesized incremental, with the bound
        // attributed to the CONSUMER pipeline (not the ephemeral, which has no node).
        var sourceNode = dag.Nodes.Single(n => n.Name == "src_crm__orders");
        var incremental = Assert.IsType<SourceDatasetDef>(sourceNode.Definition).Dataset.SyncMode?.Incremental;
        Assert.NotNull(incremental);
        Assert.True(incremental!.DeclaredInSql);
        Assert.Equal("updated_at", incremental.Cursor);
        var bound = Assert.Single(incremental.SqlBounds!);
        Assert.Equal("orders_curated", bound.Pipeline);

        // The consumer node carries the reader's rewrite and a non-empty substitution map.
        var consumerNode = dag.Nodes.Single(n => n.Name == "orders_curated");
        Assert.Equal("select 1 as x -- ephemeral rewrite", consumerNode.RenderedSql);
        Assert.NotEmpty(consumerNode.WatermarkSubstitutions);

        // The ephemeral itself contributes no node.
        Assert.DoesNotContain(dag.Nodes, n => n.Name == "orders_filtered");
    }

    [Fact]
    public void Sentinel_that_analyzer_attributes_to_nothing_fires_PZ0224_guard()
    {
        var project = WatermarkProject();
        // A reader that recognizes nothing AND reports no violation: the sentinel is rendered into the
        // pipeline's assembled SQL but never analyzed/rewritten — the loud belt-and-braces guard fires.
        var reader = new NoOpSqlAstReader();

        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(project, Ctx(project), null, reader));
        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.UnrecognizedWatermarkExpression);
        Assert.Contains("orders_curated", error.Message);
    }

    /// <summary>Reader that recognizes nothing and reports no violation — leaves the sentinel in place
    /// unrewritten. Exercises the DagCompiler loud guard for un-attributed sentinels.</summary>
    private sealed class NoOpSqlAstReader : ISqlAstReader
    {

        // The read-hints half of the seam is exercised against the real parser in Pz.DuckDb.Tests;
        // pushing nothing keeps these facts about watermark inference alone.
        public ReadHintPlan ExtractReadHints(string sql, string baseTable, string? cursorColumn) =>
            ReadHintPlan.None;
        public WatermarkAnalysis Analyze(string sql, IReadOnlyList<string> sentinels) =>
            new([], [], sql);
    }

    [Fact]
    public void Watermark_without_sql_ast_reader_throws_invalid_operation()
    {
        var project = WatermarkProject();

        var ex = Assert.Throws<InvalidOperationException>(() => DagCompiler.Compile(project, Ctx(project)));
        Assert.Equal(
            "watermark() requires an ISqlAstReader — wire DuckDbSqlAstReader (Pz.Cli does this; a test must pass a stub)",
            ex.Message);
    }

    [Fact]
    public void Watermark_shape_violation_aggregates_into_compile_error_PZ0224()
    {
        var project = WatermarkProject();
        var reader = new StubSqlAstReader(emitViolation: true);

        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(project, Ctx(project), null, reader));
        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.UnrecognizedWatermarkExpression);
        Assert.Contains("orders_curated", error.Message);
    }

    [Fact]
    public void Sql_declared_incremental_into_append_without_consent_fires_PZ0214()
    {
        var project = WatermarkProject(sinkMode: "append");
        var reader = new StubSqlAstReader();

        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(project, Ctx(project), null, reader));
        Assert.Single(ex.Errors, e => e.Code == PzErrorCode.IncrementalAppendUnacknowledged);
    }

    [Fact]
    public void Rewritten_pipeline_sql_is_byte_identical_across_two_compiles()
    {
        var project = WatermarkProject();

        var first = DagCompiler.Compile(project, Ctx(project), null, new StubSqlAstReader())
            .Nodes.Single(n => n.Name == "orders_curated");
        var second = DagCompiler.Compile(project, Ctx(project), null, new StubSqlAstReader())
            .Nodes.Single(n => n.Name == "orders_curated");

        Assert.Equal(first.RenderedSql, second.RenderedSql);
        Assert.Equal(first.Id.Value, second.Id.Value);
    }

    [Fact]
    public void Compiled_artifact_carries_sql_declared_incremental_header()
    {
        var project = WatermarkProject();
        var dag = DagCompiler.Compile(project, Ctx(project), null, new StubSqlAstReader());

        var targetDir = Path.Combine(Path.GetTempPath(), "pz-wm-header-" + Guid.NewGuid().ToString("N"));
        try
        {
            ManifestWriter.Write(dag, project, targetDir);
            var compiled = File.ReadAllText(Path.Combine(targetDir, "compiled", "orders_curated.sql"));
            Assert.Contains("-- incremental: crm.orders (cursor updated_at, declared in SQL)", compiled);
        }
        finally
        {
            if (Directory.Exists(targetDir)) { Directory.Delete(targetDir, recursive: true); }
        }
    }
}
