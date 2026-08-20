using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Templating;
using Pz.Core.Validation;

namespace Pz.Core.Tests.Dag;

/// <summary>SQL-declared incremental: <see cref="WatermarkInference.Run"/> folds a
/// stubbed <see cref="ISqlAstReader"/>'s per-pipeline analyses into synthesized <see cref="IncrementalDef"/>s
/// plus the rewrite plumbing (RewrittenSql/Substitutions). The reader is stubbed here -- Pz.Core.Tests never
/// touches DuckDB (that's <c>Pz.DuckDb.Tests/DuckDbSqlAstReaderTests.cs</c>).</summary>
public class WatermarkInferenceTests
{
    /// <summary>Canned <see cref="ISqlAstReader"/> -- ignores the actual SQL/sentinels it's called with
    /// and always returns the analysis it was constructed with.</summary>
    private sealed class StubSqlAstReader(WatermarkAnalysis analysis) : ISqlAstReader
    {

        // The read-hints half of the seam is exercised against the real parser in Pz.DuckDb.Tests;
        // pushing nothing keeps these facts about watermark inference alone.
        public ReadHintPlan ExtractReadHints(string sql, string baseTable, string? cursorColumn) =>
            ReadHintPlan.None;
        public WatermarkAnalysis Analyze(string sql, IReadOnlyList<string> sentinels) => analysis;
    }

    /// <summary>Canned <see cref="ISqlAstReader"/> that returns a different analysis per call, in call
    /// order -- needed when a test drives two distinct pipelines through <see cref="WatermarkInference.Run"/>
    /// (one <see cref="ISqlAstReader.Analyze"/> call per pipeline, given order) and each pipeline must get
    /// its own recognized comparison rather than sharing the single canned <see cref="StubSqlAstReader"/>.</summary>
    private sealed class QueueSqlAstReader(Queue<WatermarkAnalysis> analyses) : ISqlAstReader
    {

        // The read-hints half of the seam is exercised against the real parser in Pz.DuckDb.Tests;
        // pushing nothing keeps these facts about watermark inference alone.
        public ReadHintPlan ExtractReadHints(string sql, string baseTable, string? cursorColumn) =>
            ReadHintPlan.None;
        public WatermarkAnalysis Analyze(string sql, IReadOnlyList<string> sentinels) => analyses.Dequeue();
    }

    private static PipelineDef Pipeline(string name, string filePath = "pipelines/p.sql") =>
        new(name, "-- raw sql --", "table", [], [], filePath);

    private static ConnectionDef CrmSource(IReadOnlyDictionary<string, string>? columns = null) =>
        new("crm", "postgres", new Dictionary<string, object?>(),
            [new DatasetDef("orders", new Dictionary<string, object?>(),
                columns ?? new Dictionary<string, string> { ["updated_at"] = "timestamp", ["id"] = "bigint" })],
            "connections.yml");

    private static ConnectionDef CrmSourceWithIncremental(IncrementalDef incremental,
        IReadOnlyDictionary<string, string>? columns = null) =>
        new("crm", "postgres", new Dictionary<string, object?>(),
            [new DatasetDef("orders", new Dictionary<string, object?>(),
                columns ?? new Dictionary<string, string> { ["updated_at"] = "timestamp", ["id"] = "bigint" },
                new SyncModeDef(SyncMode.Incremental, incremental))],
            "connections.yml");

    /// <summary>A `sync:` dataset (opaque continuation token, no ordered cursor)
    /// -- the XOR bypass this test guards against is a pipeline calling watermark() on exactly this
    /// shape (Sync non-null, Incremental null), which the YAML-level PZ0315 check in DagCompiler cannot
    /// see since it only ever looks at the two YAML blocks, never at pipeline SQL.
    /// This is an explicitly declared `sync: {mode: auto}` block (SyncMode non-null) -- Pz.Core's
    /// SyncStateConflict check only ever sees this explicit shape, never the implicit (SyncMode null)
    /// case, which is a planner-only concern.</summary>
    private static ConnectionDef CrmSourceWithSync(IReadOnlyDictionary<string, string>? columns = null) =>
        new("crm", "postgres", new Dictionary<string, object?>(),
            [new DatasetDef("orders", new Dictionary<string, object?>(),
                columns ?? new Dictionary<string, string> { ["updated_at"] = "timestamp", ["id"] = "bigint" },
                new SyncModeDef(SyncMode.Auto, null))],
            "connections.yml");

    /// <summary>A `sync: {mode: cdc}` dataset is opaque
    /// change-capture state, not an ordered cursor -- same PZ0315 bypass risk as <see cref="CrmSourceWithSync"/>'s
    /// `mode: auto` shape, so it must trip the same SyncStateConflict guard when referenced by a SQL
    /// watermark() declaration.</summary>
    private static ConnectionDef CrmSourceWithCdc(IReadOnlyDictionary<string, string>? columns = null) =>
        new("crm", "postgres", new Dictionary<string, object?>(),
            [new DatasetDef("orders", new Dictionary<string, object?>(),
                columns ?? new Dictionary<string, string> { ["updated_at"] = "timestamp", ["id"] = "bigint" },
                new SyncModeDef(SyncMode.Cdc, null))],
            "connections.yml");

    private static RenderResult Rendered(WatermarkRef wref, bool readsSource = true) =>
        new("select ...", readsSource
                ? new HashSet<DepRef> { new DepRef.Source(wref.SourceName, wref.Dataset) }
                : new HashSet<DepRef>())
        { WatermarkRefs = [wref] };

    [Fact]
    public void Happy_path_synthesizes_incremental_from_one_recognized_comparison()
    {
        var pipeline = Pipeline("p");
        var wref = new WatermarkRef("crm", "orders");
        var rendered = Rendered(wref);
        var comparison = new WatermarkComparison(wref.Sentinel, "updated_at", "src_crm__orders",
            Inclusive: false, ValueExprSql: $"'{wref.Sentinel}'");
        var analysis = new WatermarkAnalysis([comparison], [], "REWRITTEN SQL");
        var reader = new StubSqlAstReader(analysis);
        var sources = new Dictionary<string, ConnectionDef> { ["crm"] = CrmSource() };

        var result = WatermarkInference.Run(
            [new WatermarkInference.PipelineInput(pipeline, rendered, "assembled sql")], sources, reader);

        Assert.Empty(result.Errors);
        Assert.True(result.Synthesized.TryGetValue(("crm", "orders"), out var incremental));
        Assert.Equal("updated_at", incremental!.Cursor);
        Assert.True(incremental.DeclaredInSql);
        var bound = Assert.Single(incremental.SqlBounds!);
        Assert.Equal("p", bound.Pipeline);
        Assert.False(bound.Inclusive);
        Assert.Equal(wref.Sentinel, bound.Sentinel);
        Assert.Equal("REWRITTEN SQL", result.RewrittenSql["p"]);
        var sub = Assert.Single(result.Substitutions["p"]);
        Assert.Equal(wref.Sentinel, sub.Sentinel);
        Assert.Equal("crm", sub.SourceName);
        Assert.Equal("orders", sub.Dataset);
        Assert.Equal("timestamp", sub.CursorType);
    }

    [Fact]
    public void Violation_from_reader_becomes_PZ0224_and_synthesizes_nothing()
    {
        var pipeline = Pipeline("orders_curated", "pipelines/p.sql");
        var wref = new WatermarkRef("crm", "orders");
        var rendered = Rendered(wref);
        var violation = new WatermarkShapeViolation(wref.Sentinel, "comparison is an upper bound (col < expr)");
        var analysis = new WatermarkAnalysis([], [violation], "select ...");
        var reader = new StubSqlAstReader(analysis);
        var sources = new Dictionary<string, ConnectionDef> { ["crm"] = CrmSource() };

        var result = WatermarkInference.Run(
            [new WatermarkInference.PipelineInput(pipeline, rendered, "assembled sql")], sources, reader);

        var error = Assert.Single(result.Errors);
        Assert.Equal(PzErrorCode.UnrecognizedWatermarkExpression, error.Code);
        Assert.Equal("pipelines/p.sql", error.File);
        Assert.Contains("orders_curated", error.Message);
        Assert.Contains(violation.Reason, error.Message);
        Assert.Empty(result.Synthesized);
        Assert.Empty(result.RewrittenSql);
        Assert.Empty(result.Substitutions);
    }

    [Fact]
    public void ColumnTable_mismatch_is_PZ0224_naming_both_tables()
    {
        var pipeline = Pipeline("p", "pipelines/p.sql");
        var wref = new WatermarkRef("crm", "orders");
        var rendered = Rendered(wref);
        // ColumnTable resolves to a ref()'d pipeline's staged table, not this dataset's own src_ table.
        var comparison = new WatermarkComparison(wref.Sentinel, "updated_at", "staging.some_other_pipeline",
            Inclusive: false, ValueExprSql: $"'{wref.Sentinel}'");
        var analysis = new WatermarkAnalysis([comparison], [], "select ...");
        var reader = new StubSqlAstReader(analysis);
        var sources = new Dictionary<string, ConnectionDef> { ["crm"] = CrmSource() };

        var result = WatermarkInference.Run(
            [new WatermarkInference.PipelineInput(pipeline, rendered, "assembled sql")], sources, reader);

        var error = Assert.Single(result.Errors);
        Assert.Equal(PzErrorCode.UnrecognizedWatermarkExpression, error.Code);
        Assert.Contains("does not trace to", error.Message);
        Assert.Empty(result.Synthesized);
    }

    [Fact]
    public void Missing_source_dependency_is_PZ0224_pipeline_does_not_read()
    {
        var pipeline = Pipeline("p", "pipelines/p.sql");
        var wref = new WatermarkRef("crm", "orders");
        var rendered = Rendered(wref, readsSource: false);
        var comparison = new WatermarkComparison(wref.Sentinel, "updated_at", "src_crm__orders",
            Inclusive: false, ValueExprSql: $"'{wref.Sentinel}'");
        var analysis = new WatermarkAnalysis([comparison], [], "select ...");
        var reader = new StubSqlAstReader(analysis);
        var sources = new Dictionary<string, ConnectionDef> { ["crm"] = CrmSource() };

        var result = WatermarkInference.Run(
            [new WatermarkInference.PipelineInput(pipeline, rendered, "assembled sql")], sources, reader);

        var error = Assert.Single(result.Errors);
        Assert.Equal(PzErrorCode.UnrecognizedWatermarkExpression, error.Code);
        Assert.Contains("does not read", error.Message);
        Assert.Empty(result.Synthesized);
    }

    [Fact]
    public void Two_comparisons_same_dataset_in_one_pipeline_fold_into_two_bounds_one_substitution()
    {
        var pipeline = Pipeline("p");
        var wref = new WatermarkRef("crm", "orders");
        var rendered = Rendered(wref);
        var gt = new WatermarkComparison(wref.Sentinel, "updated_at", "src_crm__orders",
            Inclusive: false, ValueExprSql: $"'{wref.Sentinel}'");
        var gte = new WatermarkComparison(wref.Sentinel, "updated_at", "src_crm__orders",
            Inclusive: true, ValueExprSql: $"'{wref.Sentinel}' - interval 2 hour");
        var analysis = new WatermarkAnalysis([gt, gte], [], "REWRITTEN");
        var reader = new StubSqlAstReader(analysis);
        var sources = new Dictionary<string, ConnectionDef> { ["crm"] = CrmSource() };

        var result = WatermarkInference.Run(
            [new WatermarkInference.PipelineInput(pipeline, rendered, "assembled sql")], sources, reader);

        Assert.Empty(result.Errors);
        var incremental = result.Synthesized[("crm", "orders")];
        Assert.Equal(2, incremental.SqlBounds!.Count);
        Assert.All(incremental.SqlBounds!, b => Assert.Equal("p", b.Pipeline));
        Assert.Contains(incremental.SqlBounds!, b => !b.Inclusive && b.ValueExprSql == gt.ValueExprSql);
        Assert.Contains(incremental.SqlBounds!, b => b.Inclusive && b.ValueExprSql == gte.ValueExprSql);
        var sub = Assert.Single(result.Substitutions["p"]);
        Assert.Equal(wref.Sentinel, sub.Sentinel);
    }

    [Fact]
    public void Errors_aggregate_across_two_violating_pipelines()
    {
        var p1 = Pipeline("p1", "pipelines/p1.sql");
        var p2 = Pipeline("p2", "pipelines/p2.sql");
        var wref = new WatermarkRef("crm", "orders");
        var violation = new WatermarkShapeViolation(wref.Sentinel, "some shape violation");
        var analysis = new WatermarkAnalysis([], [violation], "select ...");
        var reader = new StubSqlAstReader(analysis);
        var sources = new Dictionary<string, ConnectionDef> { ["crm"] = CrmSource() };
        var inputs = new[]
        {
            new WatermarkInference.PipelineInput(p1, Rendered(wref), "assembled 1"),
            new WatermarkInference.PipelineInput(p2, Rendered(wref), "assembled 2"),
        };

        var result = WatermarkInference.Run(inputs, sources, reader);

        Assert.Equal(2, result.Errors.Count);
        Assert.All(result.Errors, e => Assert.Equal(PzErrorCode.UnrecognizedWatermarkExpression, e.Code));
        Assert.Equal("pipelines/p1.sql", result.Errors[0].File);
        Assert.Equal("pipelines/p2.sql", result.Errors[1].File);
    }

    [Fact]
    public void Pipelines_with_no_watermark_refs_are_skipped_entirely()
    {
        var pipeline = Pipeline("p");
        var rendered = new RenderResult("select 1", new HashSet<DepRef>());
        var reader = new StubSqlAstReader(new WatermarkAnalysis([], [], "select 1"));
        var sources = new Dictionary<string, ConnectionDef>();

        var result = WatermarkInference.Run(
            [new WatermarkInference.PipelineInput(pipeline, rendered, "assembled sql")], sources, reader);

        Assert.Empty(result.Errors);
        Assert.Empty(result.Synthesized);
        Assert.Empty(result.RewrittenSql);
        Assert.Empty(result.Substitutions);
    }

    [Fact]
    public void Cursor_absent_from_columns_contract_is_PZ0227_and_synthesizes_nothing_for_that_dataset()
    {
        var pipeline = Pipeline("p", "pipelines/p.sql");
        var wref = new WatermarkRef("crm", "orders");
        var rendered = Rendered(wref);
        var comparison = new WatermarkComparison(wref.Sentinel, "updated_at", "src_crm__orders",
            Inclusive: false, ValueExprSql: $"'{wref.Sentinel}'");
        var analysis = new WatermarkAnalysis([comparison], [], "REWRITTEN");
        var reader = new StubSqlAstReader(analysis);
        // Columns contract present but does not declare 'updated_at'.
        var source = CrmSource(new Dictionary<string, string> { ["id"] = "bigint" });
        var sources = new Dictionary<string, ConnectionDef> { ["crm"] = source };

        var result = WatermarkInference.Run(
            [new WatermarkInference.PipelineInput(pipeline, rendered, "assembled sql")], sources, reader);

        var error = Assert.Single(result.Errors);
        Assert.Equal(PzErrorCode.WatermarkCursorUndeclared, error.Code);
        Assert.Equal("connections.yml", error.File);
        Assert.Contains("updated_at", error.Message);
        Assert.Empty(result.Synthesized);
        // The comparison was still structurally recognized, so the rewrite still applies --
        // only the type-dependent synthesis/substitution is withheld.
        Assert.Equal("REWRITTEN", result.RewrittenSql["p"]);
        Assert.False(result.Substitutions.ContainsKey("p"));
    }

    // ---- Cross-pipeline folding rules (PZ0225/PZ0226) + remaining PZ0227 cases ----

    [Fact]
    public void Yaml_incremental_and_sql_declaration_together_is_PZ0225_either_or()
    {
        var pipeline = Pipeline("p", "pipelines/p.sql");
        var wref = new WatermarkRef("crm", "orders");
        var rendered = Rendered(wref);
        var comparison = new WatermarkComparison(wref.Sentinel, "updated_at", "src_crm__orders",
            Inclusive: false, ValueExprSql: $"'{wref.Sentinel}'");
        var analysis = new WatermarkAnalysis([comparison], [], "REWRITTEN");
        var reader = new StubSqlAstReader(analysis);
        var source = CrmSourceWithIncremental(new IncrementalDef("updated_at"));
        var sources = new Dictionary<string, ConnectionDef> { ["crm"] = source };

        var result = WatermarkInference.Run(
            [new WatermarkInference.PipelineInput(pipeline, rendered, "assembled sql")], sources, reader);

        var error = Assert.Single(result.Errors);
        Assert.Equal(PzErrorCode.ConflictingIncrementalDeclaration, error.Code);
        Assert.Equal("connections.yml", error.File);
        Assert.Contains("connections.yml", error.Message);
        Assert.Contains("p", error.Message);
        Assert.NotNull(error.Hint);
        Assert.Contains("pick one route", error.Hint);
        Assert.False(result.Synthesized.ContainsKey(("crm", "orders")));
    }

    [Fact]
    public void Sql_watermark_declaration_targeting_a_sync_dataset_is_refused()
    {
        // Closes the incremental XOR sync bypass: PZ0315 (DagCompiler) refuses a dataset declaring
        // BOTH `incremental:` and `sync:` in YAML, but a `sync:` dataset referenced by watermark() in
        // pipeline SQL never touches YAML `incremental:` at all -- it would otherwise compile clean and
        // become both kinds at execution (PriorSyncState AND SQL watermark bounds both stamped).
        var pipeline = Pipeline("p", "pipelines/p.sql");
        var wref = new WatermarkRef("crm", "orders");
        var rendered = Rendered(wref);
        var comparison = new WatermarkComparison(wref.Sentinel, "updated_at", "src_crm__orders",
            Inclusive: false, ValueExprSql: $"'{wref.Sentinel}'");
        var analysis = new WatermarkAnalysis([comparison], [], "REWRITTEN");
        var reader = new StubSqlAstReader(analysis);
        var source = CrmSourceWithSync();
        var sources = new Dictionary<string, ConnectionDef> { ["crm"] = source };

        var result = WatermarkInference.Run(
            [new WatermarkInference.PipelineInput(pipeline, rendered, "assembled sql")], sources, reader);

        var error = Assert.Single(result.Errors);
        Assert.Equal(PzErrorCode.SyncStateConflict, error.Code);
        Assert.Equal("connections.yml", error.File);
        Assert.Contains("crm", error.Message);
        Assert.Contains("orders", error.Message);
        Assert.Contains("p", error.Message);
        Assert.NotNull(error.Hint);
        Assert.False(result.Synthesized.ContainsKey(("crm", "orders")));
    }

    // Same bypass as the `mode: auto` case above, for a declared `sync: {mode: cdc}` dataset.
    [Fact]
    public void Sql_watermark_declaration_targeting_a_cdc_dataset_is_refused()
    {
        var pipeline = Pipeline("p", "pipelines/p.sql");
        var wref = new WatermarkRef("crm", "orders");
        var rendered = Rendered(wref);
        var comparison = new WatermarkComparison(wref.Sentinel, "updated_at", "src_crm__orders",
            Inclusive: false, ValueExprSql: $"'{wref.Sentinel}'");
        var analysis = new WatermarkAnalysis([comparison], [], "REWRITTEN");
        var reader = new StubSqlAstReader(analysis);
        var source = CrmSourceWithCdc();
        var sources = new Dictionary<string, ConnectionDef> { ["crm"] = source };

        var result = WatermarkInference.Run(
            [new WatermarkInference.PipelineInput(pipeline, rendered, "assembled sql")], sources, reader);

        var error = Assert.Single(result.Errors);
        Assert.Equal(PzErrorCode.SyncStateConflict, error.Code);
        Assert.Equal("connections.yml", error.File);
        Assert.Contains("crm", error.Message);
        Assert.Contains("orders", error.Message);
        Assert.Contains("p", error.Message);
        Assert.NotNull(error.Hint);
        Assert.False(result.Synthesized.ContainsKey(("crm", "orders")));
    }

    [Fact]
    public void Yaml_windowed_dataset_with_sql_watermark_is_PZ0225_windowed_message_takes_precedence()
    {
        var pipeline = Pipeline("p", "pipelines/p.sql");
        var wref = new WatermarkRef("crm", "orders");
        var rendered = Rendered(wref);
        var comparison = new WatermarkComparison(wref.Sentinel, "updated_at", "src_crm__orders",
            Inclusive: false, ValueExprSql: $"'{wref.Sentinel}'");
        var analysis = new WatermarkAnalysis([comparison], [], "REWRITTEN");
        var reader = new StubSqlAstReader(analysis);
        // Windowed: MaxWindow non-null (implies YAML Incremental is also non-null, so rule 1's either/or
        // would ALSO apply -- the windowed message must win, and there must be exactly one PZ0225).
        var source = CrmSourceWithIncremental(new IncrementalDef("updated_at", MaxWindow: "P7D", Initial: "2020-01-01"));
        var sources = new Dictionary<string, ConnectionDef> { ["crm"] = source };

        var result = WatermarkInference.Run(
            [new WatermarkInference.PipelineInput(pipeline, rendered, "assembled sql")], sources, reader);

        var error = Assert.Single(result.Errors);
        Assert.Equal(PzErrorCode.ConflictingIncrementalDeclaration, error.Code);
        Assert.Contains("windowed backfill is YAML-only", error.Message);
        Assert.False(result.Synthesized.ContainsKey(("crm", "orders")));
    }

    [Fact]
    public void Cursor_with_disallowed_type_is_PZ0227_listing_allowed_types()
    {
        var pipeline = Pipeline("p", "pipelines/p.sql");
        var wref = new WatermarkRef("crm", "orders");
        var rendered = Rendered(wref);
        var comparison = new WatermarkComparison(wref.Sentinel, "updated_at", "src_crm__orders",
            Inclusive: false, ValueExprSql: $"'{wref.Sentinel}'");
        var analysis = new WatermarkAnalysis([comparison], [], "REWRITTEN");
        var reader = new StubSqlAstReader(analysis);
        var source = CrmSource(new Dictionary<string, string> { ["updated_at"] = "varchar", ["id"] = "bigint" });
        var sources = new Dictionary<string, ConnectionDef> { ["crm"] = source };

        var result = WatermarkInference.Run(
            [new WatermarkInference.PipelineInput(pipeline, rendered, "assembled sql")], sources, reader);

        var error = Assert.Single(result.Errors);
        Assert.Equal(PzErrorCode.WatermarkCursorUndeclared, error.Code);
        Assert.Equal("connections.yml", error.File);
        Assert.Contains("varchar", error.Message);
        Assert.Contains("int, bigint, decimal, date, timestamp", error.Message);
        Assert.Empty(result.Synthesized);
        // Structurally recognized, so the rewrite still applies -- only synthesis/substitution withheld.
        Assert.Equal("REWRITTEN", result.RewrittenSql["p"]);
        Assert.False(result.Substitutions.ContainsKey("p"));
    }

    [Fact]
    public void Cursor_with_mixed_case_type_is_PZ0227_matching_YAML_route_exactness()
    {
        // The cursor-type check must be Ordinal (exact-lowercase), the same as the YAML route's
        // stage-0 validation (DagCompiler Array.IndexOf(AllowedCursorTypes, ...)), CursorLiterals,
        // bound evaluation, WindowMath and PipelineExecutor — all exact-lowercase. A mixed-case
        // 'TIMESTAMP' that slipped through here would compile on the SQL route then die at runtime.
        var pipeline = Pipeline("p", "pipelines/p.sql");
        var wref = new WatermarkRef("crm", "orders");
        var rendered = Rendered(wref);
        var comparison = new WatermarkComparison(wref.Sentinel, "updated_at", "src_crm__orders",
            Inclusive: false, ValueExprSql: $"'{wref.Sentinel}'");
        var analysis = new WatermarkAnalysis([comparison], [], "REWRITTEN");
        var reader = new StubSqlAstReader(analysis);
        var source = CrmSource(new Dictionary<string, string> { ["updated_at"] = "TIMESTAMP", ["id"] = "bigint" });
        var sources = new Dictionary<string, ConnectionDef> { ["crm"] = source };

        var result = WatermarkInference.Run(
            [new WatermarkInference.PipelineInput(pipeline, rendered, "assembled sql")], sources, reader);

        var error = Assert.Single(result.Errors);
        Assert.Equal(PzErrorCode.WatermarkCursorUndeclared, error.Code);
        Assert.Contains("TIMESTAMP", error.Message);
        Assert.Contains("int, bigint, decimal, date, timestamp", error.Message);
        Assert.Empty(result.Synthesized);
    }

    [Fact]
    public void Two_violated_rules_across_a_project_aggregate_into_one_Result()
    {
        // Dataset crm.orders: YAML incremental AND SQL watermark() -> PZ0225 (either/or).
        var ordersPipeline = Pipeline("orders_pipeline", "pipelines/orders_pipeline.sql");
        var ordersWref = new WatermarkRef("crm", "orders");
        var ordersRendered = Rendered(ordersWref);
        var ordersComparison = new WatermarkComparison(ordersWref.Sentinel, "updated_at", "src_crm__orders",
            Inclusive: false, ValueExprSql: $"'{ordersWref.Sentinel}'");

        // There is no PZ0226 leg (one pipeline declaring, another consuming without a comparison): it
        // would need two pipelines reading one dataset, which PZ0349 refuses. The two legs below still
        // prove the binding rule this fact exists for: errors AGGREGATE across datasets, never fail-fast.

        // Dataset erp.invoices: cursor typed outside the allowed set -> PZ0227 (mistyped).
        var invoicesPipeline = Pipeline("invoices_pipeline", "pipelines/invoices_pipeline.sql");
        var invoicesWref = new WatermarkRef("erp", "invoices");
        var invoicesRendered = Rendered(invoicesWref);
        var invoicesComparison = new WatermarkComparison(invoicesWref.Sentinel, "updated_at",
            "src_erp__invoices", Inclusive: false, ValueExprSql: $"'{invoicesWref.Sentinel}'");

        var reader = new QueueSqlAstReader(new Queue<WatermarkAnalysis>(
        [
            new WatermarkAnalysis([ordersComparison], [], "REWRITTEN ORDERS"),
            new WatermarkAnalysis([invoicesComparison], [], "REWRITTEN INVOICES"),
        ]));

        var crmSource = new ConnectionDef("crm", "postgres", new Dictionary<string, object?>(),
            [
                new DatasetDef("orders", new Dictionary<string, object?>(),
                    new Dictionary<string, string> { ["updated_at"] = "timestamp", ["id"] = "bigint" },
                    new SyncModeDef(SyncMode.Incremental, new IncrementalDef("updated_at"))),
            ],
            "connections.yml");
        var erpSource = new ConnectionDef("erp", "postgres", new Dictionary<string, object?>(),
            [
                new DatasetDef("invoices", new Dictionary<string, object?>(),
                    new Dictionary<string, string> { ["updated_at"] = "varchar", ["id"] = "bigint" }),
            ],
            "sources/erp.yml");
        var sources = new Dictionary<string, ConnectionDef> { ["crm"] = crmSource, ["erp"] = erpSource };

        var inputs = new[]
        {
            new WatermarkInference.PipelineInput(ordersPipeline, ordersRendered, "assembled orders"),
            new WatermarkInference.PipelineInput(invoicesPipeline, invoicesRendered, "assembled invoices"),
        };

        var result = WatermarkInference.Run(inputs, sources, reader);

        Assert.Equal(2, result.Errors.Count);
        Assert.Contains(result.Errors, e => e.Code == PzErrorCode.ConflictingIncrementalDeclaration);
        Assert.Contains(result.Errors, e => e.Code == PzErrorCode.WatermarkCursorUndeclared);
    }

    // -- Bound direction ------------------------------------------------------------------------

    [Fact]
    public void A_floor_and_a_ceiling_both_land_in_the_synthesized_bounds()
    {
        var wref = new WatermarkRef("crm", "orders");
        var analysis = new WatermarkAnalysis(
            [
                new WatermarkComparison(wref.Sentinel, "updated_at", "src_crm__orders",
                    Inclusive: false, ValueExprSql: $"'{wref.Sentinel}'", IsUpper: false),
                new WatermarkComparison(wref.Sentinel, "updated_at", "src_crm__orders",
                    Inclusive: true, ValueExprSql: $"'{wref.Sentinel}' + interval 7 day", IsUpper: true),
            ],
            [], "select ...");
        var sources = new Dictionary<string, ConnectionDef> { ["crm"] = CrmSource() };

        var result = WatermarkInference.Run(
            [new WatermarkInference.PipelineInput(Pipeline("stg"), Rendered(wref), "assembled sql")],
            sources, new StubSqlAstReader(analysis));

        Assert.Empty(result.Errors);
        var bounds = result.Synthesized[("crm", "orders")].SqlBounds!;
        Assert.Equal(2, bounds.Count);
        Assert.Single(bounds, b => !b.IsUpper);
        var upper = Assert.Single(bounds, b => b.IsUpper);
        Assert.True(upper.Inclusive);
        Assert.Contains("interval", upper.ValueExprSql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void A_ceiling_with_no_floor_is_PZ0351()
    {
        // A ceiling alone is a filter, not an increment: the first run would advance the watermark to the
        // ceiling and every run after it would extract nothing.
        var wref = new WatermarkRef("crm", "orders");
        var analysis = new WatermarkAnalysis(
            [
                new WatermarkComparison(wref.Sentinel, "updated_at", "src_crm__orders",
                    Inclusive: true, ValueExprSql: $"'{wref.Sentinel}'", IsUpper: true),
            ],
            [], "select ...");
        var sources = new Dictionary<string, ConnectionDef> { ["crm"] = CrmSource() };

        var result = WatermarkInference.Run(
            [new WatermarkInference.PipelineInput(Pipeline("stg"), Rendered(wref), "assembled sql")],
            sources, new StubSqlAstReader(analysis));

        var error = Assert.Single(result.Errors, e => e.Code == PzErrorCode.WatermarkCeilingWithoutFloor);
        Assert.Contains("crm.orders", error.Message, StringComparison.Ordinal);
        Assert.NotNull(error.Hint);
        Assert.Contains("watermark", error.Hint, StringComparison.OrdinalIgnoreCase);

        // Consistency guard, matching every other per-dataset refusal in this pass: no incremental is
        // synthesized for a dataset that failed.
        Assert.False(result.Synthesized.ContainsKey(("crm", "orders")));
    }
}
