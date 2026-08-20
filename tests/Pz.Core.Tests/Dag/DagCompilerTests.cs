using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using static Pz.Core.Tests.TestProjects;

namespace Pz.Core.Tests.Dag;

public class DagCompilerTests
{
    [Fact]
    public void Source_read_by_two_pipelines_is_PZ0349()
    {
        var a = Pipe("stg_orders", "select * from {{ source('crm', 'orders') }}");
        var b = Pipe("order_audit", "select * from {{ source('crm', 'orders') }}");
        var project = Project([a, b], sources: [Crm("orders")]);

        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(project, Ctx(project)));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.SourceReadByMultiplePipelines);
        Assert.Contains("crm.orders", error.Message, StringComparison.Ordinal);
        // Deterministic ordering: both readers named, sorted ordinally, whatever the declaration order.
        Assert.True(error.Message.IndexOf("order_audit", StringComparison.Ordinal)
            < error.Message.IndexOf("stg_orders", StringComparison.Ordinal));
    }

    [Fact]
    public void Source_referenced_twice_inside_one_pipeline_is_allowed()
    {
        // The rule counts pipelines, not references -- a self-join is one reader.
        var p = Pipe("self_join",
            "select a.id from {{ source('crm', 'orders') }} a " +
            "join {{ source('crm', 'orders') }} b on a.id = b.id");
        var project = Project([p], sources: [Crm("orders")]);

        var dag = DagCompiler.Compile(project, Ctx(project));

        Assert.Single(dag.Nodes, n => n.Kind == NodeKind.SourceLoad);
    }

    [Fact]
    public void Two_pipelines_reading_different_datasets_of_one_source_is_allowed()
    {
        var a = Pipe("stg_orders", "select * from {{ source('crm', 'orders') }}");
        var b = Pipe("stg_customers", "select * from {{ source('crm', 'customers') }}");
        var project = Project([a, b], sources: [Crm("orders", "customers")]);

        var dag = DagCompiler.Compile(project, Ctx(project));

        Assert.Equal(2, dag.Nodes.Count(n => n.Kind == NodeKind.SourceLoad));
    }

    [Fact]
    public void Unreferenced_dataset_produces_no_node()
    {
        var p = Project(
            [Pipe("stg", "select * from {{ source('crm', 'orders') }}")],
            sources: [Crm("orders", "customers")]);
        var dag = DagCompiler.Compile(p, Ctx(p));
        Assert.Contains(dag.Nodes, n => n.Name == "src_crm__orders");
        Assert.DoesNotContain(dag.Nodes, n => n.Name == "src_crm__customers");
    }

    [Fact]
    public void Cycle_is_error_PZ0202()
    {
        var p = Project([
            Pipe("a", "select * from {{ ref('b') }}"),
            Pipe("b", "select * from {{ ref('a') }}")]);
        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));
        Assert.Contains(ex.Errors, e => e.Code == PzErrorCode.Cycle);
    }

    [Fact]
    public void Unresolved_ref_is_error_PZ0201()
    {
        var p = Project([Pipe("a", "select * from {{ ref('nope') }}")]);
        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));
        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.UnresolvedRef);
        Assert.Contains("nope", error.Message);
    }

    [Fact]
    public void Ephemeral_pipeline_is_inlined_as_cte()
    {
        var p = Project([
            Pipe("eph", "select 1 as x", materialization: "ephemeral"),
            Pipe("consumer", "select * from {{ ref('eph') }}")]);
        var dag = DagCompiler.Compile(p, Ctx(p));
        Assert.DoesNotContain(dag.Nodes, n => n.Name == "eph");
        var consumer = dag.Nodes.Single(n => n.Name == "consumer");
        Assert.Contains("with __pz_cte__eph as (", consumer.RenderedSql);
        Assert.Contains("select * from __pz_cte__eph", consumer.RenderedSql);
        Assert.DoesNotContain("staging.eph", consumer.RenderedSql);
    }

    [Fact]
    public void Checks_on_ephemeral_pipeline_is_error_PZ0205()
    {
        var check = new CheckDef("not_null", ["id"], new Dictionary<string, object?>());
        var p = Project([Pipe("eph", "select 1 as id", materialization: "ephemeral", checks: [check])]);
        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));
        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.ChecksOnEphemeral);
        Assert.Contains("eph", error.Message);
        Assert.DoesNotContain("pipelines/eph.sql", error.Message);
        Assert.Equal("pipelines/eph.sql", error.File);
    }

    [Fact]
    public void Multiple_checks_on_ephemeral_pipelines_all_aggregate()
    {
        var check = new CheckDef("not_null", ["id"], new Dictionary<string, object?>());
        var p = Project([
            Pipe("eph1", "select 1 as id", materialization: "ephemeral", checks: [check]),
            Pipe("eph2", "select 1 as id", materialization: "ephemeral", checks: [check]),
            Pipe("consumer1", "select * from {{ ref('eph1') }}"),
            Pipe("consumer2", "select * from {{ ref('eph2') }}")]);
        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));
        Assert.Equal(2, ex.Errors.Count(e => e.Code == PzErrorCode.ChecksOnEphemeral));
    }

    [Fact]
    public void With_cloned_dag_reflects_new_nodes()
    {
        var p = Project(
            [Pipe("stg", "select * from {{ source('crm', 'orders') }}"),
             Pipe("mart", "select * from {{ ref('stg') }}")],
            sources: [Crm("orders")]);
        var dag = DagCompiler.Compile(p, Ctx(p));
        var source = dag.Nodes.Single(n => n.Name == "src_crm__orders");
        Assert.Contains(dag.Descendants(source.Id), n => n.Name == "mart");

        var withoutMart = dag with { Nodes = dag.Nodes.Where(n => n.Name != "mart").ToList() };
        var descendant = Assert.Single(withoutMart.Descendants(source.Id));
        Assert.Equal("stg", descendant.Name);
    }

    [Fact]
    public void Checks_become_nodes_depending_on_pipeline()
    {
        var check = new CheckDef("not_null", ["id"], new Dictionary<string, object?>());
        var p = Project([Pipe("a", "select 1 as id", checks: [check])]);
        var dag = DagCompiler.Compile(p, Ctx(p));
        var pipeline = dag.Nodes.Single(n => n.Kind == NodeKind.Pipeline);
        var checkNode = dag.Nodes.Single(n => n.Kind == NodeKind.Check);
        Assert.Equal("check_a_not_null_id", checkNode.Name);
        Assert.Contains(pipeline.Id, checkNode.DependsOn);
        // CheckNodeDef wraps the check with its owning pipeline's name for CheckExecutor's benefit --
        // the canonical hash input (kind/pipeline name/check type/columns/options) is unaffected, so
        // this wrapper changes no NodeId (see GoldenCompileTests).
        Assert.Equal(new CheckNodeDef("a", check), checkNode.Definition);
    }

    /// <summary>A per-check `sample_values: false` override wins over
    /// the (default-true) project-wide `engine.check_samples`, resolved once at compile into
    /// CheckNodeDef.SampleValues.</summary>
    [Fact]
    public void Per_check_sample_off_suppresses_samples()
    {
        var check = new CheckDef("not_null", ["id"], new Dictionary<string, object?>(), SampleValues: false);
        var p = Project([Pipe("a", "select 1 as id", checks: [check])]);
        var dag = DagCompiler.Compile(p, Ctx(p));
        var checkNode = dag.Nodes.Single(n => n.Kind == NodeKind.Check);
        Assert.False(((CheckNodeDef)checkNode.Definition).SampleValues);
    }

    /// <summary>Override direction: a project-wide `engine.check_samples:
    /// false` default is overridden back ON by a per-check `sample_values: true` -- the per-check value
    /// always wins, in both directions.</summary>
    [Fact]
    public void Project_default_off_per_check_on_shows_samples()
    {
        var check = new CheckDef("not_null", ["id"], new Dictionary<string, object?>(), SampleValues: true);
        var p = Project([Pipe("a", "select 1 as id", checks: [check])], engine: new EngineConfig(CheckSamples: false));
        var dag = DagCompiler.Compile(p, Ctx(p));
        var checkNode = dag.Nodes.Single(n => n.Kind == NodeKind.Check);
        Assert.True(((CheckNodeDef)checkNode.Definition).SampleValues);
    }

    /// <summary>With no per-check override at all, a project-wide
    /// `engine.check_samples: false` suppresses samples for every check by default.</summary>
    [Fact]
    public void Project_default_off_suppresses_all_by_default()
    {
        var check = new CheckDef("not_null", ["id"], new Dictionary<string, object?>());
        var p = Project([Pipe("a", "select 1 as id", checks: [check])], engine: new EngineConfig(CheckSamples: false));
        var dag = DagCompiler.Compile(p, Ctx(p));
        var checkNode = dag.Nodes.Single(n => n.Kind == NodeKind.Check);
        Assert.False(((CheckNodeDef)checkNode.Definition).SampleValues);
    }

    [Fact]
    public void Node_id_is_stable_across_compiles()
    {
        var p = Project([Pipe("a", "select 1 as x")]);
        var first = DagCompiler.Compile(p, Ctx(p));
        var second = DagCompiler.Compile(p, Ctx(p));
        Assert.Equal(first.Nodes.Select(n => n.Id), second.Nodes.Select(n => n.Id));
    }

    [Fact]
    public void Node_id_changes_when_sql_changes()
    {
        var p1 = Project([Pipe("a", "select 1 as x")]);
        var p2 = Project([Pipe("a", "select 2 as x")]);
        var id1 = DagCompiler.Compile(p1, Ctx(p1)).Nodes.Single().Id;
        var id2 = DagCompiler.Compile(p2, Ctx(p2)).Nodes.Single().Id;
        Assert.NotEqual(id1, id2);
        Assert.Equal(16, id1.Value.Length);
        Assert.Matches("^[0-9a-f]{16}$", id1.Value);
    }

    [Fact]
    public void Topological_order_respects_dependencies()
    {
        var p = Project(
            [Pipe("stg", "select * from {{ source('crm', 'orders') }}"),
             Pipe("mart", "INSERT INTO {{ sink('lake', 'out', strategy: 'replace', format: 'parquet') }} select * from {{ ref('stg') }}")],
            sources: [Crm("orders")],
            sinks: [Sink()]);
        var order = DagCompiler.Compile(p, Ctx(p)).TopologicalOrder().Select(n => n.Name).ToList();
        Assert.True(order.IndexOf("src_crm__orders") < order.IndexOf("stg"));
        Assert.True(order.IndexOf("stg") < order.IndexOf("mart"));
        Assert.True(order.IndexOf("mart") < order.IndexOf("lake.out"));
    }

    // --- Incremental config surface + merge-keys validation ---

    [Fact]
    public void Merge_mode_without_keys_is_error_PZ0209()
    {
        var p = Project(
            [Pipe("a", Into("out", "merge") + "select 1 as x")],
            sinks: [Sink()]);
        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));
        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.MergeRequiresKeys);
        Assert.Contains("lake.out", error.Message);
        Assert.Equal("connections.yml", error.File);
    }

    // PZ0228 ("unknown write mode") has no facts here: write strategy is a sink() keyword argument,
    // and SinkFunction refuses one outside replace/append/merge at the call site -- see
    // SinkFunctionTests.Malformed_call_is_rejected.

    [Fact]
    public void Keys_without_merge_mode_is_error_PZ0211()
    {
        var p = Project(
            [Pipe("a", Into("out", "replace", ["id"]) + "select 1 as x")],
            sinks: [Sink()]);
        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));
        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.KeysWithoutMerge);
        Assert.Contains("lake.out", error.Message);
        Assert.Contains("id", error.Message);
    }

    [Fact]
    public void Cursor_missing_from_contract_is_error_PZ0212()
    {
        var columns = new Dictionary<string, string> { ["id"] = "bigint" };
        var p = Project(
            [Pipe("stg", "select * from {{ source('crm', 'orders') }}")],
            sources: [CrmIncremental("orders", "updated_at", columns)]);
        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));
        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.CursorInvalid);
        Assert.Contains("updated_at", error.Message);
        Assert.Contains("crm.orders", error.Message);
    }

    [Theory]
    [InlineData("double")]
    [InlineData("varchar")]
    [InlineData("boolean")]
    public void Cursor_declared_with_disallowed_type_is_error_PZ0212(string badType)
    {
        var columns = new Dictionary<string, string> { ["updated_at"] = badType };
        var p = Project(
            [Pipe("stg", "select * from {{ source('crm', 'orders') }}")],
            sources: [CrmIncremental("orders", "updated_at", columns)]);
        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));
        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.CursorInvalid);
        Assert.Contains("updated_at", error.Message);
        Assert.Contains("int", error.Message);
        Assert.Contains("bigint", error.Message);
        Assert.Contains("decimal", error.Message);
        Assert.Contains("date", error.Message);
        Assert.Contains("timestamp", error.Message);
    }

    [Theory]
    [InlineData("int")]
    [InlineData("bigint")]
    [InlineData("decimal")]
    [InlineData("date")]
    [InlineData("timestamp")]
    public void Cursor_declared_with_allowed_type_compiles_without_error(string goodType)
    {
        var columns = new Dictionary<string, string> { ["updated_at"] = goodType };
        var p = Project(
            [Pipe("stg", "select * from {{ source('crm', 'orders') }}")],
            sources: [CrmIncremental("orders", "updated_at", columns)]);
        var dag = DagCompiler.Compile(p, Ctx(p));
        Assert.Contains(dag.Nodes, n => n.Name == "src_crm__orders");
    }

    [Fact]
    public void Cursor_without_contract_produces_info_notice_and_no_error()
    {
        var p = Project(
            [Pipe("stg", "select * from {{ source('crm', 'orders') }}")],
            sources: [CrmIncremental("orders", "updated_at")]);
        var notices = new List<string>();
        var dag = DagCompiler.Compile(p, Ctx(p), notices);
        Assert.Contains(dag.Nodes, n => n.Name == "src_crm__orders");
        var notice = Assert.Single(notices);
        Assert.Contains("crm.orders", notice);
        Assert.Contains("updated_at", notice);
        Assert.Contains("unverified until --connect / first run", notice);
    }

    // There is no effectively-once-notice fact for `mode: replace` here: PZ03D
    // (PzErrorCode.IncompatiblePair) hard-refuses an explicitly declared incremental dataset feeding
    // mode: replace (see PairingMatrixTests.Incremental_dataset_feeding_replace_output_is_PZ03D), so the
    // notice is unreachable for one. The notice mechanism itself is exercised for the one shape that
    // remains ambiguous at compile time -- an explicit `sync: {mode: auto}` dataset -- by
    // SyncCompileTests's own effectively-once test.

    [Fact]
    public void Accept_duplicates_consent_suppresses_effectively_once_notice()
    {
        // An operator who already recorded consent via
        // accept_duplicates: true (PZ0214's own offered escape hatch) must not keep getting told,
        // on every compile, to "Use mode: merge" -- that contradicts the consent they just gave.
        var columns = new Dictionary<string, string> { ["updated_at"] = "bigint" };
        var p = Project(
            [Pipe("stg", Into("out", "append", duplicates: "accept")
                + "select * from {{ source('crm', 'orders') }}")],
            sources: [CrmIncremental("orders", "updated_at", columns)],
            sinks: [Sink()]);
        var notices = new List<string>();
        var dag = DagCompiler.Compile(p, Ctx(p), notices);
        Assert.Contains(dag.Nodes, n => n.Name == "src_crm__orders");
        Assert.DoesNotContain(notices, n => n.Contains("effectively-once"));
    }

    [Fact]
    public void Incremental_feeding_merge_sink_emits_no_such_notice()
    {
        var columns = new Dictionary<string, string> { ["updated_at"] = "bigint" };
        var p = Project(
            [Pipe("stg", Into("out", "merge", ["updated_at"]) + "select * from {{ source('crm', 'orders') }}")],
            sources: [CrmIncremental("orders", "updated_at", columns)],
            sinks: [Sink()]);
        var notices = new List<string>();
        var dag = DagCompiler.Compile(p, Ctx(p), notices);
        Assert.Contains(dag.Nodes, n => n.Name == "src_crm__orders");
        Assert.DoesNotContain(notices, n => n.Contains("effectively-once"));
    }

    [Fact]
    public void Multiple_incremental_and_merge_errors_all_aggregate()
    {
        var columns = new Dictionary<string, string> { ["updated_at"] = "varchar" };
        var p = Project(
            [Pipe("stg", Into("out", "merge") + "select * from {{ source('crm', 'orders') }}")],
            sources: [CrmIncremental("orders", "updated_at", columns)],
            sinks: [Sink()]);
        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));
        Assert.Contains(ex.Errors, e => e.Code == PzErrorCode.MergeRequiresKeys);
        Assert.Contains(ex.Errors, e => e.Code == PzErrorCode.CursorInvalid);
        Assert.Equal(2, ex.Errors.Count);
    }

    // --- Bounded-window validation (PZ0213) ---

    /// <summary>A postgres dataset with a declared columns contract, table (or query) mode, and the
    /// given incremental block -- wired into a minimal project via a pipeline referencing
    /// source('pg', 'orders'), so the SourceLoad node is actually built (mirrors the PZ0212 tests'
    /// CrmIncremental shape above).</summary>
    private static PzProject WindowedProject(IncrementalDef incremental,
        IReadOnlyDictionary<string, string>? columns = null, bool queryMode = false)
    {
        var options = new Dictionary<string, object?>
        {
            [queryMode ? "query" : "table"] = queryMode ? "select * from orders" : "orders",
        };
        var dataset = new DatasetDef("orders", options,
            columns ?? new Dictionary<string, string> { ["updated_at"] = "timestamp" },
            new SyncModeDef(SyncMode.Incremental, incremental));
        var source = new ConnectionDef("pg", "postgres", new Dictionary<string, object?>(), [dataset], "sources/pg.yml");
        return Project(
            [Pipe("stg", "select * from {{ source('pg', 'orders') }}")],
            sources: [source]);
    }

    [Fact]
    public void Windowed_dataset_compiles_and_canonicalizes_initial_until()
    {
        var p = WindowedProject(new IncrementalDef("updated_at", "1d", "2020-01-01", "2026-07-01"));
        var dag = DagCompiler.Compile(p, Ctx(p));
        var def = (SourceDatasetDef)dag.Nodes.Single(n => n.Kind == NodeKind.SourceLoad).Definition;
        Assert.Equal("2020-01-01T00:00:00.000000", def.Dataset.SyncMode!.Incremental!.Initial);   // canonicalized
        Assert.Equal("2026-07-01T00:00:00.000000", def.Dataset.SyncMode!.Incremental!.Until);
    }

    [Theory]
    [InlineData("1d", null, null, "initial")]                     // rule 1: window without initial
    [InlineData(null, "2020-01-01", null, "max_window")]          // rule 2: initial without window
    [InlineData(null, null, "2026-07-01", "max_window")]          // rule 2: until without window
    [InlineData("1000", "2020-01-01", null, "duration")]          // rule 4: digits on timestamp cursor
    [InlineData("1d", "someday", null, "canonical")]              // rule 5: unparseable initial
    [InlineData("1d", "2026-07-01", "2020-01-01", "greater")]     // rule 6: until <= initial
    [InlineData("1d", "2020-01-01", "someday", "canonical")]      // rule 5: unparseable until
    public void Invalid_window_config_is_PZ0213(string? window, string? initial, string? until, string messageFragment)
    {
        var p = WindowedProject(new IncrementalDef("updated_at", window, initial, until));
        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));
        var error = Assert.Single(ex.Errors, e => e.Code == "PZ0213");
        Assert.Contains(messageFragment, error.ToString());
    }

    [Fact]
    public void Invalid_window_config_hint_shows_sync_block_not_retired_incremental_surface()
    {
        // The hint must show the unified `sync:` block, not the retired top-level `incremental:` one.
        var p = WindowedProject(new IncrementalDef("updated_at", "1d", null, null));
        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));
        var error = Assert.Single(ex.Errors, e => e.Code == "PZ0213");
        Assert.Contains("sync:\n  mode: incremental\n  cursor: <column>\n  max_window:", error.Hint);
        Assert.DoesNotContain("incremental:\n", error.Hint);
    }

    [Fact]
    public void Windowed_dataset_without_declared_cursor_type_is_PZ0213()
    {
        var p = WindowedProject(new IncrementalDef("updated_at", "1d", "2020-01-01"), columns: new Dictionary<string, string>());
        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));
        Assert.Single(ex.Errors, e => e.Code == "PZ0213");
    }

    /// <summary>The http connector's raw-envelope windowed shape: no columns: contract —
    /// the cursor is typed by the connector's `cursor`/`cursor_type` dataset options — and `query`
    /// is a PARAMS MAP ({{ watermark }}/{{ window_upper }} bindings), not SQL text.</summary>
    private static PzProject RawModeWindowedProject(IncrementalDef incremental,
        Dictionary<string, object?>? optionOverrides = null)
    {
        var options = new Dictionary<string, object?>
        {
            ["path"] = "/repos/x/y/commits",
            ["query"] = new Dictionary<string, object?>
                { ["since"] = "{{ watermark }}", ["until"] = "{{ window_upper }}" },
            ["cursor"] = "committed_at",
            ["cursor_type"] = "timestamp",
        };
        foreach (var (k, v) in optionOverrides ?? [])
        {
            if (v is null) { options.Remove(k); } else { options[k] = v; }
        }

        var dataset = new DatasetDef("commits", options, null, new SyncModeDef(SyncMode.Incremental, incremental));
        var source = new ConnectionDef("gh", "http", new Dictionary<string, object?>(), [dataset], "sources/gh.yml");
        return Project(
            [Pipe("stg", "select * from {{ source('gh', 'commits') }}")],
            sources: [source]);
    }

    [Fact]
    public void Windowed_raw_mode_dataset_with_cursor_type_option_compiles()
    {
        var p = RawModeWindowedProject(new IncrementalDef("committed_at", "7d", "2026-07-01", "2026-07-21"));
        var dag = DagCompiler.Compile(p, Ctx(p));
        var def = (SourceDatasetDef)dag.Nodes.Single(n => n.Kind == NodeKind.SourceLoad).Definition;
        Assert.Equal("2026-07-01T00:00:00.000000", def.Dataset.SyncMode!.Incremental!.Initial);   // canonicalized
    }

    [Fact]
    public void Windowed_raw_mode_dataset_without_cursor_type_is_PZ0213()
    {
        var p = RawModeWindowedProject(new IncrementalDef("committed_at", "7d", "2026-07-01"),
            new() { ["cursor"] = null, ["cursor_type"] = null });
        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));
        Assert.Single(ex.Errors, e => e.Code == "PZ0213");
    }

    [Fact]
    public void Windowed_raw_mode_dataset_with_disallowed_cursor_type_is_PZ0213()
    {
        var p = RawModeWindowedProject(new IncrementalDef("committed_at", "7d", "2026-07-01"),
            new() { ["cursor_type"] = "varchar" });
        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));
        Assert.Single(ex.Errors, e => e.Code == "PZ0213");
    }

    [Fact]
    public void Windowed_query_mode_dataset_is_PZ0213()
    {
        var p = WindowedProject(new IncrementalDef("updated_at", "1d", "2020-01-01"), queryMode: true);
        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));
        var error = Assert.Single(ex.Errors, e => e.Code == "PZ0213");
        Assert.Contains("query", error.ToString());
    }

    [Fact]
    public void Descending_incremental_with_max_pages_is_PZ0229()
    {
        var p = RawModeWindowedProject(new IncrementalDef("committed_at", "7d", "2026-07-01"),
            new() { ["cursor_order"] = "desc", ["max_pages"] = 5L });
        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));
        var error = Assert.Single(ex.Errors, e => e.Code == "PZ0229");
        Assert.Contains("descending", error.ToString());
    }

    [Fact]
    public void Descending_plain_incremental_with_max_pages_is_PZ0229()
    {
        // Not only windowed: plain incremental + desc + cap has the identical loss shape.
        var p = RawModeWindowedProject(new IncrementalDef("committed_at"),
            new() { ["cursor_order"] = "desc", ["max_pages"] = 5L });
        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));
        Assert.Single(ex.Errors, e => e.Code == "PZ0229");
    }

    [Fact]
    public void Descending_incremental_without_max_pages_compiles()
    {
        var p = RawModeWindowedProject(new IncrementalDef("committed_at", "7d", "2026-07-01"),
            new() { ["cursor_order"] = "desc" });
        DagCompiler.Compile(p, Ctx(p)); // complete-or-fail crawls are safe: no exception
    }

    [Fact]
    public void Undeclared_order_with_max_pages_emits_notice_not_error()
    {
        var p = RawModeWindowedProject(new IncrementalDef("committed_at", "7d", "2026-07-01"),
            new() { ["max_pages"] = 5L });
        var notices = new List<string>();
        DagCompiler.Compile(p, Ctx(p), notices);
        Assert.Contains(notices, n => n.Contains("cursor_order"));
    }

    // --- PZ0213 subsumes PZ0212 for windowed datasets (one error per root cause, not two codes for
    // the same misconfiguration). Non-windowed PZ0212 behavior (the Cursor_*_PZ0212 tests above) must
    // stay byte-identical -- see the regression guard below.

    [Fact]
    public void Windowed_dataset_with_disallowed_cursor_type_is_PZ0213_not_PZ0212()
    {
        var columns = new Dictionary<string, string> { ["updated_at"] = "varchar" };
        var p = WindowedProject(new IncrementalDef("updated_at", "1d", "2020-01-01"), columns: columns);
        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));
        Assert.DoesNotContain(ex.Errors, e => e.Code == "PZ0212");
        Assert.Single(ex.Errors, e => e.Code == "PZ0213");
    }

    [Fact]
    public void Windowed_dataset_with_cursor_absent_from_populated_contract_is_PZ0213_not_PZ0212()
    {
        var columns = new Dictionary<string, string> { ["id"] = "bigint" };
        var p = WindowedProject(new IncrementalDef("updated_at", "1d", "2020-01-01"), columns: columns);
        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));
        Assert.DoesNotContain(ex.Errors, e => e.Code == "PZ0212");
        Assert.Single(ex.Errors, e => e.Code == "PZ0213");
    }

    // Regression guard: non-windowed incremental datasets keep PZ0212 -- already covered by
    // Cursor_missing_from_contract_is_error_PZ0212 and Cursor_declared_with_disallowed_type_is_error_PZ0212,
    // both of which use CrmIncremental with no max_window/initial/until, so this scenario isn't
    // duplicated here.

    // --- PZ0214 -- an incremental dataset structurally feeding a mode: append sink output without
    // accept_duplicates: true consent (see https://pipelinez.dev/concepts/delivery-guarantees/).

    [Fact]
    public void Incremental_dataset_feeding_append_sink_without_consent_is_PZ0214()
    {
        var project = Project(
            [Pipe("stg", Into("out1", "append") + "select * from {{ source('crm', 'orders') }}")],
            sources: [CrmIncremental("orders", "id", new Dictionary<string, string> { ["id"] = "bigint" })],
            sinks: [Sink()]);

        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(project, Ctx(project)));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.IncrementalAppendUnacknowledged);
        Assert.Contains("crm.orders", error.Message);
        Assert.Contains("lake.out1", error.Message);
        Assert.Contains("write:\n  strategy: append\n  duplicates: accept", error.Hint!);
    }

    [Fact]
    public void Incremental_dataset_feeding_append_sink_with_consent_compiles()
    {
        var project = Project(
            [Pipe("stg", Into("out1", "append", duplicates: "accept")
                + "select * from {{ source('crm', 'orders') }}")],
            sources: [CrmIncremental("orders", "id", new Dictionary<string, string> { ["id"] = "bigint" })],
            sinks: [Sink()]);

        var dag = DagCompiler.Compile(project, Ctx(project)); // must not throw
        Assert.NotEmpty(dag.Nodes);
    }

    [Fact]
    public void Non_incremental_dataset_feeding_append_sink_compiles_without_consent()
    {
        var project = Project(
            [Pipe("stg", Into("out1", "append") + "select * from {{ source('crm', 'orders') }}")],
            sources: [Crm("orders")],
            sinks: [Sink()]);

        var dag = DagCompiler.Compile(project, Ctx(project)); // must not throw
        Assert.NotEmpty(dag.Nodes);
    }

    [Fact]
    public void Merge_and_replace_sinks_on_incremental_datasets_compile_without_consent()
    {
        var project = Project(
            [Pipe("stg", Into("out1", "merge", ["id"]) + "select * from {{ source('crm', 'orders') }}")],
            sources: [CrmIncremental("orders", "id", new Dictionary<string, string> { ["id"] = "bigint" })],
            sinks: [Sink()]);

        var dag = DagCompiler.Compile(project, Ctx(project)); // must not throw
        Assert.NotEmpty(dag.Nodes);
    }

    [Fact]
    public void Multiple_incremental_to_append_violations_all_aggregate()
    {
        var project = Project(
            [
                Pipe("stg1", Into("out1", "append") + "select * from {{ source('crm', 'orders') }}"),
                Pipe("stg2", Into("out2", "append") + "select * from {{ source('crm', 'shipments') }}"),
            ],
            // One dataset per pipeline: PZ0349 refuses a source read by two pipelines,
            // and this fact is about sink-side errors aggregating, not about source sharing.
            sources: [CrmIncrementalMany("id", new Dictionary<string, string> { ["id"] = "bigint" },
                "orders", "shipments")],
            sinks: [Sink()]);

        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(project, Ctx(project)));

        Assert.Equal(2, ex.Errors.Count(e => e.Code == PzErrorCode.IncrementalAppendUnacknowledged));
        Assert.Contains(ex.Errors, e => e.Message.Contains("lake.out1"));
        Assert.Contains(ex.Errors, e => e.Message.Contains("lake.out2"));
    }

    /// <summary>custom_sql has no columns, so its node name comes from the required
    /// `name` option — readable and collision-free. The canonical-hash input is untouched
    /// (options already include name/sql).</summary>
    [Fact]
    public void Custom_sql_checks_are_named_from_their_name_option()
    {
        var check = new CheckDef("custom_sql", [],
            new Dictionary<string, object?> { ["name"] = "no_negatives", ["sql"] = "select 1" });
        var p = Project([Pipe("a", "select 1 as id", checks: [check])]);
        var dag = DagCompiler.Compile(p, Ctx(p));
        var checkNode = dag.Nodes.Single(n => n.Kind == NodeKind.Check);
        Assert.Equal("check_a_no_negatives", checkNode.Name);
    }

    [Fact]
    public void Custom_sql_node_id_tracks_sql_content()
    {
        DagNode Node(string sql)
        {
            var check = new CheckDef("custom_sql", [],
                new Dictionary<string, object?> { ["name"] = "n", ["sql"] = sql });
            var p = Project([Pipe("a", "select 1 as id", checks: [check])]);
            return DagCompiler.Compile(p, Ctx(p)).Nodes.Single(n => n.Kind == NodeKind.Check);
        }

        Assert.Equal(Node("select 1").Id, Node("select 1").Id);
        Assert.NotEqual(Node("select 1").Id, Node("select 2").Id);
    }

    /// <summary>freshness/accepted_values name their nodes through the normalized
    /// Columns list (`column:` singular lands there in the loader), no compiler special case.</summary>
    [Fact]
    public void Freshness_check_is_named_from_its_column()
    {
        var check = new CheckDef("freshness", ["updated_at"],
            new Dictionary<string, object?> { ["max_age"] = "24h" });
        var p = Project([Pipe("a", "select 1 as id", checks: [check])]);
        var dag = DagCompiler.Compile(p, Ctx(p));
        Assert.Equal("check_a_freshness_updated_at", dag.Nodes.Single(n => n.Kind == NodeKind.Check).Name);
    }
}
