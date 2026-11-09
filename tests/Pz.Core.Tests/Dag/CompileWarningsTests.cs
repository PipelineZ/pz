using Pz.Core.Dag;
using Pz.Core.Templating;
using Pz.Core.Validation;
using static Pz.Core.Tests.TestProjects;

namespace Pz.Core.Tests.Dag;

public class CompileWarningsTests
{
    [Fact]
    public void Clean_project_has_no_warnings()
    {
        var p = Project(
            [Pipe("totals", "INSERT INTO {{ sink('lake', 'totals', strategy: 'replace', format: 'parquet') }} select 1 as x")],
            sinks: [Sink()]);
        var dag = DagCompiler.Compile(p, Ctx(p));
        Assert.Empty(dag.Warnings);
    }

    [Fact]
    public void Load_time_warnings_on_the_project_surface_in_the_compiled_dag()
    {
        // ConnectionsLoader's "${VAR} inside entities: is never interpolated" warning (and any other
        // load-time finding) is on PzProject, not CompiledDag -- this pins that DagCompiler merges it
        // into the one channel every caller (plan/run/validate) already renders warnings through,
        // rather than that finding being silently dropped between loading and compiling.
        var loadTimeWarning = new PzWarning(PzErrorCode.EnvRefNotInterpolatedInEntity,
            "connections.yml: something", "connections.yml", null, "fix it");
        var p = Project(
            [Pipe("totals", "INSERT INTO {{ sink('lake', 'totals', strategy: 'replace', format: 'parquet') }} select 1 as x")],
            sinks: [Sink()]) with
        {
            Warnings = [loadTimeWarning],
        };

        var dag = DagCompiler.Compile(p, Ctx(p));

        Assert.Contains(dag.Warnings, w => w.Code == PzErrorCode.EnvRefNotInterpolatedInEntity);
    }

    [Fact]
    public void A_sink_no_pipeline_writes_to_produces_no_node_and_no_warning()
    {
        // An output exists precisely because a sink() call site declared it, so one can no
        // longer be declared without a writer. A sink nothing writes to is just an unused connection.
        var p = Project(
            [Pipe("a", "INSERT INTO {{ sink('lake', 'used', strategy: 'replace', format: 'parquet') }} select 1 as x")],
            sinks: [Sink(), Sink("unused")]);
        var dag = DagCompiler.Compile(p, Ctx(p)); // must NOT throw
        Assert.DoesNotContain(dag.Warnings, x => x.Code == PzErrorCode.SinkOutputUnbound);
        Assert.Single(dag.Nodes, n => n.Kind == NodeKind.SinkWrite);
        Assert.Equal("lake.used", Assert.Single(dag.Nodes, n => n.Kind == NodeKind.SinkWrite).Name);
    }

    [Theory]
    [InlineData("strategyy")]
    [InlineData("stratgy")]
    [InlineData("Strategy")]
    [InlineData("keyz")]
    public void A_kwarg_one_edit_from_a_pz_key_is_named_not_silently_sent_to_the_connector(string typo)
    {
        // pz cannot REFUSE an unrecognized kwarg -- no connector publishes a write-option vocabulary,
        // so `keyz` may genuinely be one. But a typo of `strategy` silently defaulting the write to
        // append is the failure this surface can least afford, so a near miss is said out loud.
        var p = Project(
            [Pipe("a", $"INSERT INTO {{{{ sink('lake', 'out', {typo}: 'merge') }}}} select 1 as x")],
            sinks: [Sink()]);

        var dag = DagCompiler.Compile(p, Ctx(p)); // a warning, never an error

        var warning = Assert.Single(dag.Warnings, w => w.Code == PzErrorCode.InvalidSinkCall);
        Assert.Contains(typo, warning.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("format")]
    [InlineData("path")]
    [InlineData("bucket")]
    [InlineData("compression")]
    public void A_genuine_connector_option_draws_no_near_miss_warning(string option)
    {
        var p = Project(
            [Pipe("a", $"INSERT INTO {{{{ sink('lake', 'out', {option}: 'v') }}}} select 1 as x")],
            sinks: [Sink()]);

        var dag = DagCompiler.Compile(p, Ctx(p));

        Assert.DoesNotContain(dag.Warnings, w => w.Code == PzErrorCode.InvalidSinkCall);
    }

    // The read-side twin of the two facts above -- SourceFunction.NearMissKwarg existed but was never
    // wired into DagCompiler, so `source(..., synk: {...})` used to ride along as a connector option
    // with no warning at all.
    [Theory]
    [InlineData("colums")]
    [InlineData("Sync")]
    [InlineData("retri")]
    public void A_source_kwarg_one_edit_from_a_pz_key_is_named_not_silently_sent_to_the_connector(string typo)
    {
        // `crm` declares no datasets in YAML -- the call site's kwargs are the whole story, so this
        // never trips PZ0341 (declared in both places).
        var p = Project(
            [Pipe("a", $"select * from {{{{ source('crm', 'orders', {typo}: 'x') }}}}")],
            sinks: [Sink("crm")]);

        var dag = DagCompiler.Compile(p, Ctx(p)); // a warning, never an error

        var warning = Assert.Single(dag.Warnings, w => w.Code == PzErrorCode.UnresolvedRef);
        Assert.Contains(typo, warning.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("partitions")] // a genuine, pz-documented source() kwarg -- not a near miss of anything
    [InlineData("connect_timeout")]
    public void A_genuine_source_connector_option_draws_no_near_miss_warning(string option)
    {
        var p = Project(
            [Pipe("a", $"select * from {{{{ source('crm', 'orders', {option}: 4) }}}}")],
            sinks: [Sink("crm")]);

        var dag = DagCompiler.Compile(p, Ctx(p));

        Assert.DoesNotContain(dag.Warnings, w => w.Code == PzErrorCode.UnresolvedRef);
    }

    [Fact]
    public void Dead_leaf_pipeline_is_PZ0223_warning()
    {
        var p = Project(
            [Pipe("used", "INSERT INTO {{ sink('lake', 'out', strategy: 'replace', format: 'parquet') }} select 1 as x from {{ ref('lonely') }}"),
             Pipe("lonely", "select 1 as x"),          // consumed by 'used' via ref → NOT dead
             Pipe("dangling", "select 1 as x")],        // no sink, no ref consumer → dead leaf
            sinks: [Sink()]);
        var dag = DagCompiler.Compile(p, Ctx(p));
        var w = Assert.Single(dag.Warnings, x => x.Code == PzErrorCode.DeadLeafPipeline);
        Assert.Contains("dangling", w.Message);
    }

    [Fact]
    public void Intermediate_with_no_sink_but_a_ref_consumer_is_silent()
    {
        var p = Project(
            [Pipe("stg", "select 1 as x"),
             Pipe("top", "INSERT INTO {{ sink('lake', 'out', strategy: 'replace', format: 'parquet') }} select * from {{ ref('stg') }}")],
            sinks: [Sink()]);
        var dag = DagCompiler.Compile(p, Ctx(p));
        Assert.Empty(dag.Warnings);
    }

    // -- run_id / run_started_at rendered into SQL (PZ0232) --------------------------------------

    [Theory]
    [InlineData("run_id")]
    [InlineData("run_started_at")]
    public void Pipeline_rendering_run_identity_into_its_sql_is_PZ0232_warning(string constant)
    {
        var p = Project([Pipe("a", $"select '{{{{ {constant} }}}}' as marker")]);
        var dag = DagCompiler.Compile(p, Ctx(p));
        var w = Assert.Single(dag.Warnings, x => x.Code == PzErrorCode.RunIdentityInRenderedSql);
        Assert.Contains("'a'", w.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Pipeline_referencing_run_id_twice_still_gets_exactly_one_warning()
    {
        var p = Project([Pipe("a", "select '{{ run_id }}' as x, '{{ run_id }}' as y")]);
        var dag = DagCompiler.Compile(p, Ctx(p));
        Assert.Single(dag.Warnings, x => x.Code == PzErrorCode.RunIdentityInRenderedSql);
    }

    [Fact]
    public void Node_id_is_unaffected_by_the_run_identity_warning_itself()
    {
        // The warning is advisory only -- DagCompiler must not special-case a run_id-using pipeline's
        // NodeId computation. It still varies run to run (that IS the reported problem), but the hash
        // stays the ordinary pure function of the rendered SQL text, no different from any other
        // pipeline's -- pinned by rendering the SAME run_id twice and getting the SAME id back.
        var p = Project([Pipe("a", "select '{{ run_id }}' as x")]);
        var ctx1 = new RenderContext(p, "run-fixed", new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var id1 = DagCompiler.Compile(p, ctx1).Nodes.Single().Id;
        var id2 = DagCompiler.Compile(p, ctx1).Nodes.Single().Id;
        Assert.Equal(id1, id2);
    }

    [Fact]
    public void Plain_var_and_env_do_not_draw_the_run_identity_warning()
    {
        // env()/var() are documented, intentional interpolation -- only the two RUN-scoped constants
        // (which change every run, unlike a var()/env() value) draw PZ0232.
        var p = Project([Pipe("a", "select '{{ var('min_amount') }}' as x, '{{ env('DATA_DIR') }}' as y")]);
        var ctx = Ctx(p) with { Env = new Dictionary<string, string> { ["DATA_DIR"] = "/tmp/pz-data" } };
        var dag = DagCompiler.Compile(p, ctx);
        Assert.DoesNotContain(dag.Warnings, x => x.Code == PzErrorCode.RunIdentityInRenderedSql);
    }

    /// <summary>An http dataset (or <paramref name="connector"/>) with a `watermark_format`, incremental on
    /// `updated_at` unless <paramref name="incremental"/> is false, feeding one output.</summary>
    private static Pz.Core.Model.PzProject CoarseFormat(string format, string strategy, string connector = "http",
        bool incremental = true, string cursorType = "timestamp", bool contract = false)
    {
        var options = new Dictionary<string, object?> { ["path"] = "/events", ["watermark_format"] = format };
        if (!contract)
        {
            options["cursor"] = "updated_at";
            options["cursor_type"] = cursorType;
        }

        var dataset = new Pz.Core.Model.DatasetDef("events", options,
            contract ? new Dictionary<string, string> { ["id"] = "bigint", ["updated_at"] = cursorType } : null,
            incremental
                ? new Pz.Core.Model.SyncModeDef(Pz.Core.Model.SyncMode.Incremental, new Pz.Core.Model.IncrementalDef("updated_at"))
                : null);
        return Project(
            [Pipe("events", Into("out", strategy: strategy, keys: strategy == "merge" ? ["id"] : null,
                                 duplicates: strategy == "append" && incremental ? "accept" : null) +
                            "select 1 as id from {{ source('api', 'events') }}")],
            sources: [new Pz.Core.Model.ConnectionDef("api", connector,
                new Dictionary<string, object?> { ["base_url"] = "https://api.example.com/" }, [dataset],
                "connections.yml")],
            sinks: [Sink()]);
    }

    [Theory]
    [InlineData("date")]
    [InlineData("yyyy-MM")]
    [InlineData("yyyy")]
    [InlineData("yyyyMMdd")]
    [InlineData("dd/MM/yyyy")]
    public void A_coarse_watermark_format_feeding_an_append_sink_is_a_PZ0364_warning(string format)
    {
        var p = CoarseFormat(format, "append");
        var w = Assert.Single(DagCompiler.Compile(p, Ctx(p)).Warnings, x => x.Code == PzErrorCode.ConnectorConfigWarning);
        Assert.Contains("api.events", w.Message, StringComparison.Ordinal);
        Assert.Contains($"watermark_format: {format}", w.Message, StringComparison.Ordinal);
        Assert.Contains("lake.out", w.Message, StringComparison.Ordinal);
        Assert.Contains("re-read", w.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_contract_timestamp_cursor_warns_too()
    {
        var p = CoarseFormat("date", "append", contract: true);
        Assert.Single(DagCompiler.Compile(p, Ctx(p)).Warnings, x => x.Code == PzErrorCode.ConnectorConfigWarning);
    }

    [Theory]
    [InlineData("epoch_s")]
    [InlineData("iso8601")]
    [InlineData("yyyy-MM-dd HH")]
    [InlineData("yyyy-MM-dd'T'HH:mm")]
    public void Fine_formats_are_silent(string format)
    {
        var p = CoarseFormat(format, "append");
        Assert.DoesNotContain(DagCompiler.Compile(p, Ctx(p)).Warnings, x => x.Code == PzErrorCode.ConnectorConfigWarning);
    }

    [Fact]
    public void A_merge_sink_is_silent()
    {
        var p = CoarseFormat("date", "merge");
        Assert.DoesNotContain(DagCompiler.Compile(p, Ctx(p)).Warnings, x => x.Code == PzErrorCode.ConnectorConfigWarning);
    }

    // A date cursor already renders as a date: the format adds no re-read.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void A_date_cursor_is_silent(bool contract)
    {
        var p = CoarseFormat("date", "append", cursorType: "date", contract: contract);
        Assert.DoesNotContain(DagCompiler.Compile(p, Ctx(p)).Warnings, x => x.Code == PzErrorCode.ConnectorConfigWarning);
    }

    // The option and its meaning are http's; another connector's same-named option is not ours to judge.
    [Fact]
    public void Another_connector_is_silent()
    {
        var p = CoarseFormat("date", "append", connector: "acme");
        Assert.DoesNotContain(DagCompiler.Compile(p, Ctx(p)).Warnings, x => x.Code == PzErrorCode.ConnectorConfigWarning);
    }

    // Without an incremental read nothing is floored per run; the format renders no watermark.
    [Fact]
    public void A_non_incremental_dataset_is_silent()
    {
        var p = CoarseFormat("date", "append", incremental: false);
        Assert.DoesNotContain(DagCompiler.Compile(p, Ctx(p)).Warnings, x => x.Code == PzErrorCode.ConnectorConfigWarning);
    }
}
