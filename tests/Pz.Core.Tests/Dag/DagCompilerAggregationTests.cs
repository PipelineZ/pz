using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using static Pz.Core.Tests.TestProjects;

namespace Pz.Core.Tests.Dag;

/// <summary>DagCompiler as an aggregate: every pipeline gets a chance to render even when an earlier
/// one's template is broken, and independent validation stages (incremental/merge-keys, ref/source
/// resolution, checks-on-ephemeral, ephemeral-chain) report together in one compile instead of
/// stopping at whichever runs first. Mirrors <see cref="DagCompilerTests"/>'s fixture style.</summary>
public class DagCompilerAggregationTests
{
    [Fact]
    public void Two_pipelines_with_broken_templates_both_report_in_one_compile()
    {
        var p = Project([
            Pipe("a", "select {{ var('missing_a') }}"),
            Pipe("b", "select {{ var('missing_b') }}")]);

        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));

        Assert.Contains(ex.Errors, e => e.File == "pipelines/a.sql");
        Assert.Contains(ex.Errors, e => e.File == "pipelines/b.sql");
        Assert.Equal(2, ex.Errors.Count);
    }

    [Fact]
    public void Two_pipelines_with_unresolved_refs_both_report_in_one_compile()
    {
        var p = Project([
            Pipe("a", "select * from {{ ref('nope_a') }}"),
            Pipe("b", "select * from {{ ref('nope_b') }}")]);

        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));

        Assert.Equal(2, ex.Errors.Count(e => e.Code == PzErrorCode.UnresolvedRef));
        Assert.Contains(ex.Errors, e => e.Message.Contains("nope_a"));
        Assert.Contains(ex.Errors, e => e.Message.Contains("nope_b"));
    }

    [Fact]
    public void Unresolved_ref_and_checks_on_ephemeral_both_report_in_one_compile()
    {
        // Two DIFFERENT, independent validation stages (ref/source resolution -> PZ0201, and
        // checks-on-ephemeral -> PZ0205) -- neither reads the other's result -- so a project broken in
        // both ways gets both errors from ONE compile rather than only the first stage's.
        var check = new CheckDef("not_null", ["id"], new Dictionary<string, object?>());
        var p = Project([
            Pipe("bad_ref", "select * from {{ ref('nope') }}"),
            Pipe("eph", "select 1 as id", materialization: "ephemeral", checks: [check])]);

        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));

        Assert.Contains(ex.Errors, e => e.Code == PzErrorCode.UnresolvedRef);
        Assert.Contains(ex.Errors, e => e.Code == PzErrorCode.ChecksOnEphemeral);
    }

    [Fact]
    public void Aggregated_errors_are_ordered_by_file()
    {
        var p = Project([
            Pipe("z_pipeline", "select * from {{ ref('nope_z') }}"),
            Pipe("a_pipeline", "select * from {{ ref('nope_a') }}")]);

        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));

        Assert.Equal(2, ex.Errors.Count);
        Assert.Equal("pipelines/a_pipeline.sql", ex.Errors[0].File);
        Assert.Equal("pipelines/z_pipeline.sql", ex.Errors[1].File);
    }

    [Fact]
    public void Incremental_and_cdc_pairing_violations_both_report_in_one_compile()
    {
        var erp = new ConnectionDef("erp", "postgres", new Dictionary<string, object?>(),
            [new DatasetDef("invoices", new Dictionary<string, object?>(), null, new SyncModeDef(SyncMode.Cdc, null))],
            "connections.yml");
        var p = Project(
            [
                Pipe("stg1", Into("out1", "append") + "select * from {{ source('crm', 'orders') }}"),
                Pipe("stg2", Into("out2", "replace") + "select * from {{ source('erp', 'invoices') }}"),
            ],
            sources: [CrmIncremental("orders", "id", new Dictionary<string, string> { ["id"] = "bigint" }), erp],
            sinks: [Sink()]);

        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));

        Assert.Contains(ex.Errors, e => e.Code == PzErrorCode.IncrementalAppendUnacknowledged && e.Message.Contains("lake.out1"));
        Assert.Contains(ex.Errors, e => e.Code == PzErrorCode.IncompatiblePair && e.Message.Contains("lake.out2"));
    }
}
