using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using static Pz.Core.Tests.TestProjects;

namespace Pz.Core.Tests.Dag;

/// <summary><see cref="CanonicalJson.Serialize"/> feeds every node's NodeId, and it runs during
/// <see cref="DagCompiler.Compile"/>'s node building, after rendering -- where nothing catches a raw
/// exception. Two kinds of fact live here: real source()/sink() kwarg values (a huge integer literal, a
/// decimal literal) that must compile, and the PZ0137 refusal for a value that has no canonical form,
/// named by option key and never by value.</summary>
public class UnsupportedOptionValueTests
{
    // -- Real kwargs that used to crash the compile (now supported outright) ---------------------

    [Fact]
    public void A_huge_integer_source_kwarg_no_longer_crashes_the_compile()
    {
        // 99999999999999999999999999999999999999 overflows long -- Scriban evaluates it as
        // System.Numerics.BigInteger, which CanonicalJson.Serialize used to throw NotSupportedException
        // on, uncaught anywhere between here and the raw crash. `crm` declares no datasets in YAML (see
        // TestProjects.Sink's doc comment), so the source() call site's kwargs are the whole story --
        // no PZ0341 read-surface-split.
        var p = Project(
            [Pipe("stg", "select * from {{ source('crm', 'orders', " +
                "max_id: 99999999999999999999999999999999999999) }}")],
            sources: [Sink("crm")]);

        var dag = DagCompiler.Compile(p, Ctx(p)); // must NOT throw

        Assert.Contains(dag.Nodes, n => n.Name == "src_crm__orders");
    }

    [Fact]
    public void A_decimal_sink_kwarg_no_longer_crashes_the_compile()
    {
        // 1.5m is a real decimal literal (Scriban's `m` suffix) -- previously an uncaught
        // NotSupportedException from CanonicalJson.Serialize(output.Options).
        var p = Project(
            [Pipe("a", "INSERT INTO {{ sink('lake', 'out', strategy: 'replace', format: 'parquet', " +
                "threshold: 1.5m) }}\nselect 1 as x")],
            sinks: [Sink()]);

        var dag = DagCompiler.Compile(p, Ctx(p)); // must NOT throw

        Assert.Single(dag.Nodes, n => n.Kind == NodeKind.SinkWrite);
    }

    [Fact]
    public void Same_huge_integer_kwarg_yields_the_same_node_id_across_compiles()
    {
        // The new BigInteger branch must feed a stable, deterministic hash like every other type --
        // not merely "does not throw".
        var p = Project(
            [Pipe("stg", "select * from {{ source('crm', 'orders', " +
                "max_id: 99999999999999999999999999999999999999) }}")],
            sources: [Sink("crm")]);

        var id1 = DagCompiler.Compile(p, Ctx(p)).Nodes.Single(n => n.Kind == NodeKind.SourceLoad).Id;
        var id2 = DagCompiler.Compile(p, Ctx(p)).Nodes.Single(n => n.Kind == NodeKind.SourceLoad).Id;

        Assert.Equal(id1, id2);
    }

    // -- PZ0137 backstop for whatever CanonicalJson still cannot hash ----------------------------

    [Fact]
    public void SourceLoad_dataset_with_an_unsupported_option_value_is_PZ0137_naming_the_kwarg_not_the_value()
    {
        var badOptions = new Dictionary<string, object?> { ["driver_token"] = Guid.NewGuid() };
        var source = new ConnectionDef("crm", "postgres", new Dictionary<string, object?>(),
            [new DatasetDef("orders", badOptions, null)], "connections.yml");
        var p = Project([Pipe("stg", "select * from {{ source('crm', 'orders') }}")], sources: [source]);

        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.UnsupportedOptionValue);
        Assert.Contains("crm.orders", error.Message, StringComparison.Ordinal);
        Assert.Contains("driver_token", error.Message, StringComparison.Ordinal);
        Assert.Equal("connections.yml", error.File);
        // Secret hygiene: the rejected value itself never appears in the message.
        Assert.DoesNotContain(badOptions["driver_token"]!.ToString()!, error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SinkWrite_output_with_an_unsupported_option_value_declared_in_yaml_is_PZ0137()
    {
        // output.Options is only ever populated from a sink() kwarg or an `entities: <e>: write:` YAML
        // block (ConnectionDef.EntityWrites) -- never hand-settable any other way (DagCompiler stage 0
        // rebuilds Connections[].Outputs from these two sources on every compile). Scriban's sandbox
        // cannot manufacture a Guid (every builtin object that could is stripped), so this exercises the
        // YAML surface, which types options identically to the call-site kwarg surface.
        var badValue = Guid.NewGuid();
        var writeOptions = new SinkWriteOptions("replace", [], "fail_on_change", false, null, null,
            new Dictionary<string, object?> { ["driver_token"] = badValue });
        var sink = Sink() with { EntityWrites = new Dictionary<string, SinkWriteOptions> { ["out"] = writeOptions } };
        var p = Project(
            [Pipe("a", "INSERT INTO {{ sink('lake', 'out') }}\nselect 1 as x")],
            sinks: [sink]);

        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.UnsupportedOptionValue);
        Assert.Contains("lake.out", error.Message, StringComparison.Ordinal);
        Assert.Contains("driver_token", error.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(badValue.ToString(), error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_non_finite_number_kwarg_is_PZ0137_not_a_crash()
    {
        // 1.0 / 0.0 is a legal Scriban expression and evaluates to double infinity, which has no JSON
        // number form at all.
        var p = Project(
            [Pipe("stg", "select * from {{ source('crm', 'orders', ratio: 1.0 / 0.0) }}")],
            sources: [Sink("crm")]);

        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.UnsupportedOptionValue);
        Assert.Contains("ratio", error.Message, StringComparison.Ordinal);
    }
}
