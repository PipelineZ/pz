using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using static Pz.Core.Tests.TestProjects;

namespace Pz.Core.Tests.Dag;

/// <summary>DagCompiler rules for <see cref="DatasetDef.SyncMode"/>.
/// Mirrors <see cref="DagCompilerTests"/>' project-fixture style (via <see cref="TestProjects"/>).
/// There is no PZ0315 "declares both incremental: and sync:" fact here -- the unified sync: block's
/// SyncModeDef carries exactly one Mode, so declaring two read modes is not representable.
/// The PZ0214 (<see cref="PzErrorCode.IncrementalAppendUnacknowledged"/>)
/// append-consent check (and its PZ03D/<see cref="PzErrorCode.IncompatiblePair"/> replace counterpart --
/// see <see cref="PairingMatrixTests"/>) applies to an EXPLICITLY declared `sync: {mode: incremental}`
/// dataset only. An explicit `sync: {mode: auto}` block (this file's fixture) is ambiguous at compile
/// time -- Pz.Core can't tell whether it resolves to a full read (needing no consent) or a feed (needing
/// ExecutionPlanner's shape-aware guard) -- so it trips neither rule here; see
/// <see cref="Sync_auto_dataset_feeding_append_sink_compiles_deferred_to_planner"/> below.</summary>
public class SyncCompileTests
{
    /// <summary>A single-dataset `crm.orders` source declaring a `sync:` block -- otherwise shaped like
    /// <see cref="TestProjects.CrmIncremental"/>, wired into a minimal project via a pipeline referencing
    /// source('crm', 'orders') so the SourceLoad node is actually built.
    /// An explicitly declared `sync: {mode: auto}` block (SyncMode non-null) -- Pz.Core's PZ0214/PZ03D
    /// checks never see this ambiguous shape at all; an implicit (SyncMode null) dataset that a
    /// connector resolves to Feed is likewise the planner's guard to enforce.</summary>
    private static ConnectionDef CrmSync(string dataset) =>
        new("crm", "localfiles", new Dictionary<string, object?> { ["root"] = "/data" },
            [new DatasetDef(dataset, new Dictionary<string, object?> { ["path"] = $"{dataset}.csv", ["format"] = "csv" },
                null, new SyncModeDef(SyncMode.Auto, null))],
            "connections.yml");

    [Fact]
    public void Sync_auto_dataset_feeding_append_sink_compiles_deferred_to_planner()
    {
        // Core's PZ0214 does not match an explicit `sync: {mode: auto}` dataset --
        // whether this needs append consent depends on the connector-resolved read shape, which only
        // ExecutionPlanner (holding the opened connector) can determine. Compiles clean either way here.
        var project = Project(
            [Pipe("stg", Into("out1", "append") + "select * from {{ source('crm', 'orders') }}")],
            sources: [CrmSync("orders")],
            sinks: [Sink()]);

        var dag = DagCompiler.Compile(project, Ctx(project)); // must not throw
        Assert.NotEmpty(dag.Nodes);
    }

    [Fact]
    public void Sync_auto_dataset_feeding_replace_sink_compiles_deferred_to_planner()
    {
        // Same narrowing for the PZ03D (replace) rule -- an explicit `sync: {mode: auto}` dataset never
        // trips it in Core either.
        var project = Project(
            [Pipe("stg", Into("out1", "replace") + "select * from {{ source('crm', 'orders') }}")],
            sources: [CrmSync("orders")],
            sinks: [Sink()]);

        var dag = DagCompiler.Compile(project, Ctx(project)); // must not throw
        Assert.NotEmpty(dag.Nodes);
    }

    [Fact]
    public void Sync_dataset_feeding_non_merge_sink_emits_effectively_once_notice()
    {
        var p = Project(
            [Pipe("stg", "INSERT INTO {{ sink('lake', 'out', strategy: 'replace', format: 'parquet') }} select * from {{ source('crm', 'orders') }}")],
            sources: [CrmSync("orders")],
            sinks: [Sink()]);
        var notices = new List<string>();

        var dag = DagCompiler.Compile(p, Ctx(p), notices);

        Assert.Contains(dag.Nodes, n => n.Name == "src_crm__orders");
        var notice = Assert.Single(notices, n => n.Contains("effectively-once"));
        Assert.Contains("crm.orders", notice);
        Assert.Contains("lake.out", notice);
    }
}
