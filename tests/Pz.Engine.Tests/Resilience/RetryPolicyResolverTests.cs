using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Engine.Resilience;

namespace Pz.Engine.Tests.Resilience;

public class RetryPolicyResolverTests
{
    private static ConnectionDef Source(RetryDef? retry, RetryDef? datasetRetry = null) => new("pg", "postgres",
        new Dictionary<string, object?>(),
        [new DatasetDef("orders", new Dictionary<string, object?>(), null, null, datasetRetry)],
        "sources/pg.yml", retry);

    private static ConnectionDef Sink(RetryDef? retry, RetryDef? outputRetry = null) => new("out", "postgres",
        new Dictionary<string, object?>(), [], "sinks/out.yml", retry)
    {
        Outputs = [new OutputDef("totals", "order_totals", "append", "fail_on_change",
            new Dictionary<string, object?>(), [], outputRetry)],
    };

    private static DagNode Node(NodeKind kind, object definition) =>
        new(new NodeId("aaaaaaaaaaaaaaaa"), kind, "n", [], null, definition);

    [Fact]
    public void Source_node_full_block_overrides_every_field()
    {
        var def = new SourceDatasetDef(
            Source(new RetryDef(8, TimeSpan.FromSeconds(2), TimeSpan.FromMinutes(5))), Source(null).Datasets[0]);
        var policy = RetryPolicyResolver.Resolve(Node(NodeKind.SourceLoad, def));
        Assert.Equal(new RetryPolicy(8, TimeSpan.FromSeconds(2), TimeSpan.FromMinutes(5)), policy);
    }

    [Fact]
    public void Partial_block_overlays_default_field_wise()
    {
        var def = new SourceDatasetDef(Source(new RetryDef(8, null, null)), Source(null).Datasets[0]);
        var policy = RetryPolicyResolver.Resolve(Node(NodeKind.SourceLoad, def));
        Assert.Equal(8, policy.MaxAttempts);
        Assert.Equal(RetryPolicy.Default.BaseDelay, policy.BaseDelay);
        Assert.Equal(RetryPolicy.Default.MaxDelay, policy.MaxDelay);
    }

    [Fact]
    public void Sink_node_resolves_from_its_sink()
    {
        var sink = Sink(new RetryDef(4, TimeSpan.FromSeconds(5), null));
        var def = new SinkOutputDef(sink, sink.Outputs[0]);
        var policy = RetryPolicyResolver.Resolve(Node(NodeKind.SinkWrite, def));
        Assert.Equal(4, policy.MaxAttempts);
        Assert.Equal(TimeSpan.FromSeconds(5), policy.BaseDelay);
        Assert.Equal(RetryPolicy.Default.MaxDelay, policy.MaxDelay);
    }

    [Fact]
    public void Dataset_block_wins_field_wise_over_instance_block()
    {
        var source = Source(
            new RetryDef(5, TimeSpan.FromSeconds(2), null),          // instance
            new RetryDef(10, null, TimeSpan.FromMinutes(10)));        // dataset
        var def = new SourceDatasetDef(source, source.Datasets[0]);
        var policy = RetryPolicyResolver.Resolve(Node(NodeKind.SourceLoad, def));
        Assert.Equal(10, policy.MaxAttempts);                         // dataset wins
        Assert.Equal(TimeSpan.FromSeconds(2), policy.BaseDelay);      // instance fills the gap
        Assert.Equal(TimeSpan.FromMinutes(10), policy.MaxDelay);      // dataset wins
    }

    [Fact]
    public void Output_block_wins_field_wise_over_sink_instance_block()
    {
        var sink = Sink(
            new RetryDef(5, TimeSpan.FromSeconds(2), null),           // instance
            new RetryDef(10, null, TimeSpan.FromMinutes(10)));        // output
        var def = new SinkOutputDef(sink, sink.Outputs[0]);
        var policy = RetryPolicyResolver.Resolve(Node(NodeKind.SinkWrite, def));
        Assert.Equal(10, policy.MaxAttempts);                         // output wins
        Assert.Equal(TimeSpan.FromSeconds(2), policy.BaseDelay);      // instance fills the gap
        Assert.Equal(TimeSpan.FromMinutes(10), policy.MaxDelay);      // output wins
    }

    [Fact]
    public void No_block_and_non_connector_nodes_get_default()
    {
        var sourceDef = new SourceDatasetDef(Source(null), Source(null).Datasets[0]);
        Assert.Equal(RetryPolicy.Default, RetryPolicyResolver.Resolve(Node(NodeKind.SourceLoad, sourceDef)));

        var checkDef = new CheckNodeDef("p", new CheckDef("not_null", ["id"], new Dictionary<string, object?>(), null));
        Assert.Equal(RetryPolicy.Default, RetryPolicyResolver.Resolve(Node(NodeKind.Check, checkDef)));
    }
}
