using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Engine.Execution;

namespace Pz.Engine.Tests.Execution;

/// <summary>A connection is one place, so reading <c>warehouse</c> and writing <c>warehouse</c> share one
/// permit pool, one rate limiter, and one breaker. Splitting the read and write sides into two instances
/// would double the effective ceiling: <c>max_concurrency: 4</c> on a database pz both reads and writes
/// would admit eight concurrent nodes.</summary>
public class InstanceKeyTests
{
    private static ConnectionDef Conn(string name) =>
        new(name, "postgres", new Dictionary<string, object?>(), [], "connections.yml");

    private static DagNode Read(ConnectionDef connection) =>
        new(new NodeId("aaaaaaaaaaaaaaaa"), NodeKind.SourceLoad, $"src_{connection.Name}__orders", [], null,
            new SourceDatasetDef(connection, new DatasetDef("orders", new Dictionary<string, object?>(), null)));

    private static DagNode Write(ConnectionDef connection) =>
        new(new NodeId("bbbbbbbbbbbbbbbb"), NodeKind.SinkWrite, $"{connection.Name}.mart", [], null,
            new SinkOutputDef(connection, new OutputDef("mart", "p", "append", "fail_on_change",
                new Dictionary<string, object?>())));

    [Fact]
    public void Reading_and_writing_one_connection_share_an_instance()
    {
        var conn = Conn("warehouse");

        Assert.Equal(InstanceKey.For(Read(conn)), InstanceKey.For(Write(conn)));
    }

    [Fact]
    public void Two_connections_do_not_share_an_instance() =>
        Assert.NotEqual(InstanceKey.For(Read(Conn("a"))), InstanceKey.For(Write(Conn("b"))));

    [Fact]
    public void A_pipeline_node_has_no_owning_instance() =>
        Assert.Null(InstanceKey.For(new DagNode(new NodeId("cccccccccccccccc"), NodeKind.Pipeline, "p", [],
            "select 1", new PipelineDef("p", "select 1", "table", [], [], "pipelines/p.sql"))));
}
