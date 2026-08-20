using Pz.Core.Validation;
using Pz.Engine.Execution;
using Pz.Engine.Planning;

namespace Pz.Connector.MySql.Tests;

/// <summary>The planner half of the native-only contract against the REAL connector (the planner's
/// probes never connect, so no container is needed): both directions plan onto the native tier, and
/// engine.force_universal collides with both markers as PZ0312 at plan time instead of a doomed
/// run.</summary>
public sealed class MySqlPlannerTests
{
    [Fact]
    public async Task Both_directions_plan_onto_the_native_tier()
    {
        var plan = await new ExecutionPlanner(SecretRedactionTests.Registry())
            .PlanAsync(SecretRedactionTests.MySqlToMySqlDag(), forceUniversal: false, CancellationToken.None);

        var load = Assert.Single(plan.Nodes, n => n.Kind == Pz.Core.Dag.NodeKind.SourceLoad);
        Assert.Equal(EdgeStrategy.NativeScan, load.Strategy);
        Assert.Contains("mysql_query", load.Reason, StringComparison.Ordinal);

        var write = Assert.Single(plan.Nodes, n => n.Kind == Pz.Core.Dag.NodeKind.SinkWrite);
        Assert.Equal(EdgeStrategy.NativeCopy, write.Strategy);
        Assert.Contains("mysql", write.Reason, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Force_universal_is_PZ0312_in_both_directions()
    {
        var ex = await Assert.ThrowsAsync<PzValidationException>(
            () => new ExecutionPlanner(SecretRedactionTests.Registry())
                .PlanAsync(SecretRedactionTests.MySqlToMySqlDag(), forceUniversal: true, CancellationToken.None));

        var refusals = ex.Errors.Where(e => e.Code == PzErrorCode.NativePathRequired).ToArray();
        Assert.Equal(2, refusals.Length);
        Assert.Contains(refusals, e => e.Message.Contains("source 'wh'", StringComparison.Ordinal));
        Assert.Contains(refusals, e => e.Message.Contains("mart", StringComparison.Ordinal));
    }
}
