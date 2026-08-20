using Pz.Core.Dag;
using Pz.Engine.Execution;
using Pz.Engine.Resilience;
using Pz.Engine.State;
using Pz.State.SqlServer;

namespace Pz.State.SqlServer.Tests;

/// <summary><c>SqlRunArtifactStore.ReadRun</c> must parse the `payload` column's JSON far enough to
/// populate <see cref="PriorNode.Observed"/>; parsing only far enough to validate it makes
/// `pz schema accept` silently inert under `state: {backend: sqlserver}` (it reads the prior run's
/// observed schema through <c>backends.Artifacts.ReadLatest()</c>). These tests exercise
/// <c>SerializePayload</c>/<c>ParseObservedSchema</c> directly (an internal seam via this assembly's
/// <c>InternalsVisibleTo</c>) so the invariant is pinned without Docker -- the round trip through a
/// real SQL Server store is additionally covered by
/// <c>SqlRunArtifactStoreContractTests.ReadLatest_round_trips_observed_schema</c>, which does require
/// it.</summary>
public sealed class SqlRunArtifactPayloadTests
{
    private static NodeResult SucceededSourceLoad(ObservedSchema? observed = null) =>
        new(new NodeId("n1"), NodeKind.SourceLoad, "src_a", NodeStatus.Success, 0, TimeSpan.Zero, null,
            Observed: observed);

    [Fact]
    public void SerializePayload_then_ParseObservedSchema_round_trips_columns_and_hints_hash()
    {
        var observed = new ObservedSchema(
            [new SchemaColumn("id", "BIGINT"), new SchemaColumn("email", "VARCHAR")], "hh-abc123");
        var node = SucceededSourceLoad(observed);

        var payload = SqlRunArtifactStore.SerializePayload(node);
        var parsed = SqlRunArtifactStore.ParseObservedSchema(payload);

        Assert.NotNull(parsed);
        Assert.Equal("hh-abc123", parsed.HintsHash);
        Assert.Equal(
            [("id", "BIGINT"), ("email", "VARCHAR")],
            parsed.Columns.Select(c => (c.Name, c.Type)).ToArray());
    }

    [Fact]
    public void ParseObservedSchema_is_null_when_the_payload_has_no_observed_schema()
    {
        var node = SucceededSourceLoad();

        var payload = SqlRunArtifactStore.SerializePayload(node);

        Assert.Null(payload);
        Assert.Null(SqlRunArtifactStore.ParseObservedSchema(payload));
    }

    [Fact]
    public void ParseObservedSchema_is_null_when_the_payload_carries_other_fields_but_no_observed_schema()
    {
        var node = SucceededSourceLoad() with { Ops = new OpStats(1, 0, 0) };

        var payload = SqlRunArtifactStore.SerializePayload(node);

        Assert.NotNull(payload);
        Assert.Null(SqlRunArtifactStore.ParseObservedSchema(payload));
    }
}
