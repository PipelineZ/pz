using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;
using Snowflake.Data.Client;

namespace Pz.Connector.Snowflake.Tests;

/// <summary>TestKit sink acceptance against a real Snowflake account. Same env-var gate as <see
/// cref="SnowflakeSourceAcceptance"/> (<see cref="SnowflakeFacts"/>) -- no Snowflake container exists,
/// so CI stays green (SKIP) without PZ_SNOWFLAKE_* set, and this repo's sandbox cannot exercise the
/// live half at all. Unlike the source suite, every table this suite writes to is created on demand by
/// the sink itself (SfDdl.EnsureTargetAsync); the only thing that must already exist in the test
/// account is the schema itself (Snowflake's CREATE TABLE does not create a missing schema). Seed it
/// once with, deliberately independent of the source suite's own setup:
///
/// <code>
/// CREATE SCHEMA IF NOT EXISTS "PZ_TESTKIT";
/// </code>
///
/// CheckpointOutput is left at its null default: the connector declares no CheckpointableWrites
/// capability.</summary>
public sealed class SnowflakeSinkAcceptance : SinkConnectorAcceptanceTests
{
    private const string SmallTable = "PZ_TESTKIT.SINK_ACCEPT_SMALL";
    private const string MergeTable = "PZ_TESTKIT.SINK_ACCEPT_MERGE";
    private const string ReplaceTable = "PZ_TESTKIT.SINK_ACCEPT_REPLACE";

    protected override void GateFact() => SnowflakeFacts.SkipUnlessConfigured();

    protected override ISinkConnector CreateSink() => new SnowflakeConnector();

    protected override ConnectorConfig ValidConfig => new(SnowflakeFacts.Config());

    protected override OutputSpec SmallOutput => new("sf", SmallTable, "replace", "fail_on_change",
        new Dictionary<string, object?>());

    protected override OutputSpec? MergeOutput => new("sf", MergeTable, "merge", "fail_on_change",
        new Dictionary<string, object?>()) { Keys = ["id"] };

    protected override OutputSpec? ReplaceOutput => new("sf", ReplaceTable, "replace", "fail_on_change",
        new Dictionary<string, object?>());

    // The merge target is a real, physically-persistent table in the test account (unlike the
    // in-process reference connector, CreateSink does not hand back a fresh store per call) -- drop it
    // before each merge fact so re-running the suite (including twice in a row) always starts from
    // known state, mirroring SqlServerSinkAcceptance's ResetMergeTargetAsync.
    protected override async Task ResetMergeTargetAsync()
    {
        var connectionString = SnowflakeConnector.BuildConnectionString(ValidConfig);
        await using var connection = new SnowflakeDbConnection { ConnectionString = connectionString };
        await connection.OpenAsync(CancellationToken.None).ConfigureAwait(false);
        var (schema, table) = SfDdl.SplitEntity(MergeTable);
        await using var command = connection.CreateCommand();
        command.CommandText = $"drop table if exists {SfDdl.Quote(schema)}.{SfDdl.Quote(table)}";
        await command.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
    }

    // Reads back committed data by opening the same connector as a source on the written entity
    // (unfiltered -- no query option) and draining its single partition; SnowflakeSource always plans
    // exactly one partition per dataset (native scan/range partitioning is future work), so there is
    // never more than one to drain.
    protected override async ValueTask<IReadOnlyList<RecordBatch>> ReadCommittedAsync(ISinkConnector connector, OutputSpec spec)
    {
        await using var source = await ((ISourceConnector)connector).OpenAsync(ValidConfig, CancellationToken.None);
        var partitions = await source.PlanReadAsync(
            new DatasetSpec("sf", spec.Output, new Dictionary<string, object?>()), ReadHints.None, CancellationToken.None);
        var partition = partitions[0];

        var batches = new List<RecordBatch>();
        await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            batches.Add(batch);
        }

        return batches;
    }
}
