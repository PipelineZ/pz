using Pz.Connectors.Abstractions;
using Pz.TestSupport;

namespace Pz.Connector.SqlServer.Tests;

/// <summary>Pins the actual outcome of a base-table schema change made AFTER <c>sp_cdc_enable_table</c>
/// runs. <see cref="SqlServerCdc.BuildWindowSelect"/>'s
/// data-column list comes from <see cref="SqlServerCdc.ProbeBaseColumnsAsync"/>, which probes the CURRENT
/// base table -- not the columns captured at the moment <c>sp_cdc_enable_table</c> ran. A column added to
/// the base table afterward is therefore in the probe's column list but NOT in
/// <c>fn_cdc_get_all_changes_&lt;instance&gt;</c>'s output (that function's shape is fixed at capture-enable
/// time), so the generated window SELECT references a column the change function doesn't have. This test
/// proves the resulting behavior is a loud, coded failure -- not silent data loss.</summary>
[Collection("sqlserver")]
public sealed class SqlServerCdcSchemaChangeTests(MsSqlContainerFixture fixture)
{
    private ConnectorConfig ValidConfig => new(new Dictionary<string, object?>
    {
        ["host"] = fixture.Host,
        ["port"] = fixture.Port,
        ["database"] = fixture.Database,
        ["user"] = fixture.User,
        ["password"] = fixture.Password,
        ["trust_server_certificate"] = true,
    });

    private static DatasetSpec CdcSpec(string table, string? prior, string captureInstance) =>
        new("ms", table, new Dictionary<string, object?> { ["capture_instance"] = captureInstance })
        {
            ChangeCapture = true,
            PriorSyncState = prior,
        };

    [SkippableFact]
    public async Task Column_added_after_capture_enabled_fails_the_next_incremental_read_loudly()
    {
        DockerFacts.SkipUnlessDocker();
        const string table = "cdc_schema_change";
        var instance = $"dbo_{table}";
        var cs = fixture.ConnectionString;

        await CdcTestSupport.EnsureDbCdcEnabledAsync(cs);
        await CdcTestSupport.ExecuteAsync(cs, $"if object_id('dbo.{table}') is not null drop table dbo.{table}");
        await CdcTestSupport.ExecuteAsync(cs, $"create table dbo.{table} (id int primary key, name nvarchar(50) not null)");
        await CdcTestSupport.ExecuteAsync(cs, $"insert into dbo.{table} (id, name) values (1, N'row-1')");
        await CdcTestSupport.ExecuteAsync(cs,
            $"EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'{table}', " +
            $"@role_name = NULL, @supports_net_changes = 0, @capture_instance = N'{instance}'");
        await CdcTestSupport.WaitForCapturePrimedAsync(cs, instance);

        ISourceConnector connector = new SqlServerConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);

        // First-run snapshot -- the token every incremental read resumes from.
        var snapshotPartition = Assert.Single(await source.PlanReadAsync(
            CdcSpec(table, prior: null, instance), ReadHints.None, CancellationToken.None));
        await foreach (var b in snapshotPartition.ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            b.Dispose();
        }

        Assert.True(((ISyncStatePartition)snapshotPartition).TryGetSyncStateCandidate(out var token));
        Assert.NotNull(token);

        // The schema change happens AFTER capture is already enabled -- exactly the edge case: the
        // change function's shape is frozen at sp_cdc_enable_table time, but ProbeBaseColumnsAsync
        // always reflects the base table as it is NOW.
        await CdcTestSupport.ExecuteAsync(cs, $"alter table dbo.{table} add extra_col int null");
        await CdcTestSupport.ExecuteAsync(cs, $"insert into dbo.{table} (id, name, extra_col) values (2, N'row-2', 42)");
        await CdcTestSupport.WaitForChangeCountAsync(cs, instance, expected: 1);

        var incrementalPartition = Assert.Single(await source.PlanReadAsync(
            CdcSpec(table, token, instance), ReadHints.None, CancellationToken.None));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            await foreach (var b in incrementalPartition.ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                b.Dispose();
            }
        });
        Assert.False(ex.IsTransient);
        Assert.Contains("sqlserver cdc failed", ex.Message, StringComparison.Ordinal);
        Assert.Contains("extra_col", ex.Message, StringComparison.OrdinalIgnoreCase);

        await CdcTestSupport.ExecuteAsync(cs,
            $"EXEC sys.sp_cdc_disable_table @source_schema = N'dbo', @source_name = N'{table}', " +
            $"@capture_instance = N'{instance}'");
        await CdcTestSupport.ExecuteAsync(cs, $"drop table dbo.{table}");
    }
}
