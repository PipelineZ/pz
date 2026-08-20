using Microsoft.Data.SqlClient;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.SqlServer.Tests;

/// <summary><c>SqlServerSource</c>'s <see cref="IChangeCaptureAdmin"/> implementation
/// (`pz cdc status`/`drop`), against the same shared "sqlserver" container as
/// <see cref="SqlServerCdcTests"/>. SQL Server's admin drop is a true server-side no-op: after
/// calling it, the capture instance is still enabled and status is unchanged -- pz never runs
/// <c>sp_cdc_disable_table</c> itself.</summary>
[Collection("sqlserver")]
public sealed class SqlServerCdcAdminTests(MsSqlContainerFixture fixture)
{
    private static readonly TimeSpan PollCap = TimeSpan.FromSeconds(90);
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(250);

    private ConnectorConfig ValidConfig => new(new Dictionary<string, object?>
    {
        ["host"] = fixture.Host,
        ["port"] = fixture.Port,
        ["database"] = fixture.Database,
        ["user"] = fixture.User,
        ["password"] = fixture.Password,
        ["trust_server_certificate"] = true,
    });

    // The dataset name IS the table, so one argument, not two.
    private static DatasetSpec CdcSpec(string table) =>
        new("ms", table, new Dictionary<string, object?>()) { ChangeCapture = true };

    // Uses a dedicated THROWAWAY database (not the shared "pz" one every other test in this collection
    // uses) for the same reason SqlServerCdcTests' own prereq test does: the shared db's cdc-enabled
    // state depends on which OTHER test in the collection ran first, so asserting "not created yet"
    // against it would be order-dependent flake, not a real fact.
    [SkippableFact]
    public async Task Status_reports_capture_instance_not_created_yet()
    {
        const string table = "some_table";
        var throwawayDb = $"pz_cdc_admin_{Guid.NewGuid():N}"[..30];
        await ExecuteOnMasterAsync($"create database [{throwawayDb}]");
        try
        {
            var csb = new SqlConnectionStringBuilder(fixture.ConnectionString) { InitialCatalog = throwawayDb };
            await ExecuteAsync(csb.ConnectionString, $"create table dbo.{table} (id int primary key, name nvarchar(50) not null)");

            var config = new ConnectorConfig(new Dictionary<string, object?>
            {
                ["host"] = fixture.Host,
                ["port"] = fixture.Port,
                ["database"] = throwawayDb,
                ["user"] = fixture.User,
                ["password"] = fixture.Password,
                ["trust_server_certificate"] = true,
            });

            ISourceConnector connector = new SqlServerConnector();
            await using var source = await connector.OpenAsync(config, CancellationToken.None);
            var admin = Assert.IsAssignableFrom<IChangeCaptureAdmin>(source);

            var status = await admin.GetChangeCaptureStatusAsync(CdcSpec(table), CancellationToken.None);

            Assert.False(status.Healthy);
            Assert.Equal($"dbo_{table}", status.PositionName);
            Assert.Null(status.RetainedBytes);
            Assert.Contains(status.Detail, d => d.Contains("capture instance not created yet", StringComparison.Ordinal));
            Assert.Contains(status.Detail, d => d.Contains("EXEC sys.sp_cdc_enable_db", StringComparison.Ordinal));
        }
        finally
        {
            await ExecuteOnMasterAsync(
                $"alter database [{throwawayDb}] set single_user with rollback immediate; drop database [{throwawayDb}]");
        }
    }

    [SkippableFact]
    public async Task Status_reports_healthy_after_a_real_capture_setup()
    {
        const string table = "cdc_admin_healthy";
        var instance = await SeedAndCaptureAsync(table, rows: 4);

        ISourceConnector connector = new SqlServerConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var admin = Assert.IsAssignableFrom<IChangeCaptureAdmin>(source);

        var status = await admin.GetChangeCaptureStatusAsync(CdcSpec(table), CancellationToken.None);

        Assert.True(status.Healthy);
        Assert.Equal(instance, status.PositionName);
        Assert.Null(status.RetainedBytes);
        Assert.Empty(status.Detail);
    }

    [SkippableFact]
    public async Task Drop_is_a_server_side_no_op_capture_instance_stays_enabled()
    {
        const string table = "cdc_admin_drop";
        var instance = await SeedAndCaptureAsync(table, rows: 2);

        ISourceConnector connector = new SqlServerConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var admin = Assert.IsAssignableFrom<IChangeCaptureAdmin>(source);
        var spec = CdcSpec(table);

        var before = await admin.GetChangeCaptureStatusAsync(spec, CancellationToken.None);
        Assert.True(before.Healthy);

        await admin.DropChangeCaptureStateAsync(spec, CancellationToken.None); // must not throw, must not disable

        Assert.True(await CaptureInstanceExistsAsync(instance));
        var after = await admin.GetChangeCaptureStatusAsync(spec, CancellationToken.None);
        Assert.True(after.Healthy);
        Assert.Equal(instance, after.PositionName);
    }

    // ---- helpers (mirrors SqlServerCdcTests' seeding/priming) ----

    private async Task<string> SeedAndCaptureAsync(string table, int rows)
    {
        await EnsureDbCdcEnabledAsync();
        await ExecuteAsync($"if object_id('dbo.{table}') is not null drop table dbo.{table}");
        await ExecuteAsync($"create table dbo.{table} (id int primary key, name nvarchar(50) not null)");
        await ExecuteAsync(
            $"insert into dbo.{table} (id, name) select value, concat('row-', value) from generate_series(1, {rows})");

        var instance = $"dbo_{table}";
        await ExecuteAsync(
            $"EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'{table}', " +
            $"@role_name = NULL, @supports_net_changes = 0, @capture_instance = N'{instance}'");

        await WaitForCapturePrimedAsync(instance);
        return instance;
    }

    private async Task EnsureDbCdcEnabledAsync()
    {
        await using var conn = new SqlConnection(fixture.ConnectionString);
        await conn.OpenAsync();
        await using var check = new SqlCommand("select is_cdc_enabled from sys.databases where name = db_name()", conn);
        var enabled = (bool)(await check.ExecuteScalarAsync())!;
        if (!enabled)
        {
            await using var enable = new SqlCommand("EXEC sys.sp_cdc_enable_db", conn);
            await enable.ExecuteNonQueryAsync();
        }
    }

    private async Task WaitForCapturePrimedAsync(string instance)
    {
        var deadline = DateTime.UtcNow + PollCap;
        while (DateTime.UtcNow < deadline)
        {
            await using var conn = new SqlConnection(fixture.ConnectionString);
            await conn.OpenAsync();
            var max = await SqlServerCdc.GetMaxLsnAsync(conn, CancellationToken.None);
            var min = await SqlServerCdc.GetMinLsnAsync(conn, instance, CancellationToken.None);
            if (max is { } maxLsn && maxLsn.Any(b => b != 0) && min is { } minLsn &&
                SqlServerCdc.CompareLsn(maxLsn, minLsn) >= 0)
            {
                return;
            }

            await Task.Delay(PollInterval);
        }

        throw new TimeoutException($"capture instance '{instance}' did not prime within the bounded poll " +
            "window -- is SQL Server Agent running (MSSQL_AGENT_ENABLED=true)?");
    }

    private async Task<bool> CaptureInstanceExistsAsync(string instance)
    {
        await using var conn = new SqlConnection(fixture.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(
            "select 1 from cdc.change_tables where capture_instance = @instance", conn);
        cmd.Parameters.Add(new SqlParameter("@instance", System.Data.SqlDbType.NVarChar, 386) { Value = instance });
        return await cmd.ExecuteScalarAsync() is not null;
    }

    private async Task ExecuteAsync(string sql) => await ExecuteAsync(fixture.ConnectionString, sql);

    private async Task ExecuteOnMasterAsync(string sql)
    {
        var csb = new SqlConnectionStringBuilder(fixture.ConnectionString) { InitialCatalog = "master" };
        await ExecuteAsync(csb.ConnectionString, sql);
    }

    private static async Task ExecuteAsync(string connectionString, string sql)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 120 };
        await cmd.ExecuteNonQueryAsync();
    }
}
