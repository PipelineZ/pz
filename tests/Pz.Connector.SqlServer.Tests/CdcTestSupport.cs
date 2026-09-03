using Microsoft.Data.SqlClient;

namespace Pz.Connector.SqlServer.Tests;

/// <summary>Shared bounded-poll/setup helpers for the docker-backed CDC suites (<see
/// cref="SqlServerCdcTests"/>, <see cref="SqlServerCdcSchemaChangeTests"/>) -- factored out so a fix to
/// the polling logic (or its retention-priming race-condition handling) lands once, not in two
/// independently-drifting copies.</summary>
internal static class CdcTestSupport
{
    public static readonly TimeSpan PollCap = TimeSpan.FromSeconds(90);
    public static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(250);

    public static async Task EnsureDbCdcEnabledAsync(string connectionString)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();
        await using var check = new SqlCommand("select is_cdc_enabled from sys.databases where name = db_name()", conn);
        var enabled = (bool)(await check.ExecuteScalarAsync())!;
        if (!enabled)
        {
            await using var enable = new SqlCommand("EXEC sys.sp_cdc_enable_db", conn);
            await enable.ExecuteNonQueryAsync();
        }
    }

    // Bounded poll (loop + small delay + generous cap -- never a blind sleep): waits for the capture
    // job to fully prime THIS instance -- both db-wide max_lsn non-null AND max_lsn >= this instance's
    // own min_lsn. db-wide max_lsn alone is not enough: it can already be non-null from an EARLIER
    // test's capture instance while the log reader has not yet scanned forward past the LATEST
    // instance's own start_lsn, which would make a legitimate "just enabled, nothing missed yet" state
    // look like a retention gap (fn_cdc_get_min_lsn(instance) > a stale max_lsn snapshot).
    public static async Task WaitForCapturePrimedAsync(string connectionString, string instance)
    {
        var deadline = DateTime.UtcNow + PollCap;
        while (DateTime.UtcNow < deadline)
        {
            await using var conn = new SqlConnection(connectionString);
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

    // Bounded poll for the capture job to land `expected` CHANGES in the raw change table. Counts
    // changes, not rows: an update lands two rows there (__$operation 3, the before-image, and 4, the
    // after-image), so a plain row count reaches the threshold one change early and the very next
    // poll read races the capture job for the last change -- with insert + update + delete, the
    // delete goes missing. Excluding the before-image makes one change one row.
    public static async Task WaitForChangeCountAsync(string connectionString, string instance, int expected)
    {
        var deadline = DateTime.UtcNow + PollCap;
        while (DateTime.UtcNow < deadline)
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync();
            await using var cmd = new SqlCommand(
                $"select count(*) from cdc.{MsDdl.Quote($"{instance}_CT")} where [__$operation] <> 3", conn);
            var count = (int)(await cmd.ExecuteScalarAsync())!;
            if (count >= expected)
            {
                return;
            }

            await Task.Delay(PollInterval);
        }

        throw new TimeoutException(
            $"capture job did not land {expected} change row(s) within the bounded poll window");
    }

    public static async Task ExecuteAsync(string connectionString, string sql)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 120 };
        await cmd.ExecuteNonQueryAsync();
    }
}
