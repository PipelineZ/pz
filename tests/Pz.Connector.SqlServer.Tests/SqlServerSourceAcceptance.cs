using Microsoft.Data.SqlClient;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;
using Pz.TestSupport;

namespace Pz.Connector.SqlServer.Tests;

/// <summary>TestKit source acceptance against the real connector over Testcontainers SQL Server.
/// SmallDataset declares a real partition_column/partitions pair so the partitions-union fact
/// exercises its ground-truth path. TransientFailureDataset is null: SQL Server has no self-terminate
/// query analog that SqlClient classifies IsTransient=true (KILL cannot target the own session), so
/// that optional fact no-ops; classification coverage comes from CheckConnection_bad_password
/// (permanent) and the retry-path engine tests.</summary>
[Collection("sqlserver")]
public sealed class SqlServerSourceAcceptance(MsSqlContainerFixture fixture) : SourceConnectorAcceptanceTests
{
    protected override void GateFact() => DockerFacts.SkipUnlessDocker();

    protected override ISourceConnector CreateSource() => new SqlServerConnector();

    protected override ConnectorConfig ValidConfig => new(new Dictionary<string, object?>
    {
        ["host"] = fixture.Host,
        ["port"] = fixture.Port,
        ["database"] = fixture.Database,
        ["user"] = fixture.User,
        ["password"] = fixture.Password,
        ["trust_server_certificate"] = true,
    });

    protected override DatasetSpec SmallDataset => new("ms", "orders", new Dictionary<string, object?>
    {
        ["partition_column"] = "id",
        ["partitions"] = 2,
    });

    // 500k rows via GENERATE_SERIES (SQL Server 2022+): large enough that the 5s cancellation fact
    // genuinely cancels a mid-stream read.
    protected override DatasetSpec? LargeDataset => new("ms", "large", new Dictionary<string, object?>
    {
        ["query"] = "select value as n from generate_series(1, 500000)",
    });

    protected override DatasetSpec? TransientFailureDataset => null;

    protected override DatasetSpec? GetSpecWithPartitionOverride(int partitions) => SmallDataset with
    {
        Options = new Dictionary<string, object?>(SmallDataset.Options) { ["partitions"] = partitions },
    };

    // A fresh fixture (own uniquely-named table + capture instance) per fact access -- mirrors
    // SqlServerCdcTests' per-test table convention, sharing the ONE
    // MsSqlContainerFixture (MSSQL_AGENT_ENABLED=true) every other suite in this collection already uses.
    protected override IChangeCaptureFixture? ChangeCaptureFixture => new SqlServerChangeCaptureFixture(fixture);

    // The IChangeCaptureFixture implementation backing the TestKit's ChangeCapture_* facts.
    // Owns one throwaway captured table with 20 seed rows split into disjoint update/delete pools so
    // MutateAsync never re-touches a key a prior call on the SAME instance already mutated -- every
    // acceptance fact gets its own fresh instance via the ChangeCaptureFixture property above, so no
    // state leaks between facts.
    private sealed class SqlServerChangeCaptureFixture(MsSqlContainerFixture fixture) : IChangeCaptureFixture
    {
        private static readonly TimeSpan PollCap = TimeSpan.FromSeconds(90);
        private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(250);

        private const int SeedRows = 20;
        private const int UpdatePoolStart = 1;
        private const int UpdatePoolEnd = 10; // inclusive
        private const int DeletePoolStart = 11;
        private const int DeletePoolEnd = 20; // inclusive

        private readonly string _table = $"cdc_acceptance_{Guid.NewGuid():N}"[..24];
        private readonly string _dataset = $"cdc_acceptance_ds_{Guid.NewGuid():N}"[..28];
        private readonly string _instance = $"cdc_acc_{Guid.NewGuid():N}"[..20];
        private Task<DatasetSpec>? _setup;
        private int _nextInsertId = 10_000;
        private int _nextUpdateId = UpdatePoolStart;
        private int _nextDeleteId = DeletePoolStart;
        private int _changesApplied;

        public Task<DatasetSpec> CdcSpecAsync() => _setup ??= SetupAsync();

        public async Task MutateAsync(int inserts, int updates, int deletes)
        {
            await CdcSpecAsync().ConfigureAwait(false);

            for (var i = 0; i < inserts; i++)
            {
                var id = _nextInsertId++;
                await ExecuteAsync($"insert into dbo.{_table} (id, name) values ({id}, 'new-{id}')")
                    .ConfigureAwait(false);
            }

            for (var i = 0; i < updates; i++)
            {
                if (_nextUpdateId > UpdatePoolEnd)
                {
                    throw new InvalidOperationException("SqlServerChangeCaptureFixture: update pool exhausted");
                }

                var id = _nextUpdateId++;
                await ExecuteAsync($"update dbo.{_table} set name = 'updated-{id}' where id = {id}")
                    .ConfigureAwait(false);
            }

            for (var i = 0; i < deletes; i++)
            {
                if (_nextDeleteId > DeletePoolEnd)
                {
                    throw new InvalidOperationException("SqlServerChangeCaptureFixture: delete pool exhausted");
                }

                var id = _nextDeleteId++;
                await ExecuteAsync($"delete from dbo.{_table} where id = {id}").ConfigureAwait(false);
            }

            _changesApplied += inserts + updates + deletes;
            await WaitForChangeCountAsync(_changesApplied).ConfigureAwait(false);
        }

        public async Task<string?> ServerPositionAsync()
        {
            await using var conn = new SqlConnection(fixture.ConnectionString);
            await conn.OpenAsync().ConfigureAwait(false);
            var max = await SqlServerCdc.GetMaxLsnAsync(conn, CancellationToken.None).ConfigureAwait(false);
            return max is null ? null : SqlServerCdc.FormatLsn(max);
        }

        private async Task<DatasetSpec> SetupAsync()
        {
            await EnsureDbCdcEnabledAsync().ConfigureAwait(false);
            await ExecuteAsync($"if object_id('dbo.{_table}') is not null drop table dbo.{_table}")
                .ConfigureAwait(false);
            await ExecuteAsync($"create table dbo.{_table} (id int primary key, name nvarchar(50) not null)")
                .ConfigureAwait(false);
            await ExecuteAsync(
                $"insert into dbo.{_table} (id, name) select value, concat('row-', value) " +
                $"from generate_series(1, {SeedRows})").ConfigureAwait(false);

            // @supports_net_changes = 0 (same fixture shape SqlServerCdcTests uses): leaves
            // cdc.index_columns empty, exercising DiscoverKeyColumnsAsync's primary-key fallback here too.
            await ExecuteAsync(
                $"EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'{_table}', " +
                $"@role_name = NULL, @supports_net_changes = 0, @capture_instance = N'{_instance}'")
                .ConfigureAwait(false);

            await WaitForCapturePrimedAsync().ConfigureAwait(false);

            return new DatasetSpec("ms", _table, new Dictionary<string, object?>
            {
                ["capture_instance"] = _instance,
            })
            {
                ChangeCapture = true,
                PriorSyncState = null,
            };
        }

        private async Task EnsureDbCdcEnabledAsync()
        {
            await using var conn = new SqlConnection(fixture.ConnectionString);
            await conn.OpenAsync().ConfigureAwait(false);
            if (!await SqlServerCdc.IsDbCdcEnabledAsync(conn, CancellationToken.None).ConfigureAwait(false))
            {
                await using var enable = new SqlCommand("EXEC sys.sp_cdc_enable_db", conn);
                await enable.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        // Bounded poll (loop + small delay + generous cap -- never a blind sleep), mirroring
        // SqlServerCdcTests' WaitForCapturePrimedAsync: waits for the capture job to fully prime THIS
        // instance -- both db-wide max_lsn non-null AND max_lsn >= this instance's own min_lsn.
        private async Task WaitForCapturePrimedAsync()
        {
            var deadline = DateTime.UtcNow + PollCap;
            while (DateTime.UtcNow < deadline)
            {
                await using var conn = new SqlConnection(fixture.ConnectionString);
                await conn.OpenAsync().ConfigureAwait(false);
                var max = await SqlServerCdc.GetMaxLsnAsync(conn, CancellationToken.None).ConfigureAwait(false);
                var min = await SqlServerCdc.GetMinLsnAsync(conn, _instance, CancellationToken.None).ConfigureAwait(false);
                if (max is { } maxLsn && maxLsn.Any(b => b != 0) && min is { } minLsn &&
                    SqlServerCdc.CompareLsn(maxLsn, minLsn) >= 0)
                {
                    return;
                }

                await Task.Delay(PollInterval).ConfigureAwait(false);
            }

            throw new TimeoutException(
                $"capture instance '{_instance}' did not prime within the bounded poll window -- is SQL " +
                "Server Agent running (MSSQL_AGENT_ENABLED=true)?");
        }

        // Bounded poll for the capture job to land the expected cumulative CHANGE count in the raw
        // change table, so a MutateAsync caller's very next poll read never races the async capture
        // job. Counts changes, not rows: an update lands two rows there (__$operation 3, the
        // before-image, and 4, the after-image), so a plain row count reaches the threshold one change
        // early -- with insert + update + delete, the delete goes missing from the poll. Excluding the
        // before-image makes one change one row (same rule as CdcTestSupport.WaitForChangeCountAsync).
        private async Task WaitForChangeCountAsync(int expected)
        {
            var deadline = DateTime.UtcNow + PollCap;
            while (DateTime.UtcNow < deadline)
            {
                await using var conn = new SqlConnection(fixture.ConnectionString);
                await conn.OpenAsync().ConfigureAwait(false);
                await using var cmd = new SqlCommand(
                    $"select count(*) from cdc.{MsDdl.Quote($"{_instance}_CT")} where [__$operation] <> 3", conn);
                var count = (int)(await cmd.ExecuteScalarAsync().ConfigureAwait(false))!;
                if (count >= expected)
                {
                    return;
                }

                await Task.Delay(PollInterval).ConfigureAwait(false);
            }

            throw new TimeoutException(
                $"capture job did not land {expected} change row(s) within the bounded poll window");
        }

        private async Task ExecuteAsync(string sql)
        {
            await using var conn = new SqlConnection(fixture.ConnectionString);
            await conn.OpenAsync().ConfigureAwait(false);
            await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 120 };
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        }
    }
}
