using Apache.Arrow;
using Microsoft.Data.SqlClient;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.SqlServer.Tests;

/// <summary>The SQL Server cdc source -- native CDC-table polling
/// between LSN positions, retention-gap detection, and the first-run snapshot. Shares the
/// <see cref="MsSqlContainerFixture"/> ("sqlserver" collection) with every other SQL Server suite --
/// MSSQL_AGENT_ENABLED=true is harmless to them (the agent just runs in the background alongside the
/// engine). CDC is enabled once, database-wide, on the shared "pz" database (idempotent -- every test
/// calls the same ensure-enabled helper); each test then captures its OWN uniquely-named table so
/// tests never collide. Every capture uses <c>@supports_net_changes = 0</c>, which leaves
/// <c>cdc.index_columns</c> EMPTY for that instance -- deliberately exercising
/// <see cref="SqlServerCdc.DiscoverKeyColumnsAsync"/>'s primary-key fallback on every path, not just
/// a dedicated one.</summary>
[Collection("sqlserver")]
public sealed class SqlServerCdcTests(MsSqlContainerFixture fixture)
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

    // The dataset name IS the table, so one argument, not two.
    private static DatasetSpec CdcSpec(string table, string? prior, string? captureInstance = null)
    {
        var options = new Dictionary<string, object?>();
        if (captureInstance is not null)
        {
            options["capture_instance"] = captureInstance;
        }

        return new DatasetSpec("ms", table, options) { ChangeCapture = true, PriorSyncState = prior };
    }

    [SkippableFact]
    public async Task Prereqs_report_enable_db_then_enable_table_lines()
    {
        // A dedicated throwaway database (not the shared "pz" one every other test/suite uses) so this
        // test can observe the true "cdc never enabled" state regardless of what ran before it in the
        // shared collection.
        var throwawayDb = $"pz_cdc_prereq_{Guid.NewGuid():N}"[..30];
        await ExecuteOnMasterAsync($"create database [{throwawayDb}]");
        try
        {
            var csb = new SqlConnectionStringBuilder(fixture.ConnectionString) { InitialCatalog = throwawayDb };
            var spec = CdcSpec("some_table", prior: null);

            await using (var conn = new SqlConnection(csb.ConnectionString))
            {
                await conn.OpenAsync();
                var unmet = await SqlServerCdc.ValidatePrerequisitesAsync(conn, spec, CancellationToken.None);
                Assert.Contains(unmet, l => l.Contains("EXEC sys.sp_cdc_enable_db", StringComparison.Ordinal));
            }

            await CdcTestSupport.ExecuteAsync(csb.ConnectionString, "EXEC sys.sp_cdc_enable_db");
            await CdcTestSupport.ExecuteAsync(
                csb.ConnectionString, "create table dbo.some_table (id int primary key, name nvarchar(50) not null)");

            await using (var conn = new SqlConnection(csb.ConnectionString))
            {
                await conn.OpenAsync();
                var unmet = await SqlServerCdc.ValidatePrerequisitesAsync(conn, spec, CancellationToken.None);
                Assert.DoesNotContain(unmet, l => l.Contains("EXEC sys.sp_cdc_enable_db", StringComparison.Ordinal));
                var line = Assert.Single(unmet, l => l.Contains("EXEC sys.sp_cdc_enable_table", StringComparison.Ordinal));
                Assert.Contains("some_table", line, StringComparison.Ordinal);
                Assert.Contains("dbo", line, StringComparison.Ordinal);
            }

            // Read on the unmet prerequisite fails fast, non-transient, naming the statement -- before
            // any row lands.
            ISourceConnector connector = new SqlServerConnector();
            var config = new ConnectorConfig(new Dictionary<string, object?>
            {
                ["host"] = fixture.Host,
                ["port"] = fixture.Port,
                ["database"] = throwawayDb,
                ["user"] = fixture.User,
                ["password"] = fixture.Password,
                ["trust_server_certificate"] = true,
            });
            await using var source = await connector.OpenAsync(config, CancellationToken.None);
            var partition = Assert.Single(await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None));
            var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            {
                await foreach (var b in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
                {
                    b.Dispose();
                }
            });
            Assert.False(ex.IsTransient);
            Assert.Contains("EXEC sys.sp_cdc_enable_table", ex.Message, StringComparison.Ordinal);
        }
        finally
        {
            await ExecuteOnMasterAsync(
                $"alter database [{throwawayDb}] set single_user with rollback immediate; drop database [{throwawayDb}]");
        }
    }

    [SkippableFact]
    public async Task First_run_snapshot_yields_change_rows_and_key_columns()
    {
        const string table = "cdc_snap";
        var instance = await SeedAndCaptureAsync(table, rows: 5);

        ISourceConnector connector = new SqlServerConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var spec = CdcSpec(table, prior: null, captureInstance: instance);
        var partition = Assert.Single(await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None));

        var batches = new List<RecordBatch>();
        await foreach (var b in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            batches.Add(b);
        }

        try
        {
            var schema = batches[0].Schema;
            Assert.Equal("_pz_op", schema.FieldsList[0].Name);
            Assert.Equal("_pz_lsn", schema.FieldsList[1].Name);
            Assert.Equal("_pz_changed_at", schema.FieldsList[2].Name);
            Assert.Equal("id", schema.FieldsList[3].Name);
            Assert.Equal("name", schema.FieldsList[4].Name);

            var rows = 0;
            foreach (var b in batches)
            {
                var op = (StringArray)b.Column(0);
                var lsn = (StringArray)b.Column(1);
                var changedAt = b.Column(2);
                for (var i = 0; i < b.Length; i++)
                {
                    Assert.Equal("insert", op.GetString(i));
                    Assert.Equal(SqlServerCdc.SnapshotLsn, lsn.GetString(i));
                    Assert.Equal(41, lsn.GetString(i).Length);
                    Assert.True(changedAt.IsNull(i));
                    rows++;
                }
            }

            Assert.Equal(5, rows);
        }
        finally
        {
            foreach (var b in batches)
            {
                b.Dispose();
            }
        }

        var keyPartition = Assert.IsAssignableFrom<IChangeCapturePartition>(partition);
        Assert.True(keyPartition.TryGetChangeKeyColumns(out var keys));
        Assert.Equal(["id"], keys);

        var syncPartition = Assert.IsAssignableFrom<ISyncStatePartition>(partition);
        Assert.True(syncPartition.TryGetSyncStateCandidate(out var candidate));
        Assert.NotNull(candidate);
        Assert.Matches("^[0-9A-F]{20}$", candidate);
    }

    [SkippableFact]
    public async Task Poll_yields_change_rows_with_ordered_lsns_and_advances_token()
    {
        const string table = "cdc_poll1";
        var instance = await SeedAndCaptureAsync(table, rows: 2);

        ISourceConnector connector = new SqlServerConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var candidate = await SnapshotAndTokenAsync(source, table, instance);

        await CdcTestSupport.ExecuteAsync(fixture.ConnectionString, $"insert into dbo.{table} (id, name) values (3, 'three')");
        await CdcTestSupport.ExecuteAsync(fixture.ConnectionString, $"update dbo.{table} set name = 'one-updated' where id = 1");
        await CdcTestSupport.ExecuteAsync(fixture.ConnectionString, $"delete from dbo.{table} where id = 2");
        await CdcTestSupport.WaitForChangeCountAsync(fixture.ConnectionString, instance, expected: 3);

        var (rows, newToken) = await PollAsync(source, table, instance, candidate);

        Assert.Equal(3, rows.Count);
        var insert = Assert.Single(rows, r => r.Op == "insert");
        var update = Assert.Single(rows, r => r.Op == "update");
        var delete = Assert.Single(rows, r => r.Op == "delete");

        Assert.Equal(3, insert.Id);
        Assert.Equal("three", insert.Name);
        Assert.Equal(1, update.Id);
        Assert.Equal("one-updated", update.Name);
        Assert.Equal(2, delete.Id);

        foreach (var r in rows)
        {
            Assert.False(r.ChangedAtNull, "poll change rows carry fn_cdc_map_lsn_to_time");
            Assert.Matches("^[0-9A-F]{20}-[0-9A-F]{20}$", r.Lsn);
        }

        // _pz_lsn strictly ordinal-increasing in emission order (fixed-width hex -> ordinal == numeric).
        for (var i = 1; i < rows.Count; i++)
        {
            Assert.True(string.CompareOrdinal(rows[i].Lsn, rows[i - 1].Lsn) > 0, "lsns strictly increasing");
        }

        Assert.NotNull(newToken);
        Assert.Matches("^[0-9A-F]{20}$", newToken);
        Assert.NotEqual(candidate, newToken);
    }

    [SkippableFact]
    public async Task Retention_gap_is_detected_and_never_silent()
    {
        const string table = "cdc_retention";
        var instance = await SeedAndCaptureAsync(table, rows: 2); // capture primed -> fn_cdc_get_min_lsn(instance) is a real, non-null LSN

        // Fabricate a prior token far below the real min_lsn: 20 zeros but the last hex digit '1' --
        // guaranteed smaller than any real captured min_lsn (min_lsn is only ever the LSN the capture
        // job itself observed, always well past all-but-one).
        const string fakePrior = "00000000000000000001";

        ISourceConnector connector = new SqlServerConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var spec = CdcSpec(table, prior: fakePrior, captureInstance: instance);
        var partition = Assert.Single(await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            await foreach (var b in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                b.Dispose();
            }
        });
        Assert.False(ex.IsTransient);
        Assert.Contains("retention gap", ex.Message, StringComparison.Ordinal);
        Assert.Contains("--full-refresh", ex.Message, StringComparison.Ordinal);
    }

    [SkippableFact]
    public async Task Empty_window_yields_zero_rows_and_still_advances_candidate()
    {
        const string table = "cdc_empty";
        var instance = await SeedAndCaptureAsync(table, rows: 2);

        ISourceConnector connector = new SqlServerConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var candidate = await SnapshotAndTokenAsync(source, table, instance);

        // Nothing changed since the snapshot -- from (increment(prior)) > to (still == prior's lsn).
        var (rows, newToken) = await PollAsync(source, table, instance, candidate);

        Assert.Empty(rows);
        Assert.NotNull(newToken); // unlike postgres's empty-backlog (no candidate), sqlserver ALWAYS
                                   // advances to @to -- nothing between (from, to] existed, so nothing
                                   // was skipped by advancing past it.
        Assert.Matches("^[0-9A-F]{20}$", newToken);
    }

    // ---- helpers ----

    private async Task<string> SeedAndCaptureAsync(string table, int rows)
    {
        await CdcTestSupport.EnsureDbCdcEnabledAsync(fixture.ConnectionString);
        await CdcTestSupport.ExecuteAsync(fixture.ConnectionString, $"if object_id('dbo.{table}') is not null drop table dbo.{table}");
        await CdcTestSupport.ExecuteAsync(
            fixture.ConnectionString, $"create table dbo.{table} (id int primary key, name nvarchar(50) not null)");
        await CdcTestSupport.ExecuteAsync(
            fixture.ConnectionString,
            $"insert into dbo.{table} (id, name) select value, concat('row-', value) " +
            $"from generate_series(1, {rows})");

        var instance = $"dbo_{table}";
        // @supports_net_changes = 0 deliberately leaves cdc.index_columns
        // empty for this instance, exercising DiscoverKeyColumnsAsync's primary-key fallback.
        await CdcTestSupport.ExecuteAsync(
            fixture.ConnectionString,
            $"EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'{table}', " +
            $"@role_name = NULL, @supports_net_changes = 0, @capture_instance = N'{instance}'");

        await CdcTestSupport.WaitForCapturePrimedAsync(fixture.ConnectionString, instance);
        return instance;
    }

    private async Task<string> SnapshotAndTokenAsync(ISource source, string table, string instance)
    {
        var partition = Assert.Single(await source.PlanReadAsync(
            CdcSpec(table, prior: null, instance), ReadHints.None, CancellationToken.None));
        await foreach (var b in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            b.Dispose();
        }

        Assert.True(((ISyncStatePartition)partition).TryGetSyncStateCandidate(out var candidate));
        Assert.NotNull(candidate);
        return candidate;
    }

    private sealed record Change(string Op, string Lsn, bool ChangedAtNull, int? Id, string? Name);

    private async Task<(List<Change> Rows, string? Token)> PollAsync(
        ISource source, string table, string instance, string prior)
    {
        var partition = Assert.Single(await source.PlanReadAsync(
            CdcSpec(table, prior, instance), ReadHints.None, CancellationToken.None));

        var rows = new List<Change>();
        await foreach (var b in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            var op = (StringArray)b.Column(0);
            var lsn = (StringArray)b.Column(1);
            var changedAt = b.Column(2);
            var id = (Int32Array)b.Column(3);
            var name = (StringArray)b.Column(4);
            for (var i = 0; i < b.Length; i++)
            {
                rows.Add(new Change(
                    op.GetString(i), lsn.GetString(i), changedAt.IsNull(i),
                    id.IsNull(i) ? null : id.GetValue(i), name.IsNull(i) ? null : name.GetString(i)));
            }

            b.Dispose();
        }

        var token = ((ISyncStatePartition)partition).TryGetSyncStateCandidate(out var c) ? c : null;
        return (rows, token);
    }

    private async Task ExecuteOnMasterAsync(string sql)
    {
        var csb = new SqlConnectionStringBuilder(fixture.ConnectionString) { InitialCatalog = "master" };
        await CdcTestSupport.ExecuteAsync(csb.ConnectionString, sql);
    }
}
