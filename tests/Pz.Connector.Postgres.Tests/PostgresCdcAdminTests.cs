using Npgsql;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Postgres.Tests;

/// <summary><c>PostgresSource</c>'s <see cref="IChangeCaptureAdmin"/> implementation
/// (`pz cdc status`/`drop`) against the same dedicated <c>wal_level=logical</c> container as
/// <see cref="PostgresCdcSnapshotTests"/>. A real snapshot creates the slot the status query then
/// reports on; drop tears the slot down and proves the next read re-snapshots from scratch.</summary>
[Collection("postgres-cdc")]
public sealed class PostgresCdcAdminTests(PostgresCdcContainerFixture fixture)
{
    private ConnectorConfig ValidConfig => new(new Dictionary<string, object?>
    {
        ["host"] = fixture.Host,
        ["port"] = fixture.Port,
        ["database"] = fixture.Database,
        ["user"] = fixture.User,
        ["password"] = fixture.Password,
    });

    // The dataset name IS the table, so one argument, not two.
    private static DatasetSpec CdcSpec(string table) =>
        new("pg", table, new Dictionary<string, object?>()) { ChangeCapture = true };

    [SkippableFact]
    public async Task Status_reports_slot_missing_before_first_run()
    {
        var table = await SeedTableAsync("cdc_admin_missing");
        await CreatePublicationAsync("pz_pg", table);
        var slot = PostgresCdc.SlotName(CdcSpec(table));
        await DropSlotIfExistsAsync(slot);

        ISourceConnector connector = new PostgresConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var admin = Assert.IsAssignableFrom<IChangeCaptureAdmin>(source);

        var status = await admin.GetChangeCaptureStatusAsync(CdcSpec(table), CancellationToken.None);

        Assert.False(status.Healthy);
        Assert.Equal(slot, status.PositionName);
        Assert.Null(status.RetainedBytes);
        Assert.Contains(status.Detail, d => d.Contains("slot not created yet", StringComparison.Ordinal));
    }

    [SkippableFact]
    public async Task Status_distinguishes_a_lost_slot_from_one_that_was_never_created()
    {
        var table = await SeedTableAsync("cdc_admin_lost");
        await CreatePublicationAsync("pz_pg", table);
        var spec = CdcSpec(table) with { PriorSyncState = "00000000019DD530" };
        await DropSlotIfExistsAsync(PostgresCdc.SlotName(spec));

        ISourceConnector connector = new PostgresConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var admin = Assert.IsAssignableFrom<IChangeCaptureAdmin>(source);

        var status = await admin.GetChangeCaptureStatusAsync(spec, CancellationToken.None);

        // A stored token means the dataset HAS run: the slot is gone, not pending, and the changes it was
        // holding WAL for are unrecoverable -- reporting "first run creates it" would hide a data loss.
        Assert.False(status.Healthy);
        Assert.DoesNotContain(status.Detail, d => d.Contains("slot not created yet", StringComparison.Ordinal));
        Assert.Contains(status.Detail, d => d.Contains("is GONE", StringComparison.Ordinal));
        Assert.Contains(status.Detail, d => d.Contains("--full-refresh", StringComparison.Ordinal));
    }

    [SkippableFact]
    public async Task Status_reports_unmet_prerequisites_when_slot_missing_and_no_publication()
    {
        var table = await SeedTableAsync("cdc_admin_prereq"); // no publication created
        await DropSlotIfExistsAsync(PostgresCdc.SlotName(CdcSpec(table)));

        ISourceConnector connector = new PostgresConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var admin = Assert.IsAssignableFrom<IChangeCaptureAdmin>(source);

        var status = await admin.GetChangeCaptureStatusAsync(CdcSpec(table), CancellationToken.None);

        Assert.False(status.Healthy);
        Assert.Contains(status.Detail, d => d.Contains("CREATE PUBLICATION", StringComparison.Ordinal));
    }

    [SkippableFact]
    public async Task Status_reports_healthy_slot_with_nonnegative_retained_bytes_after_a_real_run()
    {
        var table = await SeedTableAsync("cdc_admin_healthy", rows: 4);
        await CreatePublicationAsync("pz_pg", table);
        var slot = PostgresCdc.SlotName(CdcSpec(table));
        await DropSlotIfExistsAsync(slot);

        ISourceConnector connector = new PostgresConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);

        // A real first-run snapshot: this is what actually creates the replication slot server-side.
        var partition = Assert.Single(
            await source.PlanReadAsync(CdcSpec(table), ReadHints.None, CancellationToken.None));
        await foreach (var b in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            b.Dispose();
        }

        var admin = Assert.IsAssignableFrom<IChangeCaptureAdmin>(source);
        var status = await admin.GetChangeCaptureStatusAsync(CdcSpec(table), CancellationToken.None);

        Assert.True(status.Healthy);
        Assert.Equal(slot, status.PositionName);
        Assert.NotNull(status.RetainedBytes);
        Assert.True(status.RetainedBytes >= 0);
        Assert.Empty(status.Detail);
    }

    [SkippableFact]
    public async Task Drop_removes_the_slot_and_a_second_drop_is_a_no_op()
    {
        var table = await SeedTableAsync("cdc_admin_drop", rows: 3);
        await CreatePublicationAsync("pz_pg", table);
        var spec = CdcSpec(table);
        var slot = PostgresCdc.SlotName(spec);
        await DropSlotIfExistsAsync(slot);

        ISourceConnector connector = new PostgresConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var partition = Assert.Single(await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None));
        await foreach (var b in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            b.Dispose();
        }

        Assert.True(await SlotExistsAsync(slot));

        var admin = Assert.IsAssignableFrom<IChangeCaptureAdmin>(source);
        await admin.DropChangeCaptureStateAsync(spec, CancellationToken.None);

        Assert.False(await SlotExistsAsync(slot));

        // Idempotent: dropping an already-dropped dataset never throws.
        await admin.DropChangeCaptureStateAsync(spec, CancellationToken.None);
        Assert.False(await SlotExistsAsync(slot));

        // pz cdc status now reports the slot as missing again (informational-unhealthy).
        var status = await admin.GetChangeCaptureStatusAsync(spec, CancellationToken.None);
        Assert.False(status.Healthy);
        Assert.Contains(status.Detail, d => d.Contains("slot not created yet", StringComparison.Ordinal));

        // The next read re-snapshots from scratch (first-run behavior), rather than resuming a poll
        // against a slot that no longer exists.
        var partition2 = Assert.Single(await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None));
        var rows = 0;
        await foreach (var b in partition2.ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            rows += b.Length;
            b.Dispose();
        }

        Assert.Equal(3, rows);
        Assert.True(await SlotExistsAsync(slot));
    }

    // ---- helpers ----

    private async Task<NpgsqlConnection> OpenAsync()
    {
        var conn = new NpgsqlConnection(fixture.ConnectionString);
        await conn.OpenAsync().ConfigureAwait(false);
        return conn;
    }

    private async Task ExecuteAsync(string sql)
    {
        await using var conn = await OpenAsync();
        await using var cmd = new NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    private async Task<string> SeedTableAsync(string table, int rows = 2)
    {
        await ExecuteAsync($"drop table if exists public.{table} cascade");
        await ExecuteAsync($"create table public.{table} (id integer primary key, name text not null)");
        await ExecuteAsync(
            $"insert into public.{table} (id, name) select i, 'row-' || i from generate_series(1, {rows}) i");
        return table;
    }

    private async Task CreatePublicationAsync(string publication, string table)
    {
        await ExecuteAsync($"drop publication if exists {publication}");
        await ExecuteAsync($"create publication {publication} for table public.{table}");
    }

    private async Task DropSlotIfExistsAsync(string slot) =>
        await ExecuteAsync(
            $"select pg_drop_replication_slot(slot_name) from pg_replication_slots where slot_name = '{slot}'");

    private async Task<bool> SlotExistsAsync(string slot)
    {
        await using var conn = await OpenAsync();
        await using var cmd = new NpgsqlCommand("select 1 from pg_replication_slots where slot_name = @name", conn);
        cmd.Parameters.AddWithValue("name", slot);
        return await cmd.ExecuteScalarAsync().ConfigureAwait(false) is not null;
    }
}
