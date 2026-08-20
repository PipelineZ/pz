using Apache.Arrow;
using Apache.Arrow.Types;
using Npgsql;
using NpgsqlTypes;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Postgres.Tests;

/// <summary>The Postgres cdc source's foundation -- prerequisites validation, pz-owned
/// replication-slot lifecycle, and the first-run snapshot -- against a dedicated
/// <c>wal_level=logical</c> container (<see cref="PostgresCdcContainerFixture"/>). The pgoutput poll
/// path (PriorSyncState != null) is covered by <see cref="PostgresCdcPollTests"/>.</summary>
[Collection("postgres-cdc")]
public sealed class PostgresCdcSnapshotTests(PostgresCdcContainerFixture fixture)
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
    private static DatasetSpec CdcSpec(string table, string? slot = null) =>
        new("pg", table, new Dictionary<string, object?>())
        {
            ChangeCapture = true,
            ChangeCaptureSlot = slot,
            PriorSyncState = null,
        };

    // Pure helpers -- no container needed.
    [Fact]
    public void Slot_name_defaults_and_override()
    {
        // default: pz_{source}_{dataset} lowercased, non-[a-z0-9_] -> '_'
        Assert.Equal("pz_pg_my_orders", PostgresCdc.SlotName(CdcSpec("My-Orders")));
        // spec.ChangeCaptureSlot overrides verbatim
        Assert.Equal("custom_slot", PostgresCdc.SlotName(CdcSpec("orders", slot: "custom_slot")));
        // default publication: pz_{source} sanitized
        Assert.Equal("pz_my_src", PostgresCdc.PublicationName(
            new DatasetSpec("My.Src", "orders", new Dictionary<string, object?>())));
    }

    [Fact]
    public void Lsn_canonical_forms_are_exact()
    {
        var lsn = (NpgsqlLogSequenceNumber)0x016B_3C48UL;
        Assert.Equal("00000000016B3C48", PostgresCdc.FormatLsn(lsn));
        Assert.Equal(lsn, PostgresCdc.ParseLsn("00000000016B3C48"));
        Assert.Equal("00000000016B3C48", PostgresCdc.FormatLsn(PostgresCdc.ParseLsn("00000000016B3C48")));
    }

    [SkippableFact]
    public async Task Prereqs_missing_publication_yields_one_create_publication_line()
    {
        var table = await SeedTableAsync("cdc_prereq1");
        var spec = CdcSpec(table);

        await using var conn = await OpenAsync();
        var unmet = await PostgresCdc.ValidatePrerequisitesAsync(conn, spec, CancellationToken.None);

        var line = Assert.Single(unmet);
        Assert.Contains("CREATE PUBLICATION", line, StringComparison.Ordinal);
        Assert.Contains("pz_pg", line, StringComparison.Ordinal);
        Assert.Contains(table, line, StringComparison.Ordinal);

        // A DIFFERENT publication covering the table does NOT satisfy the prereq -- only the specific
        // publication pz names at StartReplication (pz_pg here) counts, otherwise the first run passes
        // and the poll fails.
        await CreatePublicationAsync("some_other_pub", table);
        var withForeignPub = await PostgresCdc.ValidatePrerequisitesAsync(conn, spec, CancellationToken.None);
        var foreignLine = Assert.Single(withForeignPub);
        Assert.Contains("CREATE PUBLICATION pz_pg", foreignLine, StringComparison.Ordinal);

        // with the SPECIFIC publication present + logical WAL + REPLICATION superuser -> all met
        await CreatePublicationAsync("pz_pg", table);
        var after = await PostgresCdc.ValidatePrerequisitesAsync(conn, spec, CancellationToken.None);
        Assert.Empty(after);
    }

    [SkippableFact]
    public async Task Prereqs_do_not_flag_server_version_on_pg16_container()
    {
        var table = await SeedTableAsync("cdc_prereq_ver");
        await CreatePublicationAsync("pz_pg", table);
        var spec = CdcSpec(table);

        await using var conn = await OpenAsync();
        var unmet = await PostgresCdc.ValidatePrerequisitesAsync(conn, spec, CancellationToken.None);

        Assert.Empty(unmet);
        Assert.DoesNotContain(unmet, line => line.Contains("PostgreSQL 14", StringComparison.Ordinal));
    }

    // A column list on a publication is a PG15+ feature the prerequisites must refuse.
    [SkippableFact]
    public async Task Prereqs_refuse_column_list_publication()
    {
        const string table = "cdc_prereq_collist";
        await ExecuteAsync($"drop table if exists public.{table} cascade");
        // A THIRD column ("extra") the (id, name) column list below omits -- the case the prereq must
        // catch: it would decode as null off every pgoutput tuple even though the row itself has a value.
        await ExecuteAsync(
            $"create table public.{table} (id integer primary key, name text not null, extra text not null)");
        var spec = CdcSpec(table);
        const string publication = "pz_pg";

        await ExecuteAsync($"drop publication if exists {publication}");
        await ExecuteAsync($"create publication {publication} for table public.{table} (id, name)");
        try
        {
            await using var conn = await OpenAsync();
            var unmet = await PostgresCdc.ValidatePrerequisitesAsync(conn, spec, CancellationToken.None);

            var line = Assert.Single(unmet);
            Assert.Contains("declares a column list", line, StringComparison.Ordinal);
            Assert.Contains(publication, line, StringComparison.Ordinal);
            Assert.Contains(table, line, StringComparison.Ordinal);
        }
        finally
        {
            await ExecuteAsync($"drop publication if exists {publication}");
        }
    }

    [SkippableFact]
    public async Task Read_on_unmet_prereq_throws_nontransient_with_statement()
    {
        var table = await SeedTableAsync("cdc_prereq2"); // no publication
        await DropSlotIfExistsAsync("pz_pg_cdc_prereq2ds");

        ISourceConnector connector = new PostgresConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var partitions = await source.PlanReadAsync(CdcSpec(table), ReadHints.None, CancellationToken.None);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            await foreach (var batch in Assert.Single(partitions).ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                batch.Dispose();
            }
        });
        Assert.False(ex.IsTransient);
        Assert.Contains("CREATE PUBLICATION", ex.Message, StringComparison.Ordinal);
    }

    [SkippableFact]
    public async Task First_run_snapshot_yields_change_rows_and_key_columns()
    {
        var table = await SeedTableAsync("cdc_snap", rows: 5);
        await CreatePublicationAsync("pz_pg", table);
        await DropSlotIfExistsAsync(PostgresCdc.SlotName(CdcSpec(table)));

        ISourceConnector connector = new PostgresConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var partition = Assert.Single(await source.PlanReadAsync(CdcSpec(table), ReadHints.None, CancellationToken.None));

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
            Assert.Equal(ArrowTypeId.String, schema.FieldsList[0].DataType.TypeId);
            Assert.Equal(ArrowTypeId.String, schema.FieldsList[1].DataType.TypeId);
            Assert.Equal(ArrowTypeId.Timestamp, schema.FieldsList[2].DataType.TypeId);
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
                    Assert.Equal("0000000000000000-000000000", lsn.GetString(i));
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

        // Key columns == primary key, discovered post-read.
        var keyPartition = Assert.IsAssignableFrom<IChangeCapturePartition>(partition);
        Assert.True(keyPartition.TryGetChangeKeyColumns(out var keys));
        Assert.Equal(["id"], keys);

        // sync-state candidate == FormatLsn(consistent point) == confirmed_flush_lsn canonical form
        var syncPartition = Assert.IsAssignableFrom<ISyncStatePartition>(partition);
        Assert.True(syncPartition.TryGetSyncStateCandidate(out var candidate));
        Assert.NotNull(candidate);
        Assert.Matches("^[0-9A-F]{16}$", candidate);
        var flush = await ConfirmedFlushAsync(PostgresCdc.SlotName(CdcSpec(table)));
        Assert.Equal(PostgresCdc.FormatLsn(NpgsqlLogSequenceNumber.Parse(flush)), candidate);
    }

    [SkippableFact]
    public async Task Slot_conflict_and_full_refresh_recreate()
    {
        var table = await SeedTableAsync("cdc_conflict", rows: 3);
        await CreatePublicationAsync("pz_pg", table);
        var slot = PostgresCdc.SlotName(CdcSpec(table));
        await DropSlotIfExistsAsync(slot);

        // a slot with a DIFFERENT plugin -> conflict
        await CreateLogicalSlotAsync(slot, "test_decoding");

        ISourceConnector connector = new PostgresConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var partition = Assert.Single(await source.PlanReadAsync(CdcSpec(table), ReadHints.None, CancellationToken.None));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            await foreach (var b in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                b.Dispose();
            }
        });
        Assert.False(ex.IsTransient);
        Assert.Contains("slot conflict", ex.Message, StringComparison.Ordinal);
        Assert.Contains(slot, ex.Message, StringComparison.Ordinal);
        Assert.Contains("pz cdc drop", ex.Message, StringComparison.Ordinal);

        await DropSlotIfExistsAsync(slot);

        // an existing pz pgoutput slot + first run (PriorSyncState null) -> dropped and recreated, fresh snapshot
        await CreateLogicalSlotAsync(slot, "pgoutput");
        var partition2 = Assert.Single(await source.PlanReadAsync(CdcSpec(table), ReadHints.None, CancellationToken.None));
        var count = 0;
        await foreach (var b in partition2.ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            count += b.Length;
            b.Dispose();
        }

        Assert.Equal(3, count);
    }

    [SkippableFact]
    public async Task GetSchema_prepends_pz_fields()
    {
        var table = await SeedTableAsync("cdc_schema");

        ISourceConnector connector = new PostgresConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var schema = await source.GetSchemaAsync(CdcSpec(table), CancellationToken.None);

        Assert.Equal("_pz_op", schema.Schema.FieldsList[0].Name);
        Assert.Equal("_pz_lsn", schema.Schema.FieldsList[1].Name);
        Assert.Equal("_pz_changed_at", schema.Schema.FieldsList[2].Name);
        Assert.Equal("id", schema.Schema.FieldsList[3].Name);
        Assert.Equal("name", schema.Schema.FieldsList[4].Name);
        Assert.Equal(5, schema.Schema.FieldsList.Count);
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

    private async Task CreateLogicalSlotAsync(string slot, string plugin) =>
        await ExecuteAsync($"select pg_create_logical_replication_slot('{slot}', '{plugin}')");

    private async Task DropSlotIfExistsAsync(string slot) =>
        await ExecuteAsync(
            $"select pg_drop_replication_slot(slot_name) from pg_replication_slots where slot_name = '{slot}'");

    private async Task<string> ConfirmedFlushAsync(string slot)
    {
        await using var conn = await OpenAsync();
        await using var cmd = new NpgsqlCommand(
            "select confirmed_flush_lsn::text from pg_replication_slots where slot_name = @name", conn);
        cmd.Parameters.AddWithValue("name", slot);
        return (string)(await cmd.ExecuteScalarAsync().ConfigureAwait(false))!;
    }
}
