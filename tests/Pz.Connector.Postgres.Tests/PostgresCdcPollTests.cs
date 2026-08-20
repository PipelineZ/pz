using Apache.Arrow;
using Npgsql;
using NpgsqlTypes;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Postgres.Tests;

/// <summary>The bounded pgoutput poll path (PriorSyncState != null).
/// Each test first runs the first-run snapshot to create the pz-owned slot and obtain the
/// resume token, mutates the source, then drives a second read against that token and asserts the
/// change-row contract, LSN ordering, replay determinism, and commit-gated slot confirmation. Shares the
/// <c>wal_level=logical</c> container with <see cref="PostgresCdcSnapshotTests"/>.</summary>
[Collection("postgres-cdc")]
public sealed class PostgresCdcPollTests(PostgresCdcContainerFixture fixture)
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
    private static DatasetSpec Snapshot(string table) =>
        new("pg", table, new Dictionary<string, object?>())
        {
            ChangeCapture = true,
            PriorSyncState = null,
        };

    private static DatasetSpec Poll(string table, string prior, string? idle = null)
    {
        var options = new Dictionary<string, object?>();
        if (idle is not null)
        {
            options["poll_idle_timeout"] = idle;
        }

        return new DatasetSpec("pg", table, options) { ChangeCapture = true, PriorSyncState = prior };
    }

    private sealed record Change(
        string Op, string Lsn, bool ChangedAtNull, int? Id, string? Name,
        decimal? Price = null, bool? Active = null, DateTimeOffset? CreatedAt = null);

    [SkippableFact]
    public async Task Insert_update_delete_yield_change_rows_with_ordered_lsns()
    {
        var table = await SeedTableAsync("cdc_poll1", rows: 2);
        // Widen past int/text: exercises the Decimal128/Boolean/Timestamp binary-decode branches of
        // DecodeAsync against a real replication stream, not just the int/text ones the rest of this suite
        // covers.
        await ExecuteAsync(
            $"alter table public.{table} add column price numeric(12,2), " +
            "add column active boolean, add column created_at timestamptz");
        await CreatePublicationAsync("pz_pg", table);
        await DropSlotIfExistsAsync(PostgresCdc.SlotName(Snapshot(table)));

        ISourceConnector connector = new PostgresConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var candidate = await SnapshotAndTokenAsync(source, table);

        await ExecuteAsync(
            $"insert into public.{table} (id, name, price, active, created_at) " +
            "values (3, 'three', 19.99, true, '2026-07-24 12:34:56+00')");
        await ExecuteAsync($"update public.{table} set name = 'one-updated' where id = 1");
        await ExecuteAsync($"delete from public.{table} where id = 2");

        var (rows, newToken) = await PollAsync(source, table, candidate);

        Assert.Equal(3, rows.Count);
        var insert = Assert.Single(rows, r => r.Op == "insert");
        var update = Assert.Single(rows, r => r.Op == "update");
        var delete = Assert.Single(rows, r => r.Op == "delete");

        Assert.Equal(3, insert.Id);
        Assert.Equal("three", insert.Name);
        Assert.Equal(19.99m, insert.Price);
        Assert.Equal(true, insert.Active);
        Assert.Equal(new DateTimeOffset(2026, 7, 24, 12, 34, 56, TimeSpan.Zero), insert.CreatedAt);
        Assert.Equal(1, update.Id);
        Assert.Equal("one-updated", update.Name);
        // Delete carries the key column non-null (REPLICA IDENTITY DEFAULT); the non-key column is null.
        Assert.Equal(2, delete.Id);
        Assert.Null(delete.Name);

        foreach (var r in rows)
        {
            Assert.False(r.ChangedAtNull, "poll change rows carry the commit timestamp");
            Assert.Matches("^[0-9A-F]{16}-[0-9]{9}$", r.Lsn);
        }

        // _pz_lsn strictly increasing in emission order (fixed-width hex + seq -> ordinal == numeric).
        for (var i = 1; i < rows.Count; i++)
        {
            Assert.True(string.CompareOrdinal(rows[i].Lsn, rows[i - 1].Lsn) > 0, "lsns strictly increasing");
        }

        // Token advances to the last consumed commit's LSN (bare 16-hex form) == last row's lsn prefix.
        Assert.NotNull(newToken);
        Assert.Matches("^[0-9A-F]{16}$", newToken);
        Assert.Equal(rows[^1].Lsn.Split('-')[0], newToken);
    }

    [SkippableFact]
    public async Task Empty_backlog_yields_no_rows_and_no_candidate()
    {
        var table = await SeedTableAsync("cdc_poll2", rows: 2);
        await CreatePublicationAsync("pz_pg", table);
        await DropSlotIfExistsAsync(PostgresCdc.SlotName(Snapshot(table)));

        ISourceConnector connector = new PostgresConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var candidate = await SnapshotAndTokenAsync(source, table);

        // Nothing changed since the snapshot; poll_idle_timeout bounds the caught-up wait to 3s.
        var (rows, newToken) = await PollAsync(source, table, candidate, idle: "3s");

        Assert.Empty(rows);
        Assert.Null(newToken); // TryGetSyncStateCandidate false -> stored token unchanged
    }

    [SkippableFact]
    public async Task Replaying_the_same_window_lands_identical_rows_and_does_not_advance_flush()
    {
        var table = await SeedTableAsync("cdc_poll3", rows: 2);
        await CreatePublicationAsync("pz_pg", table);
        var slot = PostgresCdc.SlotName(Snapshot(table));
        await DropSlotIfExistsAsync(slot);

        ISourceConnector connector = new PostgresConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var candidate = await SnapshotAndTokenAsync(source, table);

        await ExecuteAsync($"insert into public.{table} (id, name) values (5, 'five')");

        var (rows1, _) = await PollAsync(source, table, candidate);
        var flushAfter1 = await ConfirmedFlushAsync(slot);
        var (rows2, _) = await PollAsync(source, table, candidate);
        var flushAfter2 = await ConfirmedFlushAsync(slot);

        // Identical rows both times: the slot confirm never advanced past the persisted token during read.
        Assert.Equal(rows1, rows2);
        Assert.Single(rows1);
        Assert.Equal(5, rows1[0].Id);
        // confirmed_flush_lsn pinned at the persisted token across the replay (never advanced by consume).
        Assert.Equal(flushAfter1, flushAfter2);
        Assert.Equal(candidate, PostgresCdc.FormatLsn(NpgsqlLogSequenceNumber.Parse(flushAfter1)));
    }

    [SkippableFact]
    public async Task Confirm_happens_at_start_and_advances_only_across_runs()
    {
        var table = await SeedTableAsync("cdc_poll4", rows: 2);
        await CreatePublicationAsync("pz_pg", table);
        var slot = PostgresCdc.SlotName(Snapshot(table));
        await DropSlotIfExistsAsync(slot);

        ISourceConnector connector = new PostgresConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var snapshotToken = await SnapshotAndTokenAsync(source, table);

        await ExecuteAsync($"insert into public.{table} (id, name) values (7, 'seven')");

        var (rows1, token2) = await PollAsync(source, table, snapshotToken);
        Assert.Single(rows1);
        Assert.NotNull(token2);
        // Consume did NOT advance the flush: after poll 1 it is still pinned at the persisted snapshot token.
        var flushAfterPoll1 = PostgresCdc.FormatLsn(NpgsqlLogSequenceNumber.Parse(await ConfirmedFlushAsync(slot)));
        Assert.Equal(snapshotToken, flushAfterPoll1);

        // Poll 2 confirms the NEW token (token2) at start -> the flush position ADVANCES across the run
        // boundary from snapshotToken to token2. At-least-once: confirming AT a commit LSN re-includes the
        // transaction whose commit sits at that LSN (inclusive boundary), so the id=7 txn replays once --
        // the engine's collapse + idempotent merge absorb the overlap.
        var (rows2, token3) = await PollAsync(source, table, token2!, idle: "3s");
        var replayed = Assert.Single(rows2);
        Assert.Equal(7, replayed.Id);
        Assert.Equal(token2, token3); // same boundary txn re-consumed
        var flushAfterPoll2 = PostgresCdc.FormatLsn(NpgsqlLogSequenceNumber.Parse(await ConfirmedFlushAsync(slot)));
        Assert.Equal(token2, flushAfterPoll2);
        Assert.NotEqual(snapshotToken, flushAfterPoll2);
    }

    [SkippableFact]
    public async Task Intra_transaction_rows_share_commit_lsn_with_incrementing_seq()
    {
        var table = await SeedTableAsync("cdc_poll5", rows: 2);
        await CreatePublicationAsync("pz_pg", table);
        await DropSlotIfExistsAsync(PostgresCdc.SlotName(Snapshot(table)));

        ISourceConnector connector = new PostgresConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var candidate = await SnapshotAndTokenAsync(source, table);

        // One transaction: insert then update the SAME key -> two change rows in one commit.
        await ExecuteAsync(
            $"begin; insert into public.{table} (id, name) values (10, 'ten'); " +
            $"update public.{table} set name = 'ten-updated' where id = 10; commit;");

        var (rows, _) = await PollAsync(source, table, candidate);

        Assert.Equal(2, rows.Count);
        var prefixes = rows.Select(r => r.Lsn.Split('-')[0]).Distinct().ToList();
        Assert.Single(prefixes); // same commit LSN
        Assert.Equal("000000000", rows[0].Lsn.Split('-')[1]);
        Assert.Equal("000000001", rows[1].Lsn.Split('-')[1]);
        Assert.Equal("insert", rows[0].Op);
        Assert.Equal("update", rows[1].Op);
    }

    [SkippableFact]
    public async Task Key_change_emits_a_delete_for_the_old_key_before_the_new_row()
    {
        var table = await SeedTableAsync("cdc_poll6", rows: 2);
        await CreatePublicationAsync("pz_pg", table);
        await DropSlotIfExistsAsync(PostgresCdc.SlotName(Snapshot(table)));

        ISourceConnector connector = new PostgresConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var candidate = await SnapshotAndTokenAsync(source, table);

        // REPLICA IDENTITY DEFAULT: postgres sends the old key ONLY when the key changed, so this update
        // arrives as an IndexUpdateMessage carrying both the old key and the new row.
        await ExecuteAsync($"update public.{table} set id = 99 where id = 1");

        var (rows, _) = await PollAsync(source, table, candidate);

        Assert.Equal(2, rows.Count);
        var delete = Assert.Single(rows, r => r.Op == "delete");
        var update = Assert.Single(rows, r => r.Op == "update");
        // Without the old key's delete the merge target keeps a ghost row under the pre-change key forever
        // (--full-refresh cannot heal it -- merge never removes rows a snapshot does not mention).
        Assert.Equal(1, delete.Id);
        Assert.Equal(99, update.Id);
        Assert.Equal("row-1", update.Name);
        // Delete first: last-event-per-key collapse must see the delete as the old key's final event and
        // the upsert as the new key's, so ordering is what keeps both effects.
        Assert.True(string.CompareOrdinal(delete.Lsn, update.Lsn) < 0, "old-key delete precedes the new row");
    }

    // TRUNCATE cannot be expressed as row-level changes, so the poll must fail loud rather than
    // silently skip it.
    [SkippableFact]
    public async Task Truncating_the_source_fails_the_poll_and_names_full_refresh()
    {
        var table = await SeedTableAsync("cdc_poll7", rows: 3);
        await CreatePublicationAsync("pz_pg", table);
        await DropSlotIfExistsAsync(PostgresCdc.SlotName(Snapshot(table)));

        ISourceConnector connector = new PostgresConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var candidate = await SnapshotAndTokenAsync(source, table);

        await ExecuteAsync($"truncate table public.{table}");

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => PollAsync(source, table, candidate));
        Assert.Contains("truncate", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("--full-refresh", ex.Message, StringComparison.Ordinal);
        // The remediation must say --full-refresh is not sufficient ALONE: a merge output keeps rows its
        // input omits, so a bare re-snapshot would go green over the very divergence this error exists to
        // stop. Naming only --full-refresh here would ship the same wrong-remediation defect it warns about.
        Assert.Contains("destination", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.False(ex.IsTransient); // re-polling the same window can only hit the same truncate again
    }

    // A FOR ALL TABLES publication streams every table, so the decode must admit only this dataset's
    // relation as change rows.
    [SkippableFact]
    public async Task Changes_to_other_tables_under_a_for_all_tables_publication_are_ignored()
    {
        var table = await SeedTableAsync("cdc_poll8", rows: 2);
        // Same column names as the polled table on purpose: a decode that ignores the relation would land
        // this table's values in our columns as plausible-looking rows rather than obvious garbage.
        var other = await SeedTableAsync("cdc_poll8_other", rows: 2);
        await ExecuteAsync("drop publication if exists pz_pg");
        await ExecuteAsync("create publication pz_pg for all tables");
        await DropSlotIfExistsAsync(PostgresCdc.SlotName(Snapshot(table)));

        try
        {
            ISourceConnector connector = new PostgresConnector();
            await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
            var candidate = await SnapshotAndTokenAsync(source, table);

            await ExecuteAsync($"insert into public.{other} (id, name) values (3, 'other-three')");
            await ExecuteAsync($"update public.{other} set name = 'other-updated' where id = 1");
            await ExecuteAsync($"delete from public.{other} where id = 2");

            var (rows, _) = await PollAsync(source, table, candidate);
            Assert.Empty(rows); // our table never changed

            // A truncate of an unrelated table is likewise none of this dataset's business.
            await ExecuteAsync($"truncate table public.{other}");
            var (afterTruncate, _) = await PollAsync(source, table, candidate);
            Assert.Empty(afterTruncate);
        }
        finally
        {
            await ExecuteAsync("drop publication if exists pz_pg");
        }
    }

    // REPLICA IDENTITY FULL sends the old row on EVERY update, so a delete may be emitted only when
    // the key actually changed.
    [SkippableFact]
    public async Task Full_replica_identity_emits_a_delete_only_when_the_key_changes()
    {
        var table = await SeedTableAsync("cdc_poll9", rows: 2);
        await ExecuteAsync($"alter table public.{table} replica identity full");
        await CreatePublicationAsync("pz_pg", table);
        await DropSlotIfExistsAsync(PostgresCdc.SlotName(Snapshot(table)));

        ISourceConnector connector = new PostgresConnector();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var candidate = await SnapshotAndTokenAsync(source, table);

        await ExecuteAsync($"update public.{table} set name = 'renamed' where id = 1"); // key untouched
        await ExecuteAsync($"update public.{table} set id = 99 where id = 2");          // key changed

        var (rows, _) = await PollAsync(source, table, candidate);

        // 3 rows, not 4: the non-key update must NOT manufacture a spurious delete of its own key.
        Assert.Equal(3, rows.Count);
        Assert.Single(rows, r => r.Op == "delete" && r.Id == 2);
        Assert.Equal(2, rows.Count(r => r.Op == "update"));
        Assert.Single(rows, r => r.Op == "update" && r.Id == 1 && r.Name == "renamed");
        Assert.Single(rows, r => r.Op == "update" && r.Id == 99);
    }

    // ---- helpers ----

    private async Task<string> SnapshotAndTokenAsync(ISource source, string table)
    {
        var partition = Assert.Single(
            await source.PlanReadAsync(Snapshot(table), ReadHints.None, CancellationToken.None));
        await foreach (var b in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            b.Dispose();
        }

        Assert.True(((ISyncStatePartition)partition).TryGetSyncStateCandidate(out var candidate));
        Assert.NotNull(candidate);
        return candidate;
    }

    private async Task<(List<Change> Rows, string? Token)> PollAsync(
        ISource source, string table, string prior, string? idle = null)
    {
        var partition = Assert.Single(
            await source.PlanReadAsync(Poll(table, prior, idle), ReadHints.None, CancellationToken.None));

        var rows = new List<Change>();
        await foreach (var b in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            var op = (StringArray)b.Column(0);
            var lsn = (StringArray)b.Column(1);
            var changedAt = b.Column(2);
            var id = (Int32Array)b.Column(3);
            var name = (StringArray)b.Column(4);
            // Wide columns (price/active/created_at) are only present on the one test that alters its table
            // to add them; every other caller's schema stops at column 4.
            var wide = b.Schema.FieldsList.Count > 5;
            var price = wide ? (Decimal128Array)b.Column(5) : null;
            var active = wide ? (BooleanArray)b.Column(6) : null;
            var createdAt = wide ? (TimestampArray)b.Column(7) : null;
            for (var i = 0; i < b.Length; i++)
            {
                rows.Add(new Change(
                    op.GetString(i), lsn.GetString(i), changedAt.IsNull(i),
                    id.IsNull(i) ? null : id.GetValue(i), name.IsNull(i) ? null : name.GetString(i),
                    price is null || price.IsNull(i) ? null : price.GetValue(i),
                    active is null || active.IsNull(i) ? null : active.GetValue(i),
                    createdAt is null || createdAt.IsNull(i) ? null : createdAt.GetTimestamp(i)));
            }

            b.Dispose();
        }

        string? token = ((ISyncStatePartition)partition).TryGetSyncStateCandidate(out var c) ? c : null;
        return (rows, token);
    }

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

    private async Task<string> SeedTableAsync(string table, int rows)
    {
        await ExecuteAsync($"drop table if exists public.{table} cascade");
        await ExecuteAsync($"create table public.{table} (id integer primary key, name text)");
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

    private async Task<string> ConfirmedFlushAsync(string slot)
    {
        await using var conn = await OpenAsync();
        await using var cmd = new NpgsqlCommand(
            "select confirmed_flush_lsn::text from pg_replication_slots where slot_name = @name", conn);
        cmd.Parameters.AddWithValue("name", slot);
        return (string)(await cmd.ExecuteScalarAsync().ConfigureAwait(false))!;
    }
}
