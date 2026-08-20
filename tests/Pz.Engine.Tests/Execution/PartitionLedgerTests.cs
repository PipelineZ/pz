using Pz.DuckDb;
using Pz.Engine.Execution;

namespace Pz.Engine.Tests.Execution;

public sealed class PartitionLedgerTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));
    private DuckSession _duck = null!;

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _duck = DuckSession.Open(Path.Combine(_dir, "staging.duckdb"));
        await _duck.ExecuteAsync("create schema if not exists staging");
    }

    public async Task DisposeAsync()
    {
        await _duck.DisposeAsync();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public void Table_names_are_deterministic_and_safe()
    {
        var part = PartitionLedger.PartTable("abc123", "s3://bucket/some file.csv");
        var again = PartitionLedger.PartTable("abc123", "s3://bucket/some file.csv");
        var other = PartitionLedger.PartTable("abc123", "s3://bucket/other.csv");
        Assert.Equal(part, again);
        Assert.NotEqual(part, other);
        Assert.StartsWith("staging.__pz_part__abc123__", part, StringComparison.Ordinal);
        Assert.Matches("^staging\\.__pz_part__abc123__[0-9a-f]{16}$", part);
        Assert.StartsWith("staging.__pz_seg__abc123__", PartitionLedger.SegTable("abc123", "x"), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Ensure_is_idempotent_and_complete_moves_rows_atomically()
    {
        await PartitionLedger.EnsureAsync(_duck, default);
        await PartitionLedger.EnsureAsync(_duck, default);

        await _duck.ExecuteAsync("create table staging.main (id bigint)");
        var part = PartitionLedger.PartTable("n1", "p1");
        await _duck.ExecuteAsync($"create table {part} as select * from range(4) t(id)");

        await _duck.ExecuteTransactionAsync(
            PartitionLedger.CompleteStatements("node-1", "p1", "staging.main", part, 4), default);

        Assert.Equal(4L, await _duck.ScalarAsync<long>("select count(*) from staging.main"));
        var done = await PartitionLedger.ReadDoneAsync(_duck, "node-1", default);
        Assert.Equal(4L, done["p1"]);
        Assert.Equal(0L, await _duck.ScalarAsync<long>(
            "select count(*) from information_schema.tables where table_schema = 'staging' and table_name like '\\_\\_pz\\_part\\_\\_%' escape '\\'"));
    }

    [Fact]
    public async Task Checkpoint_statements_merge_segment_and_upsert_token()
    {
        await PartitionLedger.EnsureAsync(_duck, default);
        var part = PartitionLedger.PartTable("n2", "p1");
        var seg = PartitionLedger.SegTable("n2", "p1");
        await _duck.ExecuteAsync($"create table {part} as select * from range(2) t(id)");
        await _duck.ExecuteAsync($"create table {seg} as select * from range(2, 5) t(id)");

        await _duck.ExecuteTransactionAsync(
            PartitionLedger.CheckpointStatements("node-2", "p1", seg, part, "tok'en-1", 5), default);

        Assert.Equal(5L, await _duck.ScalarAsync<long>($"select count(*) from {part}"));
        var ckpts = await PartitionLedger.ReadCheckpointsAsync(_duck, "node-2", default);
        Assert.Equal(("tok'en-1", 5L), ckpts["p1"]);

        // Second checkpoint replaces the first (upsert semantics).
        await _duck.ExecuteAsync($"create table {seg} as select * from range(5, 6) t(id)");
        await _duck.ExecuteTransactionAsync(
            PartitionLedger.CheckpointStatements("node-2", "p1", seg, part, "token-2", 6), default);
        ckpts = await PartitionLedger.ReadCheckpointsAsync(_duck, "node-2", default);
        Assert.Equal(("token-2", 6L), ckpts["p1"]);
    }

    [Fact]
    public async Task Window_upsert_replaces_prior_bounds()
    {
        await PartitionLedger.EnsureAsync(_duck, default);
        await PartitionLedger.UpsertWindowAsync(_duck, "node-3", "1", "5", default);
        await PartitionLedger.UpsertWindowAsync(_duck, "node-3", "5", "9", default);
        Assert.Equal("5", await _duck.ScalarAsync<string>(
            $"select lower from {PartitionLedger.WindowTable} where node_id = 'node-3'"));
        Assert.Equal(1L, await _duck.ScalarAsync<long>(
            $"select count(*) from {PartitionLedger.WindowTable} where node_id = 'node-3'"));
    }

    [Fact]
    public async Task Cleanup_drops_all_segments_and_uncheckpointed_parts_only()
    {
        await PartitionLedger.EnsureAsync(_duck, default);
        var keptPart = PartitionLedger.PartTable("n4", "kept");
        var orphanPart = PartitionLedger.PartTable("n4", "orphan");
        var seg = PartitionLedger.SegTable("n4", "kept");
        var otherNodesPart = PartitionLedger.PartTable("zz", "foreign");
        foreach (var t in new[] { keptPart, orphanPart, seg, otherNodesPart })
        {
            await _duck.ExecuteAsync($"create table {t} (id bigint)");
        }

        await _duck.ExecuteAsync(
            $"insert into {PartitionLedger.CheckpointTable} values ('node-4', 'kept', 'tok', 0)");

        await PartitionLedger.CleanupLeftoversAsync(_duck, "node-4", "n4", default);

        long Count(string name) => _duck.ScalarAsync<long>(
            $"select count(*) from information_schema.tables where table_schema = 'staging' and table_name = '{name.Split('.')[1]}'").Result;
        Assert.Equal(1L, Count(keptPart));
        Assert.Equal(0L, Count(orphanPart));
        Assert.Equal(0L, Count(seg));
        Assert.Equal(1L, Count(otherNodesPart)); // other nodes' tables untouched
    }
}
