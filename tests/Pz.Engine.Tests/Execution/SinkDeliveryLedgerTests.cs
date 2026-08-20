using Pz.DuckDb;
using Pz.Engine.Execution;

namespace Pz.Engine.Tests.Execution;

/// <summary>The sink delivery ledger — DDL idempotence, upsert/read/clear
/// round-trip, the order-independent content fingerprint, and the catalog-qualified read that
/// pz retry's ATTACH path uses. Real DuckSession, mirroring PartitionLedgerTests.</summary>
public sealed class SinkDeliveryLedgerTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-sink-ledger-tests", Guid.NewGuid().ToString("N"));
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
    public async Task Ensure_is_idempotent_and_roundtrip_works()
    {
        await SinkDeliveryLedger.EnsureAsync(_duck, default);
        await SinkDeliveryLedger.EnsureAsync(_duck, default);

        Assert.Null(await SinkDeliveryLedger.ReadAsync(_duck, "node-1", default));

        var fp = new SinkDeliveryLedger.Fingerprint(42, "12345");
        await _duck.ExecuteTransactionAsync(SinkDeliveryLedger.UpsertStatements("node-1", 30, fp), default);
        var row = await SinkDeliveryLedger.ReadAsync(_duck, "node-1", default);
        Assert.Equal(new SinkDeliveryLedger.DeliveryRow(30, 42, "12345"), row);

        // Upsert overwrites (delete+insert), never duplicates.
        await _duck.ExecuteTransactionAsync(SinkDeliveryLedger.UpsertStatements("node-1", 35, fp), default);
        row = await SinkDeliveryLedger.ReadAsync(_duck, "node-1", default);
        Assert.Equal(35, row!.AcknowledgedRows);

        await SinkDeliveryLedger.ClearAsync(_duck, "node-1", default);
        Assert.Null(await SinkDeliveryLedger.ReadAsync(_duck, "node-1", default));
    }

    [Fact]
    public async Task Fingerprint_is_order_independent_and_content_sensitive()
    {
        await _duck.ExecuteAsync("create table staging.a (id bigint, name varchar)");
        await _duck.ExecuteAsync("insert into staging.a values (1,'x'),(2,'y'),(3,'z')");
        await _duck.ExecuteAsync("create table staging.b (id bigint, name varchar)");
        await _duck.ExecuteAsync("insert into staging.b values (3,'z'),(1,'x'),(2,'y')");
        await _duck.ExecuteAsync("create table staging.c (id bigint, name varchar)");
        await _duck.ExecuteAsync("insert into staging.c values (1,'x'),(2,'y'),(3,'DIFFERENT')");

        var a = await SinkDeliveryLedger.FingerprintAsync(_duck, "staging.a", default);
        var b = await SinkDeliveryLedger.FingerprintAsync(_duck, "staging.b", default);
        var c = await SinkDeliveryLedger.FingerprintAsync(_duck, "staging.c", default);

        Assert.Equal(a, b);            // same multiset, different physical order
        Assert.Equal(3, a.Count);
        Assert.NotEqual(a.Hash, c.Hash); // same count, different content
    }

    [Fact]
    public async Task Empty_relation_fingerprints_to_zero_hash_not_null()
    {
        await _duck.ExecuteAsync("create table staging.empty (id bigint)");
        var fp = await SinkDeliveryLedger.FingerprintAsync(_duck, "staging.empty", default);
        Assert.Equal(0, fp.Count);
        Assert.Equal("0", fp.Hash);
    }

    [Fact]
    public async Task Catalog_qualified_read_targets_an_attached_prior_staging()
    {
        var priorPath = Path.Combine(_dir, "prior.duckdb");
        await using (var prior = DuckSession.Open(priorPath))
        {
            await SinkDeliveryLedger.EnsureAsync(prior, default);
            await prior.ExecuteTransactionAsync(
                SinkDeliveryLedger.UpsertStatements("node-9", 500, new SinkDeliveryLedger.Fingerprint(1000, "77")), default);
        }

        await _duck.ExecuteAsync($"attach '{priorPath.Replace("'", "''")}' as prior_cat (read_only)");
        try
        {
            var row = await SinkDeliveryLedger.ReadAsync(_duck, "node-9", default, catalog: "prior_cat");
            Assert.Equal(new SinkDeliveryLedger.DeliveryRow(500, 1000, "77"), row);
        }
        finally
        {
            await _duck.ExecuteAsync("detach prior_cat");
        }
    }
}
