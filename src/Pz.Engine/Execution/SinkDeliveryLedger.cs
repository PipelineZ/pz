using Apache.Arrow;
using Pz.DuckDb;

namespace Pz.Engine.Execution;

/// <summary>The sink-delivery accounting table inside the run's staging.duckdb:
/// for a checkpointing sink node, how many drain-order rows the connector has durably
/// delivered, fingerprinted against the relation content that order was computed over. Written
/// ONLY at attempt teardown and cleared post-commit (the streaming drain holds the
/// one serialized DuckDB connection, so nothing here runs mid-drain). Travels with the staging
/// DB through pz retry's ATTACH; reads are catalog-qualifiable exactly like
/// <see cref="PartitionLedger"/>. Counts and an opaque decimal hash only — never data.</summary>
internal static class SinkDeliveryLedger
{
    public const string Table = "pz_meta.sink_deliveries";

    /// <summary>Content fingerprint of the drained relation: row count plus an
    /// order-independent aggregate hash (sum of per-row hashes as a HUGEINT, carried as its
    /// decimal string — HUGEINT never round-trips through ScalarAsync&lt;long&gt;). Identical
    /// multisets fingerprint identically, so `order by all` yields the same prefix.</summary>
    public sealed record Fingerprint(long Count, string Hash);

    public sealed record DeliveryRow(long AcknowledgedRows, long RelationCount, string RelationHash);

    public static string Escape(string s) => PzMeta.Escape(s);

    public static async Task EnsureAsync(IDuckSession duck, CancellationToken ct)
    {
        await PzMeta.EnsureSchemaAsync(duck, ct).ConfigureAwait(false);
        await duck.ExecuteAsync(
            $"create table if not exists {Table} (node_id varchar not null, " +
            "acknowledged_rows bigint not null, relation_count bigint not null, " +
            "relation_hash varchar not null, primary key (node_id))", ct).ConfigureAwait(false);
    }

    public static async Task<Fingerprint> FingerprintAsync(IDuckSession duck, string relation, CancellationToken ct)
    {
        var count = await duck.ScalarAsync<long>($"select count(*) from {relation}", ct).ConfigureAwait(false);
        var hash = await duck.ScalarAsync<string>(
            $"select cast(coalesce(sum(cast(hash(t) as hugeint)), 0) as varchar) from {relation} t", ct)
            .ConfigureAwait(false);
        return new Fingerprint(count, hash);
    }

    /// <summary>See <see cref="PartitionLedger.ReadDoneAsync"/>'s catalog doc — non-null
    /// targets <c>{catalog}.pz_meta.sink_deliveries</c> (a quoted ATTACH alias of the failed
    /// prior run's staging DB).</summary>
    public static async Task<DeliveryRow?> ReadAsync(
        IDuckSession duck, string nodeId, CancellationToken ct, string? catalog = null)
    {
        var table = PzMeta.Qualify(Table, catalog);
        DeliveryRow? result = null;
        await foreach (var batch in duck.QueryArrowAsync(
            "select acknowledged_rows, relation_count, relation_hash from " +
            $"{table} where node_id = '{Escape(nodeId)}'", ct: ct).ConfigureAwait(false))
        {
            using (batch)
            {
                if (batch.Length > 0)
                {
                    var acknowledged = (Int64Array)batch.Column(0);
                    var counts = (Int64Array)batch.Column(1);
                    var hashes = (StringArray)batch.Column(2);
                    result = new DeliveryRow(
                        acknowledged.GetValue(0)!.Value, counts.GetValue(0)!.Value, hashes.GetString(0));
                }
            }
        }

        return result;
    }

    /// <summary>Delete+insert (upsert-safe under crash-and-rerun) — run via
    /// <see cref="IDuckSession.ExecuteTransactionAsync"/>.</summary>
    public static IReadOnlyList<string> UpsertStatements(string nodeId, long acknowledgedRows, Fingerprint fp) =>
    [
        $"delete from {Table} where node_id = '{Escape(nodeId)}'",
        $"insert into {Table} values ('{Escape(nodeId)}', {acknowledgedRows}, {fp.Count}, '{Escape(fp.Hash)}')",
    ];

    public static Task ClearAsync(IDuckSession duck, string nodeId, CancellationToken ct) =>
        duck.ExecuteAsync($"delete from {Table} where node_id = '{Escape(nodeId)}'", ct);

    /// <summary>Seeds the local ledger from a failed prior run's row.
    /// Guards in order, any failure => silent scratch (return without seeding): (1) a local
    /// row wins — a same-run attempt's progress is fresher than anything cross-run; (2) the
    /// prior staging DB must ATTACH and its pz_meta read cleanly (a legacy prior without the
    /// table is the common case and must stay silent); (3) the prior fingerprint must equal
    /// <paramref name="fresh"/> — content-identical relation ⇒ identical `order by all`
    /// prefix. DETACH always runs. Never throws.</summary>
    public static async Task TrySeedFromPriorAsync(IDuckSession duck, string nodeId, string nodeKey,
        string priorStagingPath, Fingerprint fresh, CancellationToken ct)
    {
        if (!File.Exists(priorStagingPath))
        {
            return;
        }

        if (await ReadAsync(duck, nodeId, ct).ConfigureAwait(false) is not null)
        {
            return; // guard 1: local row wins
        }

        var alias = "pz_prior5_" + nodeKey;
        try
        {
            await using var scope = await PzMeta.AttachReadOnlyAsync(duck, alias, priorStagingPath, ct)
                .ConfigureAwait(false);

            DeliveryRow? prior;
            try
            {
                prior = await ReadAsync(duck, nodeId, ct, catalog: scope.QuotedAlias).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return; // guard 2: prior pz_meta unreadable (legacy prior) — silent scratch
            }

            if (prior is not { } row || row.AcknowledgedRows <= 0 ||
                row.RelationCount != fresh.Count || row.RelationHash != fresh.Hash)
            {
                return; // guard 3: fingerprint mismatch (or nothing recorded) — silent scratch
            }

            await duck.ExecuteTransactionAsync(
                UpsertStatements(nodeId, row.AcknowledgedRows, fresh), ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Silent by contract: cross-run resume is opportunistic; scratch is always safe.
        }
    }
}
