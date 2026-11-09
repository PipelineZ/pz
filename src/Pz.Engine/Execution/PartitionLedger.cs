using System.Security.Cryptography;
using System.Text;
using Apache.Arrow;
using Pz.Core.Dag;
using Pz.DuckDb;

namespace Pz.Engine.Execution;

/// <summary>The pz_meta accounting ledger inside the run's staging.duckdb:
/// which partitions of a node are fully staged in the main table, which have a checkpointed
/// prefix in their part table, and what extraction window the node ran under. Written atomically
/// with the data it accounts for (ExecuteTransactionAsync), so "rows are in main" ⇔ "ledger says
/// done" can never disagree — DuckDB transactionality is the coherence mechanism, and the ledger
/// travels with the staging DB through pz retry's existing ATTACH. Raw partition ids appear ONLY
/// here (and in derived table-name hashes) — never in events, notices, or error messages.</summary>
internal static class PartitionLedger
{
    public const string DoneTable = "pz_meta.partitions_done";
    public const string CheckpointTable = "pz_meta.partition_checkpoints";
    public const string WindowTable = "pz_meta.node_window";

    public static string NodeKey(DagNode node) =>
        new(node.Id.Value.Where(char.IsLetterOrDigit).ToArray());

    public static string PartTable(string nodeKey, string partitionId) =>
        $"staging.__pz_part__{nodeKey}__{PartitionHash(partitionId)}";

    public static string SegTable(string nodeKey, string partitionId) =>
        $"staging.__pz_seg__{nodeKey}__{PartitionHash(partitionId)}";

    public static string Escape(string s) => PzMeta.Escape(s);

    private static string PartitionHash(string partitionId) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(partitionId)))[..16].ToLowerInvariant();

    public static async Task EnsureAsync(IDuckSession duck, CancellationToken ct)
    {
        await PzMeta.EnsureSchemaAsync(duck, ct).ConfigureAwait(false);
        await duck.ExecuteAsync(
            $"create table if not exists {DoneTable} (node_id varchar not null, " +
            "partition_id varchar not null, rows bigint not null, primary key (node_id, partition_id))",
            ct).ConfigureAwait(false);
        await duck.ExecuteAsync(
            $"create table if not exists {CheckpointTable} (node_id varchar not null, " +
            "partition_id varchar not null, checkpoint varchar not null, rows bigint not null, " +
            "primary key (node_id, partition_id))", ct).ConfigureAwait(false);
        await duck.ExecuteAsync(
            $"create table if not exists {WindowTable} (node_id varchar not null, " +
            "lower varchar not null, upper varchar not null, primary key (node_id))",
            ct).ConfigureAwait(false);
    }

    public static Task UpsertWindowAsync(IDuckSession duck, string nodeId, string lower, string upper, CancellationToken ct) =>
        duck.ExecuteTransactionAsync(
        [
            $"delete from {WindowTable} where node_id = '{Escape(nodeId)}'",
            $"insert into {WindowTable} values ('{Escape(nodeId)}', '{Escape(lower)}', '{Escape(upper)}')",
        ], ct);

    /// <summary><paramref name="catalog"/> non-null (a quoted ATTACH alias) targets
    /// <c>{catalog}.pz_meta.partitions_done</c> instead of the local run's ledger -- the
    /// partial-reuse copy reads the FAILED prior run's ledger through this same query shape.</summary>
    public static async Task<Dictionary<string, long>> ReadDoneAsync(
        IDuckSession duck, string nodeId, CancellationToken ct, string? catalog = null)
    {
        var table = PzMeta.Qualify(DoneTable, catalog);
        var result = new Dictionary<string, long>(StringComparer.Ordinal);
        await foreach (var batch in duck.QueryArrowAsync(
            $"select partition_id, rows from {table} where node_id = '{Escape(nodeId)}'", ct: ct).ConfigureAwait(false))
        {
            using (batch)
            {
                var ids = (StringArray)batch.Column(0);
                var rows = (Int64Array)batch.Column(1);
                for (var i = 0; i < batch.Length; i++)
                {
                    result[ids.GetString(i)] = rows.GetValue(i)!.Value;
                }
            }
        }

        return result;
    }

    /// <summary>See <see cref="ReadDoneAsync"/>'s <paramref name="catalog"/> doc -- same
    /// catalog-prefix behavior, targeting <c>{catalog}.pz_meta.partition_checkpoints</c>.</summary>
    public static async Task<Dictionary<string, (string Checkpoint, long Rows)>> ReadCheckpointsAsync(
        IDuckSession duck, string nodeId, CancellationToken ct, string? catalog = null)
    {
        var table = PzMeta.Qualify(CheckpointTable, catalog);
        var result = new Dictionary<string, (string, long)>(StringComparer.Ordinal);
        await foreach (var batch in duck.QueryArrowAsync(
            $"select partition_id, checkpoint, rows from {table} where node_id = '{Escape(nodeId)}'", ct: ct)
            .ConfigureAwait(false))
        {
            using (batch)
            {
                var ids = (StringArray)batch.Column(0);
                var checkpoints = (StringArray)batch.Column(1);
                var rows = (Int64Array)batch.Column(2);
                for (var i = 0; i < batch.Length; i++)
                {
                    result[ids.GetString(i)] = (checkpoints.GetString(i), rows.GetValue(i)!.Value);
                }
            }
        }

        return result;
    }

    /// <summary>One transaction: completed partition's rows move part → main, the done row lands,
    /// and any checkpoint row for it is cleared. Delete-then-insert keeps the done row
    /// upsert-safe under a crash-and-rerun of the same completion.</summary>
    public static IReadOnlyList<string> CompleteStatements(
        string nodeId, string partitionId, string mainTable, string partTable, long rows) =>
    [
        $"insert into {mainTable} select * from {partTable}",
        $"drop table {partTable}",
        $"delete from {DoneTable} where node_id = '{Escape(nodeId)}' and partition_id = '{Escape(partitionId)}'",
        $"insert into {DoneTable} values ('{Escape(nodeId)}', '{Escape(partitionId)}', {rows})",
        $"delete from {CheckpointTable} where node_id = '{Escape(nodeId)}' and partition_id = '{Escape(partitionId)}'",
    ];

    /// <summary>One transaction: a checkpoint interval's rows move segment → part and the token is
    /// upserted with the part table's new cumulative row count — the token always covers exactly
    /// the rows durably staged.</summary>
    public static IReadOnlyList<string> CheckpointStatements(
        string nodeId, string partitionId, string segTable, string partTable, string checkpoint, long cumulativeRows) =>
    [
        $"insert into {partTable} select * from {segTable}",
        $"drop table {segTable}",
        $"delete from {CheckpointTable} where node_id = '{Escape(nodeId)}' and partition_id = '{Escape(partitionId)}'",
        $"insert into {CheckpointTable} values ('{Escape(nodeId)}', '{Escape(partitionId)}', '{Escape(checkpoint)}', {cumulativeRows})",
    ];

    /// <summary>Crash hygiene at node start: every segment table of this node is
    /// dropped (segment content is never resumable); part tables survive only when a checkpoint
    /// row vouches for them (they are the resume prefix). `_` is a LIKE wildcard, hence the
    /// explicit escape. Only THIS node's tables are touched (the nodeKey infix scopes the match).</summary>
    public static async Task CleanupLeftoversAsync(IDuckSession duck, string nodeId, string nodeKey, CancellationToken ct)
    {
        var checkpoints = await ReadCheckpointsAsync(duck, nodeId, ct).ConfigureAwait(false);
        var keep = new HashSet<string>(
            checkpoints.Keys.Select(pid => PartTable(nodeKey, pid).Split('.')[1]), StringComparer.Ordinal);

        foreach (var prefix in new[] { "__pz_seg__", "__pz_part__" })
        {
            var pattern = prefix.Replace("_", "\\_") + Escape(nodeKey) + "\\_\\_%";
            var names = new List<string>();
            await foreach (var batch in duck.QueryArrowAsync(
                "select table_name from information_schema.tables where table_schema = 'staging' " +
                $"and table_name like '{pattern}' escape '\\'", ct: ct).ConfigureAwait(false))
            {
                using (batch)
                {
                    var col = (StringArray)batch.Column(0);
                    for (var i = 0; i < batch.Length; i++)
                    {
                        names.Add(col.GetString(i));
                    }
                }
            }

            foreach (var name in names)
            {
                if (prefix == "__pz_part__" && keep.Contains(name))
                {
                    continue;
                }

                await duck.ExecuteAsync($"drop table if exists staging.\"{name}\"", ct).ConfigureAwait(false);
            }
        }
    }
}
