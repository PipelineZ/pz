using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.State;

namespace Pz.Engine.Execution;

/// <summary>Post-materialize drift gate for contract-less
/// datasets. Called with a SUCCESS result from both SourceLoad epilogues; returns that result
/// (with Observed attached), or a Failed replacement under `fail`. Ignore-policy runs never
/// reach the DESCRIBE.</summary>
internal static class SchemaDriftGate
{
    internal static async Task<NodeResult> ApplyAsync(NodeResult success, DagNode node,
        SourceDatasetDef def, ReadHints hints, string tableName, RunContext ctx, CancellationToken ct)
    {
        if (ctx.OnSourceDrift == DriftPolicy.Ignore || ctx.SchemaBaselines is null ||
            def.Dataset.Columns is { Count: > 0 })
        {
            return success; // ignore, no store (direct-built test ctx), or contract governs the read
        }

        var observed = await FetchObservedAsync(ctx, tableName, ct).ConfigureAwait(false);
        var hintsHash = SchemaDriftDiffer.HashHints(hints);
        var result = success with { Observed = new ObservedSchema(observed, hintsHash) };

        var key = SchemaBaselineStore.Key(def.Source.Name, def.Dataset.Name);
        var baseline = ctx.SchemaBaselines.Get(key, ctx.Notice);
        if (baseline is null || !string.Equals(baseline.HintsHash, hintsHash, StringComparison.Ordinal))
        {
            // First sighting, unreadable entry, or the read shape changed (config, not source):
            // (re)seed silently.
            ctx.SchemaBaselines.Set(key, new SchemaBaseline(observed, hintsHash, ctx.Paths.RunId));
            return result;
        }

        var changes = SchemaDriftDiffer.Diff(baseline.Columns, observed);
        if (changes.Count == 0)
        {
            return result;
        }

        var policy = ctx.OnSourceDrift == DriftPolicy.Fail ? "fail" : "warn";
        ctx.Events.SafeSourceDriftDetected(node, def.Source.Name, def.Dataset.Name, policy,
            changes, observed, hintsHash);

        if (ctx.OnSourceDrift == DriftPolicy.Warn)
        {
            return result; // baseline untouched: warns every run until accepted
        }

        // fail: no watermark/sync candidate may survive on a failed result — advancement must
        // treat this exactly like any other failed SourceLoad.
        return result with
        {
            Status = NodeStatus.Failed,
            WatermarkCandidate = null,
            SyncStateCandidate = null,
            Error = new PzError(PzErrorCode.SchemaDrift,
                $"source '{def.Source.Name}' dataset '{def.Dataset.Name}': schema drift against " +
                $"the accepted baseline: {Describe(changes)}",
                def.Source.FilePath, null,
                "run 'pz schema accept' to accept the new schema, or fix the source"),
        };
    }

    /// <summary>One scalar round-trip; chr(31)/chr(30) separators cannot occur in DuckDB
    /// column names/types. tableName is a pz-generated staging identifier, never user input.</summary>
    private static async Task<IReadOnlyList<SchemaColumn>> FetchObservedAsync(
        RunContext ctx, string tableName, CancellationToken ct)
    {
        var packed = await ctx.Duck.ScalarAsync<string>(
            "select coalesce(string_agg(name || chr(31) || type, chr(30) order by cid), '') " +
            $"from pragma_table_info('{tableName}')", ct).ConfigureAwait(false);
        if (string.IsNullOrEmpty(packed))
        {
            return [];
        }

        return [.. packed.Split('\u001e').Select(part =>
        {
            var sep = part.IndexOf('\u001f');
            return new SchemaColumn(part[..sep], part[(sep + 1)..]);
        })];
    }

    private static string Describe(IReadOnlyList<SchemaDriftDiffer.Change> changes) =>
        string.Join("; ", changes.Select(c => c.Kind switch
        {
            "added" => $"column '{c.Column}' added ({c.To})",
            "removed" => $"column '{c.Column}' removed (was {c.From})",
            _ => $"column '{c.Column}' retyped {c.From} -> {c.To}",
        }));
}
