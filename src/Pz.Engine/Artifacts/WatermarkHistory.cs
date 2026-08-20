using Pz.Core.Dag;

namespace Pz.Engine.Artifacts;

/// <summary>One prior run's recorded watermark for one dataset.
/// <see cref="RunStatus"/> is the RUN's status, not the node's: because
/// <see cref="Pz.Engine.State.WatermarkAdvancement"/> gates persistence on every downstream SinkWrite
/// committing, a `completed_with_failures` run's recorded candidate may never have reached the store. That
/// makes it a legal rollback target worth flagging, not an illegal one.</summary>
public sealed record WatermarkHistoryEntry(string RunId, string RunStatus, string Cursor, string Type, string Value);

/// <summary>The history scan's result. <see cref="Ambiguity"/> is non-null when the key resolved to more
/// than one recorded node in a single run, which no split can disambiguate — the caller refuses (PZ0514)
/// rather than picking one.</summary>
public sealed record WatermarkHistoryResult(IReadOnlyList<WatermarkHistoryEntry> Entries, string? Ambiguity);

/// <summary>What `pz state rollback --to-run` can target, and what
/// `pz state show &lt;key&gt;` lists as the menu. Reads `.pz/runs/*/run_results.json` through the caller's
/// <see cref="IRunArtifactStore"/> — no second parser. It works only because `pz clean` keeps every
/// `run_results.json` by default.
///
/// The awkward part: a watermark key is `&lt;source&gt;.&lt;dataset&gt;`, but a run
/// records SourceLoad nodes under `StagingName.ForSourceLoad` = `src_&lt;source&gt;__&lt;Fold(dataset)&gt;`.
/// Fold is many-to-one and connection names have no charset validation, so a dotted key like
/// `erp.dbo.orders` has no unambiguous parse. The forward map is computable though, so every split point
/// is tried and the artifacts decide which one is real.</summary>
public static class WatermarkHistory
{
    /// <summary>Every staging-relation name the key could have been recorded under — one per split point.
    /// Note the source name is interpolated raw by <see cref="StagingName.ForSourceLoad"/> while the
    /// dataset name is folded, so `erp.dbo.orders` yields `src_erp__dbo_orders` AND `src_erp.dbo__orders`.</summary>
    public static IReadOnlyList<string> CandidateNodeNames(string key)
    {
        var names = new List<string>();
        for (var i = 0; i < key.Length; i++)
        {
            if (key[i] != '.' || i == 0 || i == key.Length - 1)
            {
                continue;
            }

            names.Add(StagingName.ForSourceLoad(key[..i], key[(i + 1)..]));
        }

        return names;
    }

    public static WatermarkHistoryResult Read(IRunArtifactStore store, string key)
    {
        var candidates = CandidateNodeNames(key);
        if (candidates.Count == 0)
        {
            return new WatermarkHistoryResult([], null);
        }

        var entries = new List<WatermarkHistoryEntry>();
        foreach (var run in store.ReadAllNewestFirst())
        {
            var matches = run.Nodes
                .Where(n => n.Watermark is not null && candidates.Contains(n.Name, StringComparer.Ordinal))
                .ToList();

            if (matches.Count > 1)
            {
                // No split is defensible, so guessing would silently roll back the wrong dataset.
                return new WatermarkHistoryResult(
                    [],
                    $"run {run.RunId} records watermarks for more than one node matching '{key}' " +
                    $"({string.Join(", ", matches.Select(m => m.Name).OrderBy(n => n, StringComparer.Ordinal))})");
            }

            if (matches.Count == 1)
            {
                var wm = matches[0].Watermark!;
                entries.Add(new WatermarkHistoryEntry(run.RunId, run.Status, wm.Cursor, wm.Type, wm.Value));
            }
        }

        return new WatermarkHistoryResult(entries, null);
    }
}
