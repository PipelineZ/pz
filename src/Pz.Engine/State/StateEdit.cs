using Pz.Core.Incremental;
using Pz.Engine.Artifacts;

namespace Pz.Engine.State;

/// <summary>How usable a stored entry is. Everything but <see cref="Ok"/> arrives only by hand-editing
/// `watermarks.json`, which is exactly the population this verb serves.</summary>
public enum StateEntryHealth
{
    /// <summary>Known cursor type, canonical value. Every rule applies normally.</summary>
    Ok,

    /// <summary>Known type, but the value is not the canonical form <see cref="WindowMath"/> produces —
    /// so it cannot be compared, and the direction check is skipped rather than the operation refused.</summary>
    NonCanonicalValue,

    /// <summary>A cursor type pz has no arithmetic for. Nothing can be canonicalized or compared, so
    /// `clear` is the only remedy.</summary>
    UnknownType,
}

/// <summary>Which write `pz state` was asked for. The three share one path and one set of guards; they
/// differ only in how the target is named and which direction is permitted.</summary>
public enum StateEditAction
{
    /// <summary>To the value a named prior run advanced to. Backward only.</summary>
    Rollback,

    /// <summary>To a value given directly. Either direction.</summary>
    Set,

    /// <summary>Remove the entry, so the next run extracts in full.</summary>
    Clear,
}

/// <summary>The decision for one requested edit. <see cref="RefusalCode"/> null means allowed; exactly one
/// of <see cref="NewValue"/> (write it) and <see cref="RemoveEntry"/> (remove it) is then set.
/// <see cref="Notes"/> are printed above the confirmation prompt — they explain a rule that was relaxed or
/// a target worth a second look, never restate the action.</summary>
public sealed record StateEditPlan(
    string? RefusalCode,
    string? RefusalMessage,
    Watermark? NewValue,
    bool RemoveEntry,
    IReadOnlyList<string> Notes);

/// <summary>The whole `pz state` write policy, deliberately pure. It takes the stored entry and the
/// requested target and returns what should happen; <c>StateCommand</c> applies it. Keeping
/// the rules I/O-free is what makes every one a table test instead of a directory fixture — the same split
/// <see cref="RunRetention"/> uses.
///
/// **Nothing here may call <see cref="WindowMath.TryCanonicalize"/> or <see cref="WindowMath.Compare"/>
/// without <see cref="WindowMath.IsKnownType"/> first, and nothing may call Compare on a value that has
/// not round-tripped through TryCanonicalize.** Both throw on unrecognized input, and both
/// inputs come from a file this verb exists because humans edit badly. An escaped throw is exit 3 on the
/// repair tool.</summary>
public static class StateEdit
{
    /// <summary>What a human-written watermark records where a run would record its id. Safe because
    /// nothing in the codebase parses <see cref="Watermark.RunId"/> — the only reads are
    /// <see cref="WatermarkStore"/>'s own serializer — and it earns `pz state show` a free "a human touched
    /// this last". Self-correcting: the next successful run overwrites it with a real id, which is exactly
    /// the right lifetime for the flag.</summary>
    public const string ManualRunId = "manual";

    private const string NoEntry = "PZ0513";
    private const string BadTarget = "PZ0514";
    private const string BadValue = "PZ0515";

    public static StateEntryHealth Classify(Watermark entry)
    {
        if (!WindowMath.IsKnownType(entry.TypeName))
        {
            return StateEntryHealth.UnknownType;
        }

        return WindowMath.TryCanonicalize(entry.TypeName, entry.Value, out var canonical)
               && string.Equals(canonical, entry.Value, StringComparison.Ordinal)
            ? StateEntryHealth.Ok
            : StateEntryHealth.NonCanonicalValue;
    }

    public static StateEditPlan ForSet(Watermark? existing, string rawValue)
    {
        if (Precheck(existing, out var health, out var refusal))
        {
            return refusal!;
        }

        if (!WindowMath.TryCanonicalize(existing!.TypeName, rawValue, out var canonical))
        {
            return Refuse(BadValue,
                $"'{rawValue}' is not a valid {existing.TypeName} cursor value for '{existing.Cursor}' — " +
                $"give a value pz can canonicalize for that type (see pz state show for the stored form)");
        }

        var notes = new List<string>();
        if (health == StateEntryHealth.NonCanonicalValue)
        {
            notes.Add($"the stored value '{existing.Value}' is not canonical for type {existing.TypeName}; " +
                      "this write replaces it with a canonical one");
        }

        return new StateEditPlan(null, null, existing with { Value = canonical, RunId = ManualRunId }, false, notes);
    }

    public static StateEditPlan ForRollback(
        Watermark? existing, string targetRunId, IReadOnlyList<WatermarkHistoryEntry> history)
    {
        if (Precheck(existing, out var health, out var refusal))
        {
            return refusal!;
        }

        var target = history.FirstOrDefault(h => string.Equals(h.RunId, targetRunId, StringComparison.Ordinal));
        if (target is null)
        {
            return Refuse(BadTarget,
                $"run '{targetRunId}' recorded no watermark for this dataset — its artifacts may have been " +
                "purged, or it never touched the dataset; pick a run from pz state show");
        }

        if (!string.Equals(target.Cursor, existing!.Cursor, StringComparison.Ordinal))
        {
            return Refuse(BadTarget,
                $"run '{targetRunId}' recorded a watermark for cursor '{target.Cursor}', but this dataset now " +
                $"tracks '{existing.Cursor}' — that run's value says nothing about the current cursor's position");
        }

        if (!WindowMath.TryCanonicalize(existing.TypeName, target.Value, out var canonical))
        {
            return Refuse(BadValue,
                $"run '{targetRunId}' recorded '{target.Value}', which is not a valid {existing.TypeName} " +
                "value — that run's artifact is unusable as a target; pick another from pz state show");
        }

        var notes = new List<string>();
        if (health == StateEntryHealth.NonCanonicalValue)
        {
            // Compare would throw on the stored side, and refusing would make the tool useless exactly
            // when it is needed. Relax the check, never the operation.
            notes.Add($"the stored value '{existing.Value}' is not canonical, so pz cannot confirm this " +
                      "moves the watermark backward — the direction check was skipped");
        }
        else
        {
            var direction = WindowMath.Compare(existing.TypeName, canonical, existing.Value);
            if (direction == 0)
            {
                return Refuse(BadValue,
                    $"the watermark is already at '{canonical}' — nothing to roll back");
            }

            if (direction > 0)
            {
                return Refuse(BadValue,
                    $"'{canonical}' is NEWER than the stored '{existing.Value}', so this would move the " +
                    "watermark forward and skip source rows — use pz state set if that is what you want");
            }
        }

        if (!string.Equals(target.RunStatus, "success", StringComparison.Ordinal))
        {
            // WatermarkAdvancement only persists a candidate once every downstream SinkWrite commits, so a
            // non-success run's recorded value may never have been the stored one. Still a well-defined
            // target; just not necessarily a position the pipeline ever actually held.
            notes.Add($"run '{targetRunId}' did not fully succeed (status {target.RunStatus}), so this value " +
                      "may never have been the stored one");
        }

        return new StateEditPlan(
            null, null, existing with { Value = canonical, RunId = ManualRunId }, false, notes);
    }

    public static StateEditPlan ForClear(Watermark? existing) =>
        existing is null
            ? Refuse(NoEntry, NoEntryMessage)
            : new StateEditPlan(null, null, null, true, []);

    /// <summary>The two refusals every write shares. Returns true when the request is already dead.
    /// `clear` deliberately does NOT use this: it must work on an unknown-type entry, being that entry's
    /// only remedy.</summary>
    private static bool Precheck(Watermark? existing, out StateEntryHealth health, out StateEditPlan? refusal)
    {
        health = StateEntryHealth.Ok;
        if (existing is null)
        {
            refusal = Refuse(NoEntry, NoEntryMessage);
            return true;
        }

        health = Classify(existing);
        if (health == StateEntryHealth.UnknownType)
        {
            refusal = Refuse(BadValue,
                $"the stored cursor type '{existing.TypeName}' is not one pz knows " +
                "(int, bigint, decimal, date, timestamp), so its value cannot be compared or canonicalized — " +
                "the entry is corrupt; run pz state clear to drop it and let the next run extract in full");
            return true;
        }

        refusal = null;
        return false;
    }

    private const string NoEntryMessage =
        "there is no stored watermark for this dataset — nothing to change, and the next run already " +
        "extracts it in full; run pz state show to list the keys that exist " +
        "(for cdc sync state, use pz cdc drop)";

    private static StateEditPlan Refuse(string code, string message) =>
        new(code, message, null, false, []);
}
