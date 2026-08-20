using System.CommandLine;
using Pz.Cli;
using Pz.Cli.Rendering;
using Pz.Core.Loading;
using Pz.Core.Validation;
using Pz.Engine.Artifacts;
using Pz.Engine.Execution;
using Pz.Engine.State;

namespace Pz.Cli.Commands;

/// <summary>Inspect and repair state. A management verb like `pz cdc` and
/// `pz clean` -- it runs none of the eight phases and opens no connectors, because the occasion for
/// reaching for it is that something is already wrong.
///
/// This verb reads <c>state:</c> out of project.yml -- via <see cref="ProjectLoader.LoadStateOnly"/>,
/// NOT the full <see cref="ProjectLoader.Load"/> -- to learn which backend it resolved to. Under
/// <c>backend: local</c> that is the only thing read: no pipeline validation, no connections.yml, no
/// connectors, no network, exactly as the paragraph above promises. Under a remote backend it
/// additionally reads connections.yml (only when <c>state.connection</c> names an entry) and does real
/// network I/O to the store -- the cost of reporting the store an operator is actually reading rather
/// than a local file that may not even exist under that backend. It still checks that
/// `project.yml` exists first, so this verb cannot be pointed at a stray `.pz` elsewhere on disk.
///
/// The key is `&lt;source&gt;.&lt;dataset&gt;` and is treated as OPAQUE: dataset names carry dots of
/// their own, connection names have no charset validation, and
/// Get/Set/Remove are all keyed by the composite string -- so there is nothing to split. Only
/// <see cref="WatermarkHistory"/> needs a boundary, and it tries every split against what the run
/// artifacts actually contain.</summary>
internal static class StateCommand
{
    public static Command Create()
    {
        var state = new Command("state", "Inspect and repair watermark state in .pz/state");
        state.Subcommands.Add(CreateShow());
        state.Subcommands.Add(CreateRollback());
        state.Subcommands.Add(CreateSet());
        state.Subcommands.Add(CreateClear());
        return state;
    }

    private static Option<string?> ProjectOption() => new("--project")
    {
        Description = "Project directory (default: current directory)",
    };

    private static Command CreateShow()
    {
        var projectOption = ProjectOption();
        var keyArgument = new Argument<string?>("key")
        {
            Description = "A single <source>.<dataset> to detail, with its run history (default: list everything)",
            Arity = ArgumentArity.ZeroOrOne,
        };
        var command = new Command("show",
            "Report stored watermark and sync state. With a key, add that dataset's run-by-run history " +
            "and any manual changes. Exit 1 when a state file is corrupt.");
        command.Options.Add(projectOption);
        command.Arguments.Add(keyArgument);
        command.SetAction(parseResult => Show(
            parseResult.GetValue(projectOption) ?? Directory.GetCurrentDirectory(),
            parseResult.GetValue(keyArgument)));
        return command;
    }

    /// <summary>Shared by every subcommand: `pz state` must not be pointable at a stray `.pz`.</summary>
    internal static bool ProjectExists(string projectDir)
    {
        if (File.Exists(Path.Combine(projectDir, "project.yml")))
        {
            return true;
        }

        Console.Error.WriteLine(
            $"error {PzErrorCode.YamlShape}: project.yml is missing — run pz state from a project directory " +
            "or pass --project <dir>");
        return false;
    }

    internal static string StateDir(string projectDir) => Path.Combine(projectDir, ".pz", "state");

    /// <summary>Resolves the state backend through
    /// <see cref="ProjectLoader.LoadStateOnly"/> rather than a full <see cref="ProjectLoader.Load"/>, so
    /// `pz state` keeps its promise -- under <c>backend: local</c> it reads project.yml's
    /// <c>state:</c> key and NOTHING else: no pipeline validation, no connections.yml, no network. A
    /// broken config is the reason to reach for this verb, so it must not be the reason it refuses.
    /// Shared by <see cref="Show"/> and <see cref="Write"/> -- one resolution, one error rendering.</summary>
    private static bool TryResolveBackends(
        string projectDir, TimeProvider time, out StateBackends backends, out int exitCode)
    {
        backends = null!;
        exitCode = ExitCodes.Ok;
        try
        {
            var (name, state, connections) =
                ProjectLoader.LoadStateOnly(projectDir, SharedInputHelpers.SnapshotEnvironment());
            backends = StateBackendFactory.Create(state, name, connections, projectDir, time);
            return true;
        }
        catch (PzValidationException ex)
        {
            foreach (var error in ex.Errors)
            {
                Console.Error.WriteLine($"error {error}");
            }
        }
        catch (PzConfigException ex)
        {
            Console.Error.WriteLine($"error {ex.Error}");
        }

        exitCode = ExitCodes.ConfigError;
        return false;
    }

    internal static int Show(string projectDir, string? key)
    {
        if (!ProjectExists(projectDir))
        {
            return ExitCodes.ConfigError;
        }

        if (!TryResolveBackends(projectDir, TimeProvider.System, out var backends, out var exitCode))
        {
            return exitCode;
        }

        Console.WriteLine($"state backend: {backends.Description}");
        Console.WriteLine();

        return key is null ? ShowAll(backends) : ShowOne(projectDir, backends, key);
    }

    /// <summary>The local backend's two entries are files under <c>.pz/state/</c>; a remote backend has
    /// no such path, so the section headers name the store generically instead.</summary>
    private static bool IsLocalDescription(string description) => description.StartsWith("local ", StringComparison.Ordinal);

    private static int ShowAll(StateBackends backends)
    {
        var local = IsLocalDescription(backends.Description);
        var corrupt = false;
        var watermarks = backends.Watermarks.ListAll(notice => Notice(notice, ref corrupt));
        var syncState = backends.SyncState.ListAll(notice => Notice(notice, ref corrupt));

        var watermarksLabel = local ? "watermarks (.pz/state/watermarks.json)" : "watermarks";
        if (watermarks is { Count: > 0 })
        {
            Console.WriteLine(watermarksLabel);
            Console.WriteLine($"{"key",-28} {"cursor",-16} {"type",-10} {"value",-30} run");
            foreach (var (entryKey, wm) in watermarks)
            {
                Console.WriteLine(
                    $"{entryKey,-28} {wm.Cursor,-16} {TypeLabel(wm),-10} {ValueLabel(wm),-30} {wm.RunId}");
            }
        }
        else if (watermarks is not null)
        {
            Console.WriteLine(local ? "no watermark state (.pz/state/watermarks.json is absent)" : "no watermark state");
        }

        if (syncState is { Count: > 0 })
        {
            Console.WriteLine();
            Console.WriteLine(local ? "sync-state (.pz/state/sync-state.json)" : "sync-state");
            Console.WriteLine($"{"key",-28} {"token",-30} run");
            foreach (var (entryKey, sync) in syncState)
            {
                Console.WriteLine($"{entryKey,-28} {sync.Token,-30} {sync.RunId}");
            }
        }

        return corrupt ? ExitCodes.NodeFailures : ExitCodes.Ok;
    }

    private static int ShowOne(string projectDir, StateBackends backends, string key)
    {
        var corrupt = false;
        var stored = backends.Watermarks.Get(key, notice => Notice(notice, ref corrupt));
        if (corrupt)
        {
            return ExitCodes.NodeFailures;
        }

        if (stored is null)
        {
            Console.Error.WriteLine(
                $"error {PzErrorCode.StateKeyNotFound}: no stored watermark for '{key}' — " +
                "run pz state show to list the keys that exist (for cdc sync state, use pz cdc status)");
            return ExitCodes.ConfigError;
        }

        Console.WriteLine($"{key} — cursor {stored.Cursor} ({TypeLabel(stored)})");
        Console.WriteLine($"  current  {ValueLabel(stored),-30} run {stored.RunId}");

        var local = IsLocalDescription(backends.Description);
        var history = WatermarkHistory.Read(backends.Artifacts, key);
        if (history.Ambiguity is { } ambiguity)
        {
            Console.WriteLine();
            Console.WriteLine($"history unavailable: {ambiguity}");
        }
        else if (history.Entries.Count > 0)
        {
            Console.WriteLine();
            Console.WriteLine(local ? "history (.pz/runs/*/run_results.json, newest first)" : "history (newest first)");
            Console.WriteLine($"  {"run",-28} {"value",-30} run status");
            foreach (var entry in history.Entries)
            {
                Console.WriteLine($"  {entry.RunId,-28} {entry.Value,-30} {entry.RunStatus}");
            }
        }

        var stateDir = StateDir(projectDir);
        var manual = new StateAudit(stateDir, TimeProvider.System).Read(key);
        if (manual.Count > 0)
        {
            Console.WriteLine();
            Console.WriteLine($"manual changes ({Path.Combine(".pz", "state", StateAudit.FileName)}, newest first)");
            foreach (var line in manual.Take(5))
            {
                var reason = line.Entry.Reason is { } text ? $"  ({text})" : "";
                Console.WriteLine($"  {line.Ts}  {line.Entry.Action,-9} → {line.Entry.To ?? "removed"}{reason}");
            }

            if (manual.Count > 5)
            {
                Console.WriteLine($"  ({manual.Count - 5} more in {Path.Combine(".pz", "state", StateAudit.FileName)})");
            }
        }

        return ExitCodes.Ok;
    }

    /// <summary>A corrupt file is alertable — a run is about to re-extract the world — so it
    /// exits 1, following `pz cdc status`'s precedent of exiting 1 when anything it reports is unhealthy.</summary>
    private static void Notice(string notice, ref bool corrupt)
    {
        Console.Error.WriteLine($"warning: {notice}");
        corrupt = true;
    }

    /// <summary>An entry pz cannot do arithmetic for prints verbatim and flagged, never crashes.</summary>
    private static string TypeLabel(Watermark wm) =>
        StateEdit.Classify(wm) == StateEntryHealth.UnknownType ? $"{wm.TypeName} (unknown type)" : wm.TypeName;

    private static string ValueLabel(Watermark wm) =>
        StateEdit.Classify(wm) == StateEntryHealth.NonCanonicalValue ? $"{wm.Value} (non-canonical)" : wm.Value;

    private static Command CreateRollback()
    {
        var projectOption = ProjectOption();
        var toRunOption = new Option<string?>("--to-run")
        {
            Description = "The run whose recorded watermark becomes the new value — pick one from pz state show <key>",
        };
        var (reason, dryRun, yes) = WriteOptions();
        var keyArgument = new Argument<string?>("key")
        {
            Description = "The <source>.<dataset> whose watermark to roll back",
            Arity = ArgumentArity.ZeroOrOne,
        };
        var command = new Command("rollback",
            "Roll a stored watermark back to the value a named prior run advanced it to. Backward only — " +
            "use pz state set to move one forward.");
        command.Options.Add(projectOption);
        command.Options.Add(toRunOption);
        command.Options.Add(reason);
        command.Options.Add(dryRun);
        command.Options.Add(yes);
        command.Arguments.Add(keyArgument);
        command.SetAction(parseResult => Write(
            parseResult.GetValue(projectOption) ?? Directory.GetCurrentDirectory(),
            StateEditAction.Rollback,
            parseResult.GetValue(keyArgument),
            parseResult.GetValue(toRunOption),
            value: null,
            parseResult.GetValue(reason),
            parseResult.GetValue(dryRun),
            parseResult.GetValue(yes),
            TimeProvider.System));
        return command;
    }

    private static Command CreateSet()
    {
        var projectOption = ProjectOption();
        var valueOption = new Option<string?>("--value")
        {
            Description = "The new cursor value. Canonicalized against the stored cursor type.",
        };
        var (reason, dryRun, yes) = WriteOptions();
        var keyArgument = new Argument<string?>("key")
        {
            Description = "The <source>.<dataset> whose watermark to set",
            Arity = ArgumentArity.ZeroOrOne,
        };
        var command = new Command("set",
            "Set a stored watermark's value directly, in either direction. Existing entries only — " +
            "the cursor column and type are inherited, so this cannot invent an entry.");
        command.Options.Add(projectOption);
        command.Options.Add(valueOption);
        command.Options.Add(reason);
        command.Options.Add(dryRun);
        command.Options.Add(yes);
        command.Arguments.Add(keyArgument);
        command.SetAction(parseResult => Write(
            parseResult.GetValue(projectOption) ?? Directory.GetCurrentDirectory(),
            StateEditAction.Set,
            parseResult.GetValue(keyArgument),
            toRun: null,
            parseResult.GetValue(valueOption),
            parseResult.GetValue(reason),
            parseResult.GetValue(dryRun),
            parseResult.GetValue(yes),
            TimeProvider.System));
        return command;
    }

    private static Command CreateClear()
    {
        var projectOption = ProjectOption();
        var (reason, dryRun, yes) = WriteOptions();
        var keyArgument = new Argument<string?>("key")
        {
            Description = "The <source>.<dataset> whose watermark to remove",
            Arity = ArgumentArity.ZeroOrOne,
        };
        var command = new Command("clear",
            "Remove a stored watermark entirely, so the next run extracts that dataset in full. The only " +
            "remedy for an entry whose cursor type pz has no arithmetic for.");
        command.Options.Add(projectOption);
        command.Options.Add(reason);
        command.Options.Add(dryRun);
        command.Options.Add(yes);
        command.Arguments.Add(keyArgument);
        command.SetAction(parseResult => Write(
            parseResult.GetValue(projectOption) ?? Directory.GetCurrentDirectory(),
            StateEditAction.Clear,
            parseResult.GetValue(keyArgument),
            toRun: null,
            value: null,
            parseResult.GetValue(reason),
            parseResult.GetValue(dryRun),
            parseResult.GetValue(yes),
            TimeProvider.System));
        return command;
    }

    /// <summary>The three flags every write shares. One factory so `rollback`/`set`/`clear` cannot drift
    /// on their descriptions.</summary>
    private static (Option<string?> Reason, Option<bool> DryRun, Option<bool> Yes) WriteOptions() => (
        new Option<string?>("--reason") { Description = "Free text recorded in .pz/state/audit.jsonl" },
        new Option<bool>("--dry-run") { Description = "Print what would change, and change nothing" },
        new Option<bool>("--yes") { Description = "Skip the confirmation prompt (required when not on a TTY)" });

    /// <summary>The one write path, guards applied in order. `isInteractive` and
    /// `confirm` are test seams — the CLI passes neither and gets <see cref="CiDetector.IsInteractive()"/>
    /// plus a real prompt.</summary>
    internal static int Write(
        string projectDir, StateEditAction action, string? key, string? toRun, string? value, string? reason,
        bool dryRun, bool yes, TimeProvider time, Func<bool>? isInteractive = null, Func<bool>? confirm = null)
    {
        if (!ProjectExists(projectDir))
        {
            return ExitCodes.ConfigError;
        }

        if (string.IsNullOrEmpty(key))
        {
            return Refuse(PzErrorCode.StateArgumentInvalid,
                $"pz state {Verb(action)} needs a <source>.<dataset> key — run pz state show to list them");
        }

        if (action == StateEditAction.Set && string.IsNullOrEmpty(value))
        {
            return Refuse(PzErrorCode.StateArgumentInvalid,
                "pz state set needs --value <v> — the new cursor value for this dataset");
        }

        if (action == StateEditAction.Rollback && string.IsNullOrEmpty(toRun))
        {
            return Refuse(PzErrorCode.StateArgumentInvalid,
                "pz state rollback needs --to-run <id> — pick one from pz state show <key>");
        }

        // Guard 1: KeyedJsonStateStore.Set is a read-modify-write with no compare-and-swap, so a
        // live run's advancement would silently overwrite this edit the moment it commits.
        if (LiveRunId(projectDir) is { } liveRun)
        {
            return Refuse(PzErrorCode.StateRunInFlight,
                $"run '{liveRun}' is in flight and its watermark advancement would overwrite this change — " +
                "wait for it to finish, then retry");
        }

        if (!TryResolveBackends(projectDir, time, out var backends, out var exitCode))
        {
            return exitCode;
        }

        var stateDir = StateDir(projectDir);
        var store = backends.Watermarks;
        var corrupt = false;
        var existing = store.Get(key, notice => Notice(notice, ref corrupt));
        if (corrupt)
        {
            return ExitCodes.NodeFailures;
        }

        var plan = BuildPlan(backends.Artifacts, action, key, toRun, value, existing);
        if (plan.RefusalCode is { } code)
        {
            return Refuse(code, $"{key}: {plan.RefusalMessage}");
        }

        var prefix = dryRun ? "dry-run: " : "";
        Report(prefix, action, key, existing, plan, toRun);

        if (dryRun)
        {
            return ExitCodes.Ok;
        }

        if (!yes)
        {
            if (!(isInteractive ?? CiDetector.IsInteractive)())
            {
                return Refuse(PzErrorCode.StateArgumentInvalid,
                    "stdout is not an interactive terminal, so pz state cannot ask for confirmation — " +
                    "pass --yes to proceed, or --dry-run to see the change without making it");
            }

            Console.Write("proceed? [y/N] ");
            if (!(confirm ?? PromptYes)())
            {
                Console.WriteLine("cancelled — nothing changed");
                return ExitCodes.Ok;
            }
        }

        // Guard 1 re-probed at the last safe moment: the confirmation prompt above is an unbounded human
        // pause, and a run can start while the operator is deciding. Nothing has been written yet, so a
        // run appearing here is refused exactly like guard 1 -- no state change, no ledger line.
        if (LiveRunId(projectDir) is { } liveRunAfterPrompt)
        {
            return Refuse(PzErrorCode.StateRunInFlight,
                $"run '{liveRunAfterPrompt}' started while you were deciding — nothing was changed. " +
                "Wait for it to finish, then retry");
        }

        if (plan.RemoveEntry)
        {
            store.Remove(key);
        }
        else
        {
            store.Set(key, plan.NewValue!);
        }

        // The state write is atomic and comes first, so the ledger never claims a mutation that
        // did not happen. A failed append is loud, never silent — the operator gets the exact line.
        var entry = new StateAuditEntry(
            Verb(action), key, existing?.Cursor, existing?.TypeName, existing?.Value, existing?.RunId,
            plan.NewValue?.Value, plan.RemoveEntry ? null : action == StateEditAction.Rollback ? $"run:{toRun}" : "value",
            reason);
        var audit = new StateAudit(stateDir, time);
        try
        {
            audit.Append(entry);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            Console.Error.WriteLine(
                $"warning: the state change succeeded but could not be recorded in " +
                $"{Path.Combine(".pz", "state", StateAudit.FileName)} ({ex.Message}) — append this line by hand:");
            Console.Error.WriteLine(audit.Render(entry));
            return ExitCodes.NodeFailures;
        }

        Console.WriteLine($"done — recorded in {Path.Combine(".pz", "state", StateAudit.FileName)}");
        return ExitCodes.Ok;
    }

    private static StateEditPlan BuildPlan(
        IRunArtifactStore artifacts, StateEditAction action, string key, string? toRun, string? value, Watermark? existing)
    {
        switch (action)
        {
            case StateEditAction.Set:
                return StateEdit.ForSet(existing, value!);

            case StateEditAction.Clear:
                return StateEdit.ForClear(existing);

            case StateEditAction.Rollback:
            default:
                var history = WatermarkHistory.Read(artifacts, key);
                return history.Ambiguity is { } ambiguity
                    ? new StateEditPlan(PzErrorCode.StateRollbackTargetInvalid,
                        $"{ambiguity} — pz cannot tell which dataset you mean", null, false, [])
                    : StateEdit.ForRollback(existing, toRun!, history.Entries);
        }
    }

    /// <summary>Guard 2: say what the next run will do. The append-duplication warning, in the one
    /// form that does not require compiling the project to produce.</summary>
    private static void Report(
        string prefix, StateEditAction action, string key, Watermark? existing, StateEditPlan plan, string? toRun)
    {
        Console.WriteLine($"{prefix}{key}  {existing!.Cursor} ({existing.TypeName})");
        Console.WriteLine($"{prefix}  from  {existing.Value}  (run {existing.RunId})");
        Console.WriteLine(plan.RemoveEntry
            ? $"{prefix}  to    (removed)"
            : $"{prefix}  to    {plan.NewValue!.Value}{(toRun is null ? "" : $"  (run {toRun})")}");

        foreach (var note in plan.Notes)
        {
            Console.WriteLine($"{prefix}note: {note}");
        }

        Console.WriteLine(prefix + Consequence(action, existing, plan));
    }

    private static string Consequence(StateEditAction action, Watermark existing, StateEditPlan plan)
    {
        if (plan.RemoveEntry)
        {
            return $"next run extracts this dataset IN FULL (no stored watermark)";
        }

        var moved = plan.NewValue!.Value;
        if (StateEdit.Classify(existing) != StateEntryHealth.Ok)
        {
            // Compare throws on a non-canonical argument, so the direction is genuinely unknown here —
            // never fall back to either the forward or the backward sentence: the direction check is
            // skipped and the report says so.
            return "next run re-extracts or skips depending on direction — pz cannot compare a " +
                   "non-canonical stored value, so check the from/to values above";
        }

        var forward = Pz.Core.Incremental.WindowMath.Compare(existing.TypeName, moved, existing.Value) > 0;

        return forward
            ? $"next run skips rows where {existing.Cursor} <= {moved} — they will never be extracted"
            : $"next run re-extracts where {existing.Cursor} > {moved} — against an append-mode sink " +
              "that duplicates those rows";
    }

    /// <summary>The first run directory a live process owns, or null when none. Uses the same
    /// <see cref="RunDirLock"/> probe `pz clean` does: the OS releases it on exit including SIGKILL, so a
    /// crashed run never blocks this verb forever.</summary>
    private static string? LiveRunId(string projectDir)
    {
        var runsDir = Path.Combine(projectDir, ".pz", "runs");
        if (!Directory.Exists(runsDir))
        {
            return null;
        }

        foreach (var dir in Directory.EnumerateDirectories(runsDir).OrderBy(d => d, StringComparer.Ordinal))
        {
            if (!RunDirLock.IsFree(dir))
            {
                return Path.GetFileName(dir);
            }
        }

        return null;
    }

    private static bool PromptYes() =>
        Console.ReadLine()?.Trim() is "y" or "Y" or "yes" or "YES";

    private static string Verb(StateEditAction action) => action switch
    {
        StateEditAction.Rollback => "rollback",
        StateEditAction.Set => "set",
        _ => "clear",
    };

    private static int Refuse(string code, string message)
    {
        Console.Error.WriteLine($"error {code}: {message}");
        return ExitCodes.ConfigError;
    }
}
