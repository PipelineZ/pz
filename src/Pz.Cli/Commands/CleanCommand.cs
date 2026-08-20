using System.CommandLine;
using System.Globalization;
using Pz.Cli;
using Pz.Core.Loading;
using Pz.Core.Validation;
using Pz.Engine.Artifacts;

namespace Pz.Cli.Commands;

/// <summary>Reclaims disk from <c>.pz/runs</c>. A management verb like
/// `pz cdc` -- it runs none of the eight phases and opens no connectors, because the common reason to
/// clean is that something is already wrong. It checks only that <c>project.yml</c> exists before loading
/// anything, so it cannot be pointed at a stray <c>.pz</c> elsewhere on disk.
///
/// This verb reads <c>state:</c> out of project.yml -- via <see cref="ProjectLoader.LoadStateOnly"/>,
/// NOT the full <see cref="ProjectLoader.Load"/> -- to learn which backend it resolved to. Under
/// <c>backend: local</c> nothing else is read at all, so the rationale above holds: a config that no
/// longer parses still cannot block a cleanup. Under a remote backend it additionally reads
/// connections.yml (only when <c>state.connection</c> names an entry) and reaches the store.
///
/// Two orthogonal axes: --keep-last/--older-than pick WHICH runs are candidates, --purge picks
/// HOW DEEP. There is deliberately no --all: --keep-last 0 already means it, and --all is taken on
/// run/plan for "the whole project" -- reusing the spelling for "every run dir" would collide on the one
/// verb where the mistake is unrecoverable.</summary>
internal static class CleanCommand
{
    /// <summary>Owned by <see cref="RunRetention"/>, not redeclared here: the renderer singles out
    /// live-run decisions, and referencing the producer's constant is what stops the two drifting.</summary>
    private const string LiveRunReason = RunRetention.LiveRunReason;

    public static Command Create()
    {
        var projectOption = new Option<string?>("--project")
        {
            Description = "Project directory (default: current directory)",
        };
        var keepLastOption = new Option<int?>("--keep-last")
        {
            Description = "Keep the newest N runs (default: 1). 0 selects every run, including the newest.",
        };
        var olderThanOption = new Option<string?>("--older-than")
        {
            Description = "Select runs older than a duration like 30m, 12h, or 7d. The newest run is kept regardless.",
        };
        var purgeOption = new Option<bool>("--purge")
        {
            Description = "Delete whole run directories instead of only staging.duckdb",
        };
        var dryRunOption = new Option<bool>("--dry-run")
        {
            Description = "Print what would be deleted, and delete nothing",
        };

        var command = new Command("clean",
            "Reclaim disk from .pz/runs. By default deletes staging.duckdb from every run but the newest, " +
            "keeping every run_results.json. Never touches .pz/state, .pz/target, or .pz/packages.");
        command.Options.Add(projectOption);
        command.Options.Add(keepLastOption);
        command.Options.Add(olderThanOption);
        command.Options.Add(purgeOption);
        command.Options.Add(dryRunOption);
        command.SetAction(parseResult => Execute(
            parseResult.GetValue(projectOption) ?? Directory.GetCurrentDirectory(),
            parseResult.GetValue(keepLastOption),
            parseResult.GetValue(olderThanOption),
            parseResult.GetValue(purgeOption),
            parseResult.GetValue(dryRunOption)));

        return command;
    }

    internal static int Execute(string projectDir, int? keepLast, string? olderThanText, bool purge, bool dryRun)
    {
        if (keepLast is not null && olderThanText is not null)
        {
            Console.Error.WriteLine(
                $"error {PzErrorCode.CleanSelectorConflict}: --keep-last and --older-than are mutually exclusive — " +
                "pass one selector (--keep-last picks how many recent runs to protect, --older-than picks an age cutoff)");
            return ExitCodes.ConfigError;
        }

        if (keepLast is < 0)
        {
            Console.Error.WriteLine(
                $"error {PzErrorCode.CleanSelectorInvalid}: --keep-last must be 0 or greater (got " +
                $"'{keepLast.Value.ToString(CultureInfo.InvariantCulture)}') — 0 selects every run, 1 keeps the newest");
            return ExitCodes.ConfigError;
        }

        TimeSpan? olderThan = null;
        if (olderThanText is not null)
        {
            if (!DurationParser.TryParse(olderThanText, out var parsed) || parsed <= TimeSpan.Zero)
            {
                Console.Error.WriteLine(
                    $"error {PzErrorCode.CleanSelectorInvalid}: --older-than must be a positive duration like " +
                    $"500ms, 2s, 5m, 1h, or 7d (got '{olderThanText}')");
                return ExitCodes.ConfigError;
            }

            olderThan = parsed;
        }

        if (!File.Exists(Path.Combine(projectDir, "project.yml")))
        {
            Console.Error.WriteLine(
                $"error {PzErrorCode.YamlShape}: project.yml is missing — run pz clean from a project directory " +
                "or pass --project <dir>");
            return ExitCodes.ConfigError;
        }

        StateBackends backends;
        try
        {
            var (name, state, connections) =
                ProjectLoader.LoadStateOnly(projectDir, SharedInputHelpers.SnapshotEnvironment());
            backends = StateBackendFactory.Create(state, name, connections, projectDir, TimeProvider.System);
        }
        catch (PzValidationException ex)
        {
            foreach (var error in ex.Errors)
            {
                Console.Error.WriteLine($"error {error}");
            }

            return ExitCodes.ConfigError;
        }
        catch (PzConfigException ex)
        {
            Console.Error.WriteLine($"error {ex.Error}");
            return ExitCodes.ConfigError;
        }

        var options = new RetentionOptions(keepLast, olderThan, purge);
        var outcome = RunSweeper.Sweep(projectDir, backends.Artifacts, options, DateTimeOffset.UtcNow, dryRun);

        // A remote store has no staging database, so its sweep always deletes the whole run
        // (RunSweeper.Sweep(string, IRunArtifactStore, ...) forces Purge internally) whether or not --purge
        // was actually passed -- the report should say so rather than print "staging only" for a sweep
        // that deleted whole rows.
        var effectivePurge = purge || backends.Artifacts is not LocalRunArtifactStore;
        return Report(outcome, effectivePurge, keepLast, dryRun);
    }

    private static int Report(SweepOutcome outcome, bool purge, int? keepLast, bool dryRun)
    {
        var prefix = dryRun ? "dry-run: " : "";
        var swept = outcome.Decisions.Count(d => d.Action != SweepAction.Keep);
        var kept = outcome.Decisions.Count(d => d.Action == SweepAction.Keep && d.Reason != LiveRunReason);
        var live = outcome.Decisions.Count(d => d.Reason == LiveRunReason);

        if (swept == 0 && outcome.TmpDirsSwept == 0 && outcome.Failures.Count == 0)
        {
            Console.WriteLine($"{prefix}nothing to clean");
            return ExitCodes.Ok;
        }

        if (swept > 0)
        {
            var depth = purge ? "whole directories" : "staging only";
            Console.WriteLine(
                $"{prefix}swept   {swept} run(s) ({depth}) — freed {FormatBytes(outcome.BytesFreed)}");
        }

        if (kept > 0)
        {
            Console.WriteLine($"{prefix}kept    {kept} run(s)");
        }

        foreach (var decision in outcome.Decisions.Where(d => d.Reason == LiveRunReason))
        {
            Console.WriteLine($"{prefix}skipped {decision.Candidate.RunId} ({LiveRunReason})");
        }

        if (outcome.TmpDirsSwept > 0)
        {
            Console.WriteLine(
                $"{prefix}swept   {outcome.TmpDirsSwept} stale restore workdir(s) — freed {FormatBytes(outcome.TmpBytesFreed)}");
        }

        // --keep-last 0 --purge gives up the one thing retention exists to protect. Say so.
        if (keepLast == 0 && purge && live == 0)
        {
            Console.WriteLine($"{prefix}note: no run directories remain — pz retry has nothing to target");
        }

        foreach (var failure in outcome.Failures)
        {
            Console.Error.WriteLine($"warning: could not clean {failure}");
        }

        return outcome.Failures.Count > 0 ? ExitCodes.NodeFailures : ExitCodes.Ok;
    }

    /// <summary>Human-readable sizes for the report. Deliberately not culture-sensitive: pz's console
    /// output is invariant everywhere else too. Internal rather than private because `pz run`'s automatic
    /// retention line prints the same shape -- one formatter, one spelling of "1.2 GB".</summary>
    internal static string FormatBytes(long bytes)
    {
        string[] units = ["B", "KB", "MB", "GB", "TB"];
        double value = bytes;
        var unit = 0;
        while (value >= 1024 && unit < units.Length - 1)
        {
            value /= 1024;
            unit++;
        }

        return unit == 0
            ? $"{bytes.ToString(CultureInfo.InvariantCulture)} B"
            : value.ToString("0.#", CultureInfo.InvariantCulture) + " " + units[unit];
    }
}
