using System.Globalization;
using Pz.Core.Validation;
using Pz.Engine.Execution;

namespace Pz.Engine.Artifacts;

/// <summary>What `pz clean` decided to do with one run directory.</summary>
public enum SweepAction
{
    /// <summary>Left entirely alone.</summary>
    Keep,

    /// <summary>Only <c>staging.duckdb</c> (and its <c>.wal</c> sidecar) is deleted; the run's
    /// <c>run_results.json</c> survives, so run history and `pz retry` stay intact.</summary>
    DeleteStaging,

    /// <summary>The whole run directory is deleted (`--purge`).</summary>
    DeleteDir,
}

/// <summary>One run directory as `pz clean` found it on disk. <see cref="IsLive"/> is supplied by the
/// caller (via <c>RunDirLock.IsFree</c>) rather than probed here — that is what keeps
/// <see cref="RunRetention.Decide"/> a pure function with no filesystem access.</summary>
public sealed record RunCandidate(string RunId, bool HasStaging, long StagingBytes, long TotalBytes, bool IsLive);

/// <summary>A per-run outcome. <see cref="Reason"/> is rendered to the user, so it explains the rule
/// that fired ("newest", "live run", "older-than window") rather than restating the action.</summary>
public sealed record RunDecision(RunCandidate Candidate, SweepAction Action, string Reason);

/// <summary>The two orthogonal axes of `pz clean`: which runs are candidates
/// (<see cref="KeepLast"/> xor <see cref="OlderThan"/>) and how deep the deletion goes
/// (<see cref="Purge"/>). A null <see cref="KeepLast"/> means the default of 1.</summary>
public sealed record RetentionOptions(int? KeepLast, TimeSpan? OlderThan, bool Purge);

/// <summary>The retention policy, deliberately pure. It takes what
/// was found on disk and returns what should happen; a separate sweep applies the result. Keeping the
/// rules I/O-free is what makes every one of them a table test instead of a directory fixture.</summary>
public static class RunRetention
{
    /// <summary>The timestamp prefix of a run id: <c>yyyyMMddTHHmmssfff</c> followed by 'Z'.
    /// See <c>RunCommand.ExecuteRun</c>, which mints ids as
    /// <c>$"{DateTimeOffset.UtcNow:yyyyMMddTHHmmssfff}Z-{...:x4}"</c>.</summary>
    private const int TimestampLength = 18;

    private const string TimestampFormat = "yyyyMMddTHHmmssfff";

    /// <summary>The <see cref="RunDecision.Reason"/> that marks a directory a live process owns. Public
    /// because the CLI renderer singles these out in its report ("skipped &lt;id&gt; (live run)"), and one
    /// shared constant is what stops the policy and the renderer drifting apart silently.</summary>
    public const string LiveRunReason = "live run";

    /// <summary>Reads a run's start time out of its id. Returns false for any directory name that is not
    /// in pz's run-id shape — a hand-made or third-party directory has no derivable age, and the caller
    /// treats that as "undatable" rather than guessing from filesystem mtime (which a backup or a
    /// <c>cp -r</c> rewrites).</summary>
    public static bool TryParseRunTimestamp(string runId, out DateTimeOffset startedAt)
    {
        startedAt = default;
        if (runId.Length <= TimestampLength || runId[TimestampLength] != 'Z')
        {
            return false;
        }

        if (!DateTime.TryParseExact(
                runId[..TimestampLength], TimestampFormat, CultureInfo.InvariantCulture,
                DateTimeStyles.None, out var parsed))
        {
            return false;
        }

        startedAt = new DateTimeOffset(parsed, TimeSpan.Zero);
        return true;
    }

    /// <summary>Applies the retention rules. Output is ordered newest-first by ordinal run-id comparison
    /// — the same ordering <c>RunResultsReader.ReadLatest</c> relies on — regardless of input order, so
    /// the rendered report is deterministic.</summary>
    public static IReadOnlyList<RunDecision> Decide(
        IReadOnlyList<RunCandidate> candidates, RetentionOptions options, DateTimeOffset now)
    {
        // Datable ids first, then newest-first by ordinal comparison -- the same ordering
        // `RunResultsReader.ReadLatest` relies on. The parseability tier matters: ordinal comparison alone
        // would let a stray hand-made directory in .pz/runs (say "scratch") sort ABOVE every real run id
        // ('s' > '2'), making it the protected "newest" while the actual newest run's staging got swept.
        // A directory that is not in pz's run-id shape is not a run, so it can never be the newest one.
        var ordered = candidates
            .OrderByDescending(c => TryParseRunTimestamp(c.RunId, out _))
            .ThenByDescending(c => c.RunId, StringComparer.Ordinal)
            .ToList();

        // The newest run is never swept unless --keep-last 0 is given explicitly. This is ONE
        // rule, not a per-flag rule -- `--older-than 30d` on a project idle for a year still keeps its
        // newest run, so the same flags are not safe or destructive depending on how long it sat.
        var keepLast = options.KeepLast ?? 1;

        var decisions = new List<RunDecision>(ordered.Count);
        for (var i = 0; i < ordered.Count; i++)
        {
            decisions.Add(Decide(ordered[i], i, keepLast, options, now));
        }

        return decisions;
    }

    private static RunDecision Decide(
        RunCandidate candidate, int index, int keepLast, RetentionOptions options, DateTimeOffset now)
    {
        // The live check comes first and wins over every other rule: whatever the user asked for, a
        // directory a running process owns is never touched.
        if (candidate.IsLive)
        {
            return new RunDecision(candidate, SweepAction.Keep, LiveRunReason);
        }

        if (index < keepLast)
        {
            return new RunDecision(candidate, SweepAction.Keep, index == 0 ? "newest" : $"within keep-last {keepLast}");
        }

        if (options.OlderThan is { } window)
        {
            if (!TryParseRunTimestamp(candidate.RunId, out var startedAt))
            {
                return new RunDecision(candidate, SweepAction.Keep, "undatable directory name");
            }

            if (now - startedAt <= window)
            {
                return new RunDecision(candidate, SweepAction.Keep, "inside the older-than window");
            }
        }

        if (options.Purge)
        {
            return new RunDecision(candidate, SweepAction.DeleteDir, "selected");
        }

        return candidate.HasStaging
            ? new RunDecision(candidate, SweepAction.DeleteStaging, "selected")
            : new RunDecision(candidate, SweepAction.Keep, "no staging database");
    }
}

/// <summary>What one `pz clean` invocation did. <see cref="Failures"/> holds one human-readable line per
/// run directory that could not be deleted — the sweep reports and continues rather than aborting
/// halfway through.</summary>
public sealed record SweepOutcome(
    IReadOnlyList<RunDecision> Decisions,
    long BytesFreed,
    int TmpDirsSwept,
    long TmpBytesFreed,
    IReadOnlyList<string> Failures);

/// <summary>The disk-touching half of retention. Scans
/// <c>.pz/runs</c>, asks <see cref="RunRetention.Decide"/> what should happen, applies it, and always
/// sweeps free <c>.pz/tmp</c> workdirs. <c>.pz/state</c>, <c>.pz/target</c>, and <c>.pz/packages</c> are
/// never enumerated by any code path here — the guarantee is structural, not a conditional.</summary>
public static class RunSweeper
{
    private const string StagingFileName = "staging.duckdb";

    /// <summary>DuckDB's write-ahead log sits beside the database file and must go with it.</summary>
    private const string StagingWalFileName = "staging.duckdb.wal";

    public static IReadOnlyList<RunCandidate> Scan(string projectDir)
    {
        var runsDir = Path.Combine(projectDir, ".pz", "runs");
        if (!Directory.Exists(runsDir))
        {
            return [];
        }

        var candidates = new List<RunCandidate>();
        foreach (var dir in Directory.EnumerateDirectories(runsDir))
        {
            var runId = Path.GetFileName(dir);
            if (string.IsNullOrEmpty(runId))
            {
                continue;
            }

            var stagingBytes = FileLength(Path.Combine(dir, StagingFileName))
                + FileLength(Path.Combine(dir, StagingWalFileName));

            candidates.Add(new RunCandidate(
                runId,
                HasStaging: File.Exists(Path.Combine(dir, StagingFileName)),
                StagingBytes: stagingBytes,
                TotalBytes: DirectorySize(dir),
                IsLive: !RunDirLock.IsFree(dir)));
        }

        return candidates;
    }

    public static SweepOutcome Sweep(string projectDir, RetentionOptions options, DateTimeOffset now, bool dryRun) =>
        Sweep(projectDir, new LocalRunArtifactStore(projectDir), options, now, dryRun);

    /// <summary>The backend-aware overload. The
    /// four-argument <see cref="Sweep(string, RetentionOptions, DateTimeOffset, bool)"/> above delegates
    /// here with a fresh <see cref="LocalRunArtifactStore"/>, so the local path is unaffected --
    /// <see cref="LocalRunArtifactStore.ListCandidates"/> is this same
    /// <see cref="Scan"/> call, and <see cref="LocalRunArtifactStore.Delete"/> deletes the run directory
    /// (idempotent-if-already-gone, per the interface's documented contract).
    ///
    /// <see cref="RunRetention.Decide"/> is shared verbatim by both paths -- only how its
    /// output is applied differs, and only for <see cref="SweepAction.DeleteDir"/>, which goes through
    /// <paramref name="store"/> instead of a direct filesystem call. That is what lets a SQL-backed
    /// <paramref name="store"/> delete its <c>pz.runs</c>/<c>run_nodes</c>/<c>run_events</c> rows for a
    /// swept run instead of a local directory.
    ///
    /// <paramref name="options"/>.Purge is forced on for any non-local store: a remote candidate has no
    /// staging database to partially clean, so Decide's non-purge branch would just Keep every remote
    /// candidate forever ("no staging database") and the whole reason this overload exists -- bounding
    /// table growth -- would silently not happen.
    ///
    /// **Local staging is swept under a remote store too.** "DeleteStaging is local-only because staging
    /// never leaves the machine" says where staging
    /// LIVES, not that a remote sweep may ignore it: <c>.pz/runs/&lt;id&gt;/staging.duckdb</c> is written
    /// on every run whatever the backend, and if this sweep skipped it, neither automatic retention nor
    /// `pz clean` at ANY flag combination would ever reclaim it -- unbounded local disk growth on exactly
    /// the VM-plus-shared-store deployment the how-to endorses. So a non-local store's candidate list is
    /// unioned with <see cref="Scan"/>'s local facts (<see cref="WithLocalDiskFacts"/>), which also makes
    /// <see cref="SweepOutcome.BytesFreed"/> and the live-run guard real for those candidates, and a
    /// <see cref="SweepAction.DeleteDir"/> deletes the local run directory in addition to the store's
    /// rows. Under a remote backend that directory holds nothing BUT staging (run results live in
    /// <c>pz.runs</c>), so "delete the directory" and "delete the staging database" are the same act.
    ///
    /// <see cref="RunRetention.Decide"/> itself stays pure: only the candidate
    /// list it is handed and how its output is applied differ.</summary>
    public static SweepOutcome Sweep(
        string projectDir, IRunArtifactStore store, RetentionOptions options, DateTimeOffset now, bool dryRun)
    {
        var isLocal = store is LocalRunArtifactStore;
        var effectiveOptions = isLocal ? options : options with { Purge = true };
        var candidates = isLocal
            ? store.ListCandidates()
            : WithLocalDiskFacts(store.ListCandidates(), Scan(projectDir));
        var decisions = RunRetention.Decide(candidates, effectiveOptions, now);
        var runsDir = Path.Combine(projectDir, ".pz", "runs");
        var failures = new List<string>();
        var bytesFreed = 0L;

        foreach (var decision in decisions)
        {
            var dir = Path.Combine(runsDir, decision.Candidate.RunId);
            try
            {
                switch (decision.Action)
                {
                    case SweepAction.DeleteStaging:
                        bytesFreed += decision.Candidate.StagingBytes;
                        if (!dryRun)
                        {
                            DeleteIfExists(Path.Combine(dir, StagingFileName));
                            DeleteIfExists(Path.Combine(dir, StagingWalFileName));
                        }

                        break;

                    case SweepAction.DeleteDir:
                        bytesFreed += decision.Candidate.TotalBytes;
                        if (!dryRun)
                        {
                            store.Delete(decision.Candidate.RunId);
                            if (!isLocal && Directory.Exists(dir))
                            {
                                // The store deleted its own rows; only this line reclaims the run's local
                                // staging database. (LocalRunArtifactStore.Delete already deleted this
                                // very directory, hence the guard rather than an unconditional call.)
                                Directory.Delete(dir, recursive: true);
                            }
                        }

                        break;

                    case SweepAction.Keep:
                    default:
                        break;
                }
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or PzConfigException)
            {
                // One undeletable run must not abort the sweep. Report it and continue -- a
                // cleanup verb that stops halfway is worse than one that finishes and says what it missed.
                // PzConfigException is what a remote store's Delete throws on a connectivity failure
                // (SqlStateConnection.Unavailable, PZ0518) -- already secret-hygienic, since that method
                // never includes the connection string.
                bytesFreed -= decision.Action == SweepAction.DeleteDir
                    ? decision.Candidate.TotalBytes
                    : decision.Candidate.StagingBytes;
                failures.Add($"{decision.Candidate.RunId}: {ex.Message}");
            }
        }

        var (tmpDirs, tmpBytes) = SweepTmp(projectDir, dryRun, failures);

        return new SweepOutcome(decisions, bytesFreed, tmpDirs, tmpBytes, failures);
    }

    /// <summary>A remote store knows run ids and nothing else -- no staging size, no directory size, no
    /// live-run lock (<c>SqlRunArtifactStore.ListCandidates</c> reports zeros for all three). This gives
    /// each of its candidates the local disk facts for the same run id, and appends the local-only
    /// directories the store no longer knows about (a run whose rows an earlier sweep already deleted,
    /// or one written under a different backend) so they cannot be orphaned forever.</summary>
    private static IReadOnlyList<RunCandidate> WithLocalDiskFacts(
        IReadOnlyList<RunCandidate> remote, IReadOnlyList<RunCandidate> local)
    {
        var byId = local.ToDictionary(c => c.RunId, StringComparer.Ordinal);
        var merged = remote
            .Select(c => byId.TryGetValue(c.RunId, out var onDisk) ? onDisk : c)
            .ToList();

        var known = remote.Select(c => c.RunId).ToHashSet(StringComparer.Ordinal);
        merged.AddRange(local.Where(c => !known.Contains(c.RunId)));
        return merged;
    }

    /// <summary>Scratch space from crashed restores. No retention concept applies -- every workdir no live
    /// process owns is swept, on every invocation, whatever the run-selection flags say.</summary>
    private static (int Count, long Bytes) SweepTmp(string projectDir, bool dryRun, List<string> failures)
    {
        var tmpRoot = Path.Combine(projectDir, ".pz", "tmp");
        if (!Directory.Exists(tmpRoot))
        {
            return (0, 0);
        }

        var count = 0;
        var bytes = 0L;
        foreach (var dir in Directory.EnumerateDirectories(tmpRoot))
        {
            if (!RunDirLock.IsFree(dir))
            {
                continue;
            }

            try
            {
                var size = DirectorySize(dir);
                if (!dryRun)
                {
                    Directory.Delete(dir, recursive: true);
                }

                count++;
                bytes += size;
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                failures.Add($"{Path.GetFileName(dir)}: {ex.Message}");
            }
        }

        return (count, bytes);
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private static long FileLength(string path)
    {
        try
        {
            var info = new FileInfo(path);
            return info.Exists ? info.Length : 0;
        }
        catch (IOException)
        {
            return 0;
        }
    }

    private static long DirectorySize(string dir)
    {
        try
        {
            return Directory.EnumerateFiles(dir, "*", SearchOption.AllDirectories).Sum(FileLength);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return 0;
        }
    }
}
