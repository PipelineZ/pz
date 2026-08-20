using System.Runtime.Versioning;
using Pz.Cli;
using Pz.Cli.Commands;

namespace Pz.Cli.Tests;

/// <summary>`pz run`'s retention summary must print the count of runs actually DELETED, not the count
/// selected for deletion: <c>RunSweeper.Sweep</c> subtracts freed bytes back out on a failed delete but
/// the selection count is unaffected, so reporting the selection would render a permission-denied sweep
/// as "cleaned 3 staging database(s) ... freed 0 B" and never mention the 3 failures. Separately, a
/// sweep whose ONLY activity was a failed `.pz/tmp` workdir deletion (zero staging candidates, zero
/// successful tmp deletions) must still emit an event and a console line -- silence there would be a
/// silent failure, against this project's error philosophy.
///
/// Uses the real `samples/hello-pz` project (self-contained, relative paths, real CSV data --
/// unlike `Fixtures/hello-pz`, which is validation-error-path only) so `pz run` actually reaches
/// finalize/retention instead of failing every node before retention ever runs.
///
/// Unix permission bits only: <see cref="File.SetUnixFileMode"/> is a no-op fiction on Windows (and the
/// repo's CI/dev environment is Linux per https://pipelinez.dev/concepts/architecture-overview/), hence the platform attribute below --
/// same reasoning <see cref="Pz.EndToEnd.Tests.RetryRunTests"/> already uses for the same trick.</summary>
[SupportedOSPlatform("linux")]
[Collection("console-and-env-serialized")]
public sealed class RunRetentionFailureTests
{
    private static void CopyTree(string from, string to)
    {
        Directory.CreateDirectory(to);
        foreach (var file in Directory.EnumerateFiles(from, "*", SearchOption.AllDirectories))
        {
            var dest = Path.Combine(to, Path.GetRelativePath(from, file));
            Directory.CreateDirectory(Path.GetDirectoryName(dest)!);
            File.Copy(file, dest, overwrite: true);
        }
    }

    private static string MakePriorRun(string work, int ordinal)
    {
        var runId = $"20250101T00000{ordinal:D3}Z-{ordinal:x4}";
        var dir = Path.Combine(work, ".pz", "runs", runId);
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "run_results.json"), "{}");
        File.WriteAllBytes(Path.Combine(dir, "staging.duckdb"), new byte[4096]);
        return dir;
    }

    [Fact]
    public void Console_line_names_deletions_that_failed_without_hiding_the_count()
    {
        var work = Path.Combine(Path.GetTempPath(), "pz-run-retention-fail-tests", Guid.NewGuid().ToString("N"));
        CopyTree(Path.Combine(AppContext.BaseDirectory, "SamplesHelloPz"), work);
        var undeletableRunDir = string.Empty;
        try
        {
            // keep_last defaults to 10; 12 priors + the run that's about to happen makes 13 candidates,
            // so the 3 oldest (ordinals 1-3) are selected for staging deletion -- same shape as
            // RunCommandTests.Retention_reports_what_it_freed.
            for (var i = 1; i <= 12; i++)
            {
                var dir = MakePriorRun(work, i);
                if (i == 1)
                {
                    undeletableRunDir = dir;
                }
            }

            // Removing write (not read/execute) from the run directory itself blocks unlinking the
            // staging.duckdb file inside it -- deleting a file needs write permission on its *parent*
            // directory in POSIX, not on the file. RunSweeper's own File.Delete call surfaces this as
            // UnauthorizedAccessException, which its catch clause already handles as a per-directory
            // failure rather than aborting the whole sweep.
            File.SetUnixFileMode(undeletableRunDir, UnixFileMode.UserRead | UnixFileMode.UserExecute);

            var stdout = new StringWriter();
            var original = Console.Out;
            int exit;
            try
            {
                Console.SetOut(stdout);
                exit = CliApp.Build().Parse(["run", "--project", work]).Invoke();
            }
            finally
            {
                Console.SetOut(original);
            }

            Assert.Equal(ExitCodes.Ok, exit);

            var output = stdout.ToString();
            // 3 selected (matches the count RunRetention.Decide chose), only 2 actually freed bytes
            // (4096 bytes each), and the failure suffix names the one that could not be deleted.
            var expected = "cleaned 3 staging database(s) and 0 stale workdir(s) — " +
                $"freed {CleanCommand.FormatBytes(4096 * 2)} " +
                "(1 could not be deleted; run pz clean for details)";
            Assert.Contains(expected, output, StringComparison.Ordinal);

            // The two OTHER selected priors really were deleted; the permission-denied one was not.
            Assert.True(File.Exists(Path.Combine(undeletableRunDir, "staging.duckdb")));
        }
        finally
        {
            if (undeletableRunDir.Length > 0 && Directory.Exists(undeletableRunDir))
            {
                try
                {
                    File.SetUnixFileMode(undeletableRunDir,
                        UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
                }
                catch { /* best-effort */ }
            }

            try { Directory.Delete(work, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    [Fact]
    public void Failed_tmp_sweep_alone_still_emits_the_summary_line()
    {
        var work = Path.Combine(Path.GetTempPath(), "pz-run-retention-fail-tests", Guid.NewGuid().ToString("N"));
        CopyTree(Path.Combine(AppContext.BaseDirectory, "SamplesHelloPz"), work);
        var tmpDir = Path.Combine(work, ".pz", "tmp", "stale-restore");
        try
        {
            // No prior runs at all -- the only candidate is the run about to happen, which
            // RunRetention.Decide always keeps ("newest"), so Decisions has zero non-Keep entries and
            // TmpDirsSwept will be zero too (the one tmp workdir below fails to delete). Before this
            // fix, sweptCount == 0 && TmpDirsSwept == 0 meant no event and no console line at all,
            // silently dropping the one real failure.
            Directory.CreateDirectory(tmpDir);
            File.WriteAllBytes(Path.Combine(tmpDir, "partial.bin"), new byte[16]);
            File.SetUnixFileMode(tmpDir, UnixFileMode.UserRead | UnixFileMode.UserExecute);

            var stdout = new StringWriter();
            var original = Console.Out;
            int exit;
            try
            {
                Console.SetOut(stdout);
                exit = CliApp.Build().Parse(["run", "--project", work]).Invoke();
            }
            finally
            {
                Console.SetOut(original);
            }

            Assert.Equal(ExitCodes.Ok, exit);

            var expected = "cleaned 0 staging database(s) and 0 stale workdir(s) — " +
                $"freed {CleanCommand.FormatBytes(0)} (1 could not be deleted; run pz clean for details)";
            Assert.Contains(expected, stdout.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            if (Directory.Exists(tmpDir))
            {
                try
                {
                    File.SetUnixFileMode(tmpDir,
                        UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
                }
                catch { /* best-effort */ }
            }

            try { Directory.Delete(work, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }
}
