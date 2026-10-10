using Pz.Engine.Execution;

namespace Pz.Cli.Commands;

/// <summary>Why `pz run --until-caught-up` stopped repeating the run.</summary>
internal enum CaughtUpStop { CaughtUp, RunFailed, NoStoppingSource, MaxRuns }

/// <summary>The stop rule behind `pz run --until-caught-up`. A pass's <see cref="NodeResult.CaughtUp"/> is
/// true when that run's window reached its stop, so the loop ends on the pass that loads the final slice —
/// never one extra empty pass. Nodes with no flag (non-windowed, windowed without a stop, non-SourceLoad)
/// neither hold the loop open nor close it.</summary>
internal static class CaughtUpLoop
{
    public const int DefaultMaxRuns = 100;

    /// <summary>Null means run another pass. A failed pass wins over every other reason, so its exit code
    /// is what the loop returns; catching up on the last allowed pass counts as caught up.</summary>
    public static CaughtUpStop? Decide(int exitCode, IReadOnlyList<NodeResult> results, int pass, int maxRuns)
    {
        if (exitCode != ExitCodes.Ok)
        {
            return CaughtUpStop.RunFailed;
        }

        var flags = results.Where(r => r.CaughtUp is not null).Select(r => r.CaughtUp!.Value).ToList();
        if (flags.Count == 0)
        {
            return CaughtUpStop.NoStoppingSource;
        }

        if (flags.All(caughtUp => caughtUp))
        {
            return CaughtUpStop.CaughtUp;
        }

        return pass >= maxRuns ? CaughtUpStop.MaxRuns : null;
    }

    public static string Summary(CaughtUpStop stop, int passes, int maxRuns)
    {
        var reason = stop switch
        {
            CaughtUpStop.CaughtUp => "caught up",
            CaughtUpStop.RunFailed => "run failed",
            CaughtUpStop.NoStoppingSource =>
                "no windowed source with a stop (`until` or a SQL ceiling) in this run",
            CaughtUpStop.MaxRuns =>
                $"max runs ({maxRuns}) reached before every source caught up -- " +
                "run again to continue from the stored watermarks",
            _ => throw new ArgumentOutOfRangeException(nameof(stop), stop, "unknown stop reason"),
        };
        return $"until-caught-up: {passes} pass{(passes == 1 ? "" : "es")}, stopped: {reason}";
    }
}
