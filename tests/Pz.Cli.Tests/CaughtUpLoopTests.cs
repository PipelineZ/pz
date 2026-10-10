using Pz.Cli;
using Pz.Cli.Commands;
using Pz.Core.Dag;
using Pz.Engine.Execution;

namespace Pz.Cli.Tests;

/// <summary>The stop rule behind `pz run --until-caught-up`: given one pass's exit code and node results,
/// whether another pass runs and, if not, why the loop stopped.</summary>
public sealed class CaughtUpLoopTests
{
    [Fact]
    public void A_failed_pass_stops_the_loop()
    {
        var stop = CaughtUpLoop.Decide(ExitCodes.NodeFailures, [Load("a", caughtUp: false)], pass: 1, maxRuns: 100);

        Assert.Equal(CaughtUpStop.RunFailed, stop);
    }

    [Fact]
    public void A_source_still_behind_runs_another_pass()
    {
        var stop = CaughtUpLoop.Decide(ExitCodes.Ok, [Load("a", caughtUp: true), Load("b", caughtUp: false)],
            pass: 1, maxRuns: 100);

        Assert.Null(stop);
    }

    [Fact]
    public void Every_source_caught_up_stops_the_loop()
    {
        var stop = CaughtUpLoop.Decide(ExitCodes.Ok, [Load("a", caughtUp: true), Load("b", caughtUp: true)],
            pass: 3, maxRuns: 100);

        Assert.Equal(CaughtUpStop.CaughtUp, stop);
    }

    [Fact]
    public void Nodes_without_a_flag_do_not_hold_the_loop_open()
    {
        var stop = CaughtUpLoop.Decide(ExitCodes.Ok, [Load("a", caughtUp: null), Load("b", caughtUp: true)],
            pass: 1, maxRuns: 100);

        Assert.Equal(CaughtUpStop.CaughtUp, stop);
    }

    [Fact]
    public void No_windowed_source_with_a_stop_runs_once()
    {
        var stop = CaughtUpLoop.Decide(ExitCodes.Ok, [Load("a", caughtUp: null)], pass: 1, maxRuns: 100);

        Assert.Equal(CaughtUpStop.NoStoppingSource, stop);
    }

    [Fact]
    public void Reaching_max_runs_while_behind_stops_the_loop()
    {
        var stop = CaughtUpLoop.Decide(ExitCodes.Ok, [Load("a", caughtUp: false)], pass: 5, maxRuns: 5);

        Assert.Equal(CaughtUpStop.MaxRuns, stop);
    }

    [Fact]
    public void Catching_up_on_the_last_allowed_pass_counts_as_caught_up()
    {
        var stop = CaughtUpLoop.Decide(ExitCodes.Ok, [Load("a", caughtUp: true)], pass: 5, maxRuns: 5);

        Assert.Equal(CaughtUpStop.CaughtUp, stop);
    }

    [Fact]
    public void Max_runs_does_not_hide_a_failed_pass()
    {
        var stop = CaughtUpLoop.Decide(ExitCodes.Fatal, [], pass: 5, maxRuns: 5);

        Assert.Equal(CaughtUpStop.RunFailed, stop);
    }

    [Theory]
    [InlineData((int)CaughtUpStop.CaughtUp, 3, "until-caught-up: 3 passes, stopped: caught up")]
    [InlineData((int)CaughtUpStop.RunFailed, 2, "until-caught-up: 2 passes, stopped: run failed")]
    [InlineData((int)CaughtUpStop.Cancelled, 2, "until-caught-up: 2 passes, stopped: cancelled")]
    [InlineData((int)CaughtUpStop.MaxRuns, 5,
        "until-caught-up: 5 passes, stopped: max runs (5) reached before every source caught up -- " +
        "run again to continue from the stored watermarks")]
    [InlineData((int)CaughtUpStop.NoStoppingSource, 1,
        "until-caught-up: 1 pass, stopped: no windowed source with a stop (`until` or a SQL ceiling) in this run")]
    // The reason travels as an int: xunit needs a public signature and CaughtUpStop is internal.
    public void Summary_names_passes_and_reason(int stop, int passes, string expected)
    {
        Assert.Equal(expected, CaughtUpLoop.Summary((CaughtUpStop)stop, passes, maxRuns: 5));
    }

    private static NodeResult Load(string name, bool? caughtUp) =>
        new(new NodeId($"source_load:{name}"), NodeKind.SourceLoad, name, NodeStatus.Success, 0, TimeSpan.Zero,
            null, CaughtUp: caughtUp);
}
