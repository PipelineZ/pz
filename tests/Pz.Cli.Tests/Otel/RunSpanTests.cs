using System.Collections.Concurrent;
using System.Diagnostics;
using Pz.Cli.Commands;
using Pz.Cli.Otel;
using Pz.Diagnostics.Otel;

namespace Pz.Cli.Tests.Otel;

public sealed class RunSpanTests
{
    private static (ActivityListener, ConcurrentBag<Activity>) Listen()
    {
        var stopped = new ConcurrentBag<Activity>();
        var listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == PzActivitySource.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = stopped.Add,
        };
        ActivitySource.AddActivityListener(listener);
        return (listener, stopped);
    }

    [Fact]
    public void Early_phases_become_back_dated_children_of_a_back_dated_run_span()
    {
        var (listener, stopped) = Listen();
        using (listener)
        {
            var clock = new RunPhaseClock(TimeProvider.System);
            using (clock.Begin(RunPhaseClock.Load)) { }
            using (clock.Begin(RunPhaseClock.Compile)) { }
            var id = Guid.NewGuid().ToString("N");

            using (var run = RunCommand.StartRunActivity(null, clock.StartedAt)!)
            {
                run.SetTag("pz.run.id", id);
                clock.AttachRunSpan(run);
                using (clock.Begin(RunPhaseClock.Plan)) { }
                Assert.Equal(clock.StartedAt.UtcDateTime, run.StartTimeUtc);
            }

            var runSpan = stopped.Single(a => a.OperationName == "run" && (string?)a.GetTagItem("pz.run.id") == id);
            var phases = stopped.Where(a => a.ParentSpanId == runSpan.SpanId).Select(a => a.OperationName);
            Assert.Equal(new[] { "phase.compile", "phase.load", "phase.plan" }, phases.Order());
            Assert.All(stopped.Where(a => a.ParentSpanId == runSpan.SpanId),
                p => Assert.True(p.StartTimeUtc >= runSpan.StartTimeUtc));
        }
    }

    [Fact]
    public void Run_span_guard_marks_an_uncompleted_run_as_error_with_its_phase()
    {
        var (listener, stopped) = Listen();
        using (listener)
        {
            var clock = new RunPhaseClock(TimeProvider.System);
            var id = Guid.NewGuid().ToString("N");
            using (var run = RunCommand.StartRunActivity(null, clock.StartedAt)!)
            {
                run.SetTag("pz.run.id", id);
                clock.AttachRunSpan(run);
                using var guard = new RunSpanGuard(run, clock);
                var plan = clock.Begin(RunPhaseClock.Plan);
                // a plan failure returns early: the phase scope and the guard are disposed without Complete
                plan.Dispose();
            }

            var runSpan = stopped.Single(a => a.OperationName == "run" && (string?)a.GetTagItem("pz.run.id") == id);
            Assert.Equal(ActivityStatusCode.Error, runSpan.Status);
            Assert.Equal("run ended after plan", runSpan.StatusDescription);
            Assert.Contains(stopped, a => a.OperationName == "phase.plan" && a.ParentSpanId == runSpan.SpanId);
        }
    }

    [Fact]
    public void Run_span_guard_names_the_open_phase_when_disposed_mid_phase()
    {
        var (listener, stopped) = Listen();
        using (listener)
        {
            var clock = new RunPhaseClock(TimeProvider.System);
            var id = Guid.NewGuid().ToString("N");
            using (var run = RunCommand.StartRunActivity(null, clock.StartedAt)!)
            {
                run.SetTag("pz.run.id", id);
                clock.AttachRunSpan(run);
                var guard = new RunSpanGuard(run, clock);
                var plan = clock.Begin(RunPhaseClock.Plan);
                guard.Dispose();
                plan.Dispose();
            }

            var runSpan = stopped.Single(a => a.OperationName == "run" && (string?)a.GetTagItem("pz.run.id") == id);
            Assert.Equal(ActivityStatusCode.Error, runSpan.Status);
            Assert.Equal("run ended during plan", runSpan.StatusDescription);
        }
    }

    [Fact]
    public void Run_span_guard_marks_a_completed_run_error_when_finalize_did_not_finish()
    {
        var (listener, stopped) = Listen();
        using (listener)
        {
            var clock = new RunPhaseClock(TimeProvider.System);
            var id = Guid.NewGuid().ToString("N");
            using (var run = RunCommand.StartRunActivity(null, clock.StartedAt)!)
            {
                run.SetTag("pz.run.id", id);
                var guard = new RunSpanGuard(run, clock);
                var finalize = clock.Begin(RunPhaseClock.Finalize);
                guard.Complete("success", 1, 0, 0);
                guard.Dispose();
                finalize.Dispose();
            }

            var runSpan = stopped.Single(a => a.OperationName == "run" && (string?)a.GetTagItem("pz.run.id") == id);
            Assert.Equal(ActivityStatusCode.Error, runSpan.Status);
            Assert.Equal("run ended during finalize", runSpan.StatusDescription);
        }
    }

    [Fact]
    public void Completed_run_span_carries_status_and_counts()
    {
        var (listener, stopped) = Listen();
        using (listener)
        {
            var clock = new RunPhaseClock(TimeProvider.System);
            var id = Guid.NewGuid().ToString("N");
            using (var run = RunCommand.StartRunActivity(null, clock.StartedAt)!)
            {
                run.SetTag("pz.run.id", id);
                using var guard = new RunSpanGuard(run, clock);
                guard.Complete("completed_with_failures", 5, 1, 2);
            }

            var runSpan = stopped.Single(a => a.OperationName == "run" && (string?)a.GetTagItem("pz.run.id") == id);
            Assert.Equal("completed_with_failures", runSpan.GetTagItem("pz.run.status"));
            Assert.Equal(5, runSpan.GetTagItem("pz.run.nodes.succeeded"));
            Assert.Equal(1, runSpan.GetTagItem("pz.run.nodes.failed"));
            Assert.Equal(2, runSpan.GetTagItem("pz.run.nodes.skipped"));
            Assert.Equal(8, runSpan.GetTagItem("pz.run.node_count"));
            Assert.Equal(ActivityStatusCode.Error, runSpan.Status);
            Assert.Equal("completed_with_failures", runSpan.StatusDescription);
        }
    }
}
