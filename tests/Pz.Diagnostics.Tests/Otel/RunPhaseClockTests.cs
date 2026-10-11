using Pz.Diagnostics.Otel;

namespace Pz.Diagnostics.Tests.Otel;

public sealed class RunPhaseClockTests
{
    private sealed class ManualTime : TimeProvider
    {
        public long Ticks;
        public DateTimeOffset Now = new(2026, 10, 11, 9, 0, 0, TimeSpan.Zero);
        public override long TimestampFrequency => TimeSpan.TicksPerSecond;
        public override long GetTimestamp() => Ticks;
        public override DateTimeOffset GetUtcNow() => Now;
        public void Advance(TimeSpan by) { Ticks += by.Ticks; Now += by; }
    }

    [Fact]
    public void Snapshot_has_finished_phases_and_the_open_one_so_far()
    {
        var time = new ManualTime();
        var clock = new RunPhaseClock(time);
        using (clock.Begin(RunPhaseClock.Load)) time.Advance(TimeSpan.FromMilliseconds(10));
        using (clock.Begin(RunPhaseClock.Compile)) time.Advance(TimeSpan.FromMilliseconds(20));
        var execute = clock.Begin(RunPhaseClock.Execute);
        time.Advance(TimeSpan.FromMilliseconds(300));

        var snapshot = clock.Snapshot();

        Assert.Equal(10, snapshot.LoadMs);
        Assert.Equal(20, snapshot.CompileMs);
        Assert.Equal(300, snapshot.ExecuteMs);
        Assert.Null(snapshot.ValidateMs);
        Assert.Null(snapshot.FinalizeMs);
        Assert.Equal(RunPhaseClock.Execute, clock.Current);
        execute.Dispose();
        Assert.Null(clock.Current);
        Assert.Equal(RunPhaseClock.Execute, clock.LastBegun);
    }

    [Fact]
    public void Started_at_is_the_clock_creation_time()
    {
        var time = new ManualTime();
        var clock = new RunPhaseClock(time);
        time.Advance(TimeSpan.FromSeconds(5));
        Assert.Equal(new DateTimeOffset(2026, 10, 11, 9, 0, 0, TimeSpan.Zero), clock.StartedAt);
    }

    [Fact]
    public void An_unknown_phase_is_refused()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new RunPhaseClock(TimeProvider.System).Begin("report"));
    }
}
