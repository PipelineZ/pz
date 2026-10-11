using Pz.Engine.Execution;

namespace Pz.Engine.Tests.Execution;

public sealed class NodeFactsRecorderTests
{
    private sealed class ManualTime : TimeProvider
    {
        public long Ticks;
        public override long TimestampFrequency => TimeSpan.TicksPerSecond;
        public override long GetTimestamp() => Ticks;
        public void Advance(TimeSpan by) => Ticks += by.Ticks;
    }

    [Fact]
    public void A_stage_records_its_own_wall_time()
    {
        var time = new ManualTime();
        var recorder = new NodeFactsRecorder(time);

        using (recorder.Stage("ingest")) time.Advance(TimeSpan.FromMilliseconds(250));

        Assert.Equal(TimeSpan.FromMilliseconds(250), recorder.Stages()!.Ingest);
        Assert.Null(recorder.Stages()!.Extract);
    }

    [Fact]
    public void Reset_forgets_the_previous_attempt()
    {
        var time = new ManualTime();
        var recorder = new NodeFactsRecorder(time);
        recorder.Strategy = NodeFacts.Arrow;
        using (recorder.Stage("extract")) time.Advance(TimeSpan.FromSeconds(1));

        recorder.Reset();
        using (recorder.Stage("ingest")) time.Advance(TimeSpan.FromSeconds(2));

        Assert.Null(recorder.Stages()!.Extract);
        Assert.Equal(TimeSpan.FromSeconds(2), recorder.Stages()!.Ingest);
        Assert.Null(recorder.Strategy);
    }

    [Fact]
    public void Disposing_a_stage_twice_records_once()
    {
        var time = new ManualTime();
        var recorder = new NodeFactsRecorder(time);
        var scope = recorder.Stage("write");
        time.Advance(TimeSpan.FromSeconds(1));
        scope.Dispose();
        time.Advance(TimeSpan.FromSeconds(5));
        scope.Dispose();

        Assert.Equal(TimeSpan.FromSeconds(1), recorder.Stages()!.Write);
    }

    [Fact]
    public void No_stage_means_no_stages()
    {
        Assert.Null(new NodeFactsRecorder().Stages());
    }

    [Fact]
    public void An_unknown_stage_name_is_refused()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new NodeFactsRecorder().Stage("load"));
    }
}
