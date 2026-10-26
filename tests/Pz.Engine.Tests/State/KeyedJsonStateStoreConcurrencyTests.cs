using Pz.Core.Validation;
using Pz.Engine.State;

namespace Pz.Engine.Tests.State;

/// <summary>Overlapping runs in one project share the state files. Each store instance here stands for
/// one run's view, exactly as <c>StateBackendFactory</c> builds one per run — the same shape the SQL
/// Server and HTTP stores' concurrency suites use.</summary>
public sealed class KeyedJsonStateStoreConcurrencyTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), $"pz-kv-conc-{Guid.NewGuid():N}");

    private WatermarkStore NewRun() => WatermarkStore.Local(_dir);

    private static Watermark Wm(string value, string runId) => new("updated_at", "BIGINT", value, runId);

    [Fact]
    public void Writers_of_different_keys_never_lose_each_others_entries()
    {
        const int writers = 8;
        const int keysPerWriter = 25;

        Parallel.For(0, writers, w =>
        {
            var run = NewRun();
            for (var k = 0; k < keysPerWriter; k++)
            {
                run.Set($"src.w{w}_k{k}", Wm($"{k}", $"run-{w}"));
            }
        });

        Assert.Equal(writers * keysPerWriter, NewRun().ListAll()!.Count);
    }

    [Fact]
    public void A_writer_whose_read_went_stale_is_PZ0520()
    {
        NewRun().Set("crm.orders", Wm("10", "run-0"));
        var first = NewRun();
        var second = NewRun();
        Assert.Equal("10", first.Get("crm.orders")!.Value);
        Assert.Equal("10", second.Get("crm.orders")!.Value);

        second.Set("crm.orders", Wm("25", "run-2"));
        var ex = Assert.Throws<PzConfigException>(() => first.Set("crm.orders", Wm("20", "run-1")));

        Assert.Equal(PzErrorCode.StateConcurrencyConflict, ex.Error.Code);
        Assert.Contains("crm.orders", ex.Error.Message);
        Assert.Equal("25", NewRun().Get("crm.orders")!.Value); // the older MAX(cursor) did not regress it
    }

    [Fact]
    public void A_key_that_appeared_after_it_was_read_absent_is_PZ0520()
    {
        var first = NewRun();
        var second = NewRun();
        Assert.Null(first.Get("crm.orders"));
        Assert.Null(second.Get("crm.orders"));

        second.Set("crm.orders", Wm("25", "run-2"));
        var ex = Assert.Throws<PzConfigException>(() => first.Set("crm.orders", Wm("20", "run-1")));

        Assert.Equal(PzErrorCode.StateConcurrencyConflict, ex.Error.Code);
    }

    [Fact]
    public void One_run_may_write_the_same_key_repeatedly()
    {
        var run = NewRun();
        Assert.Null(run.Get("crm.orders"));

        run.Set("crm.orders", Wm("10", "run-1"));
        run.Set("crm.orders", Wm("20", "run-1"));

        Assert.Equal("20", NewRun().Get("crm.orders")!.Value);
    }

    /// <summary>State-editing verbs and `pz cdc drop` write without a prior read; nothing was observed,
    /// so nothing can have gone stale.</summary>
    [Fact]
    public void A_write_with_no_prior_read_overwrites()
    {
        NewRun().Set("crm.orders", Wm("10", "run-0"));

        NewRun().Set("crm.orders", Wm("5", "operator"));

        Assert.Equal("5", NewRun().Get("crm.orders")!.Value);
    }

    [Fact]
    public void A_removed_key_can_be_written_again_by_the_same_run()
    {
        var run = NewRun();
        run.Set("crm.orders", Wm("10", "run-1"));
        Assert.Equal("10", run.Get("crm.orders")!.Value);

        run.Remove("crm.orders");
        run.Set("crm.orders", Wm("1", "run-1"));

        Assert.Equal("1", NewRun().Get("crm.orders")!.Value);
    }

    [Fact]
    public void The_lock_file_is_not_an_entry_and_leaves_the_state_file_byte_stable()
    {
        NewRun().Set("crm.orders", Wm("10", "run-1"));
        var bytes = File.ReadAllBytes(Path.Combine(_dir, "watermarks.json"));

        var again = NewRun();
        again.Get("crm.orders");
        again.Set("crm.orders", Wm("10", "run-1"));

        Assert.Equal(bytes, File.ReadAllBytes(Path.Combine(_dir, "watermarks.json")));
        Assert.Single(NewRun().ListAll()!);
    }

    public void Dispose()
    {
        if (Directory.Exists(_dir))
        {
            Directory.Delete(_dir, recursive: true);
        }
    }
}
