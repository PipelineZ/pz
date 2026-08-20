using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Engine.Execution;
using Pz.Engine.State;

namespace Pz.Engine.Tests.Execution;

public class SpecBuilderTests
{
    private static SourceDatasetDef CreateSampleDef()
    {
        var source = new ConnectionDef("test-source", "TestConnector", new Dictionary<string, object?>(),
            [new DatasetDef("test-dataset", new Dictionary<string, object?>(), null, null)],
            "sources/test.yml");
        var dataset = source.Datasets[0];
        return new SourceDatasetDef(source, dataset);
    }

    private static SourceDatasetDef CreateIncrementalDef()
    {
        var source = new ConnectionDef("test-source", "TestConnector", new Dictionary<string, object?>(),
            [new DatasetDef("test-dataset", new Dictionary<string, object?>(), null,
                new SyncModeDef(SyncMode.Incremental, new IncrementalDef("id")))],
            "sources/test.yml");
        var dataset = source.Datasets[0];
        return new SourceDatasetDef(source, dataset);
    }

    private static SourceDatasetDef CreateCdcDef(string? slot = null)
    {
        var source = new ConnectionDef("test-source", "TestConnector", new Dictionary<string, object?>(),
            [new DatasetDef("test-dataset", new Dictionary<string, object?>(), null,
                new SyncModeDef(SyncMode.Cdc, null, slot))],
            "sources/test.yml");
        var dataset = source.Datasets[0];
        return new SourceDatasetDef(source, dataset);
    }

    [Fact]
    public void ForSourceLoad_with_watermark_and_false_lowerInclusive_stays_false()
    {
        var def = CreateSampleDef();
        var wm = new Watermark("id", "bigint", "123", "run-1");
        var spec = SpecBuilder.ForSourceLoad(def, wm, null, lowerInclusive: false);
        Assert.False(spec.WatermarkLowerInclusive);
        Assert.Equal("id", spec.WatermarkCursor);
        Assert.Equal("123", spec.WatermarkValue);
    }

    [Fact]
    public void ForSourceLoad_with_watermark_and_true_lowerInclusive_sets_flag()
    {
        var def = CreateSampleDef();
        var wm = new Watermark("id", "bigint", "123", "run-1");
        var spec = SpecBuilder.ForSourceLoad(def, wm, null, lowerInclusive: true);
        Assert.True(spec.WatermarkLowerInclusive);
        Assert.Equal("id", spec.WatermarkCursor);
        Assert.Equal("123", spec.WatermarkValue);
    }

    [Fact]
    public void ForSourceLoad_with_null_watermark_stays_false_even_with_true_lowerInclusive()
    {
        var def = CreateSampleDef();
        var spec = SpecBuilder.ForSourceLoad(def, null, null, lowerInclusive: true);
        Assert.False(spec.WatermarkLowerInclusive);
        Assert.Null(spec.WatermarkCursor);
        Assert.Null(spec.WatermarkValue);
    }

    // A first-run incremental dataset has no stored watermark
    // (wm is null), but the http truncation guard (HttpPartition._cursorOrdinal, gated on
    // `spec.WatermarkCursor is null`) needs to arm on that very first run -- so the spec must still
    // carry the declared cursor NAME even though it has no value yet. Non-incremental datasets
    // (covered above) must keep getting no stamp at all.
    [Fact]
    public void ForSourceLoad_first_run_incremental_stamps_cursor_with_null_value()
    {
        var def = CreateIncrementalDef();
        var spec = SpecBuilder.ForSourceLoad(def, null);
        Assert.Equal("id", spec.WatermarkCursor);
        Assert.Null(spec.WatermarkValue);
    }

    [Fact]
    public void ForSourceLoad_full_refresh_incremental_stamps_cursor_with_null_value()
    {
        // --full-refresh makes SourceLoadExecutor pass wm: null even on a dataset with a stored
        // watermark -- must produce the exact same cursor-armed/value-null shape as a genuine first
        // run, never an unstamped spec.
        var def = CreateIncrementalDef();
        var spec = SpecBuilder.ForSourceLoad(def, null, null, lowerInclusive: false);
        Assert.Equal("id", spec.WatermarkCursor);
        Assert.Null(spec.WatermarkValue);
        Assert.False(spec.WatermarkLowerInclusive);
    }

    // cdc datasets stamp DatasetSpec.ChangeCapture/
    // ChangeCaptureSlot in the BASE ForSourceLoad(def, wm, upperBound) overload -- every other
    // overload funnels through it, so this proves the executor's watermark-carrying call sites get
    // the stamp too, not just the planner's watermark-free probe.

    [Fact]
    public void ForSourceLoad_cdc_dataset_stamps_ChangeCapture_and_slot()
    {
        var def = CreateCdcDef(slot: "pz_slot");
        var spec = SpecBuilder.ForSourceLoad(def);
        Assert.True(spec.ChangeCapture);
        Assert.Equal("pz_slot", spec.ChangeCaptureSlot);
    }

    [Fact]
    public void ForSourceLoad_cdc_dataset_without_slot_stamps_null_slot()
    {
        var def = CreateCdcDef();
        var spec = SpecBuilder.ForSourceLoad(def);
        Assert.True(spec.ChangeCapture);
        Assert.Null(spec.ChangeCaptureSlot);
    }

    [Fact]
    public void ForSourceLoad_non_cdc_dataset_stamps_no_ChangeCapture()
    {
        var def = CreateSampleDef();
        var spec = SpecBuilder.ForSourceLoad(def);
        Assert.False(spec.ChangeCapture);
        Assert.Null(spec.ChangeCaptureSlot);
    }

    [Fact]
    public void ForSourceLoad_cdc_dataset_stamps_ChangeCapture_through_watermark_overload()
    {
        var def = CreateCdcDef(slot: "pz_slot");
        var wm = new Watermark("id", "bigint", "123", "run-1");
        var spec = SpecBuilder.ForSourceLoad(def, wm, null);
        Assert.True(spec.ChangeCapture);
        Assert.Equal("pz_slot", spec.ChangeCaptureSlot);
    }

    [Fact]
    public void ForSinkOutput_threads_OnDelete_from_output_def()
    {
        var sink = new ConnectionDef("test-sink", "TestConnector", new Dictionary<string, object?>(), [],
            "sinks/test.yml") { Outputs = [new OutputDef("out", "stg_orders", "merge", "fail_on_change",
                new Dictionary<string, object?>(), ["id"], OnDelete: "delete")] };
        var def = new SinkOutputDef(sink, sink.Outputs[0]);

        var spec = SpecBuilder.ForSinkOutput(def);
        Assert.Equal("delete", spec.OnDelete);
    }

    [Fact]
    public void ForSinkOutput_null_OnDelete_stays_null()
    {
        var sink = new ConnectionDef("test-sink", "TestConnector", new Dictionary<string, object?>(), [],
            "sinks/test.yml") { Outputs = [new OutputDef("out", "stg_orders", "replace", "fail_on_change", new Dictionary<string, object?>())] };
        var def = new SinkOutputDef(sink, sink.Outputs[0]);

        var spec = SpecBuilder.ForSinkOutput(def);
        Assert.Null(spec.OnDelete);
    }
}
