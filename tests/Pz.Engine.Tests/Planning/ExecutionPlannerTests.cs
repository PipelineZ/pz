using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Execution;
using Pz.Engine.Planning;

namespace Pz.Engine.Tests.Planning;

public sealed class ExecutionPlannerTests
{
    // Reuses tests/Pz.Engine.Tests fixture helpers (TestDags.SourcePipelineSink() building a 3-node
    // dag over connector name "stub" — follow the existing NodeExecutorTests fixture style).

    [Fact]
    public async Task Native_capable_source_gets_native_scan_strategy()
    {
        var (dag, registry) = TestDags.SourcePipelineSink(new StubNativeSource(), new StubUniversalSink());
        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, CancellationToken.None);

        var load = plan.Nodes.Single(n => n.Kind == NodeKind.SourceLoad);
        Assert.Equal(EdgeStrategy.NativeScan, load.Strategy);
        Assert.Equal("native scan: connector 'stub' provides stub_scan over orders (read=full)", load.Reason);
    }

    [Fact]
    public async Task Universal_source_gets_arrow_stream_strategy()
    {
        var (dag, registry) = TestDags.SourcePipelineSink(new StubUniversalSource(), new StubUniversalSink());
        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, CancellationToken.None);

        var load = plan.Nodes.Single(n => n.Kind == NodeKind.SourceLoad);
        Assert.Equal(EdgeStrategy.ArrowStream, load.Strategy);
        Assert.Equal("arrow stream: connector 'stub' has no native path (read=full)", load.Reason);
    }

    [Fact]
    public async Task Force_universal_overrides_native()
    {
        var (dag, registry) = TestDags.SourcePipelineSink(new StubNativeSource(), new StubUniversalSink());
        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: true, CancellationToken.None);

        // SourceLoad's reason carries the resolved-read-shape token; SinkWrite has no read
        // shape to report, so its reason is unchanged.
        var load = plan.Nodes.Single(n => n.Kind == NodeKind.SourceLoad);
        Assert.Equal("arrow stream: engine.force_universal = true (read=full)", load.Reason);
        var sink = plan.Nodes.Single(n => n.Kind == NodeKind.SinkWrite);
        Assert.Equal("arrow stream: engine.force_universal = true", sink.Reason);
    }

    [Fact]
    public async Task Native_only_sink_under_force_universal_is_error_PZ0312()
    {
        var (dag, registry) = TestDags.SourcePipelineSink(new StubUniversalSource(), new StubNativeOnlySink());
        var ex = await Assert.ThrowsAsync<PzValidationException>(() =>
            new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: true, CancellationToken.None));

        Assert.Contains(ex.Errors, e => e.Code == PzErrorCode.NativePathRequired);
    }

    [Fact]
    public async Task Unconfigured_dataset_on_native_capable_source_still_plans_native_scan_regression()
    {
        // Baseline regression net: an unconfigured dataset must plan a native scan, unaffected by the
        // force_universal / files_per_partition gates checked above it in the planner.
        var (dag, registry) = TestDags.SourcePipelineSink(new StubNativeSource(), new StubUniversalSink());
        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, CancellationToken.None);

        var load = plan.Nodes.Single(n => n.Kind == NodeKind.SourceLoad);
        Assert.Equal(EdgeStrategy.NativeScan, load.Strategy);
        Assert.Equal("native scan: connector 'stub' provides stub_scan over orders (read=full)", load.Reason);
    }

    [Fact]
    public async Task Pipeline_nodes_get_duck_sql()
    {
        var (dag, registry) = TestDags.SourcePipelineSink(new StubUniversalSource(), new StubUniversalSink());
        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, CancellationToken.None);

        var pipeline = plan.Nodes.Single(n => n.Kind == NodeKind.Pipeline);
        Assert.Equal(EdgeStrategy.DuckSql, pipeline.Strategy);
        Assert.Equal("duckdb sql: executes in-engine", pipeline.Reason);
    }

    [Fact]
    public async Task Partitioned_source_plan_records_partition_count()
    {
        var datasetOptions = new Dictionary<string, object?> { ["partition_column"] = "id", ["partitions"] = 4 };
        var (dag, registry) = TestDags.SourcePipelineSink(
            new StubPartitionedUniversalSource(), new StubUniversalSink(), datasetOptions);
        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, CancellationToken.None);

        var load = plan.Nodes.Single(n => n.Kind == NodeKind.SourceLoad);
        Assert.Equal(4, load.Partitions);
        Assert.Equal("arrow stream: connector 'stub' has no native path (4 partitions) (read=full)", load.Reason);
    }

    // DeclaredPartitionCount clamps to [1,16] -- the same bound
    // PostgresSource.ParsePartitionCount enforces at run time -- so a config with an out-of-range
    // "partitions" value never shows a 0 or 17 in plan.json even though the executor's real PlanReadAsync
    // call will fail that same config with a named PzConnectorException.
    [Theory]
    [InlineData(0, 1)]
    [InlineData(17, 16)]
    public async Task Partitioned_source_plan_clamps_out_of_range_partition_count(int declared, int expectedClamped)
    {
        var datasetOptions = new Dictionary<string, object?> { ["partition_column"] = "id", ["partitions"] = declared };
        var (dag, registry) = TestDags.SourcePipelineSink(
            new StubPartitionedUniversalSource(), new StubUniversalSink(), datasetOptions);
        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, CancellationToken.None);

        var load = plan.Nodes.Single(n => n.Kind == NodeKind.SourceLoad);
        Assert.Equal(expectedClamped, load.Partitions);
    }

    [Fact]
    public async Task Reason_never_contains_sql_fragment()
    {
        // StubNativeSource's fragment and StubNativeOnlySink's setup contain SECRET_MARKER.
        var (dag, registry) = TestDags.SourcePipelineSink(new StubNativeSource(), new StubNativeOnlySink());
        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, CancellationToken.None);

        Assert.All(plan.Nodes, n => Assert.DoesNotContain("SECRET_MARKER", n.Reason));
    }

    [Fact]
    public async Task Windowed_dataset_on_non_boundedwindow_connector_is_PZ0313()
    {
        // Stub source connector WITHOUT BoundedWindow; dataset declares max_window.
        var dataset = new DatasetDef("orders", new Dictionary<string, object?> { ["table"] = "orders" },
            new Dictionary<string, string> { ["updated_at"] = "timestamp" },
            new SyncModeDef(SyncMode.Incremental, new IncrementalDef("updated_at", "1d", "2020-01-01T00:00:00.000000")));
        var (dag, registry) = TestDags.DagAndRegistryWithStubSource(dataset, ConnectorCapabilities.None);
        var ex = await Assert.ThrowsAsync<PzValidationException>(
            () => new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default));
        var error = Assert.Single(ex.Errors, e => e.Code == "PZ0313");
        Assert.Contains("max_window", error.ToString());
        Assert.Contains("orders", error.ToString());
        Assert.Contains("stub", error.ToString());
    }

    [Fact]
    public async Task Windowed_dataset_on_boundedwindow_connector_plans_clean()
    {
        var dataset = new DatasetDef("orders", new Dictionary<string, object?> { ["table"] = "orders" },
            new Dictionary<string, string> { ["updated_at"] = "timestamp" },
            new SyncModeDef(SyncMode.Incremental, new IncrementalDef("updated_at", "1d", "2020-01-01T00:00:00.000000")));
        var (dag, registry) = TestDags.DagAndRegistryWithStubSource(dataset, ConnectorCapabilities.BoundedWindow);
        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default);
        Assert.Contains(plan.Nodes, n => n.Kind == NodeKind.SourceLoad);
    }

    [Fact]
    public async Task A_sql_ceiling_on_a_connector_without_BoundedWindow_is_PZ0313()
    {
        var dataset = new DatasetDef("orders", new Dictionary<string, object?> { ["table"] = "orders" },
            new Dictionary<string, string> { ["updated_at"] = "timestamp" },
            new SyncModeDef(SyncMode.Incremental, new IncrementalDef("updated_at", DeclaredInSql: true,
                SqlBounds:
                [
                    new SqlWatermarkBound("stg_orders", false, "'__pz_watermark__stub__orders__'",
                        "__pz_watermark__stub__orders__"),
                    new SqlWatermarkBound("stg_orders", true, "'__pz_watermark__stub__orders__' + interval 7 day",
                        "__pz_watermark__stub__orders__", IsUpper: true),
                ])));
        var (dag, registry) = TestDags.DagAndRegistryWithStubSource(dataset, ConnectorCapabilities.None);

        var ex = await Assert.ThrowsAsync<PzValidationException>(
            () => new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.WindowCapabilityMissing);
        Assert.Contains("orders", error.ToString());
    }

    [Fact]
    public async Task A_sql_floor_alone_needs_no_BoundedWindow()
    {
        // A floor is best-effort: an unapplied one merely over-extracts, and the pipeline filter cuts.
        // Only a CEILING constrains advancement, so only a ceiling gates.
        var dataset = new DatasetDef("orders", new Dictionary<string, object?> { ["table"] = "orders" },
            new Dictionary<string, string> { ["updated_at"] = "timestamp" },
            new SyncModeDef(SyncMode.Incremental, new IncrementalDef("updated_at", DeclaredInSql: true,
                SqlBounds:
                [
                    new SqlWatermarkBound("stg_orders", false, "'__pz_watermark__stub__orders__'",
                        "__pz_watermark__stub__orders__"),
                ])));
        var (dag, registry) = TestDags.DagAndRegistryWithStubSource(dataset, ConnectorCapabilities.None);

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default);

        Assert.Contains(plan.Nodes, n => n.Kind == NodeKind.SourceLoad);
    }

    // The path-templating capability gate (PZ0314): mirrors the PZ0313 tests above --
    // a date-templated dataset path (with cursor+window, so DagCompiler's PZ0217/0221 would pass)
    // must be refused by the planner when the source connector doesn't declare PathTemplating.

    [Fact]
    public async Task Templated_source_path_on_non_pathtemplating_connector_is_PZ0314()
    {
        var dataset = new DatasetDef("orders",
            new Dictionary<string, object?> { ["path"] = "orders/{yyyy}/{MM}/{dd}/data.parquet" },
            new Dictionary<string, string> { ["updated_at"] = "timestamp" },
            new SyncModeDef(SyncMode.Incremental, new IncrementalDef("updated_at", "1d", "2020-01-01T00:00:00.000000")));
        var (dag, registry) = TestDags.DagAndRegistryWithStubSource(dataset, ConnectorCapabilities.None);
        var ex = await Assert.ThrowsAsync<PzValidationException>(
            () => new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default));
        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.TemplatingCapabilityMissing);
        Assert.Contains("orders", error.ToString());
        Assert.Contains("stub", error.ToString());
    }

    [Fact]
    public async Task Templated_source_path_on_pathtemplating_connector_plans_clean()
    {
        var dataset = new DatasetDef("orders",
            new Dictionary<string, object?> { ["path"] = "orders/{yyyy}/{MM}/{dd}/data.parquet" },
            new Dictionary<string, string> { ["updated_at"] = "timestamp" },
            new SyncModeDef(SyncMode.Incremental, new IncrementalDef("updated_at", "1d", "2020-01-01T00:00:00.000000")));
        var (dag, registry) = TestDags.DagAndRegistryWithStubSource(dataset,
            ConnectorCapabilities.PathTemplating | ConnectorCapabilities.BoundedWindow);
        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default);
        Assert.Contains(plan.Nodes, n => n.Kind == NodeKind.SourceLoad);
    }

    [Fact]
    public async Task Partitioned_output_on_non_pathtemplating_sink_is_PZ0314()
    {
        var output = new OutputDef("out", "stg_orders", "replace", "fail_on_change",
            new Dictionary<string, object?>
            {
                ["path"] = "orders/{yyyy}/{MM}/{dd}/",
                ["partition_by"] = "event_date",
            });
        var (dag, registry) = TestDags.DagAndRegistryWithStubSink(output, ConnectorCapabilities.None);
        var ex = await Assert.ThrowsAsync<PzValidationException>(
            () => new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default));
        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.TemplatingCapabilityMissing);
        Assert.Contains("out", error.ToString());
        Assert.Contains("stub", error.ToString());
    }

    /// <summary>No calendar tokens in the path means the DESTINATION owns the layout — a format that
    /// records its own partition columns (Delta, Iceberg, Hive-layout parquet). DagCompiler no longer
    /// refuses that shape, so the planner is what must refuse a connector that cannot honour it: without
    /// the flag every row would land in one unpartitioned place.</summary>
    [Fact]
    public async Task Column_partitioned_output_on_a_sink_that_cannot_partition_is_PZ0314()
    {
        var output = new OutputDef("out", "stg_orders", "replace", "fail_on_change",
            new Dictionary<string, object?> { ["partition_by"] = new List<object?> { "dt" } });
        var (dag, registry) = TestDags.DagAndRegistryWithStubSink(output, ConnectorCapabilities.PathTemplating);

        var ex = await Assert.ThrowsAsync<PzValidationException>(
            () => new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.TemplatingCapabilityMissing);
        Assert.Contains("ColumnPartitionedWrites", error.ToString());
    }

    [Fact]
    public async Task Column_partitioned_output_on_a_sink_that_declares_it_plans_clean()
    {
        var output = new OutputDef("out", "stg_orders", "replace", "fail_on_change",
            new Dictionary<string, object?> { ["partition_by"] = new List<object?> { "dt", "region" } });
        var (dag, registry) = TestDags.DagAndRegistryWithStubSink(
            output, ConnectorCapabilities.ColumnPartitionedWrites | ConnectorCapabilities.ReplaceWrites);

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default);

        Assert.Contains(plan.Nodes, n => n.Kind == NodeKind.SinkWrite);
    }

    /// <summary>The two capabilities are not interchangeable: a connector that renders pz's calendar
    /// tokens does not thereby know how to partition a table by column value, and vice versa.</summary>
    [Fact]
    public async Task A_templated_path_still_requires_PathTemplating_not_the_column_flag()
    {
        var output = new OutputDef("out", "stg_orders", "replace", "fail_on_change",
            new Dictionary<string, object?>
            {
                ["path"] = "orders/{yyyy}/{MM}/{dd}/",
                ["partition_by"] = "event_date",
            });
        var (dag, registry) = TestDags.DagAndRegistryWithStubSink(
            output, ConnectorCapabilities.ColumnPartitionedWrites);

        var ex = await Assert.ThrowsAsync<PzValidationException>(
            () => new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.TemplatingCapabilityMissing);
        Assert.Contains("PathTemplating", error.ToString());
    }

    [Fact]
    public async Task Partitioned_output_on_pathtemplating_sink_plans_clean()
    {
        var output = new OutputDef("out", "stg_orders", "replace", "fail_on_change",
            new Dictionary<string, object?>
            {
                ["path"] = "orders/{yyyy}/{MM}/{dd}/",
                ["partition_by"] = "event_date",
            });
        // The output's mode is "replace" incidentally (every OutputDef needs
        // one) -- ReplaceWrites is added here so this test keeps exercising only the PathTemplating gate.
        var (dag, registry) = TestDags.DagAndRegistryWithStubSink(
            output, ConnectorCapabilities.PathTemplating | ConnectorCapabilities.ReplaceWrites);
        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default);
        Assert.Contains(plan.Nodes, n => n.Kind == NodeKind.SinkWrite);
    }

    // The source-side mirrors of the INativeOnlySink
    // checks — a native-only SOURCE (no universal read path) must refuse force_universal/
    // files_per_partition at plan time instead of dooming the run.

    [Fact]
    public async Task Force_universal_on_native_only_source_is_error_PZ0312()
    {
        var (dag, registry) = TestDags.SourcePipelineSink(new StubNativeOnlySource(), new StubUniversalSink());

        var ex = await Assert.ThrowsAsync<PzValidationException>(() =>
            new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: true, CancellationToken.None));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.NativePathRequired);
        Assert.Contains("engine.force_universal", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Files_per_partition_on_native_only_source_is_error_PZ0312()
    {
        var (dag, registry) = TestDags.SourcePipelineSink(
            new StubNativeOnlySource(), new StubUniversalSink(),
            datasetOptions: new Dictionary<string, object?> { ["files_per_partition"] = 512 });

        var ex = await Assert.ThrowsAsync<PzValidationException>(() =>
            new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, CancellationToken.None));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.NativePathRequired);
        Assert.Contains("files_per_partition", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Unconfigured_native_only_source_still_plans_native_scan_regression()
    {
        var (dag, registry) = TestDags.SourcePipelineSink(new StubNativeOnlySource(), new StubUniversalSink());
        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, CancellationToken.None);

        var load = plan.Nodes.Single(n => n.Kind == NodeKind.SourceLoad);
        Assert.Equal(EdgeStrategy.NativeScan, load.Strategy);
    }

    // rate_limit + non-GatedOperations connector capability gate
    // (PZ0317), de-duplicated per instance -- mirrors the PZ0313/PZ0314 arrangement above.

    [Fact]
    public async Task Rate_limit_without_capability_refused()
    {
        var (dag, registry) = TestDags.DagAndRegistryWithStubSourceRateLimit(
            ConnectorCapabilities.None, new RateLimitDef(60, null), "orders");
        var ex = await Assert.ThrowsAsync<PzValidationException>(
            () => new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.PacingUnsupported);
        Assert.Contains("orders_source", error.ToString());
        Assert.Contains("stub", error.ToString());
        Assert.Contains("GatedOperations", error.ToString());
    }

    [Fact]
    public async Task Rate_limit_with_capability_planned()
    {
        var (dag, registry) = TestDags.DagAndRegistryWithStubSourceRateLimit(
            ConnectorCapabilities.GatedOperations, new RateLimitDef(60, null), "orders");
        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default);

        Assert.Contains(plan.Nodes, n => n.Kind == NodeKind.SourceLoad);
    }

    [Fact]
    public async Task Refusal_deduped_across_datasets()
    {
        var (dag, registry) = TestDags.DagAndRegistryWithStubSourceRateLimit(
            ConnectorCapabilities.None, new RateLimitDef(60, null), "orders", "returns", "refunds");
        var ex = await Assert.ThrowsAsync<PzValidationException>(
            () => new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default));

        Assert.Single(ex.Errors, e => e.Code == PzErrorCode.PacingUnsupported);
    }

    [Fact]
    public async Task Sink_side_refused()
    {
        var (dag, registry) = TestDags.DagAndRegistryWithStubSinkRateLimit(
            ConnectorCapabilities.None, new RateLimitDef(60, null));
        var ex = await Assert.ThrowsAsync<PzValidationException>(
            () => new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.PacingUnsupported);
        Assert.Contains("orders_sink", error.ToString());
        Assert.Contains("stub", error.ToString());
        Assert.Contains("GatedOperations", error.ToString());
    }

    // PZ0317 corner: GatedOperations is a connector-wide capability flag, but a connector can adopt it
    // sink-only (azureblob-shaped: AzureConnector declares GatedOperations, yet AzureSource is
    // native-only-read), so rate_limit on such a SOURCE passes the HasFlag(GatedOperations) check while
    // being statically inert. StubNativeOnlySourceWithGatedOperations reproduces exactly that shape.

    [Fact]
    public async Task Rate_limit_on_native_only_read_source_refused_PZ0317()
    {
        var (dag, registry) = TestDags.DagAndRegistryWithNativeOnlyStubSourceRateLimit(
            new RateLimitDef(60, null), "orders");
        var ex = await Assert.ThrowsAsync<PzValidationException>(
            () => new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.PacingUnsupported);
        Assert.Contains("orders_source", error.ToString());
        Assert.Contains("reads natively", error.ToString());
    }

    [Fact]
    public async Task Rate_limit_on_native_only_read_source_dedup_still_holds()
    {
        var (dag, registry) = TestDags.DagAndRegistryWithNativeOnlyStubSourceRateLimit(
            new RateLimitDef(60, null), "orders", "returns", "refunds");
        var ex = await Assert.ThrowsAsync<PzValidationException>(
            () => new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default));

        Assert.Single(ex.Errors, e => e.Code == PzErrorCode.PacingUnsupported);
    }

    // Gate-capable reads (e.g. the http connector's universal-only, GatedOperations-capable stub)
    // must still be accepted -- Rate_limit_with_capability_planned above already covers this: that
    // stub is NOT INativeOnlySource, so the new native-only-read check never fires for it.

    [Fact]
    public async Task No_rate_limit_no_check()
    {
        var (dag, registry) = TestDags.DagAndRegistryWithStubSourceRateLimit(
            ConnectorCapabilities.None, null, "orders");
        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default);

        Assert.Contains(plan.Nodes, n => n.Kind == NodeKind.SourceLoad);
    }

    // -- Pushdown reporting ---------------------------------------------------------------------

    [Fact]
    public async Task Plan_reports_what_a_capable_connector_will_be_asked_for()
    {
        var dataset = new DatasetDef("orders", new Dictionary<string, object?>(), null);
        var (dag, registry) = TestDags.DagAndRegistryWithStubSource(dataset,
            ConnectorCapabilities.ColumnPruning | ConnectorCapabilities.PredicatePushdown,
            new ReadHintPlan(["id", "amount"], "status = 'open'"));

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default);

        var node = Assert.Single(plan.Nodes, n => n.Kind == NodeKind.SourceLoad);
        Assert.Equal(new PushdownInfo(2, PredicatePushed: true), node.Pushdown);
    }

    [Fact]
    public async Task Plan_reports_no_pushdown_for_an_incapable_connector()
    {
        // The compiler still found something pushable; this connector simply does not take it, and the
        // plan must say so rather than advertise an optimisation that will not happen.
        var dataset = new DatasetDef("orders", new Dictionary<string, object?>(), null);
        var (dag, registry) = TestDags.DagAndRegistryWithStubSource(dataset, ConnectorCapabilities.None,
            new ReadHintPlan(["id", "amount"], "status = 'open'"));

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default);

        Assert.Null(Assert.Single(plan.Nodes, n => n.Kind == NodeKind.SourceLoad).Pushdown);
    }

    [Fact]
    public async Task Plan_reports_only_the_half_the_connector_declares()
    {
        var dataset = new DatasetDef("orders", new Dictionary<string, object?>(), null);
        var (dag, registry) = TestDags.DagAndRegistryWithStubSource(dataset,
            ConnectorCapabilities.PredicatePushdown, new ReadHintPlan(["id", "amount"], "status = 'open'"));

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default);

        Assert.Equal(new PushdownInfo(null, PredicatePushed: true),
            Assert.Single(plan.Nodes, n => n.Kind == NodeKind.SourceLoad).Pushdown);
    }
}
