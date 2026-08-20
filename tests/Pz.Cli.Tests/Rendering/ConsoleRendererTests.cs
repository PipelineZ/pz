using Pz.Cli.Rendering;
using Pz.Diagnostics.Events;

namespace Pz.Cli.Tests.Rendering;

/// <summary>Byte-for-byte regression net: this is the format every CLI/e2e assertion depends on —
/// `ConsoleRenderer` must reproduce it exactly, even though rendering lives on the bus rather than
/// inside the crash-safe snapshot path.</summary>
public class ConsoleRendererTests
{
    private static readonly DateTimeOffset At = new(2026, 7, 4, 10, 0, 0, TimeSpan.Zero);

    [Theory]
    [InlineData("success", "ok")]
    [InlineData("failed", "FAIL")]
    [InlineData("skipped", "skip")]
    public void NodeCompleted_prints_today_s_line_format(string status, string marker)
    {
        var writer = new StringWriter();
        var renderer = new ConsoleRenderer(writer);

        renderer.Render(new NodeCompletedEvent(At, "run-1", "node-a", "SourceLoad", "src_crm__customers", status,
            5, 41, null, null, null));

        Assert.Equal($"{marker} src_crm__customers 5 rows 41ms{Environment.NewLine}", writer.ToString());
    }

    /// <summary>A failed node must not render as a bare
    /// `FAIL &lt;name&gt; 0 rows 978ms` with no code and no cause: run_results.json, the NDJSON stream and
    /// the MCP envelope all carry errorCode/errorMessage, so a renderer that drops them gives a human
    /// at the terminal strictly less than an agent on MCP.</summary>
    [Fact]
    public void NodeCompleted_failed_prints_the_error_code_and_message()
    {
        var writer = new StringWriter();
        var renderer = new ConsoleRenderer(writer);

        renderer.Render(new NodeCompletedEvent(At, "run-1", "node-a", "Pipeline", "wide_out", "failed",
            0, 978, "PZ0501", "Out of Memory Error: could not allocate block of size 256.0 KiB", null));

        Assert.Equal(
            $"FAIL wide_out 0 rows 978ms{Environment.NewLine}" +
            $"  PZ0501: Out of Memory Error: could not allocate block of size 256.0 KiB{Environment.NewLine}",
            writer.ToString());
    }

    /// <summary>DuckDB's own errors are multi-line (a summary, then a "Possible solutions:" list). Every
    /// line is indented under the node's line so the block reads as belonging to that node rather than
    /// running flush against the next node's output.</summary>
    [Fact]
    public void NodeCompleted_failed_indents_every_line_of_a_multi_line_message()
    {
        var writer = new StringWriter();
        var renderer = new ConsoleRenderer(writer);

        renderer.Render(new NodeCompletedEvent(At, "run-1", "node-a", "Pipeline", "wide_out", "failed",
            0, 12, "PZ0501", "Out of Memory Error\n\nPossible solutions:\n* Reducing the number of threads", null));

        Assert.Equal(
            $"FAIL wide_out 0 rows 12ms{Environment.NewLine}" +
            $"  PZ0501: Out of Memory Error{Environment.NewLine}" +
            $"  {Environment.NewLine}" +
            $"  Possible solutions:{Environment.NewLine}" +
            $"  * Reducing the number of threads{Environment.NewLine}",
            writer.ToString());
    }

    /// <summary>The error block is strictly additive to the FAILURE path — every CLI/e2e assertion
    /// depends on a successful node's line being exactly one line.</summary>
    [Fact]
    public void NodeCompleted_success_with_no_error_prints_only_its_own_line()
    {
        var writer = new StringWriter();
        var renderer = new ConsoleRenderer(writer);

        renderer.Render(new NodeCompletedEvent(At, "run-1", "node-a", "SourceLoad", "src_crm__customers",
            "success", 5, 41, null, null, null));

        Assert.Equal($"ok src_crm__customers 5 rows 41ms{Environment.NewLine}", writer.ToString());
    }

    [Fact]
    public void RetryScheduled_prints_retry_line()
    {
        var writer = new StringWriter();
        var renderer = new ConsoleRenderer(writer);

        renderer.Render(new RetryScheduledEvent(At, "run-1", "node-a", "src_crm__customers", 2, 3, 800, "timeout"));

        Assert.Equal($"retry: src_crm__customers attempt 2/3 in 800ms (timeout){Environment.NewLine}", writer.ToString());
    }

    [Fact]
    public void BreakerStateChanged_prints_breaker_line()
    {
        var writer = new StringWriter();
        var renderer = new ConsoleRenderer(writer);

        renderer.Render(new BreakerStateChangedEvent(At, "run-1", "conn:pg_prod", "closed", "open",
            "5 consecutive transient failures", 120_000));

        Assert.Equal(
            $"breaker: connection pg_prod closed->open (5 consecutive transient failures; cool down 120s){Environment.NewLine}",
            writer.ToString());
    }

    /// <summary>Exactly one warning line naming the connection, entity,
    /// and each change compactly.</summary>
    [Fact]
    public void SourceDriftDetected_prints_single_warning_line()
    {
        var writer = new StringWriter();
        var renderer = new ConsoleRenderer(writer);

        renderer.Render(new SourceDriftDetectedEvent(At, "run-1", "node-a", "pg_prod", "orders", "warn",
            [new DriftChangePayload("retyped", "amount", "BIGINT", "VARCHAR")],
            [new SchemaColumnPayload("amount", "VARCHAR")], "abc123"));

        var lines = writer.ToString().Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
        var line = Assert.Single(lines);
        Assert.Contains("pg_prod", line, StringComparison.Ordinal);
        Assert.Contains("orders", line, StringComparison.Ordinal);
        Assert.Contains("warn", line, StringComparison.Ordinal);
        Assert.Contains("retyped amount (BIGINT->VARCHAR)", line, StringComparison.Ordinal);
    }

    /// <summary>Exactly one warning line naming the output, the merge
    /// keys, and both counts — so an in-batch duplicate-key collapse is never silent on the console.</summary>
    [Fact]
    public void MergeKeyDuplicatesDetected_prints_single_warning_line()
    {
        var writer = new StringWriter();
        var renderer = new ConsoleRenderer(writer);

        renderer.Render(new MergeKeyDuplicatesDetectedEvent(At, "run-1", "node-a", "events_tgt", ["id"], 2, 3));

        var lines = writer.ToString().Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
        var line = Assert.Single(lines);
        Assert.StartsWith("warning:", line, StringComparison.Ordinal);
        Assert.Contains("events_tgt", line, StringComparison.Ordinal);
        Assert.Contains("[id]", line, StringComparison.Ordinal);
        Assert.Contains("2", line, StringComparison.Ordinal);
        Assert.Contains("3", line, StringComparison.Ordinal);
    }

    [Fact]
    public void LossyIntegerInferenceDetected_prints_single_warning_line()
    {
        var writer = new StringWriter();
        var renderer = new ConsoleRenderer(writer);

        renderer.Render(new LossyIntegerInferenceDetectedEvent(At, "run-1", "node-a", "crm", "orders", ["id", "sku"]));

        var lines = writer.ToString().Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
        var line = Assert.Single(lines);
        Assert.StartsWith("warning:", line, StringComparison.Ordinal);
        Assert.Contains("crm.orders", line, StringComparison.Ordinal);
        Assert.Contains("[id, sku]", line, StringComparison.Ordinal);
        Assert.Contains("DOUBLE", line, StringComparison.Ordinal);
        Assert.Contains("columns:", line, StringComparison.Ordinal);
    }

    [Fact]
    public void AmbiguousDateInferenceDetected_prints_single_warning_line()
    {
        var writer = new StringWriter();
        var renderer = new ConsoleRenderer(writer);

        renderer.Render(new AmbiguousDateInferenceDetectedEvent(At, "run-1", "node-a", "crm", "orders", ["when"], "%d/%m/%Y"));

        var lines = writer.ToString().Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
        var line = Assert.Single(lines);
        Assert.StartsWith("warning:", line, StringComparison.Ordinal);
        Assert.Contains("crm.orders", line, StringComparison.Ordinal);
        Assert.Contains("[when]", line, StringComparison.Ordinal);
        Assert.Contains("%d/%m/%Y", line, StringComparison.Ordinal);
        Assert.Contains("ISO 8601", line, StringComparison.Ordinal);
    }

    [Fact]
    public void Null_timings_does_not_throw_or_print_extra_lines()
    {
        var writer = new StringWriter();
        var renderer = new ConsoleRenderer(writer);

        renderer.Render(new NodeCompletedEvent(At, "run-1", "node-a", "SourceLoad", "orders", "success", 3, 10,
            null, null, null));

        Assert.Equal($"ok orders 3 rows 10ms{Environment.NewLine}", writer.ToString());
    }

    /// <summary>A failed sink node with delivery stats prints a line naming what's
    /// already visible downstream and the sink's abort semantics — "aborted" must never imply cleanup
    /// that didn't happen. The node's own error block prints first, between the node line and the
    /// delivery line: cause before consequence.</summary>
    [Fact]
    public void NodeCompleted_with_delivery_on_failure_prints_delivery_stopped_line()
    {
        var writer = new StringWriter();
        var renderer = new ConsoleRenderer(writer);

        renderer.Render(new NodeCompletedEvent(At, "run-1", "node-a", "SinkWrite", "api.orders_out", "failed",
            0, 7, "PZ0501", "boom", null, Delivery: new DeliveryPayload("none", 40, 0)));

        Assert.Equal(
            $"FAIL api.orders_out 0 rows 7ms{Environment.NewLine}" +
            $"  PZ0501: boom{Environment.NewLine}" +
            $"delivery stopped: up to 40 row(s) already visible at the destination (abort: none){Environment.NewLine}",
            writer.ToString());
    }

    /// <summary>A successful sink node that resumed past a delivery checkpoint
    /// prints a second line naming how many rows the resume skipped re-delivering.</summary>
    [Fact]
    public void NodeCompleted_with_resumed_rows_prints_resumed_line()
    {
        var writer = new StringWriter();
        var renderer = new ConsoleRenderer(writer);

        renderer.Render(new NodeCompletedEvent(At, "run-1", "node-a", "SinkWrite", "api.orders_out", "success",
            200, 41, null, null, null, Delivery: new DeliveryPayload("none", 200, 80)));

        Assert.Equal(
            $"ok api.orders_out 200 rows 41ms{Environment.NewLine}resumed past 80 delivered row(s){Environment.NewLine}",
            writer.ToString());
    }

    /// <summary>A <see cref="NodeCompletedEvent"/> without delivery stats renders exactly one line —
    /// same additive-optional discipline as <see cref="Null_timings_does_not_throw_or_print_extra_lines"/>.</summary>
    [Fact]
    public void NodeCompleted_without_delivery_prints_exactly_one_line()
    {
        var writer = new StringWriter();
        var renderer = new ConsoleRenderer(writer);

        renderer.Render(new NodeCompletedEvent(At, "run-1", "node-a", "SinkWrite", "api.orders_out", "success",
            200, 41, null, null, null));

        Assert.Equal($"ok api.orders_out 200 rows 41ms{Environment.NewLine}", writer.ToString());
    }

    [Theory]
    [MemberData(nameof(NonRenderedEvents))]
    public void Lifecycle_and_run_level_events_print_nothing(RunEvent evt)
    {
        var writer = new StringWriter();
        var renderer = new ConsoleRenderer(writer);

        renderer.Render(evt);

        Assert.Equal(string.Empty, writer.ToString());
    }

    public static TheoryData<RunEvent> NonRenderedEvents() => new()
    {
        new RunStartedEvent(At, "run-1", "hello_pz", 2),
        new NodeStartedEvent(At, "run-1", "node-a", "SourceLoad", "orders"),
        new NodeProgressEvent(At, "run-1", "node-a", "orders", 1, 2, 3),
        new RunCompletedEvent(At, "run-1", "success", 1, 0, 0, 10),
    };
}
