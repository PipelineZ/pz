using Pz.Cli.Rendering;
using Pz.Diagnostics.Events;

namespace Pz.Cli.Tests.Rendering;

/// <summary>The interactive Spectre <see cref="Spectre.Console.LiveDisplay"/> tree path can't easily be
/// driven from an automated test (it needs a real TTY), so this class is kept
/// thin and that path is verified manually. What IS covered
/// here is the non-interactive fallback: when <see cref="CiDetector.IsInteractive()"/> would say false,
/// <see cref="LiveTreeRenderer"/> must behave exactly like <see cref="ConsoleRenderer"/> — this is what
/// keeps every existing byte-for-byte CLI/e2e assertion passing when `pz run` selects
/// <see cref="LiveTreeRenderer"/> for `--log-format text` regardless of whether the terminal is a TTY.</summary>
public class LiveTreeRendererTests
{
    private static readonly DateTimeOffset At = new(2026, 7, 4, 10, 0, 0, TimeSpan.Zero);

    [Fact]
    public void Falls_back_to_ConsoleRenderer_output_when_not_interactive()
    {
        var writer = new StringWriter();
        using var renderer = new LiveTreeRenderer(fallbackWriter: writer, isInteractive: () => false);

        renderer.Render(new NodeCompletedEvent(At, "run-1", "node-a", "SourceLoad", "src_crm__customers", "success",
            5, 41, null, null, null));

        Assert.Equal($"ok src_crm__customers 5 rows 41ms{Environment.NewLine}", writer.ToString());
    }

    /// <summary>The fallback path is what proves
    /// <see cref="LiveTreeRenderer.Render"/> actually forwards a <see cref="SourceDriftDetectedEvent"/>
    /// rather than silently dropping it in its `switch` -- the interactive Spectre tree's own
    /// `DriftWarning` child-node rendering follows the same "kept thin, manually verified" precedent as
    /// its existing Hint/Provenance child nodes (neither is unit-tested either; see the class doc).</summary>
    [Fact]
    public void Falls_back_to_ConsoleRenderer_output_for_source_drift_detected_when_not_interactive()
    {
        var writer = new StringWriter();
        using var renderer = new LiveTreeRenderer(fallbackWriter: writer, isInteractive: () => false);

        renderer.Render(new SourceDriftDetectedEvent(At, "run-1", "node-a", "pg_prod", "orders", "warn",
            [new DriftChangePayload("retyped", "amount", "BIGINT", "VARCHAR")],
            [new SchemaColumnPayload("amount", "VARCHAR")], "abc123"));

        var line = Assert.Single(writer.ToString().Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries));
        Assert.Contains("pg_prod", line, StringComparison.Ordinal);
        Assert.Contains("orders", line, StringComparison.Ordinal);
        Assert.Contains("retyped amount (BIGINT->VARCHAR)", line, StringComparison.Ordinal);
    }

    /// <summary>Same fallback-path proof as the drift event above —
    /// the non-interactive path must forward a <see cref="MergeKeyDuplicatesDetectedEvent"/> rather
    /// than silently dropping it.</summary>
    [Fact]
    public void Falls_back_to_ConsoleRenderer_output_for_merge_key_duplicates_when_not_interactive()
    {
        var writer = new StringWriter();
        using var renderer = new LiveTreeRenderer(fallbackWriter: writer, isInteractive: () => false);

        renderer.Render(new MergeKeyDuplicatesDetectedEvent(At, "run-1", "node-a", "events_tgt", ["id"], 1, 1));

        var line = Assert.Single(writer.ToString().Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries));
        Assert.Contains("events_tgt", line, StringComparison.Ordinal);
        Assert.Contains("[id]", line, StringComparison.Ordinal);
    }

    /// <summary>Same forwarding guarantee for <see cref="LossyIntegerInferenceDetectedEvent"/> —
    /// the non-interactive path must not silently drop the lossy-integer warning.</summary>
    [Fact]
    public void Falls_back_to_ConsoleRenderer_output_for_lossy_integer_inference_when_not_interactive()
    {
        var writer = new StringWriter();
        using var renderer = new LiveTreeRenderer(fallbackWriter: writer, isInteractive: () => false);

        renderer.Render(new LossyIntegerInferenceDetectedEvent(At, "run-1", "node-a", "crm", "orders", ["id"]));

        var line = Assert.Single(writer.ToString().Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries));
        Assert.Contains("crm.orders", line, StringComparison.Ordinal);
        Assert.Contains("[id]", line, StringComparison.Ordinal);
    }

    /// <summary>Same forwarding guarantee for <see cref="AmbiguousDateInferenceDetectedEvent"/>.</summary>
    [Fact]
    public void Falls_back_to_ConsoleRenderer_output_for_ambiguous_dates_when_not_interactive()
    {
        var writer = new StringWriter();
        using var renderer = new LiveTreeRenderer(fallbackWriter: writer, isInteractive: () => false);

        renderer.Render(new AmbiguousDateInferenceDetectedEvent(At, "run-1", "node-a", "crm", "orders", ["when"], "%d/%m/%Y"));

        var line = Assert.Single(writer.ToString().Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries));
        Assert.Contains("crm.orders", line, StringComparison.Ordinal);
        Assert.Contains("[when]", line, StringComparison.Ordinal);
    }

    [Fact]
    public void Dispose_is_safe_when_never_interactive()
    {
        var renderer = new LiveTreeRenderer(fallbackWriter: new StringWriter(), isInteractive: () => false);
        renderer.Dispose();
    }

    [Fact]
    public void Final_frame_shows_every_node_and_stop_is_prompt()
    {
        var output = new StringWriter();
        var console = Spectre.Console.AnsiConsole.Create(new Spectre.Console.AnsiConsoleSettings
        {
            Ansi = Spectre.Console.AnsiSupport.No,
            ColorSystem = Spectre.Console.ColorSystemSupport.NoColors,
            Interactive = Spectre.Console.InteractionSupport.Yes,
            Out = new Spectre.Console.AnsiConsoleOutput(output),
        });
        var renderer = new LiveTreeRenderer(console, isInteractive: () => true);

        string[] names = ["src_a", "src_b", "src_c"];
        foreach (var name in names)
        {
            renderer.Render(new NodeStartedEvent(At, "run-1", name, "SourceLoad", name));
        }

        foreach (var name in names)
        {
            renderer.Render(new NodeCompletedEvent(At, "run-1", name, "SourceLoad", name, "success", 5, 41, null, null, null));
        }

        renderer.Dispose();

        var text = output.ToString();
        var lastFrame = text[text.LastIndexOf("pz run", StringComparison.Ordinal)..];
        foreach (var name in names)
        {
            Assert.Contains($"ok {name} 5 rows 41ms", lastFrame, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void Run_completed_stops_the_loop_and_leaves_the_final_frame()
    {
        var output = new StringWriter();
        var console = Spectre.Console.AnsiConsole.Create(new Spectre.Console.AnsiConsoleSettings
        {
            Ansi = Spectre.Console.AnsiSupport.No,
            ColorSystem = Spectre.Console.ColorSystemSupport.NoColors,
            Interactive = Spectre.Console.InteractionSupport.Yes,
            Out = new Spectre.Console.AnsiConsoleOutput(output),
        });
        var renderer = new LiveTreeRenderer(console, isInteractive: () => true);

        renderer.Render(new NodeCompletedEvent(At, "run-1", "n1", "SourceLoad", "src_a", "success", 5, 41, null, null, null));
        renderer.Render(new RunCompletedEvent(At, "run-1", "success", 1, 0, 0, 0));
        renderer.Dispose();

        var text = output.ToString();
        Assert.Contains("ok src_a 5 rows 41ms", text[text.LastIndexOf("pz run", StringComparison.Ordinal)..], StringComparison.Ordinal);
    }

    [Fact]
    public void Dispose_twice_and_render_after_dispose_do_not_throw()
    {
        var console = Spectre.Console.AnsiConsole.Create(new Spectre.Console.AnsiConsoleSettings
        {
            Ansi = Spectre.Console.AnsiSupport.No,
            Interactive = Spectre.Console.InteractionSupport.Yes,
            Out = new Spectre.Console.AnsiConsoleOutput(new StringWriter()),
        });
        var renderer = new LiveTreeRenderer(console, isInteractive: () => true);

        renderer.Dispose();
        renderer.Dispose();
        renderer.Render(new NodeCompletedEvent(At, "run-1", "n1", "SourceLoad", "src_a", "success", 5, 41, null, null, null));
        renderer.Render(new RunCompletedEvent(At, "run-1", "success", 1, 0, 0, 0));
    }
}
