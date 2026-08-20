using System.Globalization;
using Pz.Diagnostics.Events;

namespace Pz.Cli.Rendering;

/// <summary>`--log-format text` when not interactive (non-TTY stdout, or CI detected — see
/// <see cref="CiDetector"/>): plain sequential lines. This exact shape is the regression net every
/// CLI/e2e assertion depends on. Includes `retry:` lines for <see cref="RetryScheduledEvent"/> and
/// `breaker:` lines for <see cref="BreakerStateChangedEvent"/>. Writes to an injected
/// <see cref="TextWriter"/> (default resolved to <see cref="Console.Out"/> per call, so tests that swap
/// <see cref="Console.SetOut"/> around the render still capture output).
///
/// Bottleneck hints: when
/// <see cref="NodeCompletedEvent.Timings"/> is present and one side of the node's channel crosses
/// <see cref="BottleneckHint.ThresholdPct"/>, a `hint: ...` line prints immediately after the node's own
/// line. A null <see cref="NodeCompletedEvent.Timings"/> (Pipeline/Check nodes, or a node that never
/// reached the channel-instrumented path) prints nothing extra.</summary>
public sealed class ConsoleRenderer(TextWriter? writer = null) : IEventRenderer
{
    private TextWriter Writer => writer ?? Console.Out;

    public void Render(RunEvent evt)
    {
        switch (evt)
        {
            case NodeCompletedEvent e:
                Writer.WriteLine($"{Marker(e.Status)} {e.Name} {e.Rows} rows {e.DurationMs}ms");
                WriteError(e);
                if (BottleneckHint.For(e) is { } hint)
                {
                    Writer.WriteLine(hint);
                }

                // "aborted" must never claim cleanup that didn't happen.
                if (e.Delivery is { } delivery)
                {
                    if (e.Status == "failed")
                    {
                        Writer.WriteLine(
                            $"delivery stopped: up to {delivery.RowsVisible} row(s) already visible at the destination (abort: {delivery.AbortSemantics})");
                    }
                    else if (delivery.ResumedRows > 0)
                    {
                        Writer.WriteLine($"resumed past {delivery.ResumedRows} delivered row(s)");
                    }
                }

                break;

            case RetryScheduledEvent e:
                Writer.WriteLine($"retry: {e.Name} attempt {e.Attempt}/{e.MaxAttempts} in {e.DelayMs}ms ({e.Reason})");
                break;

            case BreakerStateChangedEvent e:
                Writer.WriteLine(
                    $"breaker: connection {e.Instance.Replace("conn:", "", StringComparison.Ordinal)} " +
                    $"{e.OldState}->{e.NewState} ({e.Trigger}; cool down {FormatCoolDown(e.CoolDownMs)})");
                break;

            case SourceDriftDetectedEvent e:
                Writer.WriteLine(
                    $"warning: schema drift on {e.Connection}.{e.Entity} ({e.Policy}): " +
                    $"{string.Join(", ", e.Changes.Select(FormatChange))}");
                break;

            case MergeKeyDuplicatesDetectedEvent e:
                Writer.WriteLine(
                    $"warning: output '{e.Output}': {e.DuplicateGroups} merge key group(s) " +
                    $"[{string.Join(", ", e.Keys)}] have {e.ExtraRows} duplicate staged row(s) -- merge keeps " +
                    "one connector-determined survivor per key (staging order, not cursor order); dedup in the " +
                    "pipeline if a specific row must win");
                break;

            case LossyIntegerInferenceDetectedEvent e:
                Writer.WriteLine(
                    $"warning: {e.Connection}.{e.Entity}: column(s) [{string.Join(", ", e.Columns)}] " +
                    "auto-detected as DOUBLE but hold only whole numbers beyond 2^53 -- digits may have " +
                    "been lost; declare a columns: contract (e.g. bigint or hugeint) to load them losslessly");
                break;

            case AmbiguousDateInferenceDetectedEvent e:
                Writer.WriteLine(
                    $"warning: {e.Connection}.{e.Entity}: date column(s) [{string.Join(", ", e.Columns)}] " +
                    $"parsed with assumed format {e.Format} -- every value is day/month-ambiguous, so a " +
                    "month-first source is misread on every row; normalize the source to ISO 8601, or " +
                    "declare the column varchar in a columns: contract and parse it explicitly in SQL");
                break;
        }
    }

    /// <summary>Prints the failed node's `PZ####` code and message directly under its line, so the
    /// human output carries the same diagnosis every other channel (run_results.json, the NDJSON stream,
    /// the MCP `pz_run` envelope) already does.
    ///
    /// Strictly additive to the failure path: a node with no error code (every success, and any skip)
    /// prints exactly one line, which is what the CLI/e2e assertions pin.
    /// Multi-line messages — DuckDB's errors carry a "Possible solutions:" list — are indented line by
    /// line so the block reads as belonging to this node instead of running flush against the next one's
    /// output. The message is printed in full rather than truncated to its first line: for OOM and
    /// too-large-row failures the actionable half is below the summary.</summary>
    private void WriteError(NodeCompletedEvent e)
    {
        if (e.ErrorCode is not { } code)
        {
            return;
        }

        var message = e.ErrorMessage ?? "";
        var lines = message.Split('\n');
        Writer.WriteLine($"  {code}: {lines[0].TrimEnd('\r')}");
        for (var i = 1; i < lines.Length; i++)
        {
            Writer.WriteLine($"  {lines[i].TrimEnd('\r')}");
        }
    }

    /// <summary>One compact `<kind> <column>` token per change, with
    /// a retype's `from->to` appended — mirrors how <see cref="BreakerStateChangedEvent"/>'s line
    /// compresses its own fields rather than dumping the raw payload. Internal (not private) so
    /// <see cref="LiveTreeRenderer"/> can reuse the exact same per-change wording for its own drift
    /// child-node text rather than drifting the two renderers' phrasing apart.</summary>
    internal static string FormatChange(DriftChangePayload change) => change.Kind switch
    {
        "retyped" => $"{change.Kind} {change.Column} ({change.From}->{change.To})",
        _ => $"{change.Kind} {change.Column}",
    };

    private static string Marker(string status) => status switch
    {
        "success" => "ok",
        "failed" => "FAIL",
        "skipped" => "skip",
        _ => throw new ArgumentOutOfRangeException(nameof(status), status, "unknown node status"),
    };

    /// <summary>Derives a human `<n>s` cool-down from <see cref="BreakerStateChangedEvent.CoolDownMs"/> —
    /// whole seconds print bare (`120s`), a sub-second remainder keeps up to 3 decimal places
    /// (`1.5s`) rather than truncating it away.</summary>
    private static string FormatCoolDown(long coolDownMs) =>
        $"{(coolDownMs / 1000.0).ToString("0.###", CultureInfo.InvariantCulture)}s";
}
